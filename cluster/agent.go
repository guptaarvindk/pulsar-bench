package cluster

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/minio/pulsar/workload"
)

// Agent is the worker-side HTTP server. Start it with ListenAndServe.
type Agent struct {
	mu      sync.Mutex
	cfg     *AgentConfig
	startAt time.Time
	result  *workload.Result
	err     error
	done    chan struct{} // closed when run completes
}

func NewAgent() *Agent {
	return &Agent{}
}

func (a *Agent) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/time", a.handleTime)
	mux.HandleFunc("/api/config", a.handleConfig)
	mux.HandleFunc("/api/start", a.handleStart)
	mux.HandleFunc("/api/stream", a.handleStream)
	mux.HandleFunc("/api/result", a.handleResult)
	mux.HandleFunc("/api/reset", a.handleReset)
	return mux
}

func (a *Agent) handleTime(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(TimeReply{UnixNano: time.Now().UnixNano()})
}

func (a *Agent) handleConfig(w http.ResponseWriter, r *http.Request) {
	var cfg AgentConfig
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	a.mu.Lock()
	// Reject if a benchmark is currently running (done channel open but not closed).
	if a.done != nil {
		select {
		case <-a.done:
			// Run finished — allow reconfiguration.
		default:
			a.mu.Unlock()
			http.Error(w, "benchmark in progress — wait for completion or POST /api/reset", http.StatusConflict)
			return
		}
	}
	a.cfg = &cfg
	a.result = nil
	a.err = nil
	a.done = make(chan struct{})
	a.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (a *Agent) handleStart(w http.ResponseWriter, r *http.Request) {
	var req StartRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	a.mu.Lock()
	cfg := a.cfg
	doneCh := a.done
	a.startAt = time.Unix(0, req.AtUnixNano)
	a.mu.Unlock()

	if cfg == nil {
		http.Error(w, "no config set — POST /api/config first", http.StatusBadRequest)
		return
	}

	startAt := time.Unix(0, req.AtUnixNano)

	go func() {
		// Sleep until scheduled start time
		if delay := time.Until(startAt); delay > 0 {
			time.Sleep(delay)
		}

		runner := workload.NewRunner(cfg.Paths, &cfg.Profile, true)
		result, err := runner.Run()

		a.mu.Lock()
		a.result = result
		a.err = err
		a.mu.Unlock()

		close(doneCh)
	}()

	w.WriteHeader(http.StatusOK)
}

func (a *Agent) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Transfer-Encoding", "chunked")

	flusher, canFlush := w.(http.Flusher)

	// Wait for done channel
	a.mu.Lock()
	doneCh := a.done
	a.mu.Unlock()

	if doneCh == nil {
		http.Error(w, "not started", http.StatusServiceUnavailable)
		return
	}

	// Poll until done
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-doneCh:
			goto streamResults
		case <-ticker.C:
			// keep waiting
		case <-r.Context().Done():
			return
		}
	}

streamResults:
	a.mu.Lock()
	result := a.result
	runErr := a.err
	a.mu.Unlock()

	enc := json.NewEncoder(w)

	if runErr != nil {
		msg := StreamMsg{Error: runErr.Error()}
		enc.Encode(msg) //nolint:errcheck
		if canFlush {
			flusher.Flush()
		}
		return
	}

	// Stream all samples
	for i := range result.Samples {
		sample := result.Samples[i]
		msg := StreamMsg{Sample: &sample}
		enc.Encode(msg) //nolint:errcheck
		if canFlush {
			flusher.Flush()
		}
	}

	// Final result message
	msg := StreamMsg{Result: result}
	enc.Encode(msg) //nolint:errcheck
	if canFlush {
		flusher.Flush()
	}
}

func (a *Agent) handleResult(w http.ResponseWriter, r *http.Request) {
	// Wait for done channel
	a.mu.Lock()
	doneCh := a.done
	a.mu.Unlock()

	if doneCh == nil {
		http.Error(w, "not started", http.StatusServiceUnavailable)
		return
	}

	<-doneCh

	a.mu.Lock()
	result := a.result
	runErr := a.err
	a.mu.Unlock()

	if runErr != nil {
		http.Error(w, runErr.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func (a *Agent) handleReset(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.cfg = nil
	a.result = nil
	a.err = nil
	a.done = nil
	a.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}
