package cluster

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/minio/pulsar/measure"
	"github.com/minio/pulsar/workload"
)

// Agent is the worker-side HTTP server. Start it with ListenAndServe.
type Agent struct {
	mu      sync.Mutex
	cfg     *AgentConfig
	startAt time.Time
	result  *workload.Result
	err     error
	done    chan struct{}               // closed when run completes
	live    chan measure.MetricSample   // per-second samples forwarded live; closed when run completes
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
	a.live = make(chan measure.MetricSample, 1024)
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
	liveCh := a.live
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

		// Namespace each path with this node's hostname so every agent in a
		// multi-node run writes unique object keys (no cross-node collision).
		host, _ := os.Hostname()
		paths := make([]string, len(cfg.Paths))
		for i, p := range cfg.Paths {
			paths[i] = filepath.Join(p, host)
		}

		runner := workload.NewRunner(paths, &cfg.Profile, true)
		// Forward each per-second sample live (best-effort) so the coordinator
		// can render a live aggregate. The authoritative full series still
		// travels in the final Result.
		runner.SetOnSample(func(s measure.MetricSample) {
			select {
			case liveCh <- s:
			default:
			}
		})
		result, err := runner.Run()

		a.mu.Lock()
		a.result = result
		a.err = err
		a.mu.Unlock()

		close(liveCh)
		close(doneCh)
	}()

	w.WriteHeader(http.StatusOK)
}

func (a *Agent) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Transfer-Encoding", "chunked")

	flusher, canFlush := w.(http.Flusher)

	a.mu.Lock()
	doneCh := a.done
	liveCh := a.live
	a.mu.Unlock()

	if doneCh == nil {
		http.Error(w, "not started", http.StatusServiceUnavailable)
		return
	}

	enc := json.NewEncoder(w)

	// Forward per-second samples live until the run completes (the run
	// goroutine closes the live channel when it finishes).
	for liveCh != nil {
		select {
		case s, ok := <-liveCh:
			if !ok {
				liveCh = nil
				continue
			}
			sample := s
			enc.Encode(StreamMsg{Sample: &sample}) //nolint:errcheck
			if canFlush {
				flusher.Flush()
			}
		case <-r.Context().Done():
			return
		}
	}

	// Run finished — send the authoritative final result, which carries the
	// full per-second series used for the merged report.
	a.mu.Lock()
	result := a.result
	runErr := a.err
	a.mu.Unlock()

	if runErr != nil {
		enc.Encode(StreamMsg{Error: runErr.Error()}) //nolint:errcheck
		if canFlush {
			flusher.Flush()
		}
		return
	}

	enc.Encode(StreamMsg{Result: result}) //nolint:errcheck
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
