package measure

import (
	"sync"
	"time"
)

// MetricSample is one second-resolution snapshot of all benchmark metrics.
type MetricSample struct {
	T         float64            `json:"t"`           // seconds since benchmark start
	ReadGBps  float64            `json:"read_gbps"`
	WriteGBps float64            `json:"write_gbps"`
	ReadIOPS  float64            `json:"read_iops"`
	WriteIOPS float64            `json:"write_iops"`
	TTFBP50Ms float64            `json:"ttfb_p50_ms"`
	TTFBP99Ms float64            `json:"ttfb_p99_ms"`
	OpP50Ms   float64            `json:"op_p50_ms"`
	OpP99Ms   float64            `json:"op_p99_ms"`
	CPUPct    float64            `json:"cpu_pct"`
	MemMB     float64            `json:"mem_mb"`
	DiskIOPS  map[string]float64 `json:"disk_iops,omitempty"`
}

// Sampler polls metrics at a fixed interval and stores time-series samples.
type Sampler struct {
	interval time.Duration
	tp       *Throughput
	ttfb     *Recorder
	opLat    *Recorder

	mu      sync.Mutex
	samples []MetricSample

	start       time.Time
	prevSnap    ThroughputSnapshot
	ttfbOffset  int64
	opLatOffset int64
	done        chan struct{}
	wg          sync.WaitGroup
}

// NewSampler creates a sampler. Call Start() to begin collection.
// interval is typically 1s. tp/ttfb/opLat may be nil (those metrics are skipped).
func NewSampler(interval time.Duration, tp *Throughput, ttfb, opLat *Recorder) *Sampler {
	if interval <= 0 {
		interval = time.Second
	}
	return &Sampler{
		interval: interval,
		tp:       tp,
		ttfb:     ttfb,
		opLat:    opLat,
		done:     make(chan struct{}),
	}
}

// Start begins background sampling. Call Stop() when the benchmark finishes.
func (s *Sampler) Start() {
	s.start = time.Now()
	if s.tp != nil {
		s.prevSnap = s.tp.Snapshot()
	}
	// Prime CPU and disk state (first reading is always delta=0)
	SampleCPUPct()
	SampleDiskIOPS()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.done:
				return
			case <-ticker.C:
				s.collect()
			}
		}
	}()
}

// Stop halts sampling and waits for the goroutine to exit.
func (s *Sampler) Stop() {
	close(s.done)
	s.wg.Wait()
}

// Samples returns the collected time-series (safe to call after Stop).
func (s *Sampler) Samples() []MetricSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]MetricSample, len(s.samples))
	copy(out, s.samples)
	return out
}

func (s *Sampler) collect() {
	now := time.Now()
	t := now.Sub(s.start).Seconds()
	sample := MetricSample{T: t}

	// Throughput delta
	if s.tp != nil {
		cur := s.tp.Snapshot()
		dt := cur.At.Sub(s.prevSnap.At).Seconds()
		if dt > 0 {
			sample.ReadGBps = float64(cur.BytesRead-s.prevSnap.BytesRead) / (1e9 * dt)
			sample.WriteGBps = float64(cur.BytesWritten-s.prevSnap.BytesWritten) / (1e9 * dt)
			sample.ReadIOPS = float64(cur.OpsRead-s.prevSnap.OpsRead) / dt
			sample.WriteIOPS = float64(cur.OpsWrite-s.prevSnap.OpsWrite) / dt
		}
		s.prevSnap = cur
	}

	// TTFB window stats
	if s.ttfb != nil {
		ws, newOff := s.ttfb.StatsWindow(s.ttfbOffset)
		s.ttfbOffset = newOff
		sample.TTFBP50Ms = ws.P50Ms
		sample.TTFBP99Ms = ws.P99Ms
	}

	// Op latency window stats
	if s.opLat != nil {
		ws, newOff := s.opLat.StatsWindow(s.opLatOffset)
		s.opLatOffset = newOff
		sample.OpP50Ms = ws.P50Ms
		sample.OpP99Ms = ws.P99Ms
	}

	// System metrics
	sample.CPUPct = SampleCPUPct()
	sample.MemMB = SampleMemMB()
	sample.DiskIOPS = SampleDiskIOPS()

	s.mu.Lock()
	s.samples = append(s.samples, sample)
	s.mu.Unlock()
}
