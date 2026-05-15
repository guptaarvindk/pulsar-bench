// Package measure provides latency and throughput recorders used by workloads.
package measure

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Recorder collects raw latency samples from concurrent workers in a
// lock-free manner and computes percentile distributions on demand.
type Recorder struct {
	mu      sync.Mutex
	samples []float64 // milliseconds
}

// Record adds one sample. Safe to call from multiple goroutines.
func (r *Recorder) Record(d time.Duration) {
	ms := float64(d.Nanoseconds()) / 1e6
	r.mu.Lock()
	r.samples = append(r.samples, ms)
	r.mu.Unlock()
}

// RecordTTFB is a named alias for TTFB samples (identical storage, clearer callsite).
func (r *Recorder) RecordTTFB(d time.Duration) { r.Record(d) }

// Stats computes the full latency distribution. Call after all workers finish.
func (r *Recorder) Stats() LatencyStats {
	r.mu.Lock()
	s := make([]float64, len(r.samples))
	copy(s, r.samples)
	r.mu.Unlock()

	if len(s) == 0 {
		return LatencyStats{}
	}
	sort.Float64s(s)
	return LatencyStats{
		Count:  int64(len(s)),
		MinMs:  s[0],
		P25Ms:  pct(s, 25),
		P50Ms:  pct(s, 50),
		P75Ms:  pct(s, 75),
		P90Ms:  pct(s, 90),
		P95Ms:  pct(s, 95),
		P99Ms:  pct(s, 99),
		MaxMs:  s[len(s)-1],
		MeanMs: mean(s),
		StdMs:  stddev(s),
	}
}

// Reset clears all samples (used between epochs).
func (r *Recorder) Reset() {
	r.mu.Lock()
	r.samples = r.samples[:0]
	r.mu.Unlock()
}

// Count returns current sample count without computing full stats.
func (r *Recorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.samples)
}

// LatencyStats is the computed distribution for one recorder.
type LatencyStats struct {
	Count  int64
	MinMs  float64
	P25Ms  float64
	P50Ms  float64
	P75Ms  float64
	P90Ms  float64
	P95Ms  float64
	P99Ms  float64
	MaxMs  float64
	MeanMs float64
	StdMs  float64
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := p / 100.0 * float64(len(sorted)-1)
	lo := int(idx)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	frac := idx - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
}

func mean(s []float64) float64 {
	if len(s) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range s {
		sum += v
	}
	return sum / float64(len(s))
}

func stddev(s []float64) float64 {
	if len(s) < 2 {
		return 0
	}
	m := mean(s)
	sum := 0.0
	for _, v := range s {
		d := v - m
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(s)-1))
}

// ------------------------------------------------------------------ #
// StallTracker — GPU stall fraction
// ------------------------------------------------------------------ #

// StallTracker accumulates time spent in I/O (stalled) vs time spent in
// the simulated compute gap (productive). Workers call AddIO and AddCompute
// on every iteration. StallPct() returns the I/O fraction as a percentage.
//
// Interpretation (only meaningful when ComputeGapMs > 0):
//
//	 0–10%  storage keeps up — not the bottleneck
//	10–30%  storage adds measurable latency to training
//	  >30%  storage is a significant training bottleneck
type StallTracker struct {
	ioNs      atomic.Int64
	computeNs atomic.Int64
}

func (s *StallTracker) AddIO(d time.Duration) {
	if s != nil {
		s.ioNs.Add(d.Nanoseconds())
	}
}

func (s *StallTracker) AddCompute(d time.Duration) {
	if s != nil {
		s.computeNs.Add(d.Nanoseconds())
	}
}

// StallPct returns the percentage of total time spent blocked on I/O.
// Returns 0 when no compute gap is configured (no compute time recorded).
func (s *StallTracker) StallPct() float64 {
	if s == nil {
		return 0
	}
	io := s.ioNs.Load()
	compute := s.computeNs.Load()
	total := io + compute
	if total == 0 || compute == 0 {
		return 0 // no compute gap configured — metric is meaningless
	}
	return float64(io) / float64(total) * 100.0
}
