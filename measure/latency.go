// Package measure provides latency and throughput recorders used by workloads.
package measure

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const maxSamples = 1_000_000

// Recorder collects raw latency samples from concurrent workers and
// computes percentile distributions on demand.
// samples is a reservoir capped at maxSamples to prevent OOM on long
// runs. winSamples accumulates samples since the last StatsWindow
// call and is reset on each call.
type Recorder struct {
	mu         sync.Mutex
	samples    []float64 // reservoir, capped at maxSamples
	total      int64     // total ever recorded (may exceed len(samples))
	winSamples []float64 // samples since last StatsWindow call; reset each call
}

// Record adds one sample. Safe to call from multiple goroutines.
func (r *Recorder) Record(d time.Duration) {
	ms := float64(d.Nanoseconds()) / 1e6
	r.mu.Lock()
	r.total++
	if int64(len(r.samples)) < maxSamples {
		r.samples = append(r.samples, ms)
	} else if j := rand.Int63n(r.total); j < maxSamples {
		// Reservoir sampling (Algorithm R): keep a uniform random subset of
		// the whole run. The previous circular overwrite retained only the
		// most recent maxSamples ops, so final percentiles silently became a
		// recency-biased window instead of whole-run statistics.
		r.samples[j] = ms
	}
	// Cap the window buffer too: if no Sampler is draining it via
	// StatsWindow (e.g. epoch runs), it would otherwise grow unboundedly.
	if int64(len(r.winSamples)) < maxSamples {
		r.winSamples = append(r.winSamples, ms)
	}
	r.mu.Unlock()
}

// RecordTTFB is a named alias for TTFB samples (identical storage, clearer callsite).
func (r *Recorder) RecordTTFB(d time.Duration) { r.Record(d) }

// Stats computes the full latency distribution. Call after all workers finish.
// Uses the circular sample buffer (up to maxSamples); Count reflects total
// samples ever recorded.
func (r *Recorder) Stats() LatencyStats {
	r.mu.Lock()
	s := make([]float64, len(r.samples))
	copy(s, r.samples)
	total := r.total
	r.mu.Unlock()

	if len(s) == 0 {
		return LatencyStats{}
	}
	sort.Float64s(s)
	return LatencyStats{
		Count:  total,
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

// StatsWindow computes stats for samples recorded since the last call.
// winSamples is reset on each call so this always returns stats for just
// the last window. The offset param is ignored (kept for API compatibility).
// Returns the stats and the current total count.
// Safe to call concurrently with Record().
func (r *Recorder) StatsWindow(offset int64) (LatencyStats, int64) {
	r.mu.Lock()
	win := make([]float64, len(r.winSamples))
	copy(win, r.winSamples)
	r.winSamples = r.winSamples[:0]
	total := r.total
	r.mu.Unlock()

	if len(win) == 0 {
		return LatencyStats{}, total
	}
	sort.Float64s(win)
	return LatencyStats{
		Count:  int64(len(win)),
		MinMs:  win[0],
		P50Ms:  pct(win, 50),
		P95Ms:  pct(win, 95),
		P99Ms:  pct(win, 99),
		MaxMs:  win[len(win)-1],
		MeanMs: mean(win),
	}, total
}

// MergeLatencyStats merges multiple LatencyStats into one aggregate.
// Counts are summed, min/max are taken, mean is weighted by count.
// Percentiles come from the recorder with the most samples (approximation).
func MergeLatencyStats(all []LatencyStats) LatencyStats {
	if len(all) == 0 {
		return LatencyStats{}
	}
	var merged LatencyStats
	var totalCount int64
	var weightedMean float64
	minMs := math.MaxFloat64
	var maxMs float64

	bestIdx := 0
	for i, s := range all {
		if s.Count > all[bestIdx].Count {
			bestIdx = i
		}
		totalCount += s.Count
		weightedMean += s.MeanMs * float64(s.Count)
		if s.MinMs < minMs && s.MinMs > 0 {
			minMs = s.MinMs
		}
		if s.MaxMs > maxMs {
			maxMs = s.MaxMs
		}
	}
	if minMs == math.MaxFloat64 {
		minMs = 0
	}
	merged.Count = totalCount
	if totalCount > 0 {
		merged.MeanMs = weightedMean / float64(totalCount)
	}
	merged.MinMs = minMs
	merged.MaxMs = maxMs
	best := all[bestIdx]
	merged.P25Ms = best.P25Ms
	merged.P50Ms = best.P50Ms
	merged.P75Ms = best.P75Ms
	merged.P90Ms = best.P90Ms
	merged.P95Ms = best.P95Ms
	merged.P99Ms = best.P99Ms
	return merged
}

// Reset clears all samples (used between epochs).
func (r *Recorder) Reset() {
	r.mu.Lock()
	r.samples = r.samples[:0]
	r.total = 0
	r.winSamples = r.winSamples[:0]
	r.mu.Unlock()
}

// Count returns current sample count without computing full stats.
func (r *Recorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return int(r.total)
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
