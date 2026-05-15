// Package workload implements the parallel I/O execution engine.
// The Runner prepares test data, fires N goroutines, collects metrics,
// then cleans up. Each workload type implements the Worker interface.
package workload

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/minio/pulsar/measure"
	"github.com/minio/pulsar/profile"
)

// Result is everything the reporter needs after a run completes.
type Result struct {
	Profile      string                `json:"profile"`
	WorkloadType string                `json:"workload_type"`
	Path         string                `json:"path"`
	Workers      int                   `json:"workers"`
	DirectIO     bool                  `json:"direct_io"`
	DurationS    float64               `json:"duration_s"`
	StartedAt    time.Time             `json:"started_at"`
	FinishedAt   time.Time             `json:"finished_at"`

	Throughput   measure.ThroughputStats `json:"throughput"`
	TTFB         measure.LatencyStats    `json:"ttfb"`
	OpLatency    measure.LatencyStats    `json:"op_latency"`
	Metadata     *MetadataStats          `json:"metadata,omitempty"`
	Epochs       []EpochStats            `json:"epochs,omitempty"`

	// GPUStallPct is the fraction of wall time workers spent blocked on I/O
	// rather than in the simulated compute gap. Only meaningful when
	// ComputeGapMs > 0. Interpretation:
	//   0–10%  → storage keeps up with GPU; not the bottleneck
	//  10–30%  → storage is adding measurable latency to training
	//   >30%   → storage is a significant training bottleneck
	GPUStallPct  float64               `json:"gpu_stall_pct"`

	Targets      profile.TargetConfig    `json:"targets"`
	Violations   []string                `json:"violations"`
	TargetsMissed int                    `json:"targets_missed"`
}

type MetadataStats struct {
	StatOps       int64   `json:"stat_ops"`
	StatP99Ms     float64 `json:"stat_p99_ms"`
	ReaddirOps    int64   `json:"readdir_ops"`
	ReaddirP99Ms  float64 `json:"readdir_p99_ms"`
	HitRatePct    float64 `json:"hit_rate_pct"` // inferred from repeat-access timing
}

type EpochStats struct {
	Epoch      int                     `json:"epoch"`
	Throughput measure.ThroughputStats `json:"throughput"`
	TTFB       measure.LatencyStats    `json:"ttfb"`
}

// Runner orchestrates benchmark execution.
type Runner struct {
	path    string
	p       *profile.Profile
	quiet   bool
	files   []string // paths of created test files
	rng     *rand.Rand
}

func NewRunner(path string, p *profile.Profile, quiet bool) *Runner {
	seed := p.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &Runner{
		path:  path,
		p:     p,
		quiet: quiet,
		rng:   rand.New(rand.NewSource(seed)),
	}
}

func (r *Runner) Run() (*Result, error) {
	result := &Result{
		Profile:      r.p.Name,
		WorkloadType: r.p.Workload,
		Path:         r.path,
		Workers:      r.p.Workers,
		DirectIO:     r.p.DirectIO,
		StartedAt:    time.Now(),
		Targets:      r.p.Targets,
	}

	// --- Phase 1: Prepare test data ---
	if !r.quiet {
		fmt.Printf("  → Preparing %d test file(s) × %s …\n",
			r.p.Files.Count, humanBytes(r.p.Files.SizeBytes))
	}
	if err := r.prepare(); err != nil {
		return nil, fmt.Errorf("prepare: %w", err)
	}
	defer func() {
		if r.p.Cleanup {
			r.cleanup()
		}
	}()

	// --- Phase 2: Warmup ---
	if r.p.Warmup > 0 {
		if !r.quiet {
			fmt.Printf("  → Warming up for %s …\n", r.p.Warmup.Round(time.Second))
		}
		warmupCtx, cancel := context.WithTimeout(context.Background(), r.p.Warmup)
		r.runWorkers(warmupCtx, nil, nil, nil, nil) // discard warmup metrics
		cancel()
	}

	// --- Phase 3: Measurement ---
	if !r.quiet {
		fmt.Printf("  → Running %d workers for %s …\n",
			r.p.Workers, r.p.Duration.Round(time.Second))
	}

	throughput := measure.NewThroughput()
	ttfb := &measure.Recorder{}
	opLat := &measure.Recorder{}
	stall := &measure.StallTracker{}

	measCtx, measCancel := context.WithTimeout(context.Background(), r.p.Duration)
	defer measCancel()

	switch r.p.Workload {
	case "multi-epoch":
		result.Epochs = r.runEpochs(measCtx)
		if len(result.Epochs) > 0 {
			last := result.Epochs[len(result.Epochs)-1]
			result.Throughput = last.Throughput
			result.TTFB = last.TTFB
		}
	case "metadata":
		ms := r.runMetadata(measCtx, ttfb, opLat)
		result.Metadata = ms
		result.Throughput = throughput.Stats(r.p.Duration)
	default:
		r.runWorkers(measCtx, throughput, ttfb, opLat, stall)
		result.Throughput = throughput.Stats(r.p.Duration)
		result.TTFB = ttfb.Stats()
		result.OpLatency = opLat.Stats()
		result.GPUStallPct = stall.StallPct()
	}

	result.FinishedAt = time.Now()
	result.DurationS = result.FinishedAt.Sub(result.StartedAt).Seconds()

	// --- Phase 4: Target validation ---
	result.Violations, result.TargetsMissed = r.checkTargets(result)
	return result, nil
}

// runWorkers launches p.Workers goroutines and blocks until ctx is done.
func (r *Runner) runWorkers(
	ctx context.Context,
	tp *measure.Throughput,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
	stall *measure.StallTracker,
) {
	var wg sync.WaitGroup
	for i := 0; i < r.p.Workers; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			workerRng := rand.New(rand.NewSource(r.rng.Int63()))
			w := newWorker(r.p.Workload, r.files, r.p, workerID, workerRng)
			w.Run(ctx, tp, ttfb, opLat, stall)
		}()
	}
	wg.Wait()
}

// runEpochs executes multi-epoch and returns per-epoch stats.
func (r *Runner) runEpochs(ctx context.Context) []EpochStats {
	var epochs []EpochStats
	for e := 0; e < r.p.Epochs; e++ {
		if ctx.Err() != nil {
			break
		}
		tp := measure.NewThroughput()
		ttfb := &measure.Recorder{}
		opLat := &measure.Recorder{}
		epochDur := r.p.Duration / time.Duration(r.p.Epochs)
		eCtx, cancel := context.WithTimeout(ctx, epochDur)
		r.runWorkers(eCtx, tp, ttfb, opLat, &measure.StallTracker{})
		cancel()
		epochs = append(epochs, EpochStats{
			Epoch:      e + 1,
			Throughput: tp.Stats(epochDur),
			TTFB:       ttfb.Stats(),
		})
	}
	return epochs
}

// runMetadata runs the metadata workload and returns stats.
func (r *Runner) runMetadata(
	ctx context.Context,
	ttfb *measure.Recorder,
	opLat *measure.Recorder,
) *MetadataStats {
	var wg sync.WaitGroup
	statOps := &measure.Recorder{}
	rdOps := &measure.Recorder{}

	for i := 0; i < r.p.Workers; i++ {
		wg.Add(1)
		workerRng := rand.New(rand.NewSource(r.rng.Int63()))
		files := r.files
		go func() {
			defer wg.Done()
			runMetadataWorker(ctx, files, workerRng, statOps, rdOps)
		}()
	}
	wg.Wait()

	ss := statOps.Stats()
	rs := rdOps.Stats()
	// Infer hit rate: second-access is dramatically faster than first
	var hitRate float64
	if r.p.Reuse && ss.Count > int64(len(r.files)) {
		firstHalf := ss.P50Ms
		secondHalf := ss.P25Ms
		if firstHalf > 0 {
			speedup := firstHalf / secondHalf
			hitRate = min(99, (speedup-1)/speedup*100)
		}
	}
	return &MetadataStats{
		StatOps:      ss.Count,
		StatP99Ms:    ss.P99Ms,
		ReaddirOps:   rs.Count,
		ReaddirP99Ms: rs.P99Ms,
		HitRatePct:   hitRate,
	}
}

// prepare creates test files on the target path using deterministic content.
func (r *Runner) prepare() error {
	dir := filepath.Join(r.path, ".pulsar-bench")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	buf := make([]byte, min64(r.p.BlockSize, 4*1024*1024))
	// Fill with a non-zero pattern (avoids sparse-file shortcuts on some FS)
	for i := range buf {
		buf[i] = byte(i % 251)
	}

	r.files = make([]string, 0, r.p.Files.Count)
	for i := 0; i < r.p.Files.Count; i++ {
		fpath := filepath.Join(dir, fmt.Sprintf("file-%04d.bin", i))
		r.files = append(r.files, fpath)

		// If file already exists with the right size, reuse it (--no-cleanup).
		if st, err := os.Stat(fpath); err == nil && st.Size() == r.p.Files.SizeBytes {
			continue
		}

		f, err := os.Create(fpath)
		if err != nil {
			return err
		}
		remaining := r.p.Files.SizeBytes
		for remaining > 0 {
			n := min64(int64(len(buf)), remaining)
			if _, err := f.Write(buf[:n]); err != nil {
				f.Close()
				return err
			}
			remaining -= n
		}
		f.Close()
	}
	return nil
}

func (r *Runner) cleanup() {
	dir := filepath.Join(r.path, ".pulsar-bench")
	os.RemoveAll(dir)
}

func (r *Runner) checkTargets(res *Result) ([]string, int) {
	t := r.p.Targets
	var v []string

	check := func(cond bool, msg string) {
		if cond {
			v = append(v, msg)
		}
	}

	check(t.ReadGBps > 0 && res.Throughput.ReadGBps < t.ReadGBps,
		fmt.Sprintf("read throughput %.2f GB/s < target %.2f GB/s", res.Throughput.ReadGBps, t.ReadGBps))
	check(t.WriteGBps > 0 && res.Throughput.WriteGBps < t.WriteGBps,
		fmt.Sprintf("write throughput %.2f GB/s < target %.2f GB/s", res.Throughput.WriteGBps, t.WriteGBps))
	check(t.TTFBColdP99Ms > 0 && res.TTFB.P99Ms > t.TTFBColdP99Ms,
		fmt.Sprintf("TTFB cold p99 %.1fms > target %.0fms", res.TTFB.P99Ms, t.TTFBColdP99Ms))
	check(t.TTFBWarmP99Ms > 0 && res.TTFB.P99Ms > t.TTFBWarmP99Ms && res.Epochs != nil,
		fmt.Sprintf("TTFB warm p99 %.1fms > target %.0fms", res.TTFB.P99Ms, t.TTFBWarmP99Ms))
	if res.Metadata != nil {
		check(t.StatP99Ms > 0 && res.Metadata.StatP99Ms > t.StatP99Ms,
			fmt.Sprintf("stat p99 %.1fms > target %.0fms", res.Metadata.StatP99Ms, t.StatP99Ms))
		check(t.MetaHitRatePct > 0 && res.Metadata.HitRatePct < t.MetaHitRatePct,
			fmt.Sprintf("metadata hit rate %.1f%% < target %.0f%%", res.Metadata.HitRatePct, t.MetaHitRatePct))
	}

	return v, len(v)
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
