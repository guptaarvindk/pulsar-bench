// Package workload implements the parallel I/O execution engine.
// The Runner prepares test data, fires N goroutines, collects metrics,
// then cleans up. Each workload type implements the Worker interface.
package workload

import (
	"context"
	"fmt"
	"math"
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

	PerPath      []PathResult            `json:"per_path,omitempty"`
	PerNode      []NodeResult            `json:"per_node,omitempty"`
	Accelerator  *AcceleratorStats       `json:"accelerator,omitempty"`

	Samples      []measure.MetricSample  `json:"samples,omitempty"`

	Targets      profile.TargetConfig    `json:"targets"`
	Violations   []string                `json:"violations"`
	TargetsMissed int                    `json:"targets_missed"`
}

// PathResult holds per-path metrics when multiple paths are benchmarked.
type PathResult struct {
	Path       string                  `json:"path"`
	Throughput measure.ThroughputStats `json:"throughput"`
	TTFB       measure.LatencyStats    `json:"ttfb"`
	OpLatency  measure.LatencyStats    `json:"op_latency"`
	Samples    []measure.MetricSample  `json:"samples,omitempty"`
}

// NodeResult holds per-node metrics in a multi-node run.
type NodeResult struct {
	Node       string                  `json:"node"`
	Throughput measure.ThroughputStats `json:"throughput"`
	TTFB       measure.LatencyStats    `json:"ttfb"`
	OpLatency  measure.LatencyStats    `json:"op_latency"`
}

// AcceleratorStats reports ML accelerator-level throughput metrics.
type AcceleratorStats struct {
	NumAccelerators int     `json:"num_accelerators"`
	SamplesPerSec   float64 `json:"samples_per_sec"`
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
	paths     []string
	p         *profile.Profile
	quiet     bool
	files     []string // paths of created test files (flat, across all paths)
	fileSizes []int64  // parallel to r.files: per-file size
	rng       *rand.Rand
}

func NewRunner(paths []string, p *profile.Profile, quiet bool) *Runner {
	seed := p.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &Runner{
		paths: paths,
		p:     p,
		quiet: quiet,
		rng:   rand.New(rand.NewSource(seed)),
	}
}

func (r *Runner) Run() (*Result, error) {
	// Use first path for backwards-compatible Path field
	result := &Result{
		Profile:      r.p.Name,
		WorkloadType: r.p.Workload,
		Path:         r.paths[0],
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

	measCtx, measCancel := context.WithTimeout(context.Background(), r.p.Duration)
	defer measCancel()

	if len(r.paths) > 1 {
		// Multi-path: one goroutine per path
		r.runMultiPath(measCtx, result)
	} else {
		// Single-path: identical to before
		switch r.p.Workload {
		case "multi-epoch":
			result.Epochs = r.runEpochs(measCtx)
			if len(result.Epochs) > 0 {
				last := result.Epochs[len(result.Epochs)-1]
				result.Throughput = last.Throughput
				result.TTFB = last.TTFB
			}
		case "metadata":
			throughput := measure.NewThroughput()
			ttfb := &measure.Recorder{}
			opLat := &measure.Recorder{}
			ms := r.runMetadata(measCtx, ttfb, opLat)
			result.Metadata = ms
			result.Throughput = throughput.Stats(r.p.Duration)
		default:
			throughput := measure.NewThroughput()
			ttfb := &measure.Recorder{}
			opLat := &measure.Recorder{}
			stall := &measure.StallTracker{}
			sampler := measure.NewSampler(time.Second, throughput, ttfb, opLat)
			sampler.Start()
			r.runWorkers(measCtx, throughput, ttfb, opLat, stall)
			sampler.Stop()
			result.Throughput = throughput.Stats(r.p.Duration)
			result.TTFB = ttfb.Stats()
			result.OpLatency = opLat.Stats()
			result.GPUStallPct = stall.StallPct()
			result.Samples = sampler.Samples()
		}
	}

	result.FinishedAt = time.Now()
	result.DurationS = result.FinishedAt.Sub(result.StartedAt).Seconds()

	// --- Accelerator stats ---
	if r.p.NumAccelerators > 0 && r.p.SampleSizeBytes > 0 && result.DurationS > 0 {
		result.Accelerator = &AcceleratorStats{
			NumAccelerators: r.p.NumAccelerators,
			SamplesPerSec:   float64(result.Throughput.BytesRead) / result.DurationS / float64(r.p.SampleSizeBytes),
		}
	}

	// --- Phase 4: Target validation ---
	result.Violations, result.TargetsMissed = r.checkTargets(result)
	return result, nil
}

// runMultiPath runs one goroutine per path, collects per-path results, and aggregates.
func (r *Runner) runMultiPath(ctx context.Context, result *Result) {
	n := len(r.paths)
	type pathOutcome struct {
		pr     PathResult
		stall  float64
	}
	outcomes := make([]pathOutcome, n)

	// Partition files across paths
	fileGroups := r.partitionFiles()

	workersPerPath := r.p.Workers / n
	if workersPerPath < 1 {
		workersPerPath = 1
	}

	var wg sync.WaitGroup
	for i, pth := range r.paths {
		wg.Add(1)
		i, pth := i, pth
		go func() {
			defer wg.Done()
			tp := measure.NewThroughput()
			ttfb := &measure.Recorder{}
			opLat := &measure.Recorder{}
			stall := &measure.StallTracker{}
			sampler := measure.NewSampler(time.Second, tp, ttfb, opLat)
			sampler.Start()

			// Create a sub-runner for this path's files
			subRunner := &Runner{
				paths: []string{pth},
				p:     r.p,
				quiet: true,
				files: fileGroups[i],
				rng:   rand.New(rand.NewSource(r.rng.Int63())),
			}
			// Override workers to per-path count
			subP := *r.p
			subP.Workers = workersPerPath
			subRunner.p = &subP

			subRunner.runWorkers(ctx, tp, ttfb, opLat, stall)
			sampler.Stop()

			pr := PathResult{
				Path:       pth,
				Throughput: tp.Stats(r.p.Duration),
				TTFB:       ttfb.Stats(),
				OpLatency:  opLat.Stats(),
				Samples:    sampler.Samples(),
			}
			outcomes[i] = pathOutcome{pr: pr, stall: stall.StallPct()}
		}()
	}
	wg.Wait()

	// Collect per-path results
	perPath := make([]PathResult, n)
	allLatStats := make([]measure.LatencyStats, 0, n)
	allOpStats := make([]measure.LatencyStats, 0, n)
	var totalStall float64

	for i, o := range outcomes {
		perPath[i] = o.pr
		allLatStats = append(allLatStats, o.pr.TTFB)
		allOpStats = append(allOpStats, o.pr.OpLatency)
		totalStall += o.stall
	}
	result.PerPath = perPath

	// Aggregate throughput
	var totalBytesRead, totalBytesWritten, totalReadOps, totalWriteOps int64
	for _, o := range outcomes {
		totalBytesRead += o.pr.Throughput.BytesRead
		totalBytesWritten += o.pr.Throughput.BytesWritten
		totalReadOps += o.pr.Throughput.ReadOps
		totalWriteOps += o.pr.Throughput.WriteOps
	}
	secs := r.p.Duration.Seconds()
	result.Throughput = measure.ThroughputStats{
		ElapsedS:     secs,
		BytesRead:    totalBytesRead,
		BytesWritten: totalBytesWritten,
		ReadGBps:     float64(totalBytesRead) / (1e9 * secs),
		WriteGBps:    float64(totalBytesWritten) / (1e9 * secs),
		ReadMBps:     float64(totalBytesRead) / (1e6 * secs),
		WriteMBps:    float64(totalBytesWritten) / (1e6 * secs),
		ReadOps:      totalReadOps,
		WriteOps:     totalWriteOps,
		ReadIOPS:     float64(totalReadOps) / secs,
		WriteIOPS:    float64(totalWriteOps) / secs,
	}

	// Aggregate latency
	result.TTFB = mergeLatencyStats(allLatStats)
	result.OpLatency = mergeLatencyStats(allOpStats)
	result.GPUStallPct = totalStall / float64(n)

	// Merge samples from all paths
	var allSamples []measure.MetricSample
	for _, o := range outcomes {
		allSamples = append(allSamples, o.pr.Samples...)
	}
	result.Samples = allSamples
}

// partitionFiles divides r.files evenly among paths.
func (r *Runner) partitionFiles() [][]string {
	n := len(r.paths)
	groups := make([][]string, n)
	for i := range groups {
		groups[i] = []string{}
	}
	for i, f := range r.files {
		groups[i%n] = append(groups[i%n], f)
	}
	return groups
}

// mergeLatencyStats merges multiple LatencyStats into one aggregate.
// Sum Count, take min of Min, max of Max, weighted mean for Mean.
// For P50/P95/P99: use the stat from the recorder with the most samples.
func mergeLatencyStats(all []measure.LatencyStats) measure.LatencyStats {
	if len(all) == 0 {
		return measure.LatencyStats{}
	}
	var merged measure.LatencyStats
	var totalCount int64
	var weightedMean float64
	var minMs float64 = math.MaxFloat64
	var maxMs float64

	// Find the one with most samples for percentiles
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

	// Use percentiles from the recorder with most samples (approximation)
	best := all[bestIdx]
	merged.P25Ms = best.P25Ms
	merged.P50Ms = best.P50Ms
	merged.P75Ms = best.P75Ms
	merged.P90Ms = best.P90Ms
	merged.P95Ms = best.P95Ms
	merged.P99Ms = best.P99Ms

	return merged
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

// prepare creates test files on each target path using deterministic content.
// Files are distributed evenly across paths.
func (r *Runner) prepare() error {
	rng := rand.New(rand.NewSource(r.p.Seed))
	fileSizes := profile.GenerateFileSizes(r.p.Files.Distribution, r.p.Files.Count, r.p.Files.SizeBytes, rng)

	n := len(r.paths)
	totalFiles := r.p.Files.Count

	// Create .pulsar-bench dirs under each path
	for _, pth := range r.paths {
		dir := filepath.Join(pth, ".pulsar-bench")
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	buf := make([]byte, min64(r.p.BlockSize, 4*1024*1024))
	// Fill with a non-zero pattern (avoids sparse-file shortcuts on some FS)
	for i := range buf {
		buf[i] = byte(i % 251)
	}

	r.files = make([]string, 0, totalFiles)
	r.fileSizes = make([]int64, 0, totalFiles)

	for i := 0; i < totalFiles; i++ {
		pathIdx := i % n
		pth := r.paths[pathIdx]
		dir := filepath.Join(pth, ".pulsar-bench")
		fpath := filepath.Join(dir, fmt.Sprintf("file-%04d.bin", i))
		fileSize := fileSizes[i]

		r.files = append(r.files, fpath)
		r.fileSizes = append(r.fileSizes, fileSize)

		// If file already exists with the right size, reuse it (--no-cleanup).
		if st, err := os.Stat(fpath); err == nil && st.Size() == fileSize {
			continue
		}

		f, err := os.Create(fpath)
		if err != nil {
			return err
		}
		remaining := fileSize
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
	for _, pth := range r.paths {
		dir := filepath.Join(pth, ".pulsar-bench")
		os.RemoveAll(dir)
	}
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
