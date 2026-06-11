package cluster

import (
	"testing"
	"time"

	"github.com/minio/pulsar/measure"
	"github.com/minio/pulsar/profile"
	"github.com/minio/pulsar/workload"
)

// ---------------------------------------------------------------------------
// mergeLatencyStats (via measure.MergeLatencyStats)
// ---------------------------------------------------------------------------

func TestMergeLatencyStats_Empty(t *testing.T) {
	s := mergeLatencyStats(nil)
	if s.Count != 0 {
		t.Errorf("empty merge Count = %d, want 0", s.Count)
	}
}

func TestMergeLatencyStats_Single(t *testing.T) {
	in := []measure.LatencyStats{{
		Count: 10, MinMs: 1, MaxMs: 10,
		P50Ms: 5, P99Ms: 9, MeanMs: 5,
	}}
	out := mergeLatencyStats(in)
	if out.Count != 10 || out.P50Ms != 5 || out.P99Ms != 9 {
		t.Errorf("single merge unexpected: %+v", out)
	}
}

func TestMergeLatencyStats_Two(t *testing.T) {
	a := measure.LatencyStats{Count: 100, MinMs: 1, MaxMs: 50, MeanMs: 10, P99Ms: 45}
	b := measure.LatencyStats{Count: 200, MinMs: 2, MaxMs: 80, MeanMs: 20, P99Ms: 75}
	out := mergeLatencyStats([]measure.LatencyStats{a, b})

	if out.Count != 300 {
		t.Errorf("Count = %d, want 300", out.Count)
	}
	if out.MinMs != 1 {
		t.Errorf("MinMs = %f, want 1", out.MinMs)
	}
	if out.MaxMs != 80 {
		t.Errorf("MaxMs = %f, want 80", out.MaxMs)
	}
	// weighted mean: (100*10 + 200*20) / 300 = 5000/300 ≈ 16.67
	wantMean := (100*10.0 + 200*20.0) / 300.0
	if out.MeanMs < wantMean-0.1 || out.MeanMs > wantMean+0.1 {
		t.Errorf("MeanMs = %f, want %.2f", out.MeanMs, wantMean)
	}
	// Percentiles come from the largest recorder (b has 200 samples)
	if out.P99Ms != 75 {
		t.Errorf("P99Ms = %f, want 75 (from larger recorder b)", out.P99Ms)
	}
}

// ---------------------------------------------------------------------------
// checkTargets (coordinator version)
// ---------------------------------------------------------------------------

func makeResult(readGBps, writeGBps, ttfbP99 float64, metadata *workload.MetadataStats, epochs []workload.EpochStats) *workload.Result {
	return &workload.Result{
		Throughput: measure.ThroughputStats{ReadGBps: readGBps, WriteGBps: writeGBps},
		TTFB:       measure.LatencyStats{P99Ms: ttfbP99},
		Metadata:   metadata,
		Epochs:     epochs,
	}
}

func TestCoordinatorCheckTargets_AllPass(t *testing.T) {
	p := &profile.Profile{Targets: profile.TargetConfig{
		ReadGBps: 1.0, TTFBColdP99Ms: 500,
	}}
	res := makeResult(2.0, 0, 100, nil, nil)
	v, missed := checkTargets(res, p)
	if missed != 0 {
		t.Errorf("expected 0 violations, got %d: %v", missed, v)
	}
}

func TestCoordinatorCheckTargets_ReadMiss(t *testing.T) {
	p := &profile.Profile{Targets: profile.TargetConfig{ReadGBps: 2.0}}
	res := makeResult(1.0, 0, 0, nil, nil)
	_, missed := checkTargets(res, p)
	if missed != 1 {
		t.Errorf("expected 1 violation, got %d", missed)
	}
}

func TestCoordinatorCheckTargets_TTFBColdMiss(t *testing.T) {
	p := &profile.Profile{Targets: profile.TargetConfig{TTFBColdP99Ms: 100}}
	res := makeResult(0, 0, 250, nil, nil)
	_, missed := checkTargets(res, p)
	if missed != 1 {
		t.Errorf("expected 1 violation, got %d", missed)
	}
}

func TestCoordinatorCheckTargets_TTFBWarmRequiresEpochs(t *testing.T) {
	p := &profile.Profile{Targets: profile.TargetConfig{TTFBWarmP99Ms: 50}}
	// Without epochs — should not fire
	res := makeResult(0, 0, 200, nil, nil)
	_, missed := checkTargets(res, p)
	if missed != 0 {
		t.Errorf("TTFBWarm should not fire without epochs, got %d", missed)
	}
	// One cold epoch only — still no warm data, should not fire
	res.Epochs = []workload.EpochStats{{Epoch: 1, TTFB: measure.LatencyStats{P99Ms: 200}}}
	_, missed = checkTargets(res, p)
	if missed != 0 {
		t.Errorf("TTFBWarm should not fire with only a cold epoch, got %d", missed)
	}
	// Warm epoch above target — should fire on the warm epoch's p99
	res.Epochs = append(res.Epochs, workload.EpochStats{Epoch: 2, TTFB: measure.LatencyStats{P99Ms: 80}})
	_, missed = checkTargets(res, p)
	if missed != 1 {
		t.Errorf("TTFBWarm should fire on warm epoch p99 above target, got %d", missed)
	}
}

func TestCoordinatorCheckTargets_MetadataAll(t *testing.T) {
	p := &profile.Profile{Targets: profile.TargetConfig{
		StatP99Ms: 5, ReaddirP99Ms: 500, MetaHitRatePct: 95,
	}}
	res := makeResult(0, 0, 0, &workload.MetadataStats{
		StatP99Ms: 10, ReaddirP99Ms: 800, HitRatePct: 80,
	}, nil)
	_, missed := checkTargets(res, p)
	if missed != 3 {
		t.Errorf("expected 3 metadata violations, got %d", missed)
	}
}

func TestCoordinatorCheckTargets_ZeroTargetsNeverFire(t *testing.T) {
	p := &profile.Profile{Targets: profile.TargetConfig{}}
	res := makeResult(0, 0, 99999, &workload.MetadataStats{
		StatP99Ms: 99999, ReaddirP99Ms: 99999, HitRatePct: 0,
	}, nil)
	_, missed := checkTargets(res, p)
	if missed != 0 {
		t.Errorf("zero targets should never fire, got %d", missed)
	}
}

// ---------------------------------------------------------------------------
// mergeResults
// ---------------------------------------------------------------------------

func TestMergeResults_TwoNodes(t *testing.T) {
	p := &profile.Profile{
		Name:     "training",
		Workload: "sequential-read",
		Duration: 60 * time.Second,
		Targets:  profile.TargetConfig{ReadGBps: 1.0},
	}
	now := time.Now()
	r1 := &workload.Result{
		Throughput: measure.ThroughputStats{
			BytesRead: 60_000_000_000, ReadOps: 1000,
			ReadGBps: 1.0, ElapsedS: 60,
		},
		TTFB:      measure.LatencyStats{Count: 100, P99Ms: 50, MeanMs: 10},
		Workers:   32,
		DurationS: 60,
		StartedAt: now,
		FinishedAt: now.Add(60 * time.Second),
		Path:      "/mnt/node1",
	}
	r2 := &workload.Result{
		Throughput: measure.ThroughputStats{
			BytesRead: 60_000_000_000, ReadOps: 1000,
			ReadGBps: 1.0, ElapsedS: 60,
		},
		TTFB:      measure.LatencyStats{Count: 100, P99Ms: 60, MeanMs: 12},
		Workers:   32,
		DurationS: 60,
		StartedAt: now,
		FinishedAt: now.Add(60 * time.Second),
		Path:      "/mnt/node2",
	}

	nodes := []NodeAddr{"node1:7762", "node2:7762"}
	outcomes := []nodeOutcome{
		{result: r1},
		{result: r2},
	}

	merged, err := mergeResults(nodes, outcomes, p)
	if err != nil {
		t.Fatalf("mergeResults error: %v", err)
	}

	// Total bytes = 120 GB, so ReadGBps = 120/60 = 2.0
	wantGBps := 2.0
	if merged.Throughput.ReadGBps < wantGBps-0.01 || merged.Throughput.ReadGBps > wantGBps+0.01 {
		t.Errorf("merged ReadGBps = %f, want %f", merged.Throughput.ReadGBps, wantGBps)
	}
	if merged.Workers != 64 {
		t.Errorf("merged Workers = %d, want 64", merged.Workers)
	}
	if len(merged.PerNode) != 2 {
		t.Errorf("PerNode len = %d, want 2", len(merged.PerNode))
	}
	if merged.TargetsMissed != 0 {
		t.Errorf("expected all targets met, got %d violations: %v", merged.TargetsMissed, merged.Violations)
	}
}

func TestMergeResults_NodeError(t *testing.T) {
	p := &profile.Profile{Name: "training", Duration: 60 * time.Second}
	nodes := []NodeAddr{"node1:7762", "node2:7762"}
	outcomes := []nodeOutcome{
		{result: &workload.Result{Path: "/mnt/node1", StartedAt: time.Now(), FinishedAt: time.Now()}},
		{err: errTest("node2 disk full")},
	}
	_, err := mergeResults(nodes, outcomes, p)
	if err == nil {
		t.Error("expected error when a node outcome has error, got nil")
	}
}

func TestMergeResults_NilResult(t *testing.T) {
	p := &profile.Profile{Name: "training", Duration: 60 * time.Second}
	nodes := []NodeAddr{"node1:7762"}
	outcomes := []nodeOutcome{{result: nil}}
	_, err := mergeResults(nodes, outcomes, p)
	if err == nil {
		t.Error("expected error for nil result, got nil")
	}
}

// errTest is a simple error value for tests.
type errTest string

func (e errTest) Error() string { return string(e) }
