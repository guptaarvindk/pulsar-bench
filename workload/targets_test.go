package workload

import (
	"testing"

	"github.com/minio/pulsar/measure"
	"github.com/minio/pulsar/profile"
)

// makeRunner is a helper that creates a minimal Runner for checkTargets tests.
func makeRunner(targets profile.TargetConfig) *Runner {
	return &Runner{
		p: &profile.Profile{Targets: targets},
	}
}

func TestCheckTargets_AllPass(t *testing.T) {
	r := makeRunner(profile.TargetConfig{
		ReadGBps:      1.0,
		WriteGBps:     0.5,
		TTFBColdP99Ms: 500,
	})
	res := &Result{
		Throughput: measure.ThroughputStats{ReadGBps: 2.0, WriteGBps: 1.0},
		TTFB:       measure.LatencyStats{P99Ms: 100},
	}
	violations, missed := r.checkTargets(res)
	if missed != 0 {
		t.Errorf("expected 0 violations, got %d: %v", missed, violations)
	}
}

func TestCheckTargets_ReadThroughputMiss(t *testing.T) {
	r := makeRunner(profile.TargetConfig{ReadGBps: 2.0})
	res := &Result{
		Throughput: measure.ThroughputStats{ReadGBps: 1.5},
	}
	violations, missed := r.checkTargets(res)
	if missed != 1 {
		t.Errorf("expected 1 violation, got %d", missed)
	}
	if len(violations) == 0 || violations[0] == "" {
		t.Error("violation message should not be empty")
	}
}

func TestCheckTargets_WriteThroughputMiss(t *testing.T) {
	r := makeRunner(profile.TargetConfig{WriteGBps: 1.0})
	res := &Result{
		Throughput: measure.ThroughputStats{WriteGBps: 0.5},
	}
	_, missed := r.checkTargets(res)
	if missed != 1 {
		t.Errorf("expected 1 violation, got %d", missed)
	}
}

func TestCheckTargets_TTFBColdMiss(t *testing.T) {
	r := makeRunner(profile.TargetConfig{TTFBColdP99Ms: 200})
	res := &Result{
		TTFB: measure.LatencyStats{P99Ms: 350},
	}
	_, missed := r.checkTargets(res)
	if missed != 1 {
		t.Errorf("expected 1 violation, got %d", missed)
	}
}

func TestCheckTargets_TTFBWarmMissRequiresEpochs(t *testing.T) {
	r := makeRunner(profile.TargetConfig{TTFBWarmP99Ms: 50})
	// No epochs → warm target should not fire
	res := &Result{
		TTFB: measure.LatencyStats{P99Ms: 200},
	}
	_, missed := r.checkTargets(res)
	if missed != 0 {
		t.Errorf("TTFBWarmP99Ms should not fire without epochs, got %d violations", missed)
	}

	// With epochs → should fire
	res.Epochs = []EpochStats{{Epoch: 1}}
	_, missed = r.checkTargets(res)
	if missed != 1 {
		t.Errorf("TTFBWarmP99Ms should fire with epochs, got %d violations", missed)
	}
}

func TestCheckTargets_MetadataStatMiss(t *testing.T) {
	r := makeRunner(profile.TargetConfig{StatP99Ms: 5})
	res := &Result{
		Metadata: &MetadataStats{StatP99Ms: 10, ReaddirP99Ms: 100, HitRatePct: 99},
	}
	_, missed := r.checkTargets(res)
	if missed != 1 {
		t.Errorf("expected 1 violation for stat p99, got %d", missed)
	}
}

func TestCheckTargets_MetadataReaddirMiss(t *testing.T) {
	r := makeRunner(profile.TargetConfig{ReaddirP99Ms: 500})
	res := &Result{
		Metadata: &MetadataStats{StatP99Ms: 1, ReaddirP99Ms: 1200, HitRatePct: 99},
	}
	_, missed := r.checkTargets(res)
	if missed != 1 {
		t.Errorf("expected 1 violation for readdir p99, got %d", missed)
	}
}

func TestCheckTargets_MetadataHitRateMiss(t *testing.T) {
	r := makeRunner(profile.TargetConfig{MetaHitRatePct: 95})
	res := &Result{
		Metadata: &MetadataStats{StatP99Ms: 1, ReaddirP99Ms: 100, HitRatePct: 80},
	}
	_, missed := r.checkTargets(res)
	if missed != 1 {
		t.Errorf("expected 1 violation for hit rate, got %d", missed)
	}
}

func TestCheckTargets_ZeroTargetIgnored(t *testing.T) {
	// Zero value targets mean "no target" — should never fire even with bad metrics.
	r := makeRunner(profile.TargetConfig{}) // all zeros
	res := &Result{
		Throughput: measure.ThroughputStats{ReadGBps: 0},
		TTFB:       measure.LatencyStats{P99Ms: 99999},
		Metadata:   &MetadataStats{StatP99Ms: 99999, ReaddirP99Ms: 99999, HitRatePct: 0},
	}
	_, missed := r.checkTargets(res)
	if missed != 0 {
		t.Errorf("zero targets should never fire, got %d violations", missed)
	}
}

func TestCheckTargets_MultipleViolations(t *testing.T) {
	r := makeRunner(profile.TargetConfig{
		ReadGBps:      2.0,
		TTFBColdP99Ms: 100,
		StatP99Ms:     5,
		ReaddirP99Ms:  500,
	})
	res := &Result{
		Throughput: measure.ThroughputStats{ReadGBps: 1.0},
		TTFB:       measure.LatencyStats{P99Ms: 500},
		Metadata:   &MetadataStats{StatP99Ms: 20, ReaddirP99Ms: 1500, HitRatePct: 99},
	}
	_, missed := r.checkTargets(res)
	if missed != 4 {
		t.Errorf("expected 4 violations, got %d", missed)
	}
}
