package report

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/pulsar/measure"
	"github.com/minio/pulsar/workload"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func baseResult() *workload.Result {
	return &workload.Result{
		Profile:      "training",
		WorkloadType: "sequential-read",
		Path:         "/mnt/data",
		Workers:      32,
		DurationS:    60,
		StartedAt:    time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC),
		FinishedAt:   time.Date(2025, 1, 15, 10, 1, 0, 0, time.UTC),
		Throughput: measure.ThroughputStats{
			ReadGBps: 3.5, WriteGBps: 0,
			BytesRead: 210_000_000_000, ElapsedS: 60,
		},
		TTFB: measure.LatencyStats{Count: 100, P99Ms: 45.0, MeanMs: 10},
	}
}

// ---------------------------------------------------------------------------
// buildSummary
// ---------------------------------------------------------------------------

func TestBuildSummary_BasicFields(t *testing.T) {
	r := baseResult()
	s := buildSummary(r)

	if s.Profile != "training" {
		t.Errorf("Profile = %q, want %q", s.Profile, "training")
	}
	if s.WorkloadType != "sequential-read" {
		t.Errorf("WorkloadType = %q, want %q", s.WorkloadType, "sequential-read")
	}
	if s.Path != "/mnt/data" {
		t.Errorf("Path = %q, want %q", s.Path, "/mnt/data")
	}
	if s.Workers != 32 {
		t.Errorf("Workers = %d, want 32", s.Workers)
	}
	if s.DurationS != 60 {
		t.Errorf("DurationS = %f, want 60", s.DurationS)
	}
	if s.ReadGBps != 3.5 {
		t.Errorf("ReadGBps = %f, want 3.5", s.ReadGBps)
	}
	if s.TTFBP99Ms != 45.0 {
		t.Errorf("TTFBP99Ms = %f, want 45.0", s.TTFBP99Ms)
	}
}

func TestBuildSummary_PassWhenNoViolations(t *testing.T) {
	r := baseResult()
	r.TargetsMissed = 0
	r.Violations = nil
	s := buildSummary(r)
	if !s.Pass {
		t.Error("Pass = false, want true when TargetsMissed == 0")
	}
}

func TestBuildSummary_FailWhenViolations(t *testing.T) {
	r := baseResult()
	r.TargetsMissed = 1
	r.Violations = []string{"read throughput 3.50 GB/s < target 5.00 GB/s"}
	s := buildSummary(r)
	if s.Pass {
		t.Error("Pass = true, want false when TargetsMissed > 0")
	}
	if len(s.Violations) != 1 {
		t.Errorf("Violations len = %d, want 1", len(s.Violations))
	}
}

func TestBuildSummary_EpochsPassedThrough(t *testing.T) {
	r := baseResult()
	r.Epochs = []workload.EpochStats{
		{Epoch: 1, Throughput: measure.ThroughputStats{ReadGBps: 1.0}},
		{Epoch: 2, Throughput: measure.ThroughputStats{ReadGBps: 2.5}},
	}
	s := buildSummary(r)
	if len(s.Epochs) != 2 {
		t.Errorf("Epochs len = %d, want 2", len(s.Epochs))
	}
	if s.Epochs[0].Epoch != 1 || s.Epochs[1].Epoch != 2 {
		t.Errorf("Epoch order wrong: %v", s.Epochs)
	}
}

func TestBuildSummary_MetadataPassedThrough(t *testing.T) {
	r := baseResult()
	r.Metadata = &workload.MetadataStats{
		StatP99Ms: 3.5, ReaddirP99Ms: 250, HitRatePct: 92,
		StatOps: 10000, ReaddirOps: 500,
	}
	s := buildSummary(r)
	if s.Metadata == nil {
		t.Fatal("Metadata = nil, want non-nil")
	}
	if s.Metadata.StatP99Ms != 3.5 {
		t.Errorf("StatP99Ms = %f, want 3.5", s.Metadata.StatP99Ms)
	}
	if s.Metadata.HitRatePct != 92 {
		t.Errorf("HitRatePct = %f, want 92", s.Metadata.HitRatePct)
	}
}

func TestBuildSummary_AcceleratorPassedThrough(t *testing.T) {
	r := baseResult()
	r.Accelerator = &workload.AcceleratorStats{NumAccelerators: 8, SamplesPerSec: 1200}
	s := buildSummary(r)
	if s.Accelerator == nil {
		t.Fatal("Accelerator = nil, want non-nil")
	}
	if s.Accelerator.NumAccelerators != 8 {
		t.Errorf("NumAccelerators = %d, want 8", s.Accelerator.NumAccelerators)
	}
}

func TestBuildSummary_PerNodePassedThrough(t *testing.T) {
	r := baseResult()
	r.PerNode = []workload.NodeResult{
		{Node: "node1:7762", Throughput: measure.ThroughputStats{ReadGBps: 1.5}},
		{Node: "node2:7762", Throughput: measure.ThroughputStats{ReadGBps: 2.0}},
	}
	s := buildSummary(r)
	if len(s.PerNode) != 2 {
		t.Errorf("PerNode len = %d, want 2", len(s.PerNode))
	}
	if s.PerNode[0].Node != "node1:7762" {
		t.Errorf("PerNode[0].Node = %q, want node1:7762", s.PerNode[0].Node)
	}
}

func TestBuildSummary_NilMetadataStaysNil(t *testing.T) {
	r := baseResult()
	r.Metadata = nil
	s := buildSummary(r)
	if s.Metadata != nil {
		t.Error("Metadata should be nil when result has no metadata")
	}
}

func TestBuildSummary_StartedAtFormatted(t *testing.T) {
	r := baseResult()
	s := buildSummary(r)
	// Should contain "2025-01-15"
	if !strings.Contains(s.StartedAt, "2025-01-15") {
		t.Errorf("StartedAt = %q, want to contain 2025-01-15", s.StartedAt)
	}
}

// ---------------------------------------------------------------------------
// WriteHTML
// ---------------------------------------------------------------------------

func TestWriteHTML_CreatesFile(t *testing.T) {
	r := baseResult()
	path := t.TempDir() + "/report.html"
	err := WriteHTML(path, "Test Benchmark", r)
	if err != nil {
		t.Fatalf("WriteHTML error: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("file not created: %v", err)
	}
	if info.Size() == 0 {
		t.Error("HTML file is empty")
	}
}

func TestWriteHTML_ContainsTitleAndMeta(t *testing.T) {
	r := baseResult()
	path := t.TempDir() + "/report.html"
	if err := WriteHTML(path, "Pulsar Perf Report", r); err != nil {
		t.Fatalf("WriteHTML error: %v", err)
	}
	data, _ := os.ReadFile(path)
	html := string(data)
	if !strings.Contains(html, "Pulsar Perf Report") {
		t.Error("HTML missing title")
	}
	if !strings.Contains(html, "<!DOCTYPE html>") {
		t.Error("HTML missing doctype")
	}
}

func TestWriteHTML_ContainsProfileName(t *testing.T) {
	r := baseResult()
	path := t.TempDir() + "/report.html"
	if err := WriteHTML(path, "Test", r); err != nil {
		t.Fatalf("WriteHTML error: %v", err)
	}
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), "training") {
		t.Error("HTML should contain profile name 'training'")
	}
}

func TestWriteHTML_WithEpochs(t *testing.T) {
	r := baseResult()
	r.Epochs = []workload.EpochStats{
		{Epoch: 1, Throughput: measure.ThroughputStats{ReadGBps: 1.2}, TTFB: measure.LatencyStats{P99Ms: 80}},
		{Epoch: 2, Throughput: measure.ThroughputStats{ReadGBps: 3.1}, TTFB: measure.LatencyStats{P99Ms: 25}},
	}
	path := t.TempDir() + "/report_epochs.html"
	if err := WriteHTML(path, "Epoch Test", r); err != nil {
		t.Fatalf("WriteHTML with epochs error: %v", err)
	}
	data, _ := os.ReadFile(path)
	// JSON-embedded epoch data should appear
	if !strings.Contains(string(data), `"epoch"`) {
		t.Error("HTML should contain epoch data in JSON")
	}
}

func TestWriteHTML_WithMetadata(t *testing.T) {
	r := baseResult()
	r.Metadata = &workload.MetadataStats{
		StatP99Ms: 4.2, ReaddirP99Ms: 300, HitRatePct: 88,
		StatOps: 5000, ReaddirOps: 200,
	}
	path := t.TempDir() + "/report_meta.html"
	if err := WriteHTML(path, "Meta Test", r); err != nil {
		t.Fatalf("WriteHTML with metadata error: %v", err)
	}
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), `"metadata"`) {
		t.Error("HTML should contain metadata JSON key")
	}
}

func TestWriteHTML_InvalidPath(t *testing.T) {
	r := baseResult()
	err := WriteHTML("/nonexistent-dir/report.html", "Test", r)
	if err == nil {
		t.Error("expected error for invalid path, got nil")
	}
}

func TestWriteHTML_WithSamples(t *testing.T) {
	r := baseResult()
	r.Samples = []measure.MetricSample{
		{T: 1, ReadGBps: 3.2, WriteGBps: 0},
		{T: 2, ReadGBps: 3.6, WriteGBps: 0},
		{T: 3, ReadGBps: 3.4, WriteGBps: 0},
	}
	path := t.TempDir() + "/report_samples.html"
	if err := WriteHTML(path, "Samples Test", r); err != nil {
		t.Fatalf("WriteHTML with samples error: %v", err)
	}
	data, _ := os.ReadFile(path)
	html := string(data)
	// Samples JSON is embedded; HasSamples=true means chart section is rendered
	if !strings.Contains(html, "read_gbps") {
		t.Error("HTML should contain sample data with read_gbps field")
	}
}

// ---------------------------------------------------------------------------
// humanBytes helper
// ---------------------------------------------------------------------------

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KiB"},
		{1_048_576, "1.0 MiB"},
		{1_073_741_824, "1.0 GiB"},
		{10_737_418_240, "10.0 GiB"},
	}
	for _, tt := range tests {
		got := humanBytes(tt.input)
		if got != tt.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// humanNum helper
// ---------------------------------------------------------------------------

func TestHumanNum(t *testing.T) {
	tests := []struct {
		input float64
		want  string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1.0K"},
		{1_500_000, "1.5M"},
	}
	for _, tt := range tests {
		got := humanNum(tt.input)
		if got != tt.want {
			t.Errorf("humanNum(%f) = %q, want %q", tt.input, got, tt.want)
		}
	}
}
