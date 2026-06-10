// Package profile defines the workload profile schema and loader.
// A profile is a YAML document describing which workload to run and
// with what parameters. Profiles can be built-in (by name) or
// loaded from a user-supplied YAML file.
package profile

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Profile fully describes a single benchmark run.
type Profile struct {
	// Identity
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
	Focus       string `yaml:"focus"` // short label for list output: "TTFB", "Throughput", etc.

	// Workload type — selects the I/O pattern engine.
	// Valid values: sequential-read, random-read, write, mixed,
	//               metadata, multi-epoch, agent-workspace, thrash
	Workload string `yaml:"workload"`

	// Parallelism
	Workers int `yaml:"workers"` // concurrent goroutines hammering the path

	// Timing
	Duration time.Duration `yaml:"duration"` // measurement window (after warmup)
	Warmup   time.Duration `yaml:"warmup"`   // ignored from metrics, warms caches

	// Test data
	Files FilesConfig `yaml:"files"`

	// Workload-specific knobs
	ReadPct  int  `yaml:"read_pct"`  // for mixed workload: 0–100
	WritePct int  `yaml:"write_pct"` // for mixed workload: 0–100
	Reuse    bool `yaml:"reuse"`     // repeatedly access same files (warm-cache scenario)
	Epochs   int  `yaml:"epochs"`    // for multi-epoch: how many full passes

	// I/O knobs
	// BlockSize is the bytes per read/write syscall (set by built-in profiles as int64).
	// In YAML files, use the human-readable block_size field instead: e.g. block_size: 1MB.
	BlockSize      int64  `yaml:"block_size_bytes"` // internal: bytes
	BlockSizeHuman string `yaml:"block_size"`        // YAML: "256KB", "4MB", etc.
	DirectIO       bool   `yaml:"direct_io"`         // O_DIRECT (Linux only; silently ignored on macOS)
	FsyncOnWrite   bool   `yaml:"fsync_on_write"`    // fsync() after every write

	// ComputeGapMs simulates GPU processing time between I/O bursts.
	// After reading one file (or one pass), each worker sleeps this many
	// milliseconds before issuing the next read. This models the real
	// DataLoader pattern: read a batch → GPU processes it → read next batch.
	// Set to 0 (default) for maximum I/O pressure (pure storage stress test).
	// Set to ~50–200ms to simulate typical GPU compute time per batch.
	ComputeGapMs int `yaml:"compute_gap_ms"`

	// Targets — benchmark exits with code 1 if any are breached.
	// Zero means "no target" (metric is still collected).
	Targets TargetConfig `yaml:"targets"`

	// BatchConfig describes the ML batch structure for computing samples/sec.
	NumAccelerators int   `yaml:"num_accelerators"` // simulated GPU/TPU count
	SampleSizeBytes int64 `yaml:"sample_size_bytes"` // bytes per training sample

	// BatchSizeBytes is the data read per simulated training batch. I/O time is
	// accumulated per batch and paired with one ComputeGapMs sleep, so the
	// GPU-stall metric compares per-batch I/O time against per-batch compute
	// time (mirroring a DataLoader: fetch a batch → GPU step → fetch next).
	// Defaults to 16× the block size (min 4 MiB) when unset.
	BatchSizeBytes int64  `yaml:"batch_size_bytes"`
	BatchSizeHuman string `yaml:"batch_size"` // YAML: "8MB", "16MiB", etc.

	// Misc
	Seed    int64 `yaml:"seed"`
	// Verify: if true, write a deterministic pattern and verify it on each read.
	// Detects silent data corruption. Adds CPU overhead.
	Verify  bool `yaml:"verify"`
	// IODepth is the number of concurrent I/O operations per worker.
	// Default 0 means 1 (sync, one outstanding I/O at a time).
	// Higher values increase I/O concurrency without adding more workers.
	IODepth int  `yaml:"iodepth"`
	Cleanup bool `yaml:"cleanup"` // delete test files after run
}

type FilesConfig struct {
	Count     int    `yaml:"count"`
	SizeBytes int64  `yaml:"size_bytes"` // populated by ParseSize in YAML pre-processing
	SizeHuman string `yaml:"size"`       // human-readable: "1GB", "512MB" — parsed at load time

	// Distribution controls how file sizes are generated.
	// "uniform" (default): all files are SizeBytes.
	// "imagenet": log-normal distribution matching ImageNet JPEG sizes (mean ~120KB).
	// "bert": fixed large files matching BERT HDF5 sizes (~4.3GB each).
	// "unet": fixed files matching 3D-UNet NPZ sizes (~150MB each).
	Distribution string `yaml:"distribution"`
}

type TargetConfig struct {
	// Zero means "no target set".
	TTFBColdP99Ms   float64 `yaml:"ttfb_cold_p99_ms"`
	TTFBWarmP99Ms   float64 `yaml:"ttfb_warm_p99_ms"`
	ReadGBps        float64 `yaml:"read_gbps"`
	WriteGBps       float64 `yaml:"write_gbps"`
	StatP99Ms       float64 `yaml:"stat_p99_ms"`
	ReaddirP99Ms    float64 `yaml:"readdir_p99_ms"`
	MetaHitRatePct  float64 `yaml:"meta_hit_rate_pct"` // 0–100
}

// knownWorkloads is the set of valid workload type strings.
var knownWorkloads = map[string]bool{
	"sequential-read": true,
	"random-read":     true,
	"write":           true,
	"mixed":           true,
	"metadata":        true,
	"multi-epoch":     true,
	"agent-workspace": true,
	"thrash":          true,
}

func (p *Profile) validate() error {
	if p.Workload == "" {
		return fmt.Errorf("workload must be set")
	}
	if !knownWorkloads[p.Workload] {
		known := []string{
			"sequential-read", "random-read", "write", "mixed",
			"metadata", "multi-epoch", "agent-workspace",
		}
		return fmt.Errorf("unknown workload %q — valid values: %s", p.Workload, strings.Join(known, ", "))
	}
	if p.Workers <= 0 {
		p.Workers = 8
	}
	if p.Duration <= 0 {
		p.Duration = 30 * time.Second
	}
	if p.Files.Count <= 0 {
		p.Files.Count = 8
	}
	if p.Files.SizeHuman != "" && p.Files.SizeBytes == 0 {
		sz, err := ParseSize(p.Files.SizeHuman)
		if err != nil {
			return fmt.Errorf("files.size %q: %w", p.Files.SizeHuman, err)
		}
		p.Files.SizeBytes = sz
	}
	if p.Files.SizeBytes == 0 {
		p.Files.SizeBytes = 1 << 30 // 1 GB default
	}
	if p.BlockSizeHuman != "" && p.BlockSize == 0 {
		sz, err := ParseSize(p.BlockSizeHuman)
		if err != nil {
			return fmt.Errorf("block_size %q: %w", p.BlockSizeHuman, err)
		}
		p.BlockSize = sz
	}
	if p.BlockSize == 0 {
		p.BlockSize = 256 * 1024 // 256 KiB default
	}
	if p.BatchSizeHuman != "" && p.BatchSizeBytes == 0 {
		sz, err := ParseSize(p.BatchSizeHuman)
		if err != nil {
			return fmt.Errorf("batch_size %q: %w", p.BatchSizeHuman, err)
		}
		p.BatchSizeBytes = sz
	}
	if p.BatchSizeBytes == 0 {
		p.BatchSizeBytes = 16 * p.BlockSize
		if p.BatchSizeBytes < 4<<20 {
			p.BatchSizeBytes = 4 << 20
		}
	}
	if p.Epochs == 0 {
		p.Epochs = 2
	}
	if p.ReadPct+p.WritePct == 0 {
		p.ReadPct = 100 // read-only by default
	}
	return nil
}

// LoadBuiltin returns a copy of the named built-in profile.
func LoadBuiltin(name string) (*Profile, error) {
	for _, p := range Builtin() {
		if p.Name == name {
			cp := p
			return &cp, nil
		}
	}
	names := make([]string, 0, len(Builtin()))
	for _, p := range Builtin() {
		names = append(names, p.Name)
	}
	return nil, fmt.Errorf("unknown profile %q — available: %s", name, strings.Join(names, ", "))
}

// LoadFile reads a YAML profile from disk.
func LoadFile(path string) (*Profile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p Profile
	if err := yaml.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	return &p, nil
}

// ParseSize converts human strings like "1GB", "512MB", "256KiB" to bytes.
// Suffixes are matched longest-first so "MB" is never mistaken for "B".
func ParseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	// Ordered longest-suffix-first to avoid "B" matching before "MB", "GIB", etc.
	ordered := []struct {
		suffix string
		mult   int64
	}{
		{"TIB", 1 << 40}, {"TB", 1e12},
		{"GIB", 1 << 30}, {"GB", 1e9},
		{"MIB", 1 << 20}, {"MB", 1e6},
		{"KIB", 1 << 10}, {"KB", 1e3},
		{"B", 1},
	}
	for _, u := range ordered {
		if strings.HasSuffix(s, u.suffix) {
			numStr := strings.TrimSpace(strings.TrimSuffix(s, u.suffix))
			n, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid size %q", s)
			}
			return int64(n * float64(u.mult)), nil
		}
	}
	// Plain integer = bytes
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unrecognised size %q — use e.g. 1GB, 512MB, 256KiB", s)
	}
	return n, nil
}
