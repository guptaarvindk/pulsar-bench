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
	BlockSize    int64 `yaml:"block_size"`    // bytes per read/write syscall
	DirectIO     bool  `yaml:"direct_io"`     // O_DIRECT (Linux only; skipped on macOS)
	FsyncOnWrite bool  `yaml:"fsync_on_write"` // fsync() after every write

	// Targets — benchmark exits with code 1 if any are breached.
	// Zero means "no target" (metric is still collected).
	Targets TargetConfig `yaml:"targets"`

	// Misc
	Seed    int64 `yaml:"seed"`
	Cleanup bool  `yaml:"cleanup"` // delete test files after run
}

type FilesConfig struct {
	Count     int    `yaml:"count"`
	SizeBytes int64  `yaml:"size_bytes"` // populated by ParseSize in YAML pre-processing
	SizeHuman string `yaml:"size"`       // human-readable: "1GB", "512MB" — parsed at load time
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

func (p *Profile) validate() error {
	if p.Workload == "" {
		return fmt.Errorf("workload must be set")
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
	if p.BlockSize == 0 {
		p.BlockSize = 256 * 1024 // 256 KiB — matches agstor fio default
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
