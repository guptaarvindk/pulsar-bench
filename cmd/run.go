package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/pulsar/cluster"
	"github.com/minio/pulsar/profile"
	"github.com/minio/pulsar/report"
	"github.com/minio/pulsar/workload"
	"github.com/spf13/cobra"
)

var (
	flagPaths      []string
	flagNodes      []string
	flagProfile    string
	flagWorkers    int
	flagDuration   time.Duration
	flagWarmup     time.Duration
	flagFileSize   string
	flagFileCount  int
	flagOutputJSON string
	flagNoCleanup  bool
	flagSeed       int64
	flagQuiet      bool
	flagComputeGap int
	flagDirectIO   bool
	flagNoDirectIO bool

	flagVerify      bool
	flagIODepth     int
	flagOutputCSV   string
	flagSteadyState bool
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a benchmark workload profile",
	Example: `  # Run the LLM inference profile against a mount
  pulsar run --path /mnt/storage --profile llm-inference

  # Training data loading, 32 workers, 2 minute run
  pulsar run --path /mnt/storage --profile training --workers 32 --duration 2m

  # Multi-path: benchmark multiple drives simultaneously
  pulsar run --path /mnt/nvme0 --path /mnt/nvme1 --path /mnt/nvme2 --profile training

  # Multi-node: coordinate across multiple agent nodes
  pulsar run --path /mnt/storage --profile training --nodes host1:7762 --nodes host2:7762

  # Use a custom profile YAML
  pulsar run --path /mnt/storage --profile ./my-profile.yaml

  # Output results as JSON (for the test framework)
  pulsar run --path /mnt/storage --profile checkpoint --json results.json`,

	RunE: func(cmd *cobra.Command, args []string) error {
		// Validate paths
		if len(flagPaths) == 0 {
			return fmt.Errorf("--path is required")
		}

		// Load profile
		var p *profile.Profile
		var err error
		if _, statErr := os.Stat(flagProfile); statErr == nil {
			// Treat as file path
			p, err = profile.LoadFile(flagProfile)
		} else {
			p, err = profile.LoadBuiltin(flagProfile)
		}
		if err != nil {
			return fmt.Errorf("profile %q: %w", flagProfile, err)
		}

		// CLI flags override profile values
		if cmd.Flags().Changed("workers") {
			p.Workers = flagWorkers
		}
		if cmd.Flags().Changed("duration") {
			p.Duration = flagDuration
		}
		if cmd.Flags().Changed("warmup") {
			p.Warmup = flagWarmup
		}
		if cmd.Flags().Changed("file-count") {
			p.Files.Count = flagFileCount
		}
		if cmd.Flags().Changed("file-size") {
			sz, parseErr := profile.ParseSize(flagFileSize)
			if parseErr != nil {
				return fmt.Errorf("--file-size %q: %w", flagFileSize, parseErr)
			}
			p.Files.SizeBytes = sz
		}
		if flagNoCleanup {
			p.Cleanup = false
		}
		if flagSeed != 0 {
			p.Seed = flagSeed
		}
		if cmd.Flags().Changed("compute-gap") {
			p.ComputeGapMs = flagComputeGap
		}
		if flagDirectIO {
			p.DirectIO = true
		}
		if flagNoDirectIO {
			p.DirectIO = false
		}
		if flagVerify {
			p.Verify = true
		}
		if cmd.Flags().Changed("iodepth") {
			p.IODepth = flagIODepth
		}

		// Resolve and validate all paths
		paths := make([]string, 0, len(flagPaths))
		for _, fp := range flagPaths {
			absPath, err := filepath.Abs(fp)
			if err != nil {
				return err
			}
			if _, err := os.Stat(absPath); err != nil {
				return fmt.Errorf("target path %q: %w", absPath, err)
			}
			paths = append(paths, absPath)
		}

		// Preflight checks
		if err := runPreflight(paths, p); err != nil {
			return fmt.Errorf("preflight: %w", err)
		}

		// Print run header
		if !flagQuiet {
			report.PrintHeader(p, paths)
		}

		var result *workload.Result

		// Multi-node path
		if len(flagNodes) > 0 {
			coord := &cluster.Coordinator{
				Nodes:   toNodeAddrs(flagNodes),
				Profile: p,
				Paths:   paths,
				Quiet:   flagQuiet,
			}
			result, err = coord.Run()
			if err != nil {
				return fmt.Errorf("multi-node benchmark failed: %w", err)
			}
		} else {
			// Single-node (single or multi-path)
			runner := workload.NewRunner(paths, p, flagQuiet)
			if flagSteadyState {
				runner.SetSteadyState(true)
			}
			var lp *report.LivePrinter
			if !flagQuiet {
				lp = report.NewLivePrinter(p.Duration)
				lp.Start()
			}
			result, err = runner.Run()
			if lp != nil {
				lp.Stop()
			}
			if err != nil {
				return fmt.Errorf("benchmark failed: %w", err)
			}
		}

		// Print results
		if !flagQuiet {
			report.PrintResult(result)
		}

		// CSV time-series output
		if flagOutputCSV != "" {
			report.WriteCSVResult(flagOutputCSV, result.Samples)
		}

		// JSON output for test framework consumption
		if flagOutputJSON != "" {
			data, _ := json.MarshalIndent(result, "", "  ")
			if err := os.WriteFile(flagOutputJSON, data, 0644); err != nil {
				fmt.Fprintf(os.Stderr, "warning: could not write JSON output: %v\n", err)
			}
		}

		// Exit 1 if any target was missed (so CI can catch regressions).
		// Fires regardless of --quiet so --quiet --json pipelines still
		// receive a non-zero exit code on violations.
		if result.TargetsMissed > 0 {
			os.Exit(1)
		}
		return nil
	},
}

func init() {
	runCmd.Flags().StringArrayVar(&flagPaths, "path", nil, "Target path(s) to benchmark; repeat for multiple drives (required)")
	runCmd.Flags().StringArrayVar(&flagNodes, "nodes", nil, "Agent node addresses for multi-node run, e.g. host1:7762 host2:7762")
	runCmd.Flags().StringVar(&flagProfile, "profile", "training", "Workload profile name or path to YAML file")
	runCmd.Flags().IntVar(&flagWorkers, "workers", 0, "Number of concurrent workers (overrides profile)")
	runCmd.Flags().DurationVar(&flagDuration, "duration", 0, "Benchmark duration (overrides profile, e.g. 60s, 5m)")
	runCmd.Flags().DurationVar(&flagWarmup, "warmup", 0, "Warmup duration before measurement starts")
	runCmd.Flags().StringVar(&flagFileSize, "file-size", "", "Size of each test file, e.g. 1GB, 512MB (overrides profile)")
	runCmd.Flags().IntVar(&flagFileCount, "file-count", 0, "Number of test files (overrides profile)")
	runCmd.Flags().StringVar(&flagOutputJSON, "json", "", "Write full results to this JSON file")
	runCmd.Flags().BoolVar(&flagNoCleanup, "no-cleanup", false, "Keep test files after run (useful for repeated runs)")
	runCmd.Flags().Int64Var(&flagSeed, "seed", 0, "Random seed for reproducible runs")
	runCmd.Flags().BoolVar(&flagQuiet, "quiet", false, "Suppress terminal output (use with --json)")
	runCmd.Flags().IntVar(&flagComputeGap, "compute-gap", 0, "Simulated GPU compute time between I/O ops in ms (0=disable, enables GPU stall metric)")
	runCmd.Flags().BoolVar(&flagDirectIO, "direct-io", false, "Force O_DIRECT to bypass page cache (Linux only)")
	runCmd.Flags().BoolVar(&flagNoDirectIO, "no-direct-io", false, "Disable O_DIRECT even if the profile enables it")
	runCmd.Flags().BoolVar(&flagVerify, "verify", false, "Write a deterministic pattern and verify on read (detects corruption)")
	runCmd.Flags().IntVar(&flagIODepth, "iodepth", 0, "I/O queue depth per worker (0=1, higher=more concurrent I/Os per worker)")
	runCmd.Flags().StringVar(&flagOutputCSV, "output-csv", "", "Write per-second time-series to this CSV file")
	runCmd.Flags().BoolVar(&flagSteadyState, "steady-state", false, "Run until throughput stabilizes (CV<2% for 10s) rather than fixed duration")
}

// runPreflight checks that the target path is ready before the benchmark starts.
// It verifies: path is writable and has enough free disk space.
func runPreflight(paths []string, p *profile.Profile) error {
	needed := int64(p.Files.Count) * p.Files.SizeBytes
	for _, path := range paths {
		// Check writability
		testFile := filepath.Join(path, ".pulsar-preflight-check")
		f, err := os.Create(testFile)
		if err != nil {
			return fmt.Errorf("path %q is not writable: %w", path, err)
		}
		f.Close()
		os.Remove(testFile)

		// Check free space
		free, err := freeSpaceBytes(path)
		if err == nil && free < needed+needed/10 { // +10% headroom
			return fmt.Errorf("path %q has %d GB free but benchmark needs ~%d GB",
				path, free>>30, needed>>30)
		}
	}
	return nil
}

// toNodeAddrs converts string addresses to NodeAddr, appending :7762 if no port.
func toNodeAddrs(addrs []string) []cluster.NodeAddr {
	result := make([]cluster.NodeAddr, 0, len(addrs))
	for _, addr := range addrs {
		if !strings.Contains(addr, ":") {
			addr = addr + ":7762"
		}
		result = append(result, cluster.NodeAddr(addr))
	}
	return result
}
