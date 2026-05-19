package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/minio/pulsar/workload"
	"github.com/spf13/cobra"
)

const (
	cmpReset = "\033[0m"
	cmpBold  = "\033[1m"
	cmpGreen = "\033[32m"
	cmpRed   = "\033[31m"
	cmpGray  = "\033[90m"
)

var compareCmd = &cobra.Command{
	Use:     "compare <before.json> <after.json>",
	Short:   "Compare two benchmark result JSON files",
	Example: `  pulsar compare baseline.json candidate.json`,
	Args:    cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		before, err := loadResult(args[0])
		if err != nil {
			return fmt.Errorf("loading %s: %w", args[0], err)
		}
		after, err := loadResult(args[1])
		if err != nil {
			return fmt.Errorf("loading %s: %w", args[1], err)
		}
		printComparison(before, after, args[0], args[1])
		return nil
	},
}

func loadResult(path string) (*workload.Result, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r workload.Result
	if err := json.Unmarshal(data, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func printComparison(before, after *workload.Result, beforeFile, afterFile string) {
	out := os.Stdout

	fmt.Fprintf(out, "\n  Pulsar Benchmark Comparison\n")
	fmt.Fprintf(out, "  %-26s  %-14s  %-14s  %-10s\n", "METRIC", "BEFORE", "AFTER", "DELTA")
	fmt.Fprintf(out, "  %s\n", rep("─", 70))

	// helper: format a throughput delta (higher = better)
	cmpHigher := func(label string, b, a float64, unit string) {
		if b == 0 && a == 0 {
			return
		}
		bStr := fmt.Sprintf("%.3f %s", b, unit)
		aStr := fmt.Sprintf("%.3f %s", a, unit)
		delta, color := deltaHigher(b, a)
		fmt.Fprintf(out, "  %-26s  %-14s  %-14s  %s%s%s\n",
			label, bStr, aStr, color, delta, cmpReset)
	}
	// helper: format a latency delta (lower = better)
	cmpLower := func(label string, b, a float64, unit string) {
		if b == 0 && a == 0 {
			return
		}
		bStr := fmt.Sprintf("%.2f %s", b, unit)
		aStr := fmt.Sprintf("%.2f %s", a, unit)
		delta, color := deltaLower(b, a)
		fmt.Fprintf(out, "  %-26s  %-14s  %-14s  %s%s%s\n",
			label, bStr, aStr, color, delta, cmpReset)
	}

	fmt.Fprintf(out, "\n  Before: %s  |  Profile: %s  Workers: %d  Duration: %.0fs\n",
		beforeFile, before.Profile, before.Workers, before.DurationS)
	fmt.Fprintf(out, "  After : %s  |  Profile: %s  Workers: %d  Duration: %.0fs\n\n",
		afterFile, after.Profile, after.Workers, after.DurationS)

	fmt.Fprintf(out, "  %sThroughput%s\n", cmpBold, cmpReset)
	cmpHigher("  Read GB/s", before.Throughput.ReadGBps, after.Throughput.ReadGBps, "GB/s")
	cmpHigher("  Write GB/s", before.Throughput.WriteGBps, after.Throughput.WriteGBps, "GB/s")
	cmpHigher("  Read IOPS", before.Throughput.ReadIOPS, after.Throughput.ReadIOPS, "iops")
	cmpHigher("  Write IOPS", before.Throughput.WriteIOPS, after.Throughput.WriteIOPS, "iops")

	fmt.Fprintf(out, "\n  %sTTFB%s\n", cmpBold, cmpReset)
	cmpLower("  TTFB p50", before.TTFB.P50Ms, after.TTFB.P50Ms, "ms")
	cmpLower("  TTFB p99", before.TTFB.P99Ms, after.TTFB.P99Ms, "ms")

	if before.OpLatency.Count > 0 || after.OpLatency.Count > 0 {
		fmt.Fprintf(out, "\n  %sOp Latency%s\n", cmpBold, cmpReset)
		cmpLower("  Op p50", before.OpLatency.P50Ms, after.OpLatency.P50Ms, "ms")
		cmpLower("  Op p99", before.OpLatency.P99Ms, after.OpLatency.P99Ms, "ms")
	}

	if before.Metadata != nil || after.Metadata != nil {
		fmt.Fprintf(out, "\n  %sMetadata%s\n", cmpBold, cmpReset)
		bMeta := before.Metadata
		aMeta := after.Metadata
		if bMeta == nil {
			bMeta = &workload.MetadataStats{}
		}
		if aMeta == nil {
			aMeta = &workload.MetadataStats{}
		}
		cmpLower("  stat() p99", bMeta.StatP99Ms, aMeta.StatP99Ms, "ms")
		cmpLower("  readdir() p99", bMeta.ReaddirP99Ms, aMeta.ReaddirP99Ms, "ms")
		cmpHigher("  cache hit rate", bMeta.HitRatePct, aMeta.HitRatePct, "%")
	}

	if before.GPUStallPct > 0 || after.GPUStallPct > 0 {
		fmt.Fprintf(out, "\n  %sGPU Stall%s\n", cmpBold, cmpReset)
		cmpLower("  GPU stall pct", before.GPUStallPct, after.GPUStallPct, "%")
	}

	// Pass/fail summary
	fmt.Fprintf(out, "\n  %s\n", rep("─", 70))
	bPass := "PASS"
	if before.TargetsMissed > 0 {
		bPass = fmt.Sprintf("FAIL (%d violations)", before.TargetsMissed)
	}
	aPass := "PASS"
	if after.TargetsMissed > 0 {
		aPass = fmt.Sprintf("FAIL (%d violations)", after.TargetsMissed)
	}
	fmt.Fprintf(out, "  Before: %s  |  After: %s\n\n", bPass, aPass)
}

// deltaHigher: higher is better (e.g. throughput). Returns formatted delta string + color.
func deltaHigher(before, after float64) (string, string) {
	if before == 0 {
		return "n/a", cmpGray
	}
	pct := (after - before) / before * 100
	sign := "+"
	if pct < 0 {
		sign = ""
	}
	delta := fmt.Sprintf("%s%.1f%%", sign, pct)
	if pct >= 5 {
		return delta, cmpGreen
	}
	if pct <= -5 {
		return delta, cmpRed
	}
	return delta, cmpGray
}

// deltaLower: lower is better (e.g. latency). Returns formatted delta string + color.
func deltaLower(before, after float64) (string, string) {
	if before == 0 {
		return "n/a", cmpGray
	}
	pct := (after - before) / before * 100
	sign := "+"
	if pct < 0 {
		sign = ""
	}
	delta := fmt.Sprintf("%s%.1f%%", sign, pct)
	// For latency, improvement = lower = negative pct
	if pct <= -5 {
		return delta, cmpGreen
	}
	if pct >= 5 {
		return delta, cmpRed
	}
	return delta, cmpGray
}

func rep(s string, n int) string {
	result := ""
	for i := 0; i < n; i++ {
		result += s
	}
	return result
}
