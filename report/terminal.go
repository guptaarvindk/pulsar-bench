// Package report formats benchmark results for human and machine consumption.
package report

import (
	"fmt"
	"strings"
	"time"

	"github.com/minio/pulsar/profile"
	"github.com/minio/pulsar/workload"
)

const (
	colReset  = "\033[0m"
	colBold   = "\033[1m"
	colGreen  = "\033[32m"
	colYellow = "\033[33m"
	colRed    = "\033[31m"
	colCyan   = "\033[36m"
	colGray   = "\033[90m"
)

func PrintHeader(p *profile.Profile, paths []string) {
	fmt.Println()
	fmt.Printf("%s%s  Pulsar AI Storage Benchmark%s\n", colBold, colCyan, colReset)
	fmt.Printf("%s%s%s\n", colGray, strings.Repeat("─", 60), colReset)
	fmt.Printf("  Profile   : %s%s%s  —  %s\n", colBold, p.Name, colReset, p.Description)
	if len(paths) == 1 {
		fmt.Printf("  Path      : %s\n", paths[0])
	} else {
		fmt.Printf("  Paths     : %s\n", paths[0])
		for _, pth := range paths[1:] {
			fmt.Printf("              %s\n", pth)
		}
	}
	fmt.Printf("  Workers   : %d\n", p.Workers)
	fmt.Printf("  Duration  : %s\n", p.Duration.Round(time.Second))
	fmt.Printf("  Files     : %d × %s  (total %s)\n",
		p.Files.Count, humanBytes(p.Files.SizeBytes),
		humanBytes(p.Files.SizeBytes*int64(p.Files.Count)))
	fmt.Printf("  Block I/O : %s\n", humanBytes(p.BlockSize))
	dioLabel := colGray + "no (page cache active)" + colReset
	if p.DirectIO {
		dioLabel = colGreen + "yes (O_DIRECT — page cache bypassed)" + colReset
	}
	fmt.Printf("  Direct I/O: %s\n", dioLabel)
	if p.ComputeGapMs > 0 {
		fmt.Printf("  Compute gap: %dms per batch (GPU simulation)\n", p.ComputeGapMs)
	}
	fmt.Printf("%s%s%s\n", colGray, strings.Repeat("─", 60), colReset)
	fmt.Println()
}

func PrintResult(r *workload.Result) {
	fmt.Printf("\n%s%s  Results%s\n", colBold, colCyan, colReset)
	fmt.Printf("%s%s%s\n", colGray, strings.Repeat("─", 60), colReset)

	// ── Throughput ──────────────────────────────────────────────────
	if r.Throughput.BytesRead > 0 || r.Throughput.BytesWritten > 0 {
		fmt.Printf("\n  %sThroughput%s\n", colBold, colReset)
		if r.Throughput.BytesRead > 0 {
			fmt.Printf("    Read   %s  (%s  %s ops/s)\n",
				colorGBps(r.Throughput.ReadGBps, r.Targets.ReadGBps),
				humanBytes(r.Throughput.BytesRead),
				humanNum(r.Throughput.ReadIOPS),
			)
		}
		if r.Throughput.BytesWritten > 0 {
			fmt.Printf("    Write  %s  (%s  %s ops/s)\n",
				colorGBps(r.Throughput.WriteGBps, r.Targets.WriteGBps),
				humanBytes(r.Throughput.BytesWritten),
				humanNum(r.Throughput.WriteIOPS),
			)
		}
	}

	// ── TTFB ────────────────────────────────────────────────────────
	if r.TTFB.Count > 0 {
		fmt.Printf("\n  %sTime-to-First-Byte (TTFB)%s        n=%d ops\n",
			colBold, colReset, r.TTFB.Count)
		printLatencyTable(r.TTFB.MinMs, r.TTFB.P50Ms, r.TTFB.P95Ms, r.TTFB.P99Ms, r.TTFB.MaxMs, r.Targets.TTFBColdP99Ms)
	}

	// ── Op latency ──────────────────────────────────────────────────
	if r.OpLatency.Count > 0 {
		fmt.Printf("\n  %sI/O Operation Latency%s             n=%d ops\n",
			colBold, colReset, r.OpLatency.Count)
		printLatencyTable(r.OpLatency.MinMs, r.OpLatency.P50Ms, r.OpLatency.P95Ms, r.OpLatency.P99Ms, r.OpLatency.MaxMs, 0)
	}

	// ── Metadata ────────────────────────────────────────────────────
	if r.Metadata != nil {
		m := r.Metadata
		fmt.Printf("\n  %sMetadata%s\n", colBold, colReset)
		fmt.Printf("    stat()    p99 %s   (%d ops)\n",
			colorMs(m.StatP99Ms, r.Targets.StatP99Ms), m.StatOps)
		fmt.Printf("    readdir() p99 %s   (%d ops)\n",
			colorMs(m.ReaddirP99Ms, r.Targets.ReaddirP99Ms), m.ReaddirOps)
		if m.HitRatePct > 0 {
			fmt.Printf("    cache hit rate  %s%.1f%%%s (inferred)\n",
				hitRateColor(m.HitRatePct, r.Targets.MetaHitRatePct), m.HitRatePct, colReset)
		}
	}

	// ── Multi-epoch breakdown ────────────────────────────────────────
	if len(r.Epochs) > 0 {
		fmt.Printf("\n  %sEpoch Breakdown%s\n", colBold, colReset)
		fmt.Printf("    %-8s  %-12s  %-12s  %-12s\n", "Epoch", "Read GB/s", "TTFB p50", "TTFB p99")
		fmt.Printf("    %s\n", strings.Repeat("─", 50))
		for _, e := range r.Epochs {
			label := fmt.Sprintf("Epoch %d", e.Epoch)
			warmLabel := ""
			if e.Epoch == 1 {
				warmLabel = colGray + " (cold)" + colReset
			} else {
				warmLabel = colGreen + " (warm)" + colReset
			}
			fmt.Printf("    %-8s  %-12s  %-12s  %-12s%s\n",
				label,
				fmt.Sprintf("%.2f", e.Throughput.ReadGBps),
				fmt.Sprintf("%.1fms", e.TTFB.P50Ms),
				fmt.Sprintf("%.1fms", e.TTFB.P99Ms),
				warmLabel,
			)
		}
	}

	// ── GPU stall fraction ──────────────────────────────────────────
	if r.GPUStallPct > 0 {
		fmt.Printf("\n  %sGPU Stall Fraction%s  (I/O time ÷ (I/O + compute))\n", colBold, colReset)
		stallColor := colGreen
		verdict := "storage keeps up — not the bottleneck"
		if r.GPUStallPct > 30 {
			stallColor = colRed
			verdict = "significant training bottleneck"
		} else if r.GPUStallPct > 10 {
			stallColor = colYellow
			verdict = "measurable latency added to training"
		}
		fmt.Printf("    %s%.1f%%%s  — %s\n", stallColor, r.GPUStallPct, colReset, verdict)
	}

	// ── Accelerator stats ──────────────────────────────────────────
	if r.Accelerator != nil {
		a := r.Accelerator
		fmt.Printf("\n  %sAccelerator Metrics%s  (%d GPU/TPU)\n", colBold, colReset, a.NumAccelerators)
		fmt.Printf("    Samples/sec  %.1f\n", a.SamplesPerSec)
	}

	// ── Per-path breakdown ──────────────────────────────────────────
	if len(r.PerPath) > 1 {
		PrintPerPath(r.PerPath)
	}

	// ── Per-node breakdown ──────────────────────────────────────────
	if len(r.PerNode) > 1 {
		printPerNode(r.PerNode)
	}

	// ── Target violations ───────────────────────────────────────────
	fmt.Printf("\n  %sTarget Check%s\n", colBold, colReset)
	if len(r.Violations) == 0 {
		fmt.Printf("    %s✓ All targets met%s\n", colGreen, colReset)
	} else {
		for _, v := range r.Violations {
			fmt.Printf("    %s✗ %s%s\n", colRed, v, colReset)
		}
	}

	// ── Summary line ────────────────────────────────────────────────
	fmt.Printf("\n%s%s%s\n", colGray, strings.Repeat("─", 60), colReset)
	status := fmt.Sprintf("%s  PASS%s", colGreen, colReset)
	if r.TargetsMissed > 0 {
		status = fmt.Sprintf("%s  FAIL  (%d target(s) missed)%s", colRed, r.TargetsMissed, colReset)
	}
	fmt.Printf("  %s%s%s  profile=%s  duration=%.0fs  workers=%d\n%s\n",
		colBold, status, colReset,
		r.Profile, r.DurationS, r.Workers,
		strings.Repeat("─", 60),
	)
	fmt.Println()
}

// PrintPerPath prints a per-path breakdown table.
// Only called when len(results) > 1.
func PrintPerPath(results []workload.PathResult) {
	fmt.Printf("\n  %sPer-Path Results%s\n", colBold, colReset)
	fmt.Printf("    %-30s  %-12s  %-12s  %-12s\n", "PATH", "READ", "WRITE", "TTFB p99")
	fmt.Printf("    %s\n", strings.Repeat("─", 72))

	// Find max read throughput for slow-path detection
	maxRead := 0.0
	for _, pr := range results {
		if pr.Throughput.ReadGBps > maxRead {
			maxRead = pr.Throughput.ReadGBps
		}
	}

	var totalRead, totalWrite float64
	for _, pr := range results {
		isSlow := maxRead > 0 && pr.Throughput.ReadGBps < maxRead*0.5
		readStr := "—"
		if pr.Throughput.BytesRead > 0 {
			readStr = fmt.Sprintf("%.2f GB/s", pr.Throughput.ReadGBps)
		}
		writeStr := "—"
		if pr.Throughput.BytesWritten > 0 {
			writeStr = fmt.Sprintf("%.2f GB/s", pr.Throughput.WriteGBps)
		}
		ttfbStr := "—"
		if pr.TTFB.Count > 0 {
			ttfbStr = fmtMs(pr.TTFB.P99Ms)
		}

		slowTag := ""
		if isSlow {
			slowTag = colRed + "  ← slow" + colReset
		}

		// Shorten path if too long
		displayPath := pr.Path
		if len(displayPath) > 28 {
			displayPath = "…" + displayPath[len(displayPath)-27:]
		}

		if isSlow {
			fmt.Printf("    %s%-30s%s  %-12s  %-12s  %-12s%s\n",
				colRed, displayPath, colReset, readStr, writeStr, ttfbStr, slowTag)
		} else {
			fmt.Printf("    %-30s  %-12s  %-12s  %-12s\n",
				displayPath, readStr, writeStr, ttfbStr)
		}

		totalRead += pr.Throughput.ReadGBps
		totalWrite += pr.Throughput.WriteGBps
	}

	fmt.Printf("    %s\n", strings.Repeat("─", 72))

	totalReadStr := "—"
	if totalRead > 0 {
		totalReadStr = fmt.Sprintf("%.2f GB/s", totalRead)
	}
	totalWriteStr := "—"
	if totalWrite > 0 {
		totalWriteStr = fmt.Sprintf("%.2f GB/s", totalWrite)
	}
	fmt.Printf("    %-30s  %-12s  %-12s\n", "TOTAL", totalReadStr, totalWriteStr)
}

func printPerNode(results []workload.NodeResult) {
	fmt.Printf("\n  %sPer-Node Results%s\n", colBold, colReset)
	fmt.Printf("    %-30s  %-12s  %-12s  %-12s\n", "NODE", "READ", "WRITE", "TTFB p99")
	fmt.Printf("    %s\n", strings.Repeat("─", 72))

	var totalRead, totalWrite float64
	for _, nr := range results {
		readStr := "—"
		if nr.Throughput.BytesRead > 0 {
			readStr = fmt.Sprintf("%.2f GB/s", nr.Throughput.ReadGBps)
		}
		writeStr := "—"
		if nr.Throughput.BytesWritten > 0 {
			writeStr = fmt.Sprintf("%.2f GB/s", nr.Throughput.WriteGBps)
		}
		ttfbStr := "—"
		if nr.TTFB.Count > 0 {
			ttfbStr = fmtMs(nr.TTFB.P99Ms)
		}
		fmt.Printf("    %-30s  %-12s  %-12s  %-12s\n",
			nr.Node, readStr, writeStr, ttfbStr)
		totalRead += nr.Throughput.ReadGBps
		totalWrite += nr.Throughput.WriteGBps
	}

	fmt.Printf("    %s\n", strings.Repeat("─", 72))
	totalReadStr := "—"
	if totalRead > 0 {
		totalReadStr = fmt.Sprintf("%.2f GB/s", totalRead)
	}
	totalWriteStr := "—"
	if totalWrite > 0 {
		totalWriteStr = fmt.Sprintf("%.2f GB/s", totalWrite)
	}
	fmt.Printf("    %-30s  %-12s  %-12s\n", "TOTAL", totalReadStr, totalWriteStr)
}

// ------------------------------------------------------------------ helpers

func printLatencyTable(min, p50, p95, p99, max, target float64) {
	fmt.Printf("    %-10s %-10s %-10s %-10s %-10s\n", "min", "p50", "p95", "p99", "max")
	fmt.Printf("    %-10s %-10s %-10s %-10s %-10s\n",
		fmtMs(min),
		fmtMs(p50),
		fmtMs(p95),
		colorMs(p99, target),
		fmtMs(max),
	)
}

func fmtMs(ms float64) string {
	if ms < 1 {
		return fmt.Sprintf("%.2fms", ms)
	}
	if ms < 1000 {
		return fmt.Sprintf("%.1fms", ms)
	}
	return fmt.Sprintf("%.2fs", ms/1000)
}

func colorMs(ms, target float64) string {
	s := fmtMs(ms)
	if target == 0 {
		return s
	}
	if ms <= target {
		return colGreen + s + colReset
	}
	if ms <= target*1.5 {
		return colYellow + s + colReset
	}
	return colRed + s + colReset
}

func colorGBps(gbps, target float64) string {
	s := fmt.Sprintf("%.2f GB/s", gbps)
	if target == 0 {
		return s
	}
	if gbps >= target {
		return colGreen + s + colReset
	}
	if gbps >= target*0.8 {
		return colYellow + s + colReset
	}
	return colRed + s + colReset
}

func hitRateColor(rate, target float64) string {
	if target == 0 || rate >= target {
		return colGreen
	}
	return colRed
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

func humanNum(f float64) string {
	if f >= 1e6 {
		return fmt.Sprintf("%.1fM", f/1e6)
	}
	if f >= 1e3 {
		return fmt.Sprintf("%.1fK", f/1e3)
	}
	return fmt.Sprintf("%.0f", f)
}
