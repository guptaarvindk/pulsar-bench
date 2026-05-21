package cmd

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/minio/pulsar/report"
	"github.com/spf13/cobra"
)

var (
	flagSweepOutput string
	flagSweepTitle  string
	flagSweepLabels []string
)

var sweepCmd = &cobra.Command{
	Use:   "sweep <result1.json> [result2.json ...]",
	Short: "Render a multi-result sweep chart to HTML",
	Long: `sweep loads two or more benchmark result JSON files and renders a single
HTML page with charts comparing TTFB, throughput, and op latency across
all results.

Typical use: run pulsar at several --block-size values, then visualise:

  pulsar run --path /mnt/nvme --profile llm-inference --block-size 4KB  --json bs_4k.json
  pulsar run --path /mnt/nvme --profile llm-inference --block-size 64KB --json bs_64k.json
  pulsar run --path /mnt/nvme --profile llm-inference --block-size 1MB  --json bs_1m.json
  pulsar sweep bs_4k.json bs_64k.json bs_1m.json --output sweep.html

If block_size_bytes is present in the result JSON the label defaults to a
human-readable size (e.g. "64 KiB"); otherwise the file stem is used.
Override any label with --label (repeat once per file, in order).`,

	Example: `  # Block-size sweep, auto-labels from result metadata
  pulsar sweep bs_4k.json bs_64k.json bs_1m.json --output sweep.html

  # Custom labels and title
  pulsar sweep run1.json run2.json run3.json \
    --label "4 KiB" --label "64 KiB" --label "1 MiB" \
    --title "NVMe block-size sweep" --output sweep.html`,

	Args: cobra.MinimumNArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(flagSweepLabels) > 0 && len(flagSweepLabels) != len(args) {
			return fmt.Errorf("--label count (%d) must match number of input files (%d)",
				len(flagSweepLabels), len(args))
		}

		points := make([]report.SweepPoint, 0, len(args))
		for i, path := range args {
			label := ""
			if i < len(flagSweepLabels) {
				label = flagSweepLabels[i]
			}
			pt, err := report.LoadSweepPoint(path, label)
			if err != nil {
				return fmt.Errorf("loading %s: %w", path, err)
			}
			points = append(points, pt)
		}

		title := flagSweepTitle
		if title == "" {
			// Build a default title from common stem or profile of first point
			if points[0].Profile != "" {
				title = fmt.Sprintf("Block-size sweep — %s profile", points[0].Profile)
			} else {
				title = "Block-size sweep"
			}
		}

		outPath := flagSweepOutput
		if outPath == "" {
			// derive from first input file
			stem := filepath.Base(args[0])
			stem = strings.TrimSuffix(stem, filepath.Ext(stem))
			outPath = stem + "_sweep.html"
		}

		if err := report.WriteSweepHTML(outPath, title, points); err != nil {
			return fmt.Errorf("writing %s: %w", outPath, err)
		}

		fmt.Printf("sweep chart written to %s (%d data points)\n", outPath, len(points))
		return nil
	},
}

func init() {
	sweepCmd.Flags().StringVarP(&flagSweepOutput, "output", "o", "", "Output HTML file (default: <first-input-stem>_sweep.html)")
	sweepCmd.Flags().StringVar(&flagSweepTitle, "title", "", "Chart title (default: auto-generated from profile)")
	sweepCmd.Flags().StringArrayVar(&flagSweepLabels, "label", nil, "Override label for each input file (repeat once per file, in order)")
}
