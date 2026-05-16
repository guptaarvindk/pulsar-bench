package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/minio/pulsar/report"
	"github.com/minio/pulsar/workload"
	"github.com/spf13/cobra"
)

var (
	flagReportOutput string
	flagReportTitle  string
)

var reportCmd = &cobra.Command{
	Use:   "report <results.json>",
	Short: "Generate an HTML report from a JSON results file",
	Args:  cobra.ExactArgs(1),
	Example: `  # Generate HTML report from a benchmark result
  pulsar report results.json --output report.html

  # Run benchmark and immediately generate report
  pulsar run --path /mnt/storage --profile training --json results.json
  pulsar report results.json`,

	RunE: func(cmd *cobra.Command, args []string) error {
		data, err := os.ReadFile(args[0])
		if err != nil {
			return fmt.Errorf("reading %s: %w", args[0], err)
		}
		var result workload.Result
		if err := json.Unmarshal(data, &result); err != nil {
			return fmt.Errorf("parsing JSON: %w", err)
		}

		outPath := flagReportOutput
		if outPath == "" {
			base := strings.TrimSuffix(args[0], ".json")
			outPath = base + ".html"
		}

		title := flagReportTitle
		if title == "" {
			title = fmt.Sprintf("Pulsar — %s @ %s", result.Profile, result.Path)
		}

		if err := report.WriteHTML(outPath, title, &result); err != nil {
			return fmt.Errorf("writing HTML: %w", err)
		}
		fmt.Printf("  ✓ Report written to %s\n", outPath)
		return nil
	},
}

func init() {
	reportCmd.Flags().StringVar(&flagReportOutput, "output", "", "Output HTML file path (default: <input>.html)")
	reportCmd.Flags().StringVar(&flagReportTitle, "title", "", "Report title (default: profile@path)")
}
