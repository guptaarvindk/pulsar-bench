package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "pulsar",
	Short: "Pulsar — AI storage benchmark",
	Long: `Pulsar generates realistic AI workload I/O patterns against any storage path.

It measures what matters for AI systems: time-to-first-byte, sustained
throughput under concurrency, metadata performance, and how the storage
behaves when multiple workers hammer it simultaneously.

Works against any path — local disk, NFS, FUSE mounts, object store mounts.
No agents, no daemons, no configuration required beyond --path and --profile.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(versionCmd)
	rootCmd.AddCommand(reportCmd)
	rootCmd.AddCommand(agentCmd)
	rootCmd.AddCommand(compareCmd)
	rootCmd.AddCommand(sweepCmd)
}

var buildVersion = "dev"

// SetVersion is called from main() with the version injected by ldflags.
func SetVersion(v string) {
	buildVersion = v
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("pulsar %s\n", buildVersion)
	},
}
