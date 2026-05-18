package cmd

import (
	"fmt"

	"github.com/minio/pulsar/profile"
	"github.com/spf13/cobra"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List available workload profiles",
	Run: func(cmd *cobra.Command, args []string) {
		out := cmd.OutOrStdout()
		profiles := profile.Builtin()
		fmt.Fprintf(out, "\n  %-22s  %-52s  %s\n", "PROFILE", "DESCRIPTION", "FOCUS")
		fmt.Fprintf(out, "  %-22s  %-52s  %s\n",
			"──────────────────────", "────────────────────────────────────────────────────", "──────────────")
		for _, p := range profiles {
			fmt.Fprintf(out, "  %-22s  %-52s  %s\n", p.Name, p.Description, p.Focus)
		}
		fmt.Fprintln(out)
		fmt.Fprintln(out, "  Run a profile:   pulsar run --path /mnt/storage --profile <name>")
		fmt.Fprintln(out, "  Custom profile:  pulsar run --path /mnt/storage --profile my.yaml")
		fmt.Fprintln(out)
	},
}
