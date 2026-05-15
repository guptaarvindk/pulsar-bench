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
		profiles := profile.Builtin()
		fmt.Printf("\n  %-22s  %-52s  %s\n", "PROFILE", "DESCRIPTION", "FOCUS")
		fmt.Printf("  %-22s  %-52s  %s\n",
			"──────────────────────", "────────────────────────────────────────────────────", "──────────────")
		for _, p := range profiles {
			fmt.Printf("  %-22s  %-52s  %s\n", p.Name, p.Description, p.Focus)
		}
		fmt.Println()
		fmt.Println("  Run a profile:   pulsar run --path /mnt/storage --profile <name>")
		fmt.Println("  Custom profile:  pulsar run --path /mnt/storage --profile my.yaml")
		fmt.Println()
	},
}
