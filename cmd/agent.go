package cmd

import (
	"fmt"
	"net/http"

	"github.com/minio/pulsar/cluster"
	"github.com/spf13/cobra"
)

var flagAgentPort int

var agentCmd = &cobra.Command{
	Use:   "agent",
	Short: "Run as a benchmark agent (worker node for multi-node runs)",
	Example: `  # Start agent on default port 7762
  pulsar agent

  # Start on custom port
  pulsar agent --port 8080`,
	RunE: func(cmd *cobra.Command, args []string) error {
		agent := cluster.NewAgent()
		addr := fmt.Sprintf(":%d", flagAgentPort)
		fmt.Printf("  Pulsar agent listening on %s\n", addr)
		fmt.Printf("  Waiting for coordinator...\n")
		return http.ListenAndServe(addr, agent.Handler())
	},
}

func init() {
	agentCmd.Flags().IntVar(&flagAgentPort, "port", 7762, "Port to listen on")
}
