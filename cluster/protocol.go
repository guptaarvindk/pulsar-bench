// Package cluster implements the multi-node coordinator/agent protocol.
package cluster

import (
	"github.com/minio/pulsar/measure"
	"github.com/minio/pulsar/profile"
	"github.com/minio/pulsar/workload"
)

// AgentConfig is sent from coordinator to agent to configure a run.
type AgentConfig struct {
	Profile profile.Profile `json:"profile"`
	Paths   []string        `json:"paths"`
	NodeIdx int             `json:"node_idx"`    // 0-based index of this node
	Total   int             `json:"total_nodes"` // total node count
}

// StreamMsg is one line of the NDJSON stream from agent to coordinator.
// Exactly one of Sample or Result is non-nil per message.
type StreamMsg struct {
	Sample *measure.MetricSample `json:"sample,omitempty"`
	Result *workload.Result      `json:"result,omitempty"`
	Error  string                `json:"error,omitempty"`
}

// TimeReply is returned by GET /api/time
type TimeReply struct {
	UnixNano int64 `json:"unix_nano"`
}

// StartRequest is sent to POST /api/start
type StartRequest struct {
	AtUnixNano int64 `json:"at_unix_nano"` // absolute start time
}
