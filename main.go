// Pulsar — AI Storage Benchmark
//
// A single-binary, dependency-free benchmark that generates realistic AI
// workload I/O patterns against any path: local disk, NFS, FUSE mounts,
// network-attached storage — anything that looks like a directory.
//
// Usage:
//
//	pulsar run --path /mnt/storage --profile training
//	pulsar run --path /data --profile llm-inference --workers 32 --duration 60s
//	pulsar list
//
// Build:
//
//	CGO_ENABLED=0 go build -o pulsar .
package main

import "github.com/minio/pulsar/cmd"

func main() {
	cmd.Execute()
}
