package profile

import "time"

// Builtin returns all built-in AI workload profiles in display order.
// Each profile is a distinct I/O pattern representing a real AI use case.
func Builtin() []Profile {
	return []Profile{
		llmInference(),
		training(),
		multiEpoch(),
		checkpoint(),
		agentWorkspace(),
		metadata(),
		thrash(),
		mixed(),
	}
}

// llmInference — loading large model weights for inference.
// Access pattern: large sequential reads, same files hit repeatedly,
// TTFB critical (time between request and first byte of response).
// ComputeGapMs=100ms models the time GPU spends on a token-generation batch
// before the next layer's weights are needed.
func llmInference() Profile {
	return Profile{
		Name:         "llm-inference",
		Description:  "LLM model weight loading — large files, repeated access",
		Focus:        "TTFB + Throughput",
		Workload:     "sequential-read",
		Workers:      16,
		Duration:     60 * time.Second,
		Warmup:       10 * time.Second,
		Files:        FilesConfig{Count: 8, SizeBytes: 10 << 30}, // 8 × 10 GB = 80 GB
		BlockSize:    4 * 1024 * 1024,                             // 4 MiB — large model reads
		DirectIO:     true,                                         // bypass page cache on Linux
		Reuse:        true,                                         // same shards hit on every inference
		ComputeGapMs: 100,                                          // 100ms ≈ GPU time per token batch
		Cleanup:      true,
		Targets: TargetConfig{
			TTFBColdP99Ms: 500,
			TTFBWarmP99Ms: 50,
			ReadGBps:      1.0,
		},
	}
}

// training — sequential shard reads across a full dataset.
// Pattern: each worker reads a different shard sequentially.
// Cache thrashing intentionally avoided — working set fits in cache.
// ComputeGapMs=50ms models a short GPU forward-pass per shard batch.
func training() Profile {
	return Profile{
		Name:         "training",
		Description:  "Training data loading — sequential shards, many workers",
		Focus:        "Throughput",
		Workload:     "sequential-read",
		Workers:      32,
		Duration:     60 * time.Second,
		Warmup:       10 * time.Second,
		Files:        FilesConfig{Count: 32, SizeBytes: 1 << 30}, // 32 × 1 GB = 32 GB
		BlockSize:    256 * 1024,                                  // 256 KiB — standard shard read
		DirectIO:     true,                                         // bypass page cache on Linux
		Reuse:        false,                                        // each shard read once per epoch
		ComputeGapMs: 50,                                           // 50ms ≈ GPU forward pass per batch
		Cleanup:      true,
		Targets: TargetConfig{
			ReadGBps:      1.0,
			TTFBColdP99Ms: 500,
		},
	}
}

// multiEpoch — same dataset read multiple times.
// Epoch 1: cold (cache miss). Epoch 2+: warm (cache hit).
// Measures how well the storage layer learns access patterns.
// ComputeGapMs=75ms models GPU processing time between dataset passes.
func multiEpoch() Profile {
	return Profile{
		Name:         "multi-epoch",
		Description:  "Multi-epoch training — measures cold vs warm cache performance",
		Focus:        "Cache Warmup",
		Workload:     "multi-epoch",
		Workers:      16,
		Duration:     120 * time.Second,
		Warmup:       0,
		Epochs:       3,
		Files:        FilesConfig{Count: 16, SizeBytes: 1 << 30}, // 16 GB total
		BlockSize:    256 * 1024,
		DirectIO:     true,                                         // bypass page cache on Linux
		Reuse:        true,
		ComputeGapMs: 75,                                           // 75ms ≈ GPU time between epoch passes
		Cleanup:      true,
		Targets: TargetConfig{
			TTFBColdP99Ms: 1000,
			TTFBWarmP99Ms: 50,
			ReadGBps:      1.0,
		},
	}
}

// checkpoint — writing large model checkpoints then reading them back.
// Pattern: large sequential write + fsync + sequential read for restore test.
// Write throughput and fsync latency are the key metrics.
func checkpoint() Profile {
	return Profile{
		Name:        "checkpoint",
		Description: "Model checkpoint save/restore — large write + fsync + read-back",
		Focus:       "Write Throughput",
		Workload:    "write",
		Workers:     8,
		Duration:    60 * time.Second,
		Warmup:      5 * time.Second,
		Files:        FilesConfig{Count: 4, SizeBytes: 10 << 30}, // 4 × 10 GB
		BlockSize:   4 * 1024 * 1024,                             // 4 MiB writes
		FsyncOnWrite: true,
		ReadPct:     30, // read-back after write (simulates restore verification)
		WritePct:    70,
		Cleanup:     true,
		Targets: TargetConfig{
			WriteGBps: 1.0,
		},
	}
}

// agentWorkspace — AI coding agent file operations.
// Pattern: many small files, heavy metadata ops, mixed reads and
// in-place writes (edit → rename). Models what Claude Code / Cursor do.
func agentWorkspace() Profile {
	return Profile{
		Name:        "agent-workspace",
		Description: "AI agent workspace — small files, metadata-heavy, mixed read/write",
		Focus:       "IOPS + Latency",
		Workload:    "agent-workspace",
		Workers:     16,
		Duration:    60 * time.Second,
		Warmup:      5 * time.Second,
		Files:        FilesConfig{Count: 1000, SizeBytes: 256 * 1024}, // 1000 × 256 KB
		BlockSize:   4 * 1024,                                          // 4 KiB — small edits
		ReadPct:     70,
		WritePct:    30,
		FsyncOnWrite: true,
		Reuse:       true,
		Cleanup:     true,
		Targets: TargetConfig{
			StatP99Ms: 5,
		},
	}
}

// metadata — mass stat() and readdir() operations.
// Pattern: all workers race to enumerate a large directory, then
// hammer stat() calls concurrently.
// Tests metadata cache effectiveness and cold enumeration cost.
func metadata() Profile {
	return Profile{
		Name:        "metadata",
		Description: "Dataset discovery — mass stat() and readdir() at scale",
		Focus:       "Metadata",
		Workload:    "metadata",
		Workers:     32,
		Duration:    30 * time.Second,
		Warmup:      5 * time.Second,
		Files:        FilesConfig{Count: 10000, SizeBytes: 1}, // 10K zero-byte entries
		BlockSize:   4096,
		Reuse:       true,
		Cleanup:     true,
		Targets: TargetConfig{
			StatP99Ms:      5,
			ReaddirP99Ms:   1000,
			MetaHitRatePct: 95,
		},
	}
}

// thrash — working set intentionally larger than any reasonable local cache.
// Every read is a cold miss. Shows raw backend throughput and cold-path latency.
// This is the "worst case" that reveals the floor performance.
// Direct I/O ensures OS page cache cannot mask backend latency.
func thrash() Profile {
	return Profile{
		Name:        "thrash",
		Description: "Cache thrash — working set larger than cache, measures cold-path floor",
		Focus:       "Cold-path",
		Workload:    "random-read",
		Workers:     32,
		Duration:    60 * time.Second,
		Warmup:      0,
		Files:       FilesConfig{Count: 128, SizeBytes: 1 << 30}, // 128 GB — designed to exceed cache
		BlockSize:   256 * 1024,
		DirectIO:    true,  // essential: without O_DIRECT OS page cache hides cold misses
		Reuse:       false, // random file, random offset every read → maximum eviction pressure
		Cleanup:     true,
		Targets: TargetConfig{
			TTFBColdP99Ms: 2000,
		},
	}
}

// mixed — configurable mix of reads and writes from concurrent workers.
// Default 70/30 read/write. General-purpose stress test.
func mixed() Profile {
	return Profile{
		Name:        "mixed",
		Description: "Mixed read/write — 70% read, 30% write, all workers concurrent",
		Focus:       "Mixed",
		Workload:    "mixed",
		Workers:     32,
		Duration:    60 * time.Second,
		Warmup:      10 * time.Second,
		Files:        FilesConfig{Count: 32, SizeBytes: 512 * 1024 * 1024}, // 32 × 512 MB
		BlockSize:   256 * 1024,
		ReadPct:     70,
		WritePct:    30,
		FsyncOnWrite: false,
		Cleanup:     true,
	}
}
