# Pulsar — Project Context

> This file captures the full architectural and design context for the Pulsar AI storage benchmark. It is intended for AI assistants and new contributors to get up to speed without reading every source file.

## Identity

| Field | Value |
|-------|-------|
| Module | `github.com/minio/pulsar` |
| Binary | `pulsar` |
| Remote | `git@github.com:guptaarvindk/pulsar-bench.git` |
| Branch | `main` |
| Go version | 1.22 |
| External deps | `github.com/spf13/cobra`, `gopkg.in/yaml.v3` |

**Design constraint:** zero runtime external dependencies beyond the two above. No Prometheus, no InfluxDB, no CGo, no shared libraries. Single static binary.

---

## What It Is

Pulsar generates realistic AI workload I/O patterns against any storage path (local disk, NFS, FUSE, object store mount, network-attached storage — anything that looks like a directory). It measures what matters for AI systems:

- **TTFB** (time to first byte) — critical for LLM inference
- **Sustained throughput** under high concurrency — training data loading
- **Metadata performance** — stat/readdir at scale, cache hit rate
- **GPU stall fraction** — percentage of training time blocked on I/O
- **Multi-epoch cache warmup** — cold vs. warm read performance

---

## Repository Layout

```
pulsar-bench/
├── main.go                     # Entrypoint; injects version via ldflags
├── cmd/
│   ├── root.go                 # cobra root command, SetVersion(), subcommand wiring
│   ├── run.go                  # `pulsar run` — main benchmark entrypoint + all flags
│   ├── list.go                 # `pulsar list` — print all built-in profiles
│   ├── report.go               # `pulsar report` — render HTML from a JSON result file
│   ├── agent.go                # `pulsar agent` — start the cluster agent HTTP server
│   ├── compare.go              # `pulsar compare` — diff two JSON result files
│   ├── preflight_linux.go      # freeSpaceBytes via syscall.Statfs (Linux)
│   ├── preflight_darwin.go     # freeSpaceBytes via syscall.Statfs (macOS)
│   ├── preflight_other.go      # freeSpaceBytes stub for other platforms
│   └── cmd_test.go             # Tests: SetVersion, LoadBuiltin, LoadFile, listCmd output
├── profile/
│   ├── profile.go              # Profile struct, FilesConfig, TargetConfig, LoadBuiltin, LoadFile, ParseSize
│   ├── builtin.go              # All 16 built-in profiles as Go functions
│   ├── builtin_linux.go        # availableRAMBytes() via /proc/meminfo
│   ├── builtin_other.go        # availableRAMBytes() → 32 GB constant (non-Linux)
│   ├── distribution.go         # GenerateFileSizes: imagenet/bert/unet log-normal distributions
│   ├── profile_test.go         # ParseSize tests (20 cases)
│   └── distribution_test.go    # GenerateFileSizes tests
├── workload/
│   ├── runner.go               # Runner, Result, all sub-types, workload routing, epoch/multipath logic
│   ├── worker.go               # Per-worker I/O loops: sequential read, random read, write, mixed, metadata, agent-workspace
│   ├── direct_linux.go         # O_DIRECT (0x4000) support, 4096-byte aligned buffers
│   ├── direct_other.go         # Stub for non-Linux: O_DIRECT silently disabled
│   └── targets_test.go         # checkTargets() unit tests
├── measure/
│   ├── latency.go              # Recorder, LatencyStats, MergeLatencyStats, StallTracker
│   ├── throughput.go           # Throughput (atomic counters), ThroughputStats, ThroughputSnapshot
│   ├── sampler.go              # Sampler (1s ticker), MetricSample (per-second time series)
│   ├── sysmetrics_linux.go     # CPU%, RAM, disk IOPS, net ifaces (Linux /proc)
│   ├── sysmetrics_other.go     # Stub for non-Linux
│   └── latency_test.go         # Recorder, StallTracker, MergeLatencyStats tests
├── cluster/
│   ├── protocol.go             # AgentConfig, StreamMsg, TimeReply, StartRequest
│   ├── coordinator.go          # Coordinator.Run(), mergeResults(), checkTargets(), mergeLatencyStats()
│   ├── agent.go                # Agent HTTP server: /api/time, /api/config, /api/start, /api/stream, /api/result, /api/reset
│   └── coordinator_test.go     # Tests: mergeLatencyStats, checkTargets, mergeResults
├── report/
│   ├── html.go                 # WriteHTML(), buildSummary(), reportSummary struct, self-contained HTML template
│   ├── terminal.go             # PrintHeader(), PrintResult(), PrintPerPath(), humanBytes(), humanNum(), LivePrinter
│   ├── csv.go                  # WriteCSV(), WriteCSVResult() — per-second MetricSample as CSV
│   └── report_test.go          # Tests: buildSummary fields, WriteHTML output, humanBytes/humanNum
└── README.md
```

---

## Profiles (Built-in)

All 16 profiles live in `profile/builtin.go`. **"mlperf" is never used** — profiles use workload-descriptive names only.

### AI Workload Profiles

| Name | Workload | Focus | Workers | Duration | Files | Block |
|------|----------|-------|---------|----------|-------|-------|
| `llm-inference` | sequential-read | TTFB + Throughput | 16 | 60s | 8 × 10 GB | 4 MiB |
| `training` | sequential-read | Throughput | 32 | 60s | 32 × 1 GB | 256 KiB |
| `multi-epoch` | multi-epoch | Cache Warmup | 16 | 120s | 16 × 1 GB | 256 KiB |
| `checkpoint` | mixed (70W/30R) | Write Throughput | 8 | 60s | 4 × 10 GB | 4 MiB |
| `agent-workspace` | agent-workspace | IOPS + Latency | 16 | 60s | 1000 × 256 KB | 4 KiB |
| `metadata` | metadata | Metadata | 32 | 30s | 10000 × 1B | 4096 |
| `thrash` | random-read | Cold-path | 32 | 60s | 128 × **auto** | 256 KiB |
| `mixed` | mixed (70R/30W) | Mixed | 32 | 60s | 32 × 512 MB | 256 KiB |
| `image-training` | random-read | Samples/sec | 128 | 300s | 50000 × 120 KB (imagenet dist) | 256 KiB |
| `nlp-training` | sequential-read | Samples/sec | 32 | 300s | 500 × 500 MB (bert dist) | 4 MiB |
| `medical-imaging` | sequential-read | Samples/sec | 16 | 300s | 480 × 150 MB (unet dist) | 1 MiB |

### Drive-Level Profiles (dperf-inspired)

| Name | Workload | Focus | Workers | Duration | Files | Block |
|------|----------|-------|---------|----------|-------|-------|
| `drive-seq-read` | sequential-read | Throughput | 4 | 30s | 4 × 8 GB | 1 MiB |
| `drive-seq-write` | write | Write Throughput | 4 | 30s | 4 × 8 GB | 1 MiB |
| `drive-rand-4k` | random-read | IOPS | 32 | 30s | 32 × 1 GB | 4 KiB |
| `drive-rand-128k` | random-read | Throughput | 16 | 30s | 16 × 1 GB | 128 KiB |
| `drive-mixed` | mixed (70R/30W) | Mixed IOPS | 16 | 60s | 16 × 512 MB | 4 KiB |

**`thrash` auto-sizing:** On Linux reads `MemAvailable` from `/proc/meminfo` and sets working set = `2 × availableRAM`, clamped to [64 GB, 1 TB]. On non-Linux defaults to 64 GB. Ensures every read is a cold cache miss regardless of system RAM.

### Valid Workload Types

`sequential-read`, `random-read`, `write`, `mixed`, `metadata`, `multi-epoch`, `agent-workspace`

An unknown workload type in a YAML profile produces a validation error naming the bad value.

---

## Profile YAML Schema

```yaml
name: my-profile
workload: sequential-read        # one of the valid workload types
workers: 32
duration: 60s
warmup: 10s
files:
  count: 32
  size: 1GB                      # human-readable; parsed by ParseSize()
  distribution: imagenet         # optional: imagenet | bert | unet | uniform (default)
block_size: 256KB                # human-readable; do NOT use block_size_bytes in YAML
direct_io: true
fsync_on_write: false
reuse: false
epochs: 3                        # for multi-epoch workload
read_pct: 70                     # for mixed workload
write_pct: 30
compute_gap_ms: 50               # simulated GPU time (ms) between I/O bursts
num_accelerators: 8
sample_size_bytes: 122880
seed: 0
cleanup: true
verify: false                    # write deterministic pattern + verify on read (detects corruption)
iodepth: 0                       # concurrent I/Os per worker (0=1; higher = goroutine fan-out)
targets:
  ttfb_cold_p99_ms: 500
  ttfb_warm_p99_ms: 50
  read_gbps: 1.0
  write_gbps: 0.0
  stat_p99_ms: 5
  readdir_p99_ms: 1000
  meta_hit_rate_pct: 95
```

**Key gotcha:** `block_size` in YAML is a `string` field (`BlockSizeHuman`). The internal integer field `BlockSize int64` uses the tag `yaml:"block_size_bytes"` and is only used by built-in profiles set directly in Go. When loading from YAML, always use `block_size: 256KB`.

### ParseSize() Rules

- Suffixes: `TiB`, `TB`, `GiB`, `GB`, `MiB`, `MB`, `KiB`, `KB`, `B` (longest-match first, case-insensitive)
- Plain integer = bytes
- SI (`MB`=10⁶) and IEC (`MiB`=2²⁰) both supported

---

## Key Data Structures

### `workload.Result` (the central output type)

```go
type Result struct {
    Profile, WorkloadType, Path string
    Workers int; DirectIO bool; DurationS float64
    StartedAt, FinishedAt time.Time
    Throughput  measure.ThroughputStats
    TTFB        measure.LatencyStats    // time-to-first-byte
    OpLatency   measure.LatencyStats    // full I/O op latency
    Metadata    *MetadataStats          // nil unless workload=="metadata"
    Epochs      []EpochStats            // nil unless workload=="multi-epoch"
    GPUStallPct float64                 // I/O/(I/O+compute) × 100
    PerPath     []PathResult            // multi-path breakdown
    PerNode     []NodeResult            // multi-node breakdown
    Accelerator *AcceleratorStats       // samples/sec per GPU
    Samples     []measure.MetricSample  // 1s time series
    Targets     profile.TargetConfig
    Violations  []string; TargetsMissed int
}
```

### `measure.LatencyStats`

Computed by `Recorder.Stats()` after all workers finish. Fields: `Count`, `MinMs`, `P25Ms`, `P50Ms`, `P75Ms`, `P90Ms`, `P95Ms`, `P99Ms`, `MaxMs`, `MeanMs`, `StdMs`.

### `measure.MergeLatencyStats([]LatencyStats) LatencyStats`

**Canonical implementation in `measure/latency.go`**. Both `workload` and `cluster` packages use thin wrappers. Merging rules:
- `Count` = sum
- `MeanMs` = weighted average by count
- `MinMs` = minimum across all (ignoring zero)
- `MaxMs` = maximum across all
- Percentiles (`P25`–`P99`) come from the recorder with the **largest** sample count (approximation)

### `measure.StallTracker`

Accumulates `AddIO(d)` and `AddCompute(d)` calls from workers. `StallPct()` returns `io/(io+compute) × 100`. Returns 0 if no compute time recorded (ComputeGapMs == 0).

### `measure.MetricSample` (1-second snapshot)

```go
type MetricSample struct {
    T         float64  // seconds since start (field name is T, json:"t")
    ReadGBps, WriteGBps float64
    ReadIOPS, WriteIOPS float64
    TTFBP50Ms, TTFBP99Ms float64
    OpP50Ms, OpP99Ms float64
    CPUPct, MemMB float64
    DiskIOPS  map[string]float64
    NetIfaces map[string]NetIfaceStats
}
```

---

## O_DIRECT

- Linux flag: `0x4000` (defined in `workload/direct_linux.go` as `oDirectFlag`)
- Non-Linux: O_DIRECT is silently ignored (`direct_other.go` stub)
- Buffer requirements: address 4096-aligned, size multiple of 4096, offset multiple of 4096
- `makeAlignedBuf(size int)`: over-allocates by 4096, finds aligned start via `unsafe.Pointer`
- `alignedBlockSize(n)`: rounds up to 4096
- `alignedOffset(off)`: snaps down to 4096

---

## Workload Routing (Runner)

`Runner.Run()` in `workload/runner.go` routes by workload type **before** checking path count:

```go
switch r.p.Workload {
case "multi-epoch":
    // always uses runEpochs(), never runMultiPath
case "metadata":
    // always uses runMetadata(), never runMultiPath
default:
    if len(r.paths) > 1 {
        r.runMultiPath(...)
    } else {
        r.runWorkers(...)
    }
}
```

This was an explicit bug fix — metadata and multi-epoch must not fall into `runMultiPath` because that calls `runWorkers`, which has no case for those workload types.

### Data Race Fix (Seeds)

`*rand.Rand` is **not goroutine-safe**. Seeds are pre-generated sequentially before goroutines launch:

```go
seeds := make([]int64, r.p.Workers)
for i := range seeds {
    seeds[i] = r.rng.Int63()  // sequential, from r.rng held by Runner
}
// then each goroutine: rng := rand.New(rand.NewSource(seeds[i]))
```

Same pattern applied in `runWorkers`, `runMetadata`, and `runMultiPath`.

---

## Multi-Node Protocol

### Agent HTTP API (port 7762 default)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/time` | Returns `{"unix_nano": N}` for clock skew check |
| POST | `/api/config` | Sends `AgentConfig{Profile, Paths}` to configure run; returns 409 if run in progress |
| POST | `/api/start` | Sends `{"at_unix_nano": N}` — synchronized start time |
| GET | `/api/stream` | NDJSON stream of `StreamMsg{Sample?, Result?, Error?}` — stays open for full run duration |
| GET | `/api/result` | Final result JSON (after stream closes) |
| POST | `/api/reset` | Abort current run; allow reconfiguration |

### Coordinator Sequence

1. **Clock skew check** — all nodes must be within 2s of coordinator (NTP guard)
2. **Send config** — POST /api/config to all nodes sequentially
3. **Synchronized start** — POST /api/start to all nodes in parallel (`startAt = now + 3s`)
4. **Stream results** — GET /api/stream from all nodes concurrently; collect `MetricSample` and `Result`
5. **Merge** — `mergeResults()` sums throughput, merges latency stats, checks targets

### HTTP Clients

```go
httpClient    = &http.Client{Timeout: 30 * time.Second}   // config/start/reset calls
streamClient  = &http.Client{Timeout: 0}                  // stream — no timeout (open for full run)
```

### mergeResults() Aggregation

- Throughput: summed across all nodes; `ReadGBps = totalBytesRead / (1e9 × secs)`
- Workers: summed
- TTFB/OpLatency: `MergeLatencyStats`
- StartedAt/FinishedAt: earliest start, latest finish
- GPUStallPct: averaged
- AcceleratorStats: only if `p.NumAccelerators > 0 && p.SampleSizeBytes > 0`

---

## File Size Distributions (`profile/distribution.go`)

`GenerateFileSizes(count int, baseSizeBytes int64, dist string) []int64`

| Distribution | Behavior |
|---|---|
| `""` or `"uniform"` | All files = `baseSizeBytes` |
| `"imagenet"` | Log-normal, mean ~120 KB, σ=0.5 |
| `"bert"` | All files = `baseSizeBytes` if non-zero, else 4.3 GB (one BERT HDF5 shard) |
| `"unet"` | All files = `baseSizeBytes` if non-zero, else 150 MB (one 3D-UNet NPZ volume) |

The `baseSizeBytes` override for bert/unet was an explicit bug fix — profiles that override file size in YAML (`files.size`) must not have that override silently ignored.

---

## Target Checking

`checkTargets(res *workload.Result, p *profile.Profile) ([]string, int)` — called at end of both single-node and multi-node runs.

Rules (zero target = skip):
- `ReadGBps`, `WriteGBps`: result must be **≥** target
- `TTFBColdP99Ms`: result P99 must be **≤** target (always checked when non-zero)
- `TTFBWarmP99Ms`: result P99 must be **≤** target **and** `res.Epochs != nil` (only fires for multi-epoch runs)
- `StatP99Ms`, `ReaddirP99Ms`: checked only if `res.Metadata != nil`
- `MetaHitRatePct`: result must be **≥** target (lower bound)

Exit code 1 if `result.TargetsMissed > 0`, regardless of `--quiet`.

---

## HTML Report

`report.WriteHTML(path, title string, r *workload.Result) error`

- Self-contained single HTML file (Chart.js loaded from CDN)
- Summary JSON embedded as JS variable via `template.JS`
- Time-series samples embedded as JS variable
- Sections: KPI cards, throughput table, TTFB table, per-path table, per-node table, epoch breakdown, metadata table, Chart.js line charts (read/write GB/s, TTFB p99 over time)
- Footer links to `github.com/minio/pulsar`

`buildSummary(r *workload.Result) reportSummary` maps `Result` fields into `reportSummary` for JSON embedding. Includes `Epochs`, `Metadata`, `Accelerator`, `PerPath`, `PerNode`.

---

## CLI Reference

```
pulsar run --path <PATH> --profile <NAME|FILE.yaml> [flags]

Flags:
  --path            Target path(s); repeat for multi-path
  --nodes           Agent addresses for multi-node (host:7762)
  --profile         Built-in profile name or YAML file path (default: training)
  --workers N       Override profile workers
  --duration D      Override profile duration (e.g. 60s, 5m)
  --warmup D        Override warmup window
  --file-size S     Override file size (e.g. 1GB, 512MB)
  --file-count N    Override file count
  --json FILE       Write result JSON to file
  --output-csv FILE Write per-second time-series to CSV file
  --no-cleanup      Keep test files after run
  --seed N          Random seed for reproducibility
  --quiet           Suppress terminal output (exit code still set)
  --compute-gap N   GPU compute gap in ms (enables stall metric)
  --direct-io       Force O_DIRECT (Linux only)
  --no-direct-io    Disable O_DIRECT even if profile enables it
  --verify          Write deterministic pattern and verify on read (detects corruption)
  --iodepth N       Concurrent I/Os per worker via goroutine fan-out (default 1)
  --steady-state    Run until throughput stabilizes (CV<2% for 10s) rather than fixed duration

pulsar list                           # show all built-in profiles
pulsar version                        # print version
pulsar report res.json                # render HTML from JSON result file (positional arg)
pulsar agent --port 7762              # start agent for multi-node runs
pulsar compare before.json after.json # diff two result JSON files
```

### Pre-flight Checks

Before starting a benchmark, `pulsar run` automatically:
1. **Checks writability** — creates and removes a small probe file at each path
2. **Checks free disk space** — verifies at least `files.count × files.size + 10%` headroom is available; errors out if insufficient
3. Implemented via `runPreflight()` in `cmd/run.go`; disk space via OS-specific `freeSpaceBytes()` (Linux/macOS use `syscall.Statfs`)

### Version Injection

```bash
go build -ldflags "-X main.version=$(git describe --tags --always)" -o pulsar .
```

`main.version` → `cmd.SetVersion(version)` → stored in `cmd.buildVersion` → used by `version` subcommand.

---

## New Features (v2)

### `--verify` — Data Integrity Checking
`verifyFill(buf, fileIndex, blockOffset)` fills each block with a deterministic XorShift64 pattern keyed on file index + block offset. `verifyCheck` re-derives the pattern and compares byte-by-byte. Errors are logged to stderr but do not abort the run. Adds CPU overhead proportional to I/O size.

### `--iodepth N` — Goroutine-based I/O Fan-out
Each worker launches N sub-goroutines (sharing the same metrics recorders) for concurrent I/O. File assignment: `fileIdx = (workerID × iodepth + subID) % len(files)`. For NFS/FUSE paths this effectively increases parallelism. Not true kernel-level async I/O (no io_uring).

### `--steady-state` — Run Until Stable
`watchSteadyState()` runs alongside the measurement goroutines. Every second it checks the last 10 samples of throughput. If `stddev/mean < 2%` for 10 consecutive seconds, it cancels the measurement context early. Max runtime is 10 minutes regardless.

### `--output-csv FILE` — Time-Series Export
Writes the per-second `MetricSample` slice as CSV after the run. Columns: `t_s, read_gbps, write_gbps, read_iops, write_iops, ttfb_p50_ms, ttfb_p99_ms, op_p50_ms, op_p99_ms, cpu_pct, mem_mb`. Implemented in `report/csv.go`.

### `pulsar compare before.json after.json` — Result Diff
Loads two JSON result files and prints a side-by-side table showing before/after values and % delta for all key metrics. Green = improvement (≥5%), red = regression (≥5%), gray = within noise. Implemented in `cmd/compare.go`.

### `report.LivePrinter` — Live Progress Output
`NewLivePrinter(total time.Duration)` returns a printer that writes a `\r`-overwriting progress line to stderr every second showing elapsed/total time and the latest throughput/TTFB/CPU snapshot. Call `lp.Start()` before the run, `lp.Update(sample)` for each sample, `lp.Stop()` when done. Implemented in `report/terminal.go`.

### Recorder OOM Prevention
`measure.Recorder` now caps the main sample buffer at 1,000,000 entries (8 MB) using circular overwrite. A separate `winSamples` slice collects samples since the last `StatsWindow()` call for accurate per-second stats; it is reset on each call. Total sample count is tracked in `total int64` and returned by `Count()`.

### Thrash Auto-sizing
`thrash()` profile calls `availableRAMBytes()` (Linux: `/proc/meminfo MemAvailable`; other: 32 GB default) and sets working set = `2 × availableRAM`, clamped to [64 GB, 1 TB].

---

## Testing

All packages have test coverage. Run with race detector:

```bash
go test -race ./...
```

| Package | Test file | What's covered |
|---------|-----------|----------------|
| `profile` | `profile_test.go` | `ParseSize` (20 cases: SI/IEC/plain int/edge cases) |
| `profile` | `distribution_test.go` | `GenerateFileSizes` for all distributions; bert/unet respect `baseSizeBytes` |
| `measure` | `latency_test.go` | `Recorder` (empty/single/percentiles/concurrent/StatsWindow/circular cap), `StallTracker`, `MergeLatencyStats` |
| `workload` | `targets_test.go` | `checkTargets` (9 cases: all-pass, each target type miss, zero targets); `verifyFill`/`verifyCheck` (5 cases: round-trip, corruption detection, cross-block isolation, multiple blocks, large buffer) |
| `cluster` | `coordinator_test.go` | `mergeLatencyStats` (empty/single/two), coordinator `checkTargets` (6 cases), `mergeResults` (2-node/error/nil) |
| `cmd` | `cmd_test.go` | `SetVersion`, `LoadBuiltin` all 16 profiles + unknown, `LoadFile` (valid/bad workload/block_size formats/missing workload), `listCmd` output; `loadResult` (valid/invalid JSON), `compareCmd` missing-file error, `runPreflight` (valid path/non-existent dir) |
| `report` | `report_test.go` | `buildSummary` fields + pass/fail + epoch/metadata/accelerator/per-node passthrough, `WriteHTML` (create/title/profile/epochs/metadata/samples/invalid path), `humanBytes`, `humanNum`; `LivePrinter` (start/stop, idempotent stop, update-before-start) |
| `report` | `csv_test.go` | `WriteCSV` (empty/single/multi-sample/all-fields, invalid path); `WriteCSVResult` (nil samples no-op) |

---

## Key Design Decisions & Bug Fixes (Historical)

| Issue | Fix |
|-------|-----|
| `*rand.Rand` not goroutine-safe | Pre-generate all seeds sequentially before goroutine launch in `runWorkers`, `runMetadata`, `runMultiPath` |
| `metadata`/`multi-epoch` routed through `runMultiPath` → zero results | Route by workload type **before** checking path count |
| `block_size` YAML parsed as `int64` (always 0) | Split into `BlockSizeHuman string \`yaml:"block_size"\`` + `BlockSize int64 \`yaml:"block_size_bytes"\`` |
| `mergeLatencyStats` duplicated in `cluster` and `workload` with different sentinel logic | Canonical in `measure.MergeLatencyStats()`; both packages use thin wrappers |
| `nlp-training` 2.15 TB dataset (bert distribution ignored `baseSizeBytes`) | `bertSize = baseSizeBytes` when non-zero, fallback to 4.3 GB only when zero |
| `agent.go` allowed reconfiguration mid-run | HTTP 409 guard: check `done` channel state in `handleConfig` |
| CPU% wrong by factor of NumCPU (Linux `sysmetrics`) | Removed `* float64(runtime.NumCPU())` multiplication |
| `isPartition()` incorrectly classified NVMe whole disks as partitions | Rewrote: NVMe whole disk has no `pN` suffix; MD RAID/DM never partitions |
| `listCmd` used `fmt.Printf` — `SetOut()` in tests had no effect | Switch to `fmt.Fprintf(cmd.OutOrStdout(), ...)` |
| MLPerf-branded profile names | Renamed to workload-descriptive names: `image-training`, `nlp-training`, `medical-imaging` |
| `checkpoint` profile used `write` workload (read-back not tested) | Changed to `mixed` with `WritePct: 70, ReadPct: 30` |
| `i declared not used` in coordinator after removing `NodeIdx`/`Total` from protocol | Changed `for i, node` to `for _, node` in config send loop |
| `TTFBWarmP99Ms` not checked in coordinator `checkTargets` | Added check with `res.Epochs != nil` guard |
| Agent-workspace rename race: all workers used same tmp filename | `fmt.Sprintf("tmp-rename-%d.bin", w.id)` |
| Fsync error swallowed in `loopWrite` | Propagated error; timing now includes fsync in both `opLat` and `stall` |
| Write loop used profile file size instead of actual file size | Fixed to use `os.Stat()` for actual written size |
| `multi-epoch` aggregate throughput: summed instead of averaged | `ReadGBps = sum / numEpochs` |

---

## What Does NOT Exist (Intentional)

- No Prometheus/metrics exporter — results are terminal + JSON + HTML + CSV only
- No persistent database — each run is self-contained
- No authentication on the agent HTTP API — intended for trusted cluster networks only
- No CGo — pure Go, single static binary
- No Docker image — ship the binary
- No kernel-level async I/O (io_uring / libaio) — `--iodepth` uses goroutine fan-out, which is appropriate for NFS/FUSE/object-store paths; for raw NVMe benchmarking use fio

---

## Build

```bash
# Development
CGO_ENABLED=0 go build -o pulsar .

# Release (with version)
CGO_ENABLED=0 go build -ldflags "-X main.version=v1.2.3 -s -w" -o pulsar .

# Cross-compile for Linux (from macOS)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o pulsar-linux-amd64 .
```

Note: O_DIRECT only works on Linux. On macOS the flag is silently ignored and the binary still compiles and runs correctly.
