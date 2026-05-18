# Pulsar — AI Storage Benchmark

Pulsar is a single-binary, zero-dependency storage benchmark that generates realistic AI workload I/O patterns against any mountable path: local NVMe, NFS, FUSE mounts, network-attached storage — anything that looks like a directory.

```
  ──────────────────────────────────────────────────────────────
  Pulsar AI Storage Benchmark
  ──────────────────────────────────────────────────────────────
  Profile   : training  —  Training data loading — sequential shards, many workers
  Path      : /mnt/nvme0
  Workers   : 32    Duration : 1m0s    Files : 32 × 1.0 GiB    Block I/O : 256 KiB
  ──────────────────────────────────────────────────────────────
  → Preparing 32 test file(s) × 1.0 GiB …
  → Warming up for 10s …
  → Running 32 workers for 1m0s …

  Throughput
    Read   3.41 GB/s  (201.4 GiB  13.3K ops/s)

  Time-to-First-Byte (TTFB)        n=13312 ops
    min  0.18ms   p50  1.2ms   p95  8.4ms   p99  22.7ms   max  310.4ms

  Target Check
    ✓ All targets met
  ──────────────────────────────────────────────────────────────
    PASS  profile=training  duration=70.3s  workers=32
  ──────────────────────────────────────────────────────────────
```

---

## Contents

- [Why Pulsar](#why-pulsar)
- [Install](#install)
- [Quick Start](#quick-start)
- [Profiles](#profiles)
- [Flags Reference](#flags-reference)
- [Multi-Path (Multiple Drives)](#multi-path-multiple-drives)
- [Multi-Node](#multi-node)
- [HTML Reports](#html-reports)
- [Custom Profiles](#custom-profiles)
- [CI Integration](#ci-integration)
- [How it Works](#how-it-works)
- [Build from Source](#build-from-source)

---

## Why Pulsar

Most storage benchmarks (fio, iozone, bonnie++) measure raw I/O primitives. Pulsar measures what AI workloads actually do:

| Workload | What Pulsar measures |
|---|---|
| LLM inference | Time-to-first-byte of large model weight files under repeated access |
| Training data loading | Sustained throughput across many concurrent shard readers |
| Multi-epoch training | Cold vs. warm cache performance across dataset passes |
| Checkpointing | Write throughput + fsync latency for model saves |
| Metadata-heavy workloads | `stat()` + `readdir()` IOPS at scale |
| Image / NLP / Medical imaging | Samples/sec per accelerator with realistic file size distributions |

**Key properties:**
- Single static binary — no Python, no pip, no Java runtime
- Runs on Linux and macOS (O_DIRECT on Linux, graceful fallback on macOS)
- Profiles cover the full AI data pipeline: inference, training, checkpointing, agent workspace
- Per-second time-series: throughput, TTFB, IOPS, CPU, memory, disk IOPS, network — exportable to HTML with Chart.js graphs
- Multi-path: benchmark multiple drives simultaneously, spot slow drives automatically
- Multi-node: coordinate a run across a storage cluster with a single command
- CI-friendly: `--json` output + exit code 1 when targets are missed

---

## Install

### Pre-built binary (Linux/macOS)

```bash
# Linux x86-64
curl -Lo pulsar https://github.com/minio/pulsar/releases/latest/download/pulsar-linux-amd64
chmod +x pulsar && sudo mv pulsar /usr/local/bin/

# Linux ARM64
curl -Lo pulsar https://github.com/minio/pulsar/releases/latest/download/pulsar-linux-arm64
chmod +x pulsar && sudo mv pulsar /usr/local/bin/

# macOS Apple Silicon
curl -Lo pulsar https://github.com/minio/pulsar/releases/latest/download/pulsar-darwin-arm64
chmod +x pulsar && sudo mv pulsar /usr/local/bin/
```

### Build from source

```bash
git clone https://github.com/minio/pulsar
cd pulsar
make build          # builds ./pulsar for the current platform
make dist           # cross-compiles linux-amd64, linux-arm64, darwin-arm64 into dist/
```

Requires Go 1.22+. No CGO, no external dependencies.

---

## Quick Start

```bash
# Run the training profile against a mount point
pulsar run --path /mnt/storage --profile training

# LLM inference — measures TTFB of large repeated file reads
pulsar run --path /mnt/storage --profile llm-inference

# 2-minute run with 64 workers
pulsar run --path /mnt/storage --profile training --workers 64 --duration 2m

# Save results as JSON and generate an HTML report
pulsar run --path /mnt/storage --profile training --json results.json
pulsar report results.json --output report.html

# List all available profiles
pulsar list
```

---

## Profiles

Run `pulsar list` to see all profiles. Each profile targets a specific AI I/O pattern:

| Profile | Focus | Workload | Files | Description |
|---|---|---|---|---|
| `llm-inference` | TTFB + Throughput | sequential-read | 8 × 10 GiB | LLM model weight loading — large files, repeated access. 100ms compute gap models GPU token-batch time. |
| `training` | Throughput | sequential-read | 32 × 1 GiB | Training data shard loading — 32 workers, each reading a different shard. 50ms compute gap. |
| `multi-epoch` | Cache Warmup | multi-epoch | 16 × 1 GiB | Reads the same dataset 3× — epoch 1 is cold, epoch 2+ warm. Measures cache learning speed. |
| `checkpoint` | Write Throughput | mixed | 4 × 10 GiB | 70% sequential writes with fsync + 30% read-back. Models model checkpoint save/restore. |
| `agent-workspace` | IOPS + Latency | mixed | 1000 × 256 KiB | Small files, heavy metadata, 70/30 read/write. Models AI coding agent file operations. |
| `metadata` | Metadata | stat+readdir | 10000 × 1 B | Mass `stat()` and `readdir()` concurrency. Tests metadata cache and cold enumeration cost. |
| `thrash` | Cold-path | random-read | 128 × 1 GiB | 128 GB working set — intentionally exceeds cache. Measures raw backend cold-path floor. |
| `mixed` | Mixed | mixed | 32 × 512 MiB | 70% read / 30% write, all workers concurrent. General-purpose stress test. |
| `image-training` | Samples/sec | random-read | 50000 × ~120 KiB | Image classification training — log-normal file sizes (ImageNet pattern), random access, 8 accelerators. |
| `nlp-training` | Samples/sec | sequential-read | 500 × 500 MiB | NLP/LLM pretraining — large files, repeated sequential passes (BERT pattern), 8 accelerators. |
| `medical-imaging` | Samples/sec | sequential-read | 480 × 150 MiB | Medical imaging training — volumetric files (3D-UNet pattern), 4 samples per volume, 8 accelerators. |

All profiles with `DirectIO: true` open files with `O_DIRECT` on Linux to bypass the page cache. On macOS, O_DIRECT is silently skipped.

### Performance targets

Each profile defines pass/fail targets. If any target is missed, Pulsar prints `✗` lines and exits with code 1 (useful in CI):

```
  Target Check
    ✓ Read throughput  3.41 GB/s  ≥  1.0 GB/s
    ✗ TTFB cold p99    620.4ms    >  500ms  ← MISSED
```

---

## Flags Reference

### `pulsar run`

| Flag | Default | Description |
|---|---|---|
| `--path` | *(required)* | Target path to benchmark. Repeat for multiple drives: `--path /mnt/nvme0 --path /mnt/nvme1` |
| `--profile` | `training` | Profile name (`pulsar list`) or path to a YAML file |
| `--workers` | *(profile)* | Number of concurrent I/O workers |
| `--duration` | *(profile)* | Benchmark duration, e.g. `60s`, `5m` |
| `--warmup` | *(profile)* | Warmup period before measurement begins |
| `--file-size` | *(profile)* | Override file size, e.g. `512MB`, `2GB` |
| `--file-count` | *(profile)* | Override number of test files |
| `--json` | *(off)* | Write full results + time-series to this JSON file |
| `--no-cleanup` | false | Keep test files after the run (speeds up repeated runs) |
| `--seed` | 0 | Random seed for reproducible file access patterns |
| `--quiet` | false | Suppress all terminal output (use with `--json`) |
| `--compute-gap` | 0 | Simulated GPU compute time (ms) between I/O ops. Enables GPU stall fraction metric. |
| `--direct-io` | false | Force O_DIRECT even if the profile doesn't set it (Linux only) |
| `--no-direct-io` | false | Disable O_DIRECT even if the profile sets it |
| `--nodes` | *(off)* | Agent addresses for multi-node mode, e.g. `--nodes host1:7762 --nodes host2:7762` |

### `pulsar agent`

| Flag | Default | Description |
|---|---|---|
| `--port` | `7762` | Port for the agent HTTP server |

### `pulsar report`

| Flag | Default | Description |
|---|---|---|
| `--output` | `report.html` | Output HTML file path |
| `--title` | *(from JSON)* | Custom title for the report |

---

## Multi-Path (Multiple Drives)

Benchmark multiple drives simultaneously in a single run. Each drive gets its own worker pool, and results are broken out per drive:

```bash
pulsar run \
  --path /mnt/nvme0 \
  --path /mnt/nvme1 \
  --path /mnt/nvme2 \
  --profile training
```

Output includes a per-drive breakdown table. Drives delivering less than 50% of the fastest drive are flagged in red as potential bottlenecks.

```
  Per-Path Breakdown
  PATH           READ GB/S   READ IOPS   TTFB P50   TTFB P99
  /mnt/nvme0     3.41        13.3K       1.2ms      22.7ms
  /mnt/nvme1     3.38        13.2K       1.3ms      24.1ms
  /mnt/nvme2  ✗  1.21        4.7K        3.8ms      88.4ms   ← slow
```

---

## Multi-Node

For storage clusters, Pulsar can coordinate a synchronized benchmark across multiple nodes.

### 1. Start agents on each storage node

```bash
# On node1, node2, node3 — each targeting its local storage
pulsar agent --port 7762
```

The agent is a lightweight HTTP server. It waits for the coordinator to send a profile, starts at a synchronized wall-clock time, and streams results back.

### 2. Run from the coordinator

```bash
pulsar run \
  --path /mnt/storage \
  --profile training \
  --nodes node1:7762 \
  --nodes node2:7762 \
  --nodes node3:7762
```

The coordinator:
1. Checks NTP clock skew between all nodes (fails if > 2 seconds)
2. Pushes the profile to all agents
3. Issues a synchronized start (`now + 3s`) so all nodes begin simultaneously
4. Streams per-second metrics from all agents
5. Merges results into a single combined report

Multi-node results include a per-node breakdown table alongside the aggregate numbers.

> **Default port:** `7762`. If no port is specified in `--nodes`, `:7762` is appended automatically.

---

## HTML Reports

Generate a self-contained HTML report with time-series charts from any JSON result file:

```bash
# Run and save JSON
pulsar run --path /mnt/storage --profile training --json results.json

# Generate HTML report
pulsar report results.json --output report.html --title "NVMe Training Benchmark"
```

The report includes:
- Summary cards: throughput, TTFB p99, IOPS, GPU stall %, samples/sec
- 8 time-series charts: read/write throughput, TTFB, IOPS, op latency, CPU%, memory, disk IOPS per drive, network RX/TX per interface
- Per-drive breakdown table (multi-path runs)
- Per-node breakdown table (multi-node runs)
- Epoch breakdown (multi-epoch profile)
- Metadata stats (metadata profile)
- Target pass/fail summary

Charts use [Chart.js](https://www.chartjs.org/) embedded inline — no internet connection required to view the report.

---

## Custom Profiles

Create a YAML profile to describe any I/O workload:

```yaml
# my-profile.yaml
name: my-workload
description: "Custom read workload"
workload: sequential-read
workers: 16
duration: 2m
warmup: 10s
files:
  count: 16
  size: 2GB
block_size: 1MB
direct_io: true
reuse: false
cleanup: true
targets:
  read_gbps: 2.0
  ttfb_cold_p99_ms: 200
```

```bash
pulsar run --path /mnt/storage --profile ./my-profile.yaml
```

### Profile fields

| Field | Type | Description |
|---|---|---|
| `name` | string | Profile identifier |
| `description` | string | Human-readable description |
| `workload` | string | One of: `sequential-read`, `random-read`, `write`, `mixed`, `multi-epoch`, `agent-workspace`, `metadata` |
| `workers` | int | Concurrent I/O workers |
| `duration` | duration | Benchmark duration (e.g. `60s`, `5m`) |
| `warmup` | duration | Warmup period before measurement |
| `files.count` | int | Number of test files |
| `files.size` | string | Size per file (e.g. `1GB`, `256MB`) |
| `files.distribution` | string | File size distribution: `imagenet` (log-normal ~120KB), `bert` (500MB fixed), `unet` (150MB fixed) |
| `block_size` | string | I/O block size, human-readable (e.g. `256KB`, `4MB`) |
| `direct_io` | bool | Use O_DIRECT to bypass page cache (Linux only) |
| `reuse` | bool | Reuse the same files across workers (true) or assign exclusive files (false) |
| `read_pct` / `write_pct` | int | Read/write ratio for `mixed` workload |
| `fsync_on_write` | bool | Call `fsync` after each write |
| `epochs` | int | Number of passes for `multi-epoch` workload |
| `compute_gap_ms` | int | Simulated GPU compute time (ms) between I/O ops |
| `num_accelerators` | int | Number of simulated accelerators (for samples/sec metric) |
| `sample_size_bytes` | int | Bytes per training sample (for samples/sec metric) |
| `seed` | int64 | Random seed |
| `cleanup` | bool | Remove test files after the run |
| `targets.read_gbps` | float | Minimum read throughput target |
| `targets.write_gbps` | float | Minimum write throughput target |
| `targets.ttfb_cold_p99_ms` | float | Maximum cold TTFB p99 (ms) |
| `targets.ttfb_warm_p99_ms` | float | Maximum warm TTFB p99 (ms) |
| `targets.stat_p99_ms` | float | Maximum `stat()` latency p99 (ms) |
| `targets.readdir_p99_ms` | float | Maximum `readdir()` latency p99 (ms) |
| `targets.meta_hit_rate_pct` | float | Minimum metadata cache hit rate (%) |

---

## CI Integration

Use `--json` + `--quiet` to get machine-readable output with no terminal noise. Pulsar exits with code `1` if any target is missed:

```bash
pulsar run \
  --path /mnt/storage \
  --profile training \
  --json results.json \
  --quiet

# $? is 0 (pass) or 1 (target missed)
```

Example GitHub Actions step:

```yaml
- name: Storage benchmark
  run: |
    pulsar run --path /mnt/storage --profile training \
      --json results.json --quiet
  
- name: Upload benchmark results
  uses: actions/upload-artifact@v4
  with:
    name: benchmark-results
    path: results.json
  if: always()
```

---

## How it Works

### I/O engine

- Each worker opens its assigned file(s) and loops for the full benchmark duration
- `sequential-read`: reads the file top-to-bottom in `block_size` chunks; wraps at EOF
- `random-read`: reads random aligned offsets within the file
- `write`: writes sequentially, optionally calling `fsync` after each block
- `mixed`: each worker independently decides read vs. write based on `read_pct`/`write_pct`
- `multi-epoch`: full sequential pass = one epoch; records per-epoch stats
- `agent-workspace`: stat, read, write, rename operations on a shared file pool
- `metadata`: concurrent `stat()` calls + periodic `readdir()` over a large file tree

### O_DIRECT

On Linux, profiles with `direct_io: true` open files with `O_DIRECT` (`0x4000`). The kernel bypasses the page cache entirely, reads go straight to the storage device. Pulsar allocates 4096-byte aligned buffers and rounds all offsets to 4096-byte boundaries as required. On macOS, O_DIRECT is silently ignored.

### Metrics

Every second, a background sampler records:
- **Throughput**: atomic snapshot delta divided by elapsed wall time
- **TTFB**: time from `open()` to first byte received; windowed P50/P99 per second
- **Op latency**: full I/O operation duration; windowed P50/P99
- **CPU%**: `/proc/stat` delta (Linux only)
- **Memory**: VmRSS from `/proc/self/status` (Linux), `runtime.MemStats.Sys` (macOS)
- **Disk IOPS**: `/proc/diskstats` delta per device (Linux only)
- **Network**: `/proc/net/dev` delta per interface, RX/TX MB/s and packets/s (Linux only)

### GPU stall fraction

When `compute_gap_ms > 0`, each worker pauses for that duration after every I/O op to simulate GPU compute time. The stall fraction is:

```
stall% = io_time / (io_time + compute_time) × 100
```

A stall fraction near 100% means the GPU is waiting on storage; near 0% means storage keeps up with compute.

### Multi-node protocol

1. Coordinator checks NTP skew against all agents via RTT/2-corrected round trips (fails if > 2s)
2. Coordinator POSTs the full profile JSON to `/api/config` on each agent
3. Coordinator POSTs `{"at_unix_nano": <T+3s>}` to `/api/start` — all agents sleep until `T`
4. Coordinator GETs `/api/stream` from all agents simultaneously; each streams NDJSON `MetricSample` records, then a final `Result` record
5. Coordinator merges all `Result` records: sums throughput, combines latency histograms, appends per-node breakdown

---

## Build from Source

```bash
git clone https://github.com/minio/pulsar
cd pulsar

# Current platform
make build

# Cross-compile all targets (Linux amd64/arm64, macOS arm64)
make dist

# Run tests
make test

# Lint
make lint
```

### Version injection

The binary version is injected at build time via ldflags:

```bash
go build -ldflags "-X main.version=$(git describe --tags --always)" -o pulsar .
```

---

## License

Apache 2.0 — see [LICENSE](LICENSE).
