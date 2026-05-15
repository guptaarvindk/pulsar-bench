# Pulsar — Sample Output

## `pulsar list`

```
  PROFILE                 DESCRIPTION                                           FOCUS
  ──────────────────────  ────────────────────────────────────────────────────  ──────────────
  llm-inference           LLM model weight loading — large files, repeated      TTFB + Throughput
  training                Training data loading — sequential shards, many wor   Throughput
  multi-epoch             Multi-epoch training — measures cold vs warm cache p   Cache Warmup
  checkpoint              Model checkpoint save/restore — large write + fsync    Write Throughput
  agent-workspace         AI agent workspace — small files, metadata-heavy, m    IOPS + Latency
  metadata                Dataset discovery — mass stat() and readdir() at sc    Metadata
  thrash                  Cache thrash — working set larger than cache, measu    Cold-path
  mixed                   Mixed read/write — 70% read, 30% write, all workers   Mixed

  Run a profile:   pulsar run --path /mnt/storage --profile <name>
  Custom profile:  pulsar run --path /mnt/storage --profile my.yaml
```

---

## `pulsar run --path /mnt/storage --profile training --workers 32`

```
  ──────────────────────────────────────────────────────────────
  Pulsar AI Storage Benchmark
  ──────────────────────────────────────────────────────────────
  Profile   : training  —  Training data loading — sequential shards, many workers
  Path      : /mnt/storage
  Workers   : 32
  Duration  : 1m0s
  Files     : 32 × 1.0 GiB
  Block I/O : 256.0 KiB
  ──────────────────────────────────────────────────────────────

  → Preparing 32 test file(s) × 1.0 GiB …
  → Warming up for 10s …
  → Running 32 workers for 1m0s …


  Results
  ──────────────────────────────────────────────────────────────

  Throughput
    Read   3.41 GB/s  (201.4 GiB  13.3K ops/s)

  Time-to-First-Byte (TTFB)        n=13312 ops
    min        p50        p95        p99        max
    0.18ms     1.2ms      8.4ms      22.7ms     310.4ms

  I/O Operation Latency             n=820480 ops
    min        p50        p95        p99        max
    0.04ms     0.31ms     1.8ms      4.2ms      44.1ms

  Target Check
    ✓ All targets met

  ──────────────────────────────────────────────────────────────
    PASS  profile=training  duration=70.3s  workers=32
  ──────────────────────────────────────────────────────────────
```

---

## `pulsar run --path /mnt/storage --profile multi-epoch --workers 16`

```
  Throughput
    Read   4.12 GB/s  (292.1 GiB  16.1K ops/s)

  Time-to-First-Byte (TTFB)        n=4832 ops
    min        p50        p95        p99        max
    0.12ms     3.8ms      18.2ms     44.1ms     512.0ms

  Epoch Breakdown
    Epoch     Read GB/s     TTFB p50      TTFB p99
    ──────────────────────────────────────────────────
    Epoch 1   1.24          88.4ms        412.0ms  (cold)
    Epoch 2   4.12          3.8ms         44.1ms   (warm)
    Epoch 3   4.19          2.1ms         18.6ms   (warm)

  Target Check
    ✓ All targets met
```

---

## `pulsar run --path /mnt/storage --profile metadata --workers 32`

```
  Metadata
    stat()    p99  2.1ms    (284,320 ops)
    readdir() p99  41.3ms   (8,880 ops)
    cache hit rate  97.2% (inferred)

  Target Check
    ✓ All targets met
```

---

## `pulsar run --path /mnt/storage --profile thrash --workers 32`
*(Worst-case: cache is overwhelmed, every read goes to backend)*

```
  Throughput
    Read   0.81 GB/s  (48.9 GiB  3.2K ops/s)

  Time-to-First-Byte (TTFB)        n=3200 ops
    min        p50        p95        p99        max
    12.4ms     142.3ms    680.1ms    1240.8ms   3812.0ms

  Target Check
    ✗ TTFB cold p99 1240.8ms > target 2000ms    ← within target
    ✓ All targets met

  ──────────────────────────────────────────────────────────────
    PASS  profile=thrash  duration=62.1s  workers=32
  ──────────────────────────────────────────────────────────────
```

---

## Scale Sweep Experiment (`experiments/scale_sweep.py`)

```
  Scale sweep: training @ /mnt/storage
  Workers: [1, 2, 4, 8, 16, 32, 64]
  Duration per run: 30s  Warmup: 5s

  Workers    Read GB/s    TTFB p50    TTFB p99
  ────────  ──────────  ──────────  ──────────
         1        0.23       8.2ms      44.1ms
         2        0.46       8.4ms      46.3ms
         4        0.91       9.1ms      48.8ms
         8        1.78      10.2ms      52.1ms
        16        3.12      12.4ms      61.3ms
        32        3.41      22.7ms     110.4ms    ← throughput plateau
        64        3.44      48.1ms     310.2ms    ← latency degrades

  → Storage saturates at approximately 32 workers
    Peak throughput: 3.44 GB/s at 64 workers
    Saturation point: 32 workers (throughput gain < 5% beyond this)
    p99 at saturation: 110ms — acceptable for batch training
    p99 at 64 workers: 310ms — too high for interactive inference
```

---

## TTFB Analysis (`experiments/ttfb_analysis.py`)

```
  ──────────────────────────────────────────────────────────────────────
  Profile                 TTFB p50      TTFB p95      TTFB p99      Max
  ──────────────────────────────────────────────────────────────────────
  llm-inference              2.1ms        12.4ms        28.1ms     210ms
  training                   1.2ms         8.4ms        22.7ms     310ms
  multi-epoch (warm)         2.1ms         9.8ms        18.6ms     120ms
  checkpoint                 4.8ms        32.1ms        80.4ms     640ms
  agent-workspace            0.4ms         2.1ms         8.8ms      44ms
  thrash                   142ms         680ms         1241ms     3812ms
  ──────────────────────────────────────────────────────────────────────

  Worst TTFB p99: thrash       (1241.0ms)
  Best  TTFB p99: agent-workspace (8.8ms)

  Cache warming effect (multi-epoch):
    Epoch 1 (cold) TTFB p99: 412.0ms
    Epoch N (warm) TTFB p99:  18.6ms
    Speedup: 22.2×
```
