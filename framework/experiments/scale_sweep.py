"""
Concurrency Saturation Sweep
─────────────────────────────
Answers: "How many concurrent workers does it take to saturate
the storage? What does throughput do as concurrency climbs?"

Runs the same workload profile at increasing worker counts:
  1 → 2 → 4 → 8 → 16 → 32 → 64 → 128

Produces a saturation curve showing where throughput plateaus and
where latency starts to degrade. This is the most important single
experiment for characterising storage performance.

Usage:
    python experiments/scale_sweep.py --path /mnt/storage --profile training
    python experiments/scale_sweep.py --path /mnt/storage --profile training \\
        --workers 1,2,4,8,16,32,64 --duration 30s
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pulsar_framework.experiment import Experiment
from pulsar_framework.runner import RunConfig
from pulsar_framework.report.charts import plot_saturation_curve


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--path", required=True, help="Target storage path")
    p.add_argument("--profile", default="training", help="Pulsar workload profile")
    p.add_argument("--workers", default="1,2,4,8,16,32,64",
                   help="Comma-separated worker counts to sweep")
    p.add_argument("--duration", default="30s", help="Duration per run")
    p.add_argument("--warmup", default="5s", help="Warmup per run")
    p.add_argument("--repeat", type=int, default=1, help="Repeats per worker count")
    p.add_argument("--results-dir", default="./results")
    p.add_argument("--no-cleanup", action="store_true")
    p.add_argument("--metrics-url", help="Prometheus metrics URL (optional)")
    return p.parse_args()


def main():
    args = parse_args()
    worker_counts = [int(w.strip()) for w in args.workers.split(",")]

    exp = Experiment(
        name=f"scale-sweep-{args.profile}",
        results_dir=args.results_dir,
    )

    # Register metrics source if configured
    if args.metrics_url:
        exp.add_metrics_source("storage", args.metrics_url)
    else:
        exp.add_metrics_from_env("storage", "STORAGE_METRICS_URL")

    base = RunConfig(
        path=args.path,
        profile=args.profile,
        duration=args.duration,
        warmup=args.warmup,
        no_cleanup=args.no_cleanup,
    )

    print(f"\n  Scale sweep: {args.profile} @ {args.path}")
    print(f"  Workers: {worker_counts}")
    print(f"  Duration per run: {args.duration}  Warmup: {args.warmup}")
    print(f"  Repeats: {args.repeat}\n")

    results = exp.run_matrix(
        base=base,
        vary={"workers": worker_counts},
        repeat=args.repeat,
    )

    exp_result = exp.report(save=True)

    # Print saturation summary
    print("\n  Saturation Curve:")
    print(f"  {'Workers':>8}  {'Read GB/s':>10}  {'TTFB p50':>10}  {'TTFB p99':>10}")
    print(f"  {'─'*8}  {'─'*10}  {'─'*10}  {'─'*10}")
    for r in results:
        print(
            f"  {r.workers:>8}  "
            f"  {r.read_gbps:>8.2f}  "
            f"  {r.ttfb_p50_ms:>8.1f}ms  "
            f"  {r.ttfb_p99_ms:>8.1f}ms"
        )

    # Find saturation point (throughput increases less than 5% with 2x workers)
    prev_gbps = None
    saturation_workers = None
    for r in results:
        if prev_gbps is not None and r.read_gbps > 0:
            gain = (r.read_gbps - prev_gbps) / max(prev_gbps, 0.001)
            if gain < 0.05:
                saturation_workers = r.workers
                break
        prev_gbps = r.read_gbps

    if saturation_workers:
        print(f"\n  → Storage saturates at approximately {saturation_workers} workers")
    else:
        print(f"\n  → Throughput still scaling at {worker_counts[-1]} workers — try higher counts")

    # Optional: generate saturation curve chart
    try:
        chart_path = plot_saturation_curve(
            results,
            title=f"Saturation Curve — {args.profile}",
            out_path=f"{args.results_dir}/saturation_{args.profile}.png",
        )
        print(f"  Chart saved: {chart_path}")
    except Exception as e:
        pass  # Chart is optional — report is the primary output

    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
