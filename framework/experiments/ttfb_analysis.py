"""
TTFB Deep Dive
──────────────
Answers: "What is the full distribution of time-to-first-byte across
all workload profiles? Which profile has the worst tail latency?"

Runs all (or selected) profiles and compares TTFB distributions
side by side. Especially useful for comparing cold vs warm behaviour
and spotting p99 outliers that would stall AI inference.

Usage:
    python experiments/ttfb_analysis.py --path /mnt/storage
    python experiments/ttfb_analysis.py --path /mnt/storage \\
        --profiles llm-inference,training,multi-epoch
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pulsar_framework.experiment import Experiment
from pulsar_framework.runner import RunConfig

DEFAULT_PROFILES = [
    "llm-inference",
    "training",
    "multi-epoch",
    "checkpoint",
    "agent-workspace",
    "thrash",
]


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--path", required=True)
    p.add_argument("--profiles", default=",".join(DEFAULT_PROFILES),
                   help="Comma-separated profile list")
    p.add_argument("--workers", type=int, default=16)
    p.add_argument("--duration", default="30s")
    p.add_argument("--warmup", default="5s")
    p.add_argument("--results-dir", default="./results")
    return p.parse_args()


def main():
    args = parse_args()
    profiles = [p.strip() for p in args.profiles.split(",")]

    exp = Experiment(name="ttfb-analysis", results_dir=args.results_dir)
    exp.add_metrics_from_env("storage", "STORAGE_METRICS_URL")

    print(f"\n  TTFB Analysis: {len(profiles)} profiles × {args.workers} workers")
    print(f"  Path: {args.path}\n")

    results = []
    for profile in profiles:
        cfg = RunConfig(
            path=args.path,
            profile=profile,
            workers=args.workers,
            duration=args.duration,
            warmup=args.warmup,
        )
        results.append(exp.run(cfg))

    print("\n" + "─" * 70)
    print(f"  {'Profile':<22}  {'TTFB p50':>10}  {'TTFB p95':>10}  {'TTFB p99':>10}  {'Max':>10}")
    print("─" * 70)
    for r in results:
        t = r.ttfb
        print(
            f"  {r.profile:<22}  "
            f"  {t.get('p50_ms',0):>8.1f}ms  "
            f"  {t.get('p95_ms',0):>8.1f}ms  "
            f"  {t.get('p99_ms',0):>8.1f}ms  "
            f"  {t.get('max_ms',0):>8.1f}ms"
        )
    print("─" * 70)

    # Highlight worst p99
    worst = max(results, key=lambda r: r.ttfb.get("p99_ms", 0))
    best = min(results, key=lambda r: r.ttfb.get("p99_ms", float("inf")))
    print(f"\n  Worst TTFB p99: {worst.profile}  ({worst.ttfb_p99_ms:.1f}ms)")
    print(f"  Best  TTFB p99: {best.profile}  ({best.ttfb_p99_ms:.1f}ms)")

    # Multi-epoch: show cold vs warm delta
    me = next((r for r in results if r.profile == "multi-epoch"), None)
    if me and me.epochs:
        cold = me.epochs[0]["ttfb"].get("p99_ms", 0) if me.epochs else 0
        warm = me.epochs[-1]["ttfb"].get("p99_ms", 0) if len(me.epochs) > 1 else 0
        if cold > 0 and warm > 0:
            speedup = cold / warm
            print(f"\n  Cache warming effect (multi-epoch):")
            print(f"    Epoch 1 (cold) TTFB p99: {cold:.1f}ms")
            print(f"    Epoch N (warm) TTFB p99: {warm:.1f}ms")
            print(f"    Speedup: {speedup:.1f}×")

    exp.report(save=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
