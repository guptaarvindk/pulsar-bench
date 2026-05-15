"""Optional chart generation using matplotlib. Gracefully skipped if not installed."""
from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pulsar_framework.runner import RunResult


def plot_saturation_curve(
    results: list["RunResult"],
    title: str = "Saturation Curve",
    out_path: str = "saturation.png",
) -> str:
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker

    workers = [r.workers for r in results]
    gbps = [r.read_gbps for r in results]
    p99 = [r.ttfb_p99_ms for r in results]

    fig, ax1 = plt.subplots(figsize=(9, 5))
    ax2 = ax1.twinx()

    ax1.plot(workers, gbps, "o-", color="#2196F3", linewidth=2, label="Read GB/s")
    ax1.set_xlabel("Concurrent Workers", fontsize=12)
    ax1.set_ylabel("Throughput (GB/s)", color="#2196F3", fontsize=12)
    ax1.tick_params(axis="y", labelcolor="#2196F3")

    ax2.plot(workers, p99, "s--", color="#F44336", linewidth=2, label="TTFB p99 (ms)")
    ax2.set_ylabel("TTFB p99 (ms)", color="#F44336", fontsize=12)
    ax2.tick_params(axis="y", labelcolor="#F44336")

    ax1.set_xscale("log", base=2)
    ax1.xaxis.set_major_formatter(ticker.ScalarFormatter())
    ax1.set_xticks(workers)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    plt.title(title, fontsize=14, fontweight="bold")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    return out_path


def plot_ttfb_comparison(
    results: list["RunResult"],
    title: str = "TTFB by Profile",
    out_path: str = "ttfb.png",
) -> str:
    import matplotlib.pyplot as plt
    import numpy as np

    profiles = [r.profile for r in results]
    p50 = [r.ttfb.get("p50_ms", 0) for r in results]
    p99 = [r.ttfb.get("p99_ms", 0) for r in results]

    x = np.arange(len(profiles))
    width = 0.35

    fig, ax = plt.subplots(figsize=(max(8, len(profiles) * 1.5), 5))
    ax.bar(x - width / 2, p50, width, label="p50", color="#4CAF50", alpha=0.8)
    ax.bar(x + width / 2, p99, width, label="p99", color="#F44336", alpha=0.8)

    ax.set_xlabel("Profile")
    ax.set_ylabel("TTFB (ms)")
    ax.set_title(title, fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels(profiles, rotation=25, ha="right")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    return out_path
