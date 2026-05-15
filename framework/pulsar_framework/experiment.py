"""
Experiment orchestrator.

An Experiment is a repeatable, parameterized benchmark study that:
  1. Optionally configures the storage system under test
  2. Runs one or more Pulsar benchmark invocations
  3. Collects metrics from any registered Prometheus sources
  4. Saves results and produces a report

Experiments compose the three framework primitives:
  PulsarRunner      — runs the benchmark
  StorageController — controls the storage system config
  MultiSourceCollector — scrapes system metrics

Usage:
    from pulsar_framework.experiment import Experiment
    from pulsar_framework.runner import RunConfig

    exp = Experiment(name="scale-sweep")
    exp.configure_storage({"fuse_threads": 64})   # optional
    results = exp.run_matrix(
        base=RunConfig(path="/mnt/storage", profile="training"),
        vary={"workers": [4, 8, 16, 32, 64]},
    )
    exp.report()
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pulsar_framework.config.controller import StorageController, NoopController, from_env
from pulsar_framework.metrics.prometheus import MultiSourceCollector
from pulsar_framework.runner import PulsarRunner, RunConfig, RunResult

log = logging.getLogger(__name__)


@dataclass
class ExperimentResult:
    name: str
    started_at: str
    finished_at: str
    runs: list[RunResult] = field(default_factory=list)
    metrics_snapshots: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "run_count": len(self.runs),
            "passes": sum(1 for r in self.runs if r.passed),
            "runs": [r.__dict__ for r in self.runs],
        }

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2, default=str)
        log.info("Experiment results saved: %s", path)


class Experiment:
    """
    Orchestrates a complete benchmark experiment:
    storage config → benchmark runs → metrics collection → report.
    """

    def __init__(
        self,
        name: str,
        results_dir: str | Path = "./results",
        storage: StorageController | None = None,
        runner: PulsarRunner | None = None,
    ) -> None:
        self.name = name
        self.results_dir = Path(results_dir)
        self._storage = storage or from_env()
        self._runner = runner or PulsarRunner()
        self._metrics = MultiSourceCollector()
        self._results: list[RunResult] = []
        self._started_at = datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------ #
    # Metrics source registration
    # ------------------------------------------------------------------ #

    def add_metrics_source(self, name: str, url: str) -> None:
        """
        Register a Prometheus metrics endpoint.
        Metrics are scraped before and after each run and attached to results.
        """
        self._metrics.register(name, url)

    def add_metrics_from_env(self, name: str, env_var: str) -> bool:
        """Register a metrics source from an env var. Returns True if registered."""
        return self._metrics.register_from_env(name, env_var)

    # ------------------------------------------------------------------ #
    # Storage system control
    # ------------------------------------------------------------------ #

    def configure_storage(self, overrides: dict[str, Any]) -> None:
        """Apply config overrides to the storage system and wait for ready."""
        log.info("[%s] Configuring storage: %s", self.name, overrides)
        self._storage.set_config(overrides)

    def with_storage_config(self, overrides: dict[str, Any]):
        """Context manager: apply config, run experiment, restore original."""
        return self._storage.with_config(overrides)

    # ------------------------------------------------------------------ #
    # Run methods
    # ------------------------------------------------------------------ #

    def run(self, cfg: RunConfig) -> RunResult:
        """Execute a single benchmark run with metrics collection."""
        log.info("[%s] Starting run: profile=%s workers=%s", self.name, cfg.profile, cfg.workers)

        before_metrics = self._metrics.snapshot_all()
        result = self._runner.run(cfg)
        after_metrics = self._metrics.snapshot_all()

        # Attach metric deltas to the result metadata
        if before_metrics or after_metrics:
            deltas = self._metrics.delta_all(before_metrics, after_metrics)
            result.__dict__["metric_deltas"] = {
                k: v for k, v in deltas.items() if abs(v) > 0
            }

        self._results.append(result)
        return result

    def run_matrix(
        self,
        base: RunConfig,
        vary: dict[str, list],
        repeat: int = 1,
    ) -> list[RunResult]:
        """
        Sweep a parameter matrix. Optionally repeat each combination for variance.

        Example:
            exp.run_matrix(
                base=RunConfig(path="/mnt/s", profile="training"),
                vary={"workers": [4, 8, 16, 32, 64]},
                repeat=3,
            )
        """
        import itertools
        keys = list(vary.keys())
        results = []
        for combo in itertools.product(*vary.values()):
            params = dict(zip(keys, combo))
            for rep in range(repeat):
                cfg_dict = {**base.__dict__, **params}
                if repeat > 1:
                    cfg_dict["seed"] = (cfg_dict.get("seed") or 42) + rep
                cfg = RunConfig(**cfg_dict)
                log.info("Matrix run %s/%s: %s", rep + 1, repeat, params)
                results.append(self.run(cfg))
        return results

    # ------------------------------------------------------------------ #
    # Reporting
    # ------------------------------------------------------------------ #

    def report(self, save: bool = True) -> ExperimentResult:
        finished = datetime.now(timezone.utc).isoformat()
        exp_result = ExperimentResult(
            name=self.name,
            started_at=self._started_at,
            finished_at=finished,
            runs=list(self._results),
        )

        if save:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            exp_result.save(self.results_dir / f"{self.name}_{ts}.json")

        _print_experiment_summary(exp_result)
        return exp_result


def _print_experiment_summary(exp: ExperimentResult) -> None:
    from rich.console import Console
    from rich.table import Table
    from rich import box

    console = Console()
    console.print(f"\n[bold cyan]Experiment: {exp.name}[/]")
    console.print(f"[dim]{exp.started_at} → {exp.finished_at}[/]\n")

    table = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold")
    table.add_column("Profile", style="cyan")
    table.add_column("Workers", justify="right")
    table.add_column("Read GB/s", justify="right")
    table.add_column("Write GB/s", justify="right")
    table.add_column("TTFB p50", justify="right")
    table.add_column("TTFB p99", justify="right")
    table.add_column("Status", justify="center")

    for r in exp.runs:
        status = "[green]PASS[/]" if r.passed else f"[red]FAIL ({r.targets_missed})[/]"
        table.add_row(
            r.profile,
            str(r.workers),
            f"{r.read_gbps:.2f}" if r.read_gbps else "—",
            f"{r.write_gbps:.2f}" if r.write_gbps else "—",
            f"{r.ttfb_p50_ms:.1f}ms" if r.ttfb_p50_ms else "—",
            f"{r.ttfb_p99_ms:.1f}ms" if r.ttfb_p99_ms else "—",
            status,
        )

    console.print(table)
    passes = sum(1 for r in exp.runs if r.passed)
    console.print(f"[bold]{passes}/{len(exp.runs)} runs passed[/]\n")
