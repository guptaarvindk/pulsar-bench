"""
Pulsar benchmark runner.

Invokes the `pulsar` binary and returns structured results. The binary
path is resolved from PATH or PULSAR_BIN env var. All benchmark
parameters are passed as CLI flags — the framework never mutates
profile files on disk.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)


def _find_binary() -> str:
    """Find the pulsar binary — env override, then PATH."""
    if env := os.environ.get("PULSAR_BIN"):
        return env
    found = shutil.which("pulsar")
    if found:
        return found
    raise FileNotFoundError(
        "pulsar binary not found. Set PULSAR_BIN=/path/to/pulsar "
        "or place it on your PATH."
    )


@dataclass
class RunConfig:
    """
    Complete configuration for one Pulsar benchmark run.
    Maps 1:1 to pulsar CLI flags — any zero/None value uses the profile default.
    """
    path: str                      # target path (required)
    profile: str = "training"      # built-in profile name or path to YAML
    workers: int | None = None     # override profile workers
    duration: str | None = None    # override duration, e.g. "60s", "2m"
    warmup: str | None = None      # warmup duration
    file_size: str | None = None   # e.g. "1GB", "512MB"
    file_count: int | None = None  # number of test files
    seed: int | None = None        # for reproducibility
    no_cleanup: bool = False       # keep test files for repeated runs

    # Extra key-value overrides written into a temp YAML profile
    # so arbitrary profile fields can be set without adding CLI flags.
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class RunResult:
    """Structured result from one pulsar run."""
    profile: str
    workload_type: str
    path: str
    workers: int
    duration_s: float
    started_at: str
    finished_at: str

    throughput: dict = field(default_factory=dict)
    ttfb: dict = field(default_factory=dict)
    op_latency: dict = field(default_factory=dict)
    metadata: dict | None = None
    epochs: list[dict] = field(default_factory=list)

    targets: dict = field(default_factory=dict)
    violations: list[str] = field(default_factory=list)
    targets_missed: int = 0

    # Framework-added fields
    config: RunConfig | None = None
    elapsed_wall_s: float = 0.0

    @classmethod
    def from_json(cls, data: dict, config: RunConfig | None = None) -> "RunResult":
        r = cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
        r.config = config
        return r

    @property
    def read_gbps(self) -> float:
        return self.throughput.get("read_g_bps", 0.0)

    @property
    def write_gbps(self) -> float:
        return self.throughput.get("write_g_bps", 0.0)

    @property
    def ttfb_p50_ms(self) -> float:
        return self.ttfb.get("p50_ms", 0.0)

    @property
    def ttfb_p99_ms(self) -> float:
        return self.ttfb.get("p99_ms", 0.0)

    @property
    def passed(self) -> bool:
        return self.targets_missed == 0


class PulsarRunner:
    """
    Runs the Pulsar benchmark binary and returns structured results.

    Usage:
        runner = PulsarRunner()
        result = runner.run(RunConfig(path="/mnt/storage", profile="training", workers=32))
    """

    def __init__(self, binary: str | None = None) -> None:
        self._bin = binary or _find_binary()
        log.info("Pulsar binary: %s", self._bin)

    def run(self, cfg: RunConfig, timeout: int = 600) -> RunResult:
        """Execute one benchmark run and return structured results."""
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as jf:
            json_path = jf.name

        try:
            cmd = self._build_cmd(cfg, json_path)
            log.info("Running: %s", " ".join(cmd))

            t0 = time.monotonic()
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            elapsed = time.monotonic() - t0

            if proc.stdout:
                print(proc.stdout, end="")
            if proc.stderr:
                log.warning("pulsar stderr: %s", proc.stderr)

            # Load JSON output written by --json flag
            result_data = {}
            if Path(json_path).exists():
                with open(json_path) as f:
                    result_data = json.load(f)

            result = RunResult.from_json(result_data, cfg)
            result.elapsed_wall_s = elapsed
            return result

        finally:
            Path(json_path).unlink(missing_ok=True)

    def run_matrix(
        self,
        base: RunConfig,
        vary: dict[str, list],
        timeout: int = 600,
    ) -> list[RunResult]:
        """
        Run the benchmark across a parameter matrix.
        `vary` maps a RunConfig field name to a list of values to sweep.

        Example:
            runner.run_matrix(base_cfg, vary={"workers": [4, 8, 16, 32, 64]})
        """
        import itertools
        keys = list(vary.keys())
        values = list(vary.values())
        results = []
        for combo in itertools.product(*values):
            cfg_dict = {**base.__dict__}
            for k, v in zip(keys, combo):
                cfg_dict[k] = v
            cfg = RunConfig(**cfg_dict)
            log.info("Matrix run: %s", {k: v for k, v in zip(keys, combo)})
            result = self.run(cfg, timeout=timeout)
            results.append(result)
        return results

    def _build_cmd(self, cfg: RunConfig, json_path: str) -> list[str]:
        cmd = [self._bin, "run", "--path", cfg.path, "--json", json_path]

        # If extra overrides exist, write a temp YAML profile that extends the base
        if cfg.extra:
            profile_path = self._write_temp_profile(cfg)
            cmd += ["--profile", profile_path]
        else:
            cmd += ["--profile", cfg.profile]

        if cfg.workers is not None:
            cmd += ["--workers", str(cfg.workers)]
        if cfg.duration is not None:
            cmd += ["--duration", cfg.duration]
        if cfg.warmup is not None:
            cmd += ["--warmup", cfg.warmup]
        if cfg.file_size is not None:
            cmd += ["--file-size", cfg.file_size]
        if cfg.file_count is not None:
            cmd += ["--file-count", str(cfg.file_count)]
        if cfg.seed is not None:
            cmd += ["--seed", str(cfg.seed)]
        if cfg.no_cleanup:
            cmd += ["--no-cleanup"]
        return cmd

    def _write_temp_profile(self, cfg: RunConfig) -> str:
        """Write a temporary YAML profile merging the base profile with extra overrides."""
        data = {"name": cfg.profile, **cfg.extra}
        tf = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w")
        yaml.dump(data, tf)
        tf.close()
        return tf.name
