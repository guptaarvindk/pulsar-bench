"""
Generic Prometheus scraper.

Reads metrics from any Prometheus-compatible endpoint (agstor, minfs-cache,
Nebula, Prometheus node_exporter, custom exporters, …). The framework
does not know or care what system is on the other end — it just scrapes
text-format metrics and lets experiments query them by name.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Iterator

import requests

log = logging.getLogger(__name__)


@dataclass
class Sample:
    name: str
    labels: dict[str, str]
    value: float
    timestamp: float = field(default_factory=time.time)


class PrometheusClient:
    """
    Scrapes one Prometheus metrics endpoint and exposes metrics as a
    flat name→value dict (no labels) or as a list of Sample objects.
    """

    def __init__(self, url: str, timeout: float = 5.0) -> None:
        self._url = url
        self._timeout = timeout
        self._session = requests.Session()

    def fetch(self) -> dict[str, float]:
        """
        Return {metric_name: latest_value}. When the same metric name
        appears multiple times (different labels), the last value wins.
        Use fetch_samples() if label cardinality matters.
        """
        return {s.name: s.value for s in self.fetch_samples()}

    def fetch_samples(self) -> list[Sample]:
        """Return all samples including label sets."""
        resp = self._session.get(self._url, timeout=self._timeout)
        resp.raise_for_status()
        return list(_parse_text(resp.text))

    def delta(self, metric: str, before: dict, after: dict) -> float:
        """Compute counter increment between two snapshots."""
        return after.get(metric, 0.0) - before.get(metric, 0.0)

    def snapshot(self) -> dict[str, float]:
        """Alias for fetch() — clearer name at callsites."""
        return self.fetch()

    @property
    def available(self) -> bool:
        try:
            self._session.get(self._url, timeout=2).raise_for_status()
            return True
        except Exception:
            return False


class MultiSourceCollector:
    """
    Aggregates metrics from multiple Prometheus endpoints under
    namespaced keys: {source_name}.{metric_name}.

    Sources are registered by name so experiments can reference them
    symbolically without hard-coding URLs.

    Example:
        collector = MultiSourceCollector()
        collector.register("storage", "http://storage-node:9100/metrics")
        collector.register("cache",   "http://cache-node:9200/metrics")

        before = collector.snapshot_all()
        run_benchmark()
        after = collector.snapshot_all()

        cache_hits = after["cache.cache_hits_total"] - before["cache.cache_hits_total"]
    """

    def __init__(self) -> None:
        self._sources: dict[str, PrometheusClient] = {}

    def register(self, name: str, url: str, timeout: float = 5.0) -> None:
        self._sources[name] = PrometheusClient(url, timeout=timeout)
        log.info("Registered metrics source %r at %s", name, url)

    def register_from_env(self, name: str, env_var: str) -> bool:
        """Register a source from an environment variable. Returns True if registered."""
        import os
        url = os.environ.get(env_var)
        if url:
            self.register(name, url)
            return True
        log.debug("Metrics source %r not registered — %s not set", name, env_var)
        return False

    def snapshot_all(self) -> dict[str, float]:
        """Scrape all registered sources and return namespaced flat dict."""
        out: dict[str, float] = {}
        for name, client in self._sources.items():
            try:
                for k, v in client.fetch().items():
                    out[f"{name}.{k}"] = v
            except Exception as e:
                log.warning("Failed to scrape %r: %s", name, e)
        return out

    def delta_all(self, before: dict, after: dict) -> dict[str, float]:
        return {k: after.get(k, 0) - before.get(k, 0) for k in after}

    @property
    def sources(self) -> list[str]:
        return list(self._sources.keys())


# ------------------------------------------------------------------ #
# Prometheus text format parser (stdlib only, no prometheus_client)
# ------------------------------------------------------------------ #

def _parse_text(text: str) -> Iterator[Sample]:
    """
    Parse Prometheus text exposition format.
    Handles: gauges, counters, summaries (individual lines), histograms.
    Ignores: TYPE/HELP comment lines, malformed lines.
    """
    ts_now = time.time()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # Split off optional timestamp
        parts = line.rsplit(" ", 1)
        if len(parts) == 2:
            try:
                float(parts[1])
                line = parts[0]
            except ValueError:
                pass
        # Split metric+labels from value
        parts = line.rsplit(" ", 1)
        if len(parts) != 2:
            continue
        name_labels, val_str = parts
        try:
            value = float(val_str)
        except ValueError:
            continue

        # Parse labels
        labels: dict[str, str] = {}
        if "{" in name_labels:
            name_part, label_part = name_labels.split("{", 1)
            label_part = label_part.rstrip("}")
            for pair in label_part.split(","):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    labels[k.strip()] = v.strip().strip('"')
            name = name_part.strip()
        else:
            name = name_labels.strip()

        yield Sample(name=name, labels=labels, value=value, timestamp=ts_now)
