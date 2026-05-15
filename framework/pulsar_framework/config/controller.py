"""
Generic storage system configuration controller.

The framework defines a simple interface (StorageController) that any
storage system can implement. This lets experiments control system
parameters — thread counts, cache sizes, prefetch settings, etc. —
and then run the Pulsar benchmark to observe the effect.

Built-in controllers ship for common systems. Adding a new one is
~20 lines: implement the interface and register it.
"""

from __future__ import annotations

import logging
import os
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
import yaml

log = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Interface
# ------------------------------------------------------------------ #

class StorageController(ABC):
    """
    Abstraction over a configurable storage system.

    Experiments interact with the system through this interface so that
    the same experiment code works against different backends.
    """

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def get_config(self) -> dict[str, Any]:
        """Return current running configuration as a flat dict."""
        ...

    @abstractmethod
    def set_config(self, overrides: dict[str, Any]) -> None:
        """Apply configuration overrides. System restarts as needed."""
        ...

    @abstractmethod
    def restart(self) -> None:
        """Restart the storage system with the current config."""
        ...

    @abstractmethod
    def health_check(self) -> bool:
        """Return True if the system is healthy and ready for I/O."""
        ...

    def with_config(self, overrides: dict[str, Any]):
        """Context manager: apply overrides, yield, restore original."""
        return _ConfigContext(self, overrides)


class _ConfigContext:
    def __init__(self, ctrl: StorageController, overrides: dict) -> None:
        self._ctrl = ctrl
        self._overrides = overrides
        self._original: dict = {}

    def __enter__(self) -> StorageController:
        self._original = self._ctrl.get_config()
        self._ctrl.set_config(self._overrides)
        return self._ctrl

    def __exit__(self, *_) -> None:
        self._ctrl.set_config(self._original)


# ------------------------------------------------------------------ #
# YAML-file-based controller (works for anything with a config file)
# ------------------------------------------------------------------ #

class YamlFileController(StorageController):
    """
    Controls a storage system by editing its YAML config file and
    restarting via a shell command.

    This is the simplest possible controller — no agent, no API.
    Works for any system that reads a config file on startup.
    """

    def __init__(
        self,
        name: str,
        config_path: str | Path,
        restart_cmd: str,
        health_url: str | None = None,
        health_timeout: float = 30.0,
    ) -> None:
        self._name = name
        self._config_path = Path(config_path)
        self._restart_cmd = restart_cmd
        self._health_url = health_url
        self._health_timeout = health_timeout

    @property
    def name(self) -> str:
        return self._name

    def get_config(self) -> dict[str, Any]:
        with open(self._config_path) as f:
            return yaml.safe_load(f) or {}

    def set_config(self, overrides: dict[str, Any]) -> None:
        current = self.get_config()
        _deep_merge(current, overrides)
        with open(self._config_path, "w") as f:
            yaml.dump(current, f)
        log.info("[%s] Config updated: %s", self._name, overrides)
        self.restart()

    def restart(self) -> None:
        log.info("[%s] Restarting via: %s", self._name, self._restart_cmd)
        subprocess.run(self._restart_cmd, shell=True, check=True)
        if self._health_url:
            self._wait_healthy()

    def health_check(self) -> bool:
        if not self._health_url:
            return True
        try:
            r = requests.get(self._health_url, timeout=3)
            return r.status_code < 400
        except Exception:
            return False

    def _wait_healthy(self) -> None:
        import time
        deadline = time.monotonic() + self._health_timeout
        while time.monotonic() < deadline:
            if self.health_check():
                log.info("[%s] Health check passed", self._name)
                return
            time.sleep(1)
        raise TimeoutError(
            f"[{self._name}] did not become healthy within {self._health_timeout}s"
        )


# ------------------------------------------------------------------ #
# HTTP admin API controller (for systems with a REST admin interface)
# ------------------------------------------------------------------ #

class AdminAPIController(StorageController):
    """
    Controls a storage system via HTTP admin API.
    Assumes GET /api/v1/config returns current config as JSON,
    and POST /api/v1/config applies overrides.

    Implement subclasses to adapt to specific API shapes.
    """

    def __init__(self, name: str, admin_url: str) -> None:
        self._name = name
        self._url = admin_url.rstrip("/")
        self._session = requests.Session()

    @property
    def name(self) -> str:
        return self._name

    def get_config(self) -> dict[str, Any]:
        resp = self._session.get(f"{self._url}/api/v1/config", timeout=10)
        resp.raise_for_status()
        return resp.json()

    def set_config(self, overrides: dict[str, Any]) -> None:
        resp = self._session.post(
            f"{self._url}/api/v1/config",
            json=overrides,
            timeout=30,
        )
        resp.raise_for_status()
        log.info("[%s] Config applied: %s", self._name, overrides)

    def restart(self) -> None:
        resp = self._session.post(f"{self._url}/api/v1/restart", timeout=10)
        resp.raise_for_status()

    def health_check(self) -> bool:
        try:
            r = self._session.get(f"{self._url}/health", timeout=3)
            return r.status_code < 400
        except Exception:
            return False


# ------------------------------------------------------------------ #
# No-op controller (used when no external system is configured)
# ------------------------------------------------------------------ #

class NoopController(StorageController):
    """
    Placeholder controller when no external system is configured.
    All operations are no-ops. Experiments still run — they just
    cannot control system parameters.
    """

    def __init__(self, name: str = "noop") -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def get_config(self) -> dict:
        return {}

    def set_config(self, overrides: dict) -> None:
        log.debug("[noop] set_config ignored: %s", overrides)

    def restart(self) -> None:
        log.debug("[noop] restart is a no-op")

    def health_check(self) -> bool:
        return True


# ------------------------------------------------------------------ #
# Registry — look up a controller by name from environment
# ------------------------------------------------------------------ #

def from_env() -> StorageController:
    """
    Build a controller from environment variables.
    STORAGE_ADMIN_URL → AdminAPIController
    STORAGE_CONFIG_FILE + STORAGE_RESTART_CMD → YamlFileController
    (nothing) → NoopController
    """
    if url := os.environ.get("STORAGE_ADMIN_URL"):
        name = os.environ.get("STORAGE_NAME", "storage")
        log.info("Using AdminAPIController for %r at %s", name, url)
        return AdminAPIController(name, url)

    if cfg_file := os.environ.get("STORAGE_CONFIG_FILE"):
        restart = os.environ.get("STORAGE_RESTART_CMD", "")
        health = os.environ.get("STORAGE_HEALTH_URL")
        name = os.environ.get("STORAGE_NAME", "storage")
        log.info("Using YamlFileController for %r, config=%s", name, cfg_file)
        return YamlFileController(name, cfg_file, restart, health)

    log.info("No STORAGE_ADMIN_URL or STORAGE_CONFIG_FILE — using NoopController")
    return NoopController()


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def _deep_merge(base: dict, overrides: dict) -> None:
    for k, v in overrides.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
