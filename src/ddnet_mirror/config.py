"""Configuration models and hot-reload manager (SIGHUP)."""

from __future__ import annotations

import signal
import threading
import time
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator


class ServerConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8080


class LoggingConfig(BaseModel):
    level: str = "INFO"
    dir: str = "logs"
    max_bytes: int = 5 * 1024 * 1024  # rotate a single file once it exceeds this
    max_age_seconds: float = 7 * 24 * 3600  # rotate a file once it is this old


class CacheConfig(BaseModel):
    path: str = "var/servers.json"


class UpstreamConfig(BaseModel):
    endpoints: list[str]
    timeout_s: float = 20.0
    retries: int = 2
    impersonate: str = "chrome"

    @field_validator("endpoints")
    @classmethod
    def _nonempty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("upstream.endpoints must not be empty")
        return v


class ThrottleConfig(BaseModel):
    min_interval_s: float = 1.0


class BackgroundConfig(BaseModel):
    base_interval_s: float = 60.0
    extend_after_idle_cycles: int = 5
    extended_interval_s: float = 300.0

    @field_validator("extend_after_idle_cycles")
    @classmethod
    def _at_least_one(cls, v: int) -> int:
        if v < 1:
            raise ValueError("background.extend_after_idle_cycles must be >= 1")
        return v


class BypassConfig(BaseModel):
    base_url: str = "http://127.0.0.1:9100"
    timeout_s: float = 30.0
    cookie_ttl_s: float = 1800.0
    auth_token: str | None = None
    # Cloudflare Access Service Token credentials sent as
    # Cf-Access-Client-Id / Cf-Access-Client-Secret (edge-level auth in front of tunnel).
    access_client_id: str | None = None
    access_client_secret: str | None = None


class DataSourceConfig(BaseModel):
    name: str
    type: str
    url: str | None = None
    path: str | None = None
    strategy: str = "append"
    key: str | None = None
    enabled: bool = True
    target_key: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class HealthConfig(BaseModel):
    # No probe loop by design: master health comes from the round-robin refresh
    # poll state, bypass health from on-use. Reserved for future knobs.
    pass


class AppConfig(BaseModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    upstream: UpstreamConfig
    throttle: ThrottleConfig = Field(default_factory=ThrottleConfig)
    background: BackgroundConfig = Field(default_factory=BackgroundConfig)
    bypass: BypassConfig = Field(default_factory=BypassConfig)
    datasources: list[DataSourceConfig] = Field(default_factory=list)
    health: HealthConfig = Field(default_factory=HealthConfig)

    @field_validator("datasources", mode="before")
    @classmethod
    def _datasources_none_to_empty(cls, v):
        # YAML `datasources:` with no content parses to None; treat as empty list.
        if v is None:
            return []
        return v


def load_config(path: str | Path) -> AppConfig:
    """Read + validate a YAML config file into an AppConfig."""
    path = Path(path)
    raw = yaml.safe_load(path.read_text("utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"config file must contain a mapping: {path}")
    return AppConfig.model_validate(raw)


class ConfigManager:
    """Holds the current AppConfig and swaps it atomically on reload.

    In-flight requests keep whatever config object they already read; new
    reads see the new config. Thread-safe via an RLock.
    """

    def __init__(self, path: str | Path, clock=time.time) -> None:
        self._path = Path(path)
        self._clock = clock
        self._lock = threading.RLock()
        self._config = load_config(self._path)
        self._status = "ok"
        self._last_error: str | None = None
        self._reloaded_at: float = clock()

    @property
    def path(self) -> Path:
        return self._path

    @property
    def config(self) -> AppConfig:
        with self._lock:
            return self._config

    @property
    def status(self) -> str:
        with self._lock:
            return self._status

    @property
    def last_error(self) -> str | None:
        with self._lock:
            return self._last_error

    @property
    def reloaded_at(self) -> float:
        with self._lock:
            return self._reloaded_at

    def reload(self) -> bool:
        """Re-read and validate the config file; keep the old config on failure."""
        try:
            new = load_config(self._path)
        except Exception as exc:  # noqa: BLE001 - must keep serving on bad config
            with self._lock:
                self._status = "error"
                self._last_error = f"{type(exc).__name__}: {exc}"
            return False
        with self._lock:
            self._config = new
            self._status = "ok"
            self._last_error = None
            self._reloaded_at = self._clock()
        return True

    def install_sighup_handler(self, loop) -> None:
        """Wire SIGHUP (systemctl reload) to config reload on an asyncio loop."""
        try:
            loop.add_signal_handler(signal.SIGHUP, self.reload)
        except (NotImplementedError, RuntimeError, ValueError):
            # non-main thread or platform without SIGHUP: reload via signal is unavailable
            pass