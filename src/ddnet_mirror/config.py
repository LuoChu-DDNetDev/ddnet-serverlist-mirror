"""Configuration models and hot-reload manager (SIGHUP)."""

from __future__ import annotations

import signal
import threading
import time
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    """Base for every config model: unknown keys are an error, not a silent no-op.

    A typo like `min_intervals_s` used to be ignored and fall back to the default,
    which is indistinguishable from "the setting had no effect".
    """

    model_config = ConfigDict(extra="forbid")


class ServerConfig(StrictModel):
    host: str = "0.0.0.0"
    port: int = 8080


class LoggingConfig(StrictModel):
    level: str = "INFO"
    dir: str = "logs"
    max_bytes: int = 5 * 1024 * 1024  # rotate a single file once it exceeds this
    max_age_seconds: float = 7 * 24 * 3600  # rotate a file once it is this old
    # File name template and the stamp inserted for the startup timestamp.
    filename: str = "mirror_{stamp}.log"
    timestamp_format: str = "%Y-%m-%d_%H-%M-%S"


class CacheConfig(StrictModel):
    path: str = "var/servers.json"
    # Keep a gzip copy of the cached body in memory so every client does not pay
    # a fresh 1.2 MiB transfer or a per-request compression pass.
    precompress: bool = True
    gzip_level: int = 6


class UpstreamConfig(StrictModel):
    endpoints: list[str]
    timeout_s: float = 20.0
    retries: int = 2
    impersonate: str = "chrome"
    # Hard ceiling for one whole fetch() across every endpoint and retry. Without
    # it a fetch could cost endpoints * (1 + retries) * timeout_s (240s by default).
    total_budget_s: float = 25.0
    # After this many *consecutive failed fetches* an endpoint is removed from the
    # round-robin rotation (so a dead master stops eating the full timeout).
    # Retries inside one fetch count as a single failure for this counter.
    down_after_fails: int = 2
    # Seconds to wait before re-probing a downed endpoint to see if it recovered.
    down_retry_after_s: float = 60.0

    @field_validator("endpoints")
    @classmethod
    def _nonempty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("upstream.endpoints must not be empty")
        return v

    @model_validator(mode="after")
    def _budget_covers_one_attempt(self) -> UpstreamConfig:
        if self.total_budget_s < self.timeout_s:
            raise ValueError(
                "upstream.total_budget_s must be >= upstream.timeout_s "
                f"({self.total_budget_s} < {self.timeout_s})"
            )
        return self


class ThrottleConfig(StrictModel):
    # Single window controlling both: (a) the fastest one upstream re-request and
    # (b) cache freshness. A request inside the window is answered from cache and
    # never reaches upstream; see RefreshOrchestrator.get_immediate.
    min_interval_s: float = 3.0
    # How long an immediate request may wait on a synchronous refresh before it
    # gives up and serves the old cache. The refresh itself is not cancelled.
    immediate_budget_s: float = 5.0


class BackgroundConfig(StrictModel):
    base_interval_s: float = 60.0
    extend_after_idle_cycles: int = 5
    extended_interval_s: float = 300.0
    # Fraction (+/-) jitter applied to background sleep to mask the polling rhythm.
    jitter: float = 0.1

    @field_validator("extend_after_idle_cycles")
    @classmethod
    def _at_least_one(cls, v: int) -> int:
        if v < 1:
            raise ValueError("background.extend_after_idle_cycles must be >= 1")
        return v


class BypassPeerConfig(StrictModel):
    """One bypass machine (Service B) plus the egress that machine's cookies are valid for."""

    name: str
    base_url: str = "http://127.0.0.1:9100"
    # `cf_clearance` is bound to the egress IP that solved it, so the cookie replay
    # has to leave from this peer's network. Point this at an HTTP/SOCKS proxy on
    # (or routed through) the peer. Only the bypass path uses it; normal fetches
    # go out directly from service A.
    proxy_url: str | None = None
    timeout_s: float = 30.0
    cookie_ttl_s: float = 1800.0
    auth_token: str | None = None
    # Cloudflare Access Service Token credentials sent as
    # Cf-Access-Client-Id / Cf-Access-Client-Secret (edge-level auth in front of tunnel).
    access_client_id: str | None = None
    access_client_secret: str | None = None
    enabled: bool = True


# Pre-peers configs had these directly under `bypass:`; folded into one peer.
_LEGACY_PEER_KEYS = (
    "base_url",
    "proxy_url",
    "timeout_s",
    "cookie_ttl_s",
    "auth_token",
    "access_client_id",
    "access_client_secret",
)


class BypassConfig(StrictModel):
    # Tried in order: the first peer that solves wins, the rest are fallbacks.
    peers: list[BypassPeerConfig] = Field(default_factory=list)
    # A peer that fails this many times in a row is skipped for down_retry_after_s.
    down_after_fails: int = 2
    down_retry_after_s: float = 60.0

    @model_validator(mode="before")
    @classmethod
    def _fold_legacy_flat_peer(cls, v: Any) -> Any:
        if not isinstance(v, dict):
            return v
        legacy = {k: v[k] for k in _LEGACY_PEER_KEYS if k in v}
        if not legacy:
            return v
        if v.get("peers"):
            raise ValueError("bypass: use either `peers` or the legacy flat fields, not both")
        folded = {k: val for k, val in v.items() if k not in _LEGACY_PEER_KEYS}
        folded["peers"] = [{"name": "default", **legacy}]
        return folded

    @property
    def enabled_peers(self) -> list[BypassPeerConfig]:
        return [p for p in self.peers if p.enabled]


class DataSourceConfig(StrictModel):
    name: str
    type: Literal["http", "file", "db"]
    url: str | None = None
    path: str | None = None
    strategy: Literal["append", "merge", "override", "additional"] = "append"
    key: str | None = None
    enabled: bool = True
    target_key: str | None = None
    extra: dict[str, Any] = Field(default_factory=dict)


class HealthConfig(StrictModel):
    # No probe loop against the masters by design: health for them comes from the
    # round-robin refresh poll state. Bypass peers are probed on demand (TTL-cached).
    # Without a token /health only exposes a minimal, non-identifying subset.
    auth_token: str | None = None
    probe_ttl_s: float = 30.0


class MetricsConfig(StrictModel):
    enabled: bool = True
    path: str = "/metrics"
    # Defaults to health.auth_token when unset; set explicitly to use another one.
    auth_token: str | None = None


class LimitsConfig(StrictModel):
    enabled: bool = True
    rps: float = 5.0  # sustained requests per second per client
    burst: float = 20.0  # bucket size
    max_concurrency: int = 64  # in-flight requests before shedding with 503
    # Only enable behind a trusted reverse proxy, otherwise clients can spoof it.
    trust_forwarded_for: bool = False
    exempt_paths: list[str] = Field(default_factory=lambda: ["/health", "/metrics"])
    max_tracked_clients: int = 10000


class AppConfig(StrictModel):
    server: ServerConfig = Field(default_factory=ServerConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    upstream: UpstreamConfig
    throttle: ThrottleConfig = Field(default_factory=ThrottleConfig)
    background: BackgroundConfig = Field(default_factory=BackgroundConfig)
    bypass: BypassConfig = Field(default_factory=BypassConfig)
    datasources: list[DataSourceConfig] = Field(default_factory=list)
    health: HealthConfig = Field(default_factory=HealthConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    limits: LimitsConfig = Field(default_factory=LimitsConfig)

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
        except (RuntimeError, ValueError):
            # non-main thread or platform without SIGHUP: reload via signal is
            # unavailable (NotImplementedError is a RuntimeError subclass).
            pass