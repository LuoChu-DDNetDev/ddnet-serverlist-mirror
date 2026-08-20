"""Health aggregation for /api/v1/health."""

from __future__ import annotations

import asyncio
import os
import time
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests

from .cache import CacheStore
from .config import ConfigManager
from .nodriver_client import NodriverClient
from .upstream import EndpointStatus, UpstreamClient


class HealthAggregator:
    def __init__(
        self,
        manager: ConfigManager,
        cache: CacheStore,
        upstream: UpstreamClient,
        bypass: NodriverClient,
        clock=time.time,
        session_factory=None,
    ) -> None:
        self._manager = manager
        self._cache = cache
        self._upstream = upstream
        self._bypass = bypass
        self._clock = clock
        self._started_at = clock()
        self._session = None
        self._session_factory = session_factory
        self._probe_results: dict[str, dict] = {}
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="health-probe")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._session is not None:
            try:
                await self._session.close()
            finally:
                self._session = None

    async def _get_session(self) -> cffi_requests.AsyncSession:
        if self._session is None:
            cfg = self._manager.config
            self._session = (
                self._session_factory()
                if self._session_factory is not None
                else cffi_requests.AsyncSession(
                    impersonate=cfg.upstream.impersonate,
                    timeout=cfg.health.upstream_timeout_s,
                )
            )
        return self._session

    async def _run(self) -> None:
        while True:
            try:
                await self._probe_once()
            except Exception:  # noqa: BLE001 - probe must never kill the loop
                pass
            await asyncio.sleep(self._manager.config.health.probe_interval_s)

    async def _probe_once(self) -> None:
        session = await self._get_session()
        timeout = self._manager.config.health.upstream_timeout_s
        for url in self._upstream.endpoints:
            host = urlparse(url).netloc
            ts = self._clock()
            ok = False
            error: str | None = None
            try:
                resp = await session.get(url, timeout=timeout, allow_redirects=True)
                ok = resp.status_code == 200
                if not ok:
                    error = f"HTTP {resp.status_code}"
            except Exception as exc:  # noqa: BLE001
                error = f"{type(exc).__name__}: {exc}"
            latency_ms = round((self._clock() - ts) * 1000.0, 1)
            self._probe_results[host] = {
                "ok": ok,
                "latency_ms": latency_ms,
                "last_error": error,
                "last_probe_ts": self._clock(),
            }

        ok, latency, err = await self._bypass.check_health()
        self._probe_results["__bypass__"] = {
            "ok": ok,
            "latency_ms": latency,
            "last_error": err,
            "last_probe_ts": self._clock(),
        }

    def snapshot(self) -> dict:
        self._upstream._sync_endpoints()
        upstream_status: dict[str, dict] = {}
        for url in self._upstream.endpoints:
            host = urlparse(url).netloc
            st = self._upstream.statuses.get(url)
            if st is None:  # endpoint never fetched yet (e.g. just added on reload)
                st = EndpointStatus(url=url, host=host)
            entry = {
                "ok": st.ok,
                "latency_ms": st.latency_ms,
                "last_error": st.last_error,
                "last_ok_ts": st.last_ok_ts,
                "last_try_ts": st.last_try_ts,
                "tries": st.tries,
            }
            probe = self._probe_results.get(host)
            if probe:
                entry["probe"] = probe
            upstream_status[host] = entry

        any_up_ok = any(s["ok"] for s in upstream_status.values())
        cache_ok = self._cache.exists()
        if cache_ok and any_up_ok:
            overall = "running"
        elif cache_ok:
            overall = "degraded"  # serving stale cache, upstream unreachable
        else:
            overall = "error"

        return {
            "status": overall,
            "process": {
                "pid": os.getpid(),
                "started_at": self._started_at,
                "uptime_seconds": round(self._clock() - self._started_at, 1),
            },
            "cache": {
                "exists": cache_ok,
                "updated_at": self._cache.mtime(),
                "size_bytes": self._cache.size(),
            },
            "upstream": upstream_status,
            "bypass": dict(self._bypass.health),
            "config": {
                "status": self._manager.status,
                "reloaded_at": self._manager.reloaded_at,
                "path": str(self._manager.path),
                "last_error": self._manager.last_error,
            },
        }