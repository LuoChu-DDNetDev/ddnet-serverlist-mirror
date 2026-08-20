"""Health aggregation for /api/v1/health.

There is no separate probe loop: hitting an upstream to "check health" would
itself be load on the masters. Instead the reported state *is* the round-robin
poll state — every master's health comes from the last time the background
refresher actually fetched (one master per cycle, rotating). The bypass service
status updates only when it is actually used to solve a challenge.
"""

from __future__ import annotations

import os
import time
from urllib.parse import urlparse

from .cache import CacheStore
from .config import ConfigManager
from .nodriver_client import NodriverClient
from .upstream import EndpointStatus, UpstreamClient


class HealthAggregator:
    """Read-only view over the live poll state; holds no background task."""

    def __init__(
        self,
        manager: ConfigManager,
        cache: CacheStore,
        upstream: UpstreamClient,
        bypass: NodriverClient,
        clock=time.time,
    ) -> None:
        self._manager = manager
        self._cache = cache
        self._upstream = upstream
        self._bypass = bypass
        self._clock = clock
        self._started_at = clock()

    async def start(self) -> None:
        # No probe loop by design — see module docstring.
        return None

    async def stop(self) -> None:
        return None

    def snapshot(self) -> dict:
        self._upstream._sync_endpoints()
        upstream_status: dict[str, dict] = {}
        for url in self._upstream.endpoints:
            host = urlparse(url).netloc
            st = self._upstream.statuses.get(url)
            if st is None:  # endpoint never fetched yet (e.g. just added on reload)
                st = EndpointStatus(url=url, host=host)
            upstream_status[host] = {
                "ok": st.ok,
                "latency_ms": st.latency_ms,
                "last_error": st.last_error,
                "last_ok_ts": st.last_ok_ts,
                "last_try_ts": st.last_try_ts,
                "tries": st.tries,
            }

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