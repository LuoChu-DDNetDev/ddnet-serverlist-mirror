"""Per-client rate limiting and global concurrency shedding.

Hand-rolled token bucket instead of an extra dependency: the state is a single
dict of (tokens, last_seen) per client key, capped in size so a flood of source
addresses cannot grow it without bound.

A 1.2 MiB response per request makes shedding load a self-preservation measure,
not a nicety: without a concurrency cap the process happily buffers hundreds of
copies of the payload.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from . import metrics
from .config import ConfigManager, LimitsConfig


class TokenBucketLimiter:
    """Token bucket per client key, with a bounded number of tracked keys."""

    def __init__(self, cfg: Callable[[], LimitsConfig], clock=time.monotonic) -> None:
        self._cfg = cfg
        self._clock = clock
        self._buckets: dict[str, tuple[float, float]] = {}  # key -> (tokens, last_ts)

    def allow(self, key: str) -> bool:
        cfg = self._cfg()
        now = self._clock()
        tokens, last = self._buckets.get(key, (cfg.burst, now))
        tokens = min(cfg.burst, tokens + (now - last) * cfg.rps)
        if tokens < 1.0:
            self._buckets[key] = (tokens, now)
            return False
        self._prune(cfg, now)
        self._buckets[key] = (tokens - 1.0, now)
        return True

    def _prune(self, cfg: LimitsConfig, now: float) -> None:
        if len(self._buckets) < cfg.max_tracked_clients:
            return
        # Drop the keys that have been idle longest; they are at full tokens anyway.
        stale = sorted(self._buckets.items(), key=lambda kv: kv[1][1])
        for key, _ in stale[: max(1, len(stale) // 4)]:
            self._buckets.pop(key, None)

    def client_key(self, request: Request) -> str:
        cfg = self._cfg()
        if cfg.trust_forwarded_for:
            fwd = request.headers.get("x-forwarded-for", "")
            if fwd:
                return fwd.split(",")[0].strip()
        return request.client.host if request.client else "unknown"


class LimitsMiddleware(BaseHTTPMiddleware):
    """Rate limit per client, then cap total in-flight requests."""

    def __init__(self, app, manager: ConfigManager) -> None:
        super().__init__(app)
        self._manager = manager
        self._limiter = TokenBucketLimiter(lambda: manager.config.limits)
        self._semaphores: dict[int, asyncio.Semaphore] = {}  # keyed by configured size

    def _cfg(self) -> LimitsConfig:
        return self._manager.config.limits

    def _semaphore(self, size: int) -> asyncio.Semaphore:
        # Re-created when the configured size changes (SIGHUP).
        sem = self._semaphores.get(size)
        if sem is None:
            self._semaphores = {size: asyncio.Semaphore(size)}
            sem = self._semaphores[size]
        return sem

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable]
    ) -> JSONResponse:
        cfg = self._cfg()
        if not cfg.enabled or request.url.path in cfg.exempt_paths:
            return await call_next(request)

        if not self._limiter.allow(self._limiter.client_key(request)):
            metrics.rate_limited_total.labels(reason="rps").inc()
            retry_after = max(1, int(1.0 / cfg.rps)) if cfg.rps > 0 else 1
            return JSONResponse(
                {"detail": "rate limit exceeded"},
                status_code=429,
                headers={"Retry-After": str(retry_after)},
            )

        sem = self._semaphore(cfg.max_concurrency)
        if sem.locked():
            metrics.rate_limited_total.labels(reason="concurrency").inc()
            return JSONResponse(
                {"detail": "server busy"}, status_code=503, headers={"Retry-After": "1"}
            )
        async with sem:
            return await call_next(request)
