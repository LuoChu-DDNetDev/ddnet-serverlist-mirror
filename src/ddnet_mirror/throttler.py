"""Refresh orchestration: min-interval throttle, single-flight coalescing, background backoff.

Rules (from spec):
- Fastest one upstream re-request per `min_interval_s` (default 1s). A request
  within 1s of the last cache write is served straight from cache.
- Concurrent immediate requests coalesce into a single upstream fetch.
- Background refresh starts at `base_interval_s` (1min). After N consecutive
  intervals with no immediate request, back off to `extended_interval_s` (5min).
  An immediate request resets the idleness counters and restores the base cadence.

All wall-clock access goes through an injectable `clock` for deterministic tests.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from .cache import CacheStore
from .config import BackgroundConfig, ConfigManager, ThrottleConfig

logger = logging.getLogger(__name__)


class RefreshOrchestrator:
    def __init__(
        self,
        manager: ConfigManager,
        cache: CacheStore,
        refresher: Callable[[], Awaitable[None]],
        clock=time.time,
    ) -> None:
        self._manager = manager
        self._cache = cache
        self._refresher = refresher
        self._clock = clock
        self._in_flight: asyncio.Future | None = None
        self._idle_cycles = 0
        self._last_immediate_ts: float | None = None
        self._current_interval = manager.config.background.base_interval_s
        self._bg_task: asyncio.Task | None = None
        self.last_refresh_ok_ts: float | None = None
        self.last_refresh_error: str | None = None

    # ------------------------------------------------------------------ helpers
    def _bg(self) -> BackgroundConfig:
        return self._manager.config.background

    def _throttle(self) -> ThrottleConfig:
        return self._manager.config.throttle

    # ------------------------------------------------------------- public API
    async def get_cached(self) -> bytes | None:
        """Serve the current cache; never touches upstream."""
        return self._cache.read_raw()

    async def get_immediate(self) -> bytes | None:
        """Serve cache; if it is stale by more than the min interval, refresh (coalesced)."""
        now = self._clock()
        self._mark_immediate(now)
        min_interval = self._throttle().min_interval_s
        mtime = self._cache.mtime()
        if mtime is not None and (now - mtime) < min_interval:
            # Cache is fresher than the throttle window: no upstream call.
            return self._cache.read_raw()
        await self._coalesced_refresh()
        return self._cache.read_raw()

    def _mark_immediate(self, now: float) -> None:
        self._last_immediate_ts = now
        self._idle_cycles = 0
        self._current_interval = self._bg().base_interval_s

    # ------------------------------------------------------------ single-flight
    async def _coalesced_refresh(self) -> None:
        fut = self._in_flight
        if fut is not None:
            # Another caller is already refreshing: share that fetch.
            try:
                await asyncio.shield(fut)
            except Exception:  # noqa: BLE001 - failure reported by the owner
                pass
            return

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._in_flight = fut
        try:
            try:
                await self._refresher()
                self.last_refresh_ok_ts = self._clock()
                self.last_refresh_error = None
            except Exception as exc:  # noqa: BLE001
                self.last_refresh_error = f"{type(exc).__name__}: {exc}"
                logger.error("refresh failed: %s", exc)
                fut.set_exception(exc)
            else:
                fut.set_result(None)
        finally:
            self._in_flight = None

    # -------------------------------------------------------------- background
    async def start(self) -> None:
        if self._cache.mtime() is None:
            logger.info("no cache on start; refreshing once to warm up")
            await self._coalesced_refresh()
        self._bg_task = asyncio.create_task(self._run_background(), name="refresh-background")
        logger.info(
            "background refresh started (%.0fs base / %.0fs idle-extended)",
            self._bg().base_interval_s, self._bg().extended_interval_s
        )

    async def stop(self) -> None:
        if self._bg_task is not None:
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass
            self._bg_task = None

    def _decide(self, now: float, last_immediate: float | None, window: float) -> tuple[int, float]:
        """Compute (idle_cycles, next_interval) for a completed background window.

        Pure decision logic, exposed for deterministic tests.
        """
        bg = self._bg()
        if last_immediate is not None and (now - last_immediate) < window:
            idle = 0  # an immediate request arrived within this window
        else:
            idle = self._idle_cycles + 1
        if idle >= bg.extend_after_idle_cycles:
            return idle, bg.extended_interval_s
        return idle, bg.base_interval_s

    async def _run_background(self) -> None:
        while True:
            interval = self._current_interval
            await asyncio.sleep(interval)
            self._idle_cycles, self._current_interval = self._decide(
                self._clock(), self._last_immediate_ts, window=interval
            )
            try:
                await self._coalesced_refresh()
            except Exception:  # noqa: BLE001 - already logged inside coalesce
                pass