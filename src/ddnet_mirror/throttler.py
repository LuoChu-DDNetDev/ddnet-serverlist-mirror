"""Refresh orchestration: throttle gate, single-flight coalescing, background backoff.

Rules:
- `throttle.min_interval_s` (default 3s) is a hard floor between two upstream
  fetches. The gate is keyed on the timestamp of the last *upstream attempt*,
  not on the cache mtime: the cache mtime is written by the refresh itself, so
  using it as the trigger made every request look "fresh enough to prefetch"
  and produced one upstream fetch per request.
- An immediate request inside the gate is answered from cache and counted in
  `gated_requests`. A request that finds a refresh already in flight joins it
  (single-flight) instead of starting another one.
- An immediate request outside the gate refreshes synchronously, but waits at
  most `throttle.immediate_budget_s`; on timeout the old cache is served while
  the refresh keeps running.
- Background refresh starts at `base_interval_s` (1min). After N consecutive
  intervals with no immediate request, back off to `extended_interval_s` (5min).
  An immediate request resets the idleness counters and restores the base cadence.

All wall-clock access goes through an injectable `clock` for deterministic tests.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable

from . import metrics
from .cache import CacheSnapshot, CacheStore
from .config import BackgroundConfig, ConfigManager, ThrottleConfig

logger = logging.getLogger(__name__)


def _consume(fut: asyncio.Future) -> None:
    """Retrieve a future's exception so asyncio does not log it as never-retrieved."""
    if not fut.cancelled():
        fut.exception()


class RefreshOrchestrator:
    def __init__(
        self,
        manager: ConfigManager,
        cache: CacheStore,
        refresher: Callable[[], Awaitable[None]],
        clock=time.time,
        rng: random.Random | None = None,
    ) -> None:
        self._manager = manager
        self._cache = cache
        self._refresher = refresher
        self._clock = clock
        self._rng = rng or random.Random()
        self._in_flight: asyncio.Future | None = None
        self._idle_cycles = 0
        self._last_immediate_ts: float | None = None
        self._last_upstream_ts: float | None = None
        self._current_interval = manager.config.background.base_interval_s
        self._bg_task: asyncio.Task | None = None
        self._pending: asyncio.Task | None = None  # refresh outliving its waiter
        self.last_refresh_ok_ts: float | None = None
        self.last_refresh_error: str | None = None
        self.upstream_attempts = 0  # refreshes that actually reached the refresher
        self.gated_requests = 0  # immediate requests answered without upstream

    # ------------------------------------------------------------------ helpers
    def _bg(self) -> BackgroundConfig:
        return self._manager.config.background

    def _throttle(self) -> ThrottleConfig:
        return self._manager.config.throttle

    # ------------------------------------------------------------- public API
    async def get_cached(self) -> bytes | None:
        """Serve the current cache; never touches upstream."""
        return self._cache.read_raw()

    def snapshot_cached(self) -> CacheSnapshot | None:
        """Current cache with ETag/gzip variants; never touches upstream."""
        return self._cache.snapshot()

    async def get_immediate(self) -> bytes | None:
        """Serve cache; only reach upstream when the throttle gate is open."""
        await self._ensure_immediate()
        return self._cache.read_raw()

    async def snapshot_immediate(self) -> CacheSnapshot | None:
        await self._ensure_immediate()
        return self._cache.snapshot()

    async def _ensure_immediate(self) -> None:
        now = self._clock()
        self._mark_immediate(now)
        window = self._throttle().min_interval_s

        mtime = self._cache.mtime()
        if mtime is not None and (now - mtime) < window:
            self.gated_requests += 1  # cache younger than the window: nothing to gain
            metrics.throttle_gated_total.inc()
            return

        if self._in_flight is None and self._gate_closed(now, window):
            self.gated_requests += 1
            metrics.throttle_gated_total.inc()
            return

        # Either the gate is open (start a fetch) or one is already in flight
        # (join it, which costs no extra upstream request).
        await self._refresh_bounded()

    def _gate_closed(self, now: float, window: float) -> bool:
        last = self._last_upstream_ts
        return last is not None and (now - last) < window

    def _mark_immediate(self, now: float) -> None:
        self._last_immediate_ts = now
        self._idle_cycles = 0
        self._current_interval = self._bg().base_interval_s

    async def _refresh_bounded(self) -> None:
        """Coalesced refresh, but never block the caller past `immediate_budget_s`."""
        budget = self._throttle().immediate_budget_s
        if budget is None or budget <= 0:
            await self._coalesced_refresh()
            return
        task = asyncio.ensure_future(self._coalesced_refresh())
        self._pending = task
        try:
            await asyncio.wait_for(asyncio.shield(task), budget)
        except TimeoutError:
            # Keep the fetch running for whoever asks next; serve the old cache now.
            logger.warning("immediate refresh over %.1fs budget; serving cache", budget)
            task.add_done_callback(_consume)

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
        fut.add_done_callback(_consume)
        self._in_flight = fut
        # Close the gate before the fetch starts, so a failing upstream cannot be
        # hammered by a burst of requests either.
        self._last_upstream_ts = self._clock()
        self.upstream_attempts += 1
        try:
            try:
                await self._refresher()
                self.last_refresh_ok_ts = self._clock()
                self.last_refresh_error = None
                metrics.refresh_total.labels(result="ok").inc()
            except Exception as exc:  # noqa: BLE001
                self.last_refresh_error = f"{type(exc).__name__}: {exc}"
                logger.error("refresh failed: %s", exc)
                metrics.refresh_total.labels(result="fail").inc()
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
        for task in (self._bg_task, self._pending):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001 - shutdown path, already logged
                pass
        self._bg_task = None
        self._pending = None

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
            await asyncio.sleep(self._jittered(interval))
            self._idle_cycles, self._current_interval = self._decide(
                self._clock(), self._last_immediate_ts, window=interval
            )
            if self._gate_closed(self._clock(), self._throttle().min_interval_s):
                continue  # an immediate request just refreshed; keep the floor global
            try:
                await self._coalesced_refresh()
            except Exception:  # noqa: BLE001 - already logged inside coalesce
                pass

    def _jittered(self, interval: float) -> float:
        """Sleep duration with +/-jitter fraction to mask the polling rhythm."""
        j = self._bg().jitter
        return interval * (1.0 + (self._rng.random() * 2 - 1) * j)