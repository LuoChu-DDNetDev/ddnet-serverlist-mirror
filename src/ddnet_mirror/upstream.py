"""Upstream fetching: round-robin load balancing, CF challenge detection, cookie retry."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests

from .config import UpstreamConfig

# Substrings that indicate a Cloudflare interstitial (human or bot challenge).
CHALLENGE_MARKERS = (
    "__cf_chl_",
    "challenge-platform",
    "cf-browser-verification",
    "cf-chl-widget",
    "cf_chl_opt",
    "just a moment",
    "checking your browser",
    "attention required",
)


class UpstreamError(Exception):
    """Known failure for a single upstream endpoint attempt."""


class AggregateUpstreamError(UpstreamError):
    """All endpoints failed within the total retry budget."""

    def __init__(self, errors: list[Exception]) -> None:
        self.errors = errors
        parts = " / ".join(f"{type(e).__name__}: {e}" for e in errors[:8])
        super().__init__(f"all upstream endpoints failed: {parts}")


@dataclass
class FetchResult:
    data: bytes
    url: str
    host: str


@dataclass
class EndpointStatus:
    url: str
    host: str
    ok: bool = False
    total: int = 0  # total attempts against this endpoint
    fails: int = 0  # failed attempts against this endpoint
    consec_fails: int = 0  # consecutive failures since the last success
    down_until: float | None = None  # skip this endpoint in rotation until this time
    latency_ms: float | None = None
    last_error: str | None = None
    last_ok_ts: float | None = None
    last_try_ts: float | None = None


def _is_challenge(resp) -> bool:
    """Heuristic Cloudflare challenge detection (header + body markers)."""
    if resp.headers.get("cf-mitigated") == "challenge":
        return True
    if resp.status_code in (403, 503, 429):
        body = (getattr(resp, "text", "") or "")[:4096].lower()
        if any(marker in body for marker in CHALLENGE_MARKERS):
            return True
    return False


class UpstreamClient:
    """Round-robin client across the configured upstream endpoints.

    Config is read lazily via a provider callable so hot reloads (SIGHUP)
    take effect on the next fetch without restarting the service.
    """

    def __init__(
        self,
        cfg: UpstreamConfig | Callable[[], UpstreamConfig],
        clock=time.time,
        session_factory: Callable[[], cffi_requests.AsyncSession] | None = None,
    ) -> None:
        self._cfg: Callable[[], UpstreamConfig] = cfg if callable(cfg) else lambda: cfg
        self._clock = clock
        self._cursor = 0
        self._session: cffi_requests.AsyncSession | None = None
        self._session_factory = session_factory
        self._statuses: dict[str, EndpointStatus] = {}
        self._sync_endpoints()

    def _sync_endpoints(self) -> None:
        """Rebuild the endpoint map if the config's endpoint list changed."""
        self._statuses = {
            url: self._statuses.get(url) or EndpointStatus(url=url, host=urlparse(url).netloc)
            for url in self._cfg().endpoints
        }

    @property
    def cfg(self) -> UpstreamConfig:
        return self._cfg()

    @property
    def endpoints(self) -> list[str]:
        # Live from config so hot reloads (SIGHUP) are visible immediately.
        return list(self._cfg().endpoints)

    @property
    def statuses(self) -> dict[str, EndpointStatus]:
        return self._statuses

    async def close(self) -> None:
        if self._session is not None:
            try:
                await self._session.close()
            finally:
                self._session = None

    def _mark_ok(self, url: str, latency_ms: float) -> None:
        st = self._statuses[url]
        st.total += 1
        st.ok = True
        st.consec_fails = 0
        st.down_until = None
        st.latency_ms = latency_ms
        st.last_error = None
        st.last_ok_ts = self._clock()
        st.last_try_ts = self._clock()

    def _mark_fail(self, url: str, err: Exception) -> None:
        st = self._statuses[url]
        st.total += 1
        st.fails += 1
        st.consec_fails += 1
        st.ok = False
        down_after = self._cfg().down_after_fails
        if st.consec_fails >= down_after:
            st.down_until = self._clock() + self._cfg().down_retry_after_s
        st.last_error = f"{type(err).__name__}: {err}"
        st.last_try_ts = self._clock()

    async def _get_session(self) -> cffi_requests.AsyncSession:
        if self._session is None:
            cfg = self._cfg()
            if self._session_factory is not None:
                self._session = self._session_factory()
            else:
                self._session = cffi_requests.AsyncSession(
                    impersonate=cfg.impersonate,
                    timeout=cfg.timeout_s,
                )
        return self._session

    def _next_start(self) -> int:
        start = self._cursor % len(self.endpoints)
        self._cursor += 1
        return start

    async def fetch(
        self,
        bypass_solver: Callable[[str, str], Awaitable[object]] | None = None,
    ) -> FetchResult:
        """Load-balanced fetch; returns the first success across endpoints.

        The rotation starts after the previous fetch's start index so load
        spreads across the masters. An endpoint marked down (consecutive-fails
        >= threshold) is skipped unless it is due for a re-probe or is the only
        candidate left.
        """
        self._sync_endpoints()
        urls = self.endpoints
        start = self._next_start()
        ordered = urls[start:] + urls[:start]
        retries = self._cfg().retries
        errors: list[Exception] = []
        now = self._clock()

        candidates = [
            u for u in ordered
            if self._statuses[u].down_until is None or self._statuses[u].down_until <= now
        ]
        if not candidates:
            # everything is down (or skipped); force a full re-probe of the rotation
            candidates = ordered

        for url in candidates:
            for _ in range(1 + retries):
                try:
                    return await self._attempt(url, bypass_solver)
                except UpstreamError as exc:
                    errors.append(exc)
                    self._mark_fail(url, exc)
        raise AggregateUpstreamError(errors)

    async def _attempt(
        self,
        url: str,
        bypass_solver: Callable[[str, str], Awaitable[object]] | None = None,
    ) -> FetchResult:
        ts0 = self._clock()
        session = await self._get_session()
        host = urlparse(url).netloc
        try:
            resp = await session.get(url)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - treat transport errors as endpoint failure
            raise UpstreamError(f"transport error on {host}: {type(exc).__name__}: {exc}") from exc

        if _is_challenge(resp):
            if bypass_solver is None:
                raise UpstreamError(f"cloudflare challenge, no bypass solver configured: {host}")
            try:
                solved = await bypass_solver(host, url)
            except Exception as exc:  # noqa: BLE001
                raise UpstreamError(f"bypass solver failed for {host}: {exc}") from exc
            cookies = getattr(solved, "cookies", None) or {}
            ua = getattr(solved, "user_agent", "") or ""
            if not cookies:
                raise UpstreamError(f"bypass solved without cookies: {host}")
            headers = {"User-Agent": ua} if ua else {}
            try:
                resp = await session.get(url, headers=headers, cookies=dict(cookies))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise UpstreamError(
                    f"cookie retry transport error on {host}: {type(exc).__name__}: {exc}"
                ) from exc
            if _is_challenge(resp):
                raise UpstreamError(f"cloudflare challenge persisted after bypass solve: {host}")

        if resp.status_code != 200:
            raise UpstreamError(f"HTTP {resp.status_code} from {host}")
        body = getattr(resp, "content", b"")
        if not body:
            raise UpstreamError(f"empty body from {host}")
        try:
            resp.json()  # sanity check before we cache anything
        except Exception as exc:  # noqa: BLE001
            raise UpstreamError(f"invalid JSON from {host}: {exc}") from exc
        latency_ms = (self._clock() - ts0) * 1000.0
        self._mark_ok(url, latency_ms)
        return FetchResult(data=body, url=url, host=host)