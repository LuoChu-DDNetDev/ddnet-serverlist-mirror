"""Upstream fetching: round-robin load balancing, CF challenge detection, cookie retry."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests

from . import metrics
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
    consec_fails: int = 0  # consecutive failed fetches since the last success
    down_until: float | None = None  # skip this endpoint in rotation until this time
    latency_ms: float | None = None
    last_error: str | None = None
    last_ok_ts: float | None = None
    last_try_ts: float | None = None
    challenges: int = 0  # challenges seen
    challenges_persisted: int = 0  # still challenged after a bypass solve (egress mismatch?)


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

    `cookie_provider` lets a previously solved Cloudflare clearance be attached
    to the *first* request for a host (instead of always eating a challenge
    first), together with the egress proxy those cookies are bound to.
    """

    def __init__(
        self,
        cfg: UpstreamConfig | Callable[[], UpstreamConfig],
        clock=time.time,
        session_factory: Callable[[], cffi_requests.AsyncSession] | None = None,
        cookie_provider: Callable[[str], object | None] | None = None,
        challenge_hook: Callable[[str, object], None] | None = None,
    ) -> None:
        self._cfg: Callable[[], UpstreamConfig] = cfg if callable(cfg) else lambda: cfg
        self._clock = clock
        self._cursor = 0
        self._session: cffi_requests.AsyncSession | None = None
        self._session_key: tuple | None = None
        self._session_factory = session_factory
        self._cookie_provider = cookie_provider
        self._challenge_hook = challenge_hook
        self._statuses: dict[str, EndpointStatus] = {}
        self._sync_endpoints()

    def _sync_endpoints(self) -> None:
        """Rebuild the endpoint map if the config's endpoint list changed."""
        self._statuses = {
            url: self._statuses.get(url) or EndpointStatus(url=url, host=urlparse(url).netloc)
            for url in self._cfg().endpoints
        }

    # Public alias: callers outside this module should not poke the private one.
    sync_endpoints = _sync_endpoints

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
        metrics.upstream_fetch_total.labels(host=st.host, result="ok").inc()
        metrics.upstream_latency.labels(host=st.host).observe(latency_ms / 1000.0)

    def _mark_attempt_fail(self, url: str, err: Exception) -> None:
        """One failed attempt. Retries inside a fetch land here, not in consec_fails."""
        st = self._statuses[url]
        st.total += 1
        st.fails += 1
        st.ok = False
        st.last_error = f"{type(err).__name__}: {err}"
        st.last_try_ts = self._clock()
        metrics.upstream_fetch_total.labels(host=st.host, result="fail").inc()

    def _bump_consec_fail(self, url: str) -> None:
        """One failed *fetch* for this endpoint (all its retries exhausted)."""
        st = self._statuses[url]
        st.consec_fails += 1
        if st.consec_fails >= self._cfg().down_after_fails:
            st.down_until = self._clock() + self._cfg().down_retry_after_s

    def _mark_fail(self, url: str, err: Exception) -> None:
        """Attempt failure that also counts as an endpoint-level (fetch) failure."""
        self._mark_attempt_fail(url, err)
        self._bump_consec_fail(url)

    async def _get_session(self) -> cffi_requests.AsyncSession:
        cfg = self._cfg()
        key = (cfg.impersonate, cfg.timeout_s)
        if self._session is not None and self._session_key != key:
            # SIGHUP changed the fingerprint/timeout: the old session would keep
            # using the values it was built with.
            await self.close()
        if self._session is None:
            if self._session_factory is not None:
                self._session = self._session_factory()
            else:
                self._session = cffi_requests.AsyncSession(
                    impersonate=cfg.impersonate,
                    timeout=cfg.timeout_s,
                )
            self._session_key = key
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
        spreads across the masters. An endpoint marked down (consecutive failed
        fetches >= threshold) is skipped unless it is due for a re-probe or is
        the only candidate left. The whole call is capped by
        `upstream.total_budget_s` so one request cannot cost
        endpoints * (1 + retries) * timeout_s.
        """
        self._sync_endpoints()
        urls = self.endpoints
        start = self._next_start()
        ordered = urls[start:] + urls[:start]
        retries = self._cfg().retries
        errors: list[Exception] = []
        now = self._clock()
        deadline = now + self._cfg().total_budget_s

        candidates = [
            u for u in ordered
            if self._statuses[u].down_until is None or self._statuses[u].down_until <= now
        ]
        if not candidates:
            # everything is down (or skipped); force a full re-probe of the rotation
            candidates = ordered

        for url in candidates:
            failed = False
            for _ in range(1 + retries):
                remaining = deadline - self._clock()
                if remaining <= 0:
                    if failed:
                        self._bump_consec_fail(url)
                    errors.append(
                        UpstreamError(f"total budget {self._cfg().total_budget_s}s exhausted")
                    )
                    raise AggregateUpstreamError(errors)
                try:
                    result = await self._attempt(url, bypass_solver, budget_s=remaining)
                except UpstreamError as exc:
                    errors.append(exc)
                    self._mark_attempt_fail(url, exc)
                    failed = True
                else:
                    return result
            if failed:
                # All retries for this endpoint used up: one endpoint-level failure.
                self._bump_consec_fail(url)
        raise AggregateUpstreamError(errors)

    @staticmethod
    def _clearance_kwargs(solved: object) -> dict:
        """Request kwargs that replay a solved clearance from the right egress."""
        cookies = getattr(solved, "cookies", None) or {}
        kwargs: dict = {"cookies": dict(cookies)}
        ua = getattr(solved, "user_agent", "") or ""
        if ua:
            kwargs["headers"] = {"User-Agent": ua}
        proxy = getattr(solved, "proxy_url", None)
        if proxy:
            # cf_clearance is bound to the solving peer's egress IP.
            kwargs["proxy"] = proxy
        return kwargs

    async def _attempt(
        self,
        url: str,
        bypass_solver: Callable[[str, str], Awaitable[object]] | None = None,
        budget_s: float | None = None,
    ) -> FetchResult:
        ts0 = self._clock()
        session = await self._get_session()
        host = urlparse(url).netloc
        st = self._statuses[url]
        kwargs: dict = {}
        if budget_s is not None:
            # Never wait longer than what is left of the whole-fetch budget.
            kwargs["timeout"] = min(self._cfg().timeout_s, max(budget_s, 0.001))

        warm = self._cookie_provider(host) if self._cookie_provider is not None else None
        if warm is not None and getattr(warm, "cookies", None):
            # Carry the clearance we already have instead of eating a challenge first.
            kwargs.update(self._clearance_kwargs(warm))

        try:
            resp = await session.get(url, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - treat transport errors as endpoint failure
            raise UpstreamError(f"transport error on {host}: {type(exc).__name__}: {exc}") from exc

        if _is_challenge(resp):
            st.challenges += 1
            metrics.upstream_challenge_total.labels(host=host, result="seen").inc()
            if warm is not None and self._challenge_hook is not None:
                # The cached clearance was rejected; drop it before solving again.
                self._challenge_hook(host, warm)
            if bypass_solver is None:
                raise UpstreamError(f"cloudflare challenge, no bypass solver configured: {host}")
            try:
                solved = await bypass_solver(host, url)
            except Exception as exc:  # noqa: BLE001
                raise UpstreamError(f"bypass solver failed for {host}: {exc}") from exc
            if not (getattr(solved, "cookies", None) or {}):
                raise UpstreamError(f"bypass solved without cookies: {host}")
            retry_kwargs = dict(kwargs)
            retry_kwargs.pop("cookies", None)
            retry_kwargs.pop("headers", None)
            retry_kwargs.pop("proxy", None)
            retry_kwargs.update(self._clearance_kwargs(solved))
            try:
                resp = await session.get(url, **retry_kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise UpstreamError(
                    f"cookie retry transport error on {host}: {type(exc).__name__}: {exc}"
                ) from exc
            if _is_challenge(resp):
                st.challenges_persisted += 1
                metrics.upstream_challenge_total.labels(host=host, result="persisted").inc()
                if self._challenge_hook is not None:
                    # Blame that peer so the next attempt solves via the next one.
                    self._challenge_hook(host, solved)
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