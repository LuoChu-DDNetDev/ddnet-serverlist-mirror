"""Client for the external nodriver bypass services (Service B), as a priority pool.

Several bypass machines can be configured; they are tried in config order and the
first one that solves the challenge wins. Service B only solves the Cloudflare JS
challenge and returns cookies + UA — Service A replays the request through
curl_cffi.

`cf_clearance` is bound to the egress IP that solved it, so a result carries the
`proxy_url` of the peer that produced it: the replay must leave from that peer's
network or Cloudflare will challenge again.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import httpx

from . import metrics
from .config import BypassConfig, BypassPeerConfig

logger = logging.getLogger(__name__)


class BypassError(Exception):
    """Bypass service failed or returned unusable data."""


@dataclass
class BypassResult:
    cookies: dict[str, str]
    user_agent: str
    expires: float | None
    solved_at: float
    peer: str = ""
    # Egress the cookies are valid for; None means "same egress as service A".
    proxy_url: str | None = None
    cookie_ttl_s: float = 1800.0

    def valid_at(self, now: float) -> bool:
        expires = self.expires if self.expires is not None else self.solved_at + self.cookie_ttl_s
        return now < expires


@dataclass
class PeerState:
    name: str
    ok: bool = False
    latency_ms: float | None = None
    last_error: str | None = None
    last_ok_ts: float | None = None
    solves: int = 0
    fails: int = 0
    consec_fails: int = 0
    down_until: float | None = None
    probe: dict = field(default_factory=dict)  # /health probe result, TTL-cached

    def as_dict(self, warn: str | None = None) -> dict:
        out = {
            "ok": self.ok,
            "latency_ms": self.latency_ms,
            "last_error": self.last_error,
            "last_ok_ts": self.last_ok_ts,
            "solves": self.solves,
            "fails": self.fails,
            "consec_fails": self.consec_fails,
            "down_until": self.down_until,
        }
        if self.probe:
            out["probe"] = dict(self.probe)
        if warn:
            out["warn"] = warn
        return out


class NodriverClient:
    """Priority pool over the configured bypass peers."""

    def __init__(
        self,
        cfg: BypassConfig | Callable[[], BypassConfig],
        clock=time.time,
        http_factory: Callable[[], httpx.AsyncClient] | None = None,
    ) -> None:
        # Lazily read config so SIGHUP reloads take effect on the next call.
        self._cfg: Callable[[], BypassConfig] = cfg if callable(cfg) else lambda: cfg
        self._clock = clock
        self._http_factory = http_factory
        self._clients: dict[str, tuple[httpx.AsyncClient, float]] = {}  # name -> (client, timeout)
        self._states: dict[str, PeerState] = {}
        self._cache: dict[str, BypassResult] = {}  # host -> cookies
        self._inflight: dict[str, asyncio.Future] = {}

    # ------------------------------------------------------------------ config
    @property
    def peers(self) -> list[BypassPeerConfig]:
        return self._cfg().enabled_peers

    def _state(self, name: str) -> PeerState:
        st = self._states.get(name)
        if st is None:
            st = self._states[name] = PeerState(name=name)
        return st

    @staticmethod
    def base_url(peer: BypassPeerConfig) -> str:
        return peer.base_url.rstrip("/")

    def _headers(self, peer: BypassPeerConfig) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if peer.auth_token:
            headers["Authorization"] = f"Bearer {peer.auth_token}"
        if peer.access_client_id and peer.access_client_secret:
            headers["CF-Access-Client-Id"] = peer.access_client_id
            headers["CF-Access-Client-Secret"] = peer.access_client_secret
        return headers

    async def _client(self, peer: BypassPeerConfig) -> httpx.AsyncClient:
        """One client per peer, rebuilt when the configured timeout changes (SIGHUP)."""
        entry = self._clients.get(peer.name)
        if entry is not None:
            client, timeout = entry
            if timeout == peer.timeout_s:
                return client
            await client.aclose()
        client = (
            self._http_factory()
            if self._http_factory is not None
            else httpx.AsyncClient(timeout=peer.timeout_s)
        )
        self._clients[peer.name] = (client, peer.timeout_s)
        return client

    async def close(self) -> None:
        for client, _ in self._clients.values():
            await client.aclose()
        self._clients.clear()

    # ------------------------------------------------------------- cookie cache
    def peek(self, host: str) -> BypassResult | None:
        """Unexpired cookies for a host, so the first request can carry them."""
        cached = self._cache.get(host)
        if cached is None:
            return None
        if not cached.valid_at(self._clock()):
            self._cache.pop(host, None)
            return None
        return cached

    def invalidate(self, host: str) -> None:
        self._cache.pop(host, None)

    def on_challenge_persisted(self, host: str, result: object) -> None:
        """Cookies were rejected: drop them and blame the peer that produced them."""
        self.invalidate(host)
        peer = getattr(result, "peer", "") or ""
        if peer:
            self._mark_fail(peer, BypassError(f"cookies rejected for {host}"))
            logger.warning("bypass peer %s: cookies rejected for %s", peer, host)

    # ------------------------------------------------------------ peer bookkeeping
    def _mark_ok(self, name: str, latency_ms: float) -> None:
        st = self._state(name)
        st.ok = True
        st.solves += 1
        st.consec_fails = 0
        st.down_until = None
        st.latency_ms = round(latency_ms, 1)
        st.last_error = None
        st.last_ok_ts = self._clock()
        metrics.bypass_solve_total.labels(peer=name, result="ok").inc()

    def _mark_fail(self, name: str, err: Exception, latency_ms: float | None = None) -> None:
        cfg = self._cfg()
        st = self._state(name)
        st.ok = False
        st.fails += 1
        st.consec_fails += 1
        st.latency_ms = round(latency_ms, 1) if latency_ms is not None else st.latency_ms
        st.last_error = f"{type(err).__name__}: {err}"
        if st.consec_fails >= cfg.down_after_fails:
            st.down_until = self._clock() + cfg.down_retry_after_s
        metrics.bypass_solve_total.labels(peer=name, result="fail").inc()

    def _candidates(self) -> list[BypassPeerConfig]:
        now = self._clock()
        peers = self.peers
        live = [p for p in peers if (self._state(p.name).down_until or 0) <= now]
        return live or peers  # all down: re-probe the whole list rather than give up

    # -------------------------------------------------------------------- solve
    async def solve(self, host: str, url: str) -> BypassResult:
        """Cached cookies, or a fresh solve from the first peer that manages it."""
        cached = self.peek(host)
        if cached is not None:
            return cached

        fut = self._inflight.get(host)
        if fut is not None:
            try:
                return await asyncio.shield(fut)
            except Exception:  # noqa: BLE001 - failed slot, fall through to a fresh solve
                pass

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        fut.add_done_callback(lambda f: f.cancelled() or f.exception())
        self._inflight[host] = fut
        try:
            result = await self._solve_any_peer(host, url)
        except BaseException as exc:
            fut.set_exception(exc)
            raise
        finally:
            self._inflight.pop(host, None)
        fut.set_result(result)
        self._cache[host] = result
        return result

    async def _solve_any_peer(self, host: str, url: str) -> BypassResult:
        peers = self._candidates()
        if not peers:
            raise BypassError("no bypass peers configured")
        errors: list[str] = []
        for peer in peers:
            try:
                return await self._request_solve(peer, host, url)
            except BypassError as exc:
                errors.append(f"{peer.name}: {exc}")
                logger.warning("bypass peer %s failed for %s: %s", peer.name, host, exc)
        raise BypassError(f"all bypass peers failed: {' / '.join(errors)}")

    async def _request_solve(self, peer: BypassPeerConfig, host: str, url: str) -> BypassResult:
        ts0 = self._clock()
        client = await self._client(peer)
        try:
            resp = await client.post(
                f"{self.base_url(peer)}/solve",
                headers=self._headers(peer),
                json={"url": url, "host": host},
            )
        except httpx.HTTPError as exc:
            self._mark_fail(peer.name, exc)
            raise BypassError(f"request failed: {exc}") from exc

        latency_ms = (self._clock() - ts0) * 1000.0
        if resp.status_code != 200:
            err = BypassError(f"HTTP {resp.status_code}")
            self._mark_fail(peer.name, err, latency_ms)
            raise err
        data = resp.json()
        cookies = data.get("cookies") or {}
        if not cookies:
            err = BypassError("no cookies in response")
            self._mark_fail(peer.name, err, latency_ms)
            raise err

        self._mark_ok(peer.name, latency_ms)
        return BypassResult(
            cookies={str(k): str(v) for k, v in cookies.items()},
            user_agent=str(data.get("user_agent") or ""),
            expires=float(data["expires"]) if data.get("expires") is not None else None,
            solved_at=self._clock(),
            peer=peer.name,
            proxy_url=peer.proxy_url,
            cookie_ttl_s=peer.cookie_ttl_s,
        )

    # ------------------------------------------------------------------- health
    async def check_health(self, peer: BypassPeerConfig) -> tuple[bool, float | None, str | None]:
        ts0 = self._clock()
        try:
            client = await self._client(peer)
            resp = await client.get(f"{self.base_url(peer)}/health", headers=self._headers(peer))
            latency = round((self._clock() - ts0) * 1000.0, 1)
            if resp.status_code == 200:
                return True, latency, None
            return False, latency, f"HTTP {resp.status_code}"
        except httpx.HTTPError as exc:
            return False, None, f"{type(exc).__name__}: {exc}"

    async def probe_peers(self, ttl: float = 30.0) -> None:
        """Refresh each peer's /health, at most once per `ttl` seconds."""
        now = self._clock()
        for peer in self.peers:
            st = self._state(peer.name)
            if st.probe and now - st.probe.get("at", 0.0) < ttl:
                continue
            ok, latency, err = await self.check_health(peer)
            st.probe = {"ok": ok, "latency_ms": latency, "error": err, "at": now}

    @property
    def health(self) -> dict:
        peers = self.peers
        out: dict = {"peers": {}}
        for peer in peers:
            warn = None
            if peer.proxy_url is None:
                warn = "no proxy_url: cookies only work if this peer shares service A's egress IP"
            out["peers"][peer.name] = self._state(peer.name).as_dict(warn)
        out["ok"] = any(p["ok"] for p in out["peers"].values()) if out["peers"] else False
        out["configured"] = len(peers)
        out["cookies_cached"] = sorted(self._cache)
        return out
