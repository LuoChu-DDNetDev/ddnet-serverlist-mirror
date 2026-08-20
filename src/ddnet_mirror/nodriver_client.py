"""Client for the external nodriver bypass service (Service B).

Service B only solves the Cloudflare JS challenge and returns cookies + UA.
Service A replays the request through curl_cffi with those cookies.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass

import httpx

from .config import BypassConfig


class BypassError(Exception):
    """Bypass service failed or returned unusable data."""


@dataclass
class BypassResult:
    cookies: dict[str, str]
    user_agent: str
    expires: float | None
    solved_at: float


class NodriverClient:
    def __init__(
        self,
        cfg: BypassConfig | Callable[[], BypassConfig],
        clock=time.time,
        http_factory: Callable[[], httpx.AsyncClient] | None = None,
    ) -> None:
        # Lazily read config so SIGHUP reloads take effect on the next call.
        self._cfg: Callable[[], BypassConfig] = cfg if callable(cfg) else lambda: cfg
        self._clock = clock
        self._client: httpx.AsyncClient | None = None
        self._http_factory = http_factory
        self._cache: dict[str, BypassResult] = {}
        self._inflight: dict[str, asyncio.Future] = {}
        self.health: dict = {"ok": False, "latency_ms": None, "last_error": None, "last_ok_ts": None}

    @property
    def base_url(self) -> str:
        return self._cfg().base_url.rstrip("/")

    @property
    def timeout(self) -> float:
        return self._cfg().timeout_s

    @property
    def cookie_ttl(self) -> float:
        return self._cfg().cookie_ttl_s

    @property
    def auth_token(self) -> str | None:
        return self._cfg().auth_token

    @property
    def access_client_id(self) -> str | None:
        return self._cfg().access_client_id

    @property
    def access_client_secret(self) -> str | None:
        return self._cfg().access_client_secret

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = (
                self._http_factory()
                if self._http_factory is not None
                else httpx.AsyncClient(timeout=self.timeout)
            )
        return self._client

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        if self.access_client_id and self.access_client_secret:
            headers["CF-Access-Client-Id"] = self.access_client_id
            headers["CF-Access-Client-Secret"] = self.access_client_secret
        return headers

    async def solve(self, host: str, url: str) -> BypassResult:
        """Return cached cookies for a host, or solve a fresh challenge (single-flight per host)."""
        now = self._clock()
        cached = self._cache.get(host)
        if cached is not None:
            expires = cached.expires if cached.expires is not None else cached.solved_at + self.cookie_ttl
            if now < expires:
                return cached

        fut = self._inflight.get(host)
        if fut is not None:
            try:
                return await asyncio.shield(fut)
            except Exception:  # noqa: BLE001 - failed slot, fall through to fresh solve
                pass

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._inflight[host] = fut
        try:
            result = await self._request_solve(host, url)
        except BaseException as exc:
            fut.set_exception(exc)
            raise
        finally:
            self._inflight.pop(host, None)
        fut.set_result(result)
        self._cache[host] = result
        return result

    async def _request_solve(self, host: str, url: str) -> BypassResult:
        ts0 = self._clock()
        client = await self._get_client()
        try:
            resp = await client.post(
                f"{self.base_url}/solve", headers=self._headers(), json={"url": url, "host": host}
            )
        except httpx.HTTPError as exc:
            self.health = {
                "ok": False, "latency_ms": None, "last_error": f"{type(exc).__name__}: {exc}",
                "last_ok_ts": None,
            }
            raise BypassError(f"bypass request failed: {exc}") from exc

        latency_ms = (self._clock() - ts0) * 1000.0
        if resp.status_code != 200:
            self.health = {
                "ok": False, "latency_ms": latency_ms, "last_error": f"HTTP {resp.status_code}",
                "last_ok_ts": None,
            }
            raise BypassError(f"bypass service returned HTTP {resp.status_code}")
        data = resp.json()
        cookies = data.get("cookies") or {}
        if not cookies:
            self.health = {
                "ok": False, "latency_ms": latency_ms, "last_error": "no cookies in response",
                "last_ok_ts": None,
            }
            raise BypassError("bypass service returned no cookies")

        result = BypassResult(
            cookies={str(k): str(v) for k, v in cookies.items()},
            user_agent=str(data.get("user_agent") or ""),
            expires=float(data["expires"]) if data.get("expires") is not None else None,
            solved_at=self._clock(),
        )
        self.health = {
                "ok": True, "latency_ms": round(latency_ms, 1), "last_error": None,
                "last_ok_ts": self._clock(),
            }
        return result

    async def check_health(self) -> tuple[bool, float | None, str | None]:
        ts0 = self._clock()
        try:
            client = await self._get_client()
            resp = await client.get(f"{self.base_url}/health", headers=self._headers())
            latency = round((self._clock() - ts0) * 1000.0, 1)
            if resp.status_code == 200:
                return True, latency, None
            return False, latency, f"HTTP {resp.status_code}"
        except httpx.HTTPError as exc:
            return False, None, f"{type(exc).__name__}: {exc}"