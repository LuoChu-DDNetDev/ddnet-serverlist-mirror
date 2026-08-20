import asyncio

import pytest

from ddnet_mirror.config import BypassConfig
from ddnet_mirror.nodriver_client import BypassError, NodriverClient


class FakeResp:
    def __init__(self, status=200, payload=None):
        self.status_code = status
        self._payload = payload or {}

    def json(self):
        return self._payload


class FakeHTTP:
    def __init__(self, *responses):
        self.queue = list(responses)
        self.calls = []

    async def post(self, url, **kwargs):
        self.calls.append(("post", url, kwargs))
        return self.queue.pop(0)

    async def get(self, url, **kwargs):
        self.calls.append(("get", url, kwargs))
        return self.queue.pop(0)

    async def aclose(self):
        pass


def make_client(clock, http):
    cfg = BypassConfig(base_url="http://bypass:9100")
    return NodriverClient(cfg, clock=clock, http_factory=lambda: http)


@pytest.mark.asyncio
async def test_solve_returns_cookies():
    clock = {"now": 1000.0}
    http = FakeHTTP(FakeResp(200, {"cookies": {"cf_clearance": "abc"}, "user_agent": "UA", "expires": 5000}))
    cli = make_client(lambda: clock["now"], http)
    res = await cli.solve("master1.ddnet.org", "https://master1.ddnet.org/ddnet/15/servers.json")
    assert res.cookies == {"cf_clearance": "abc"}
    assert res.user_agent == "UA"
    assert res.expires == 5000.0
    assert cli.health["ok"] is True


@pytest.mark.asyncio
async def test_cached_cookie_reused_until_expiry():
    clock = {"now": 1000.0}
    http = FakeHTTP(FakeResp(200, {"cookies": {"cf_clearance": "abc"}, "user_agent": "UA", "expires": None}))
    cli = make_client(lambda: clock["now"], http)
    await cli.solve("h", "http://u")
    clock["now"] = 2500.0  # within ttl (solved_at=1000 + 1800s)
    res = await cli.solve("h", "http://u")
    assert res.cookies == {"cf_clearance": "abc"}
    assert len(http.calls) == 1  # no second solve

    clock["now"] = 3000.0  # expired (2800)
    http.queue.append(FakeResp(200, {"cookies": {"cf_clearance": "new"}, "user_agent": "UA"}))
    res = await cli.solve("h", "http://u")
    assert res.cookies == {"cf_clearance": "new"}
    assert len(http.calls) == 2


@pytest.mark.asyncio
async def test_concurrent_solves_single_flight():
    clock = {"now": 1000.0}

    async def delayed_post(url, **kwargs):
        await asyncio.sleep(0.02)
        return FakeResp(200, {"cookies": {"cf_clearance": "abc"}, "user_agent": "UA"})

    async def delayed_get(url, **kwargs):
        return FakeResp(200, {})

    class SlowHTTP:
        def __init__(self):
            self.calls = []

        async def post(self, url, **kwargs):
            self.calls.append(("post", url))
            return await delayed_post(url)

        async def get(self, url, **kwargs):
            return await delayed_get(url)

        async def aclose(self):
            pass

    http = SlowHTTP()
    cli = make_client(lambda: clock["now"], http)
    results = await asyncio.gather(
        cli.solve("h", "http://u"), cli.solve("h", "http://u"), cli.solve("h", "http://u")
    )
    assert len(http.calls) == 1  # single-flight
    assert all(r.cookies == {"cf_clearance": "abc"} for r in results)


@pytest.mark.asyncio
async def test_solve_error_raises():
    clock = {"now": 1000.0}
    http = FakeHTTP(FakeResp(500))
    cli = make_client(lambda: clock["now"], http)
    with pytest.raises(BypassError):
        await cli.solve("h", "http://u")


@pytest.mark.asyncio
async def test_check_health():
    clock = {"now": 1000.0}
    http = FakeHTTP(FakeResp(200, {"status": "ok"}))
    cli = make_client(lambda: clock["now"], http)
    ok, latency, err = await cli.check_health()
    assert ok is True
    assert latency is not None
    assert err is None


def test_auth_header_sent():
    clock = {"now": 1000.0}
    cfg = BypassConfig(base_url="http://bypass:9100", auth_token="s3cret")
    cli = NodriverClient(cfg, clock=lambda: clock["now"], http_factory=lambda: FakeHTTP())
    assert cli._headers()["Authorization"] == "Bearer s3cret"


def test_trailing_slash_normalized():
    clock = {"now": 1000.0}
    cfg = BypassConfig(base_url="http://bypass:9100/")
    cli = NodriverClient(cfg, clock=lambda: clock["now"], http_factory=lambda: FakeHTTP())
    assert cli.base_url == "http://bypass:9100"


def test_cloudflare_access_headers_sent():
    clock = {"now": 1000.0}
    cfg = BypassConfig(
        base_url="http://bypass:9100",
        access_client_id="cf-id",
        access_client_secret="cf-secret",
    )
    cli = NodriverClient(cfg, clock=lambda: clock["now"], http_factory=lambda: FakeHTTP())
    headers = cli._headers()
    assert headers["CF-Access-Client-Id"] == "cf-id"
    assert headers["CF-Access-Client-Secret"] == "cf-secret"


def test_access_headers_absent_when_unset():
    clock = {"now": 1000.0}
    cfg = BypassConfig(base_url="http://bypass:9100")
    cli = NodriverClient(cfg, clock=lambda: clock["now"], http_factory=lambda: FakeHTTP())
    headers = cli._headers()
    assert "CF-Access-Client-Id" not in headers
    assert "CF-Access-Client-Secret" not in headers