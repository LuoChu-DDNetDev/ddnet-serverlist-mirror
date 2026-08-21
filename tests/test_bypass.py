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


SOLVED = {"cookies": {"cf_clearance": "abc"}, "user_agent": "UA", "expires": 5000}


def make_client(clock, http, **peer_kw):
    # legacy flat form: folded into a single peer named "default"
    cfg = BypassConfig(base_url="http://bypass:9100", **peer_kw)
    return NodriverClient(cfg, clock=clock, http_factory=lambda: http)


def test_legacy_flat_config_folds_into_one_peer():
    cfg = BypassConfig(base_url="http://bypass:9100/", auth_token="t", cookie_ttl_s=60)
    assert [p.name for p in cfg.peers] == ["default"]
    assert cfg.peers[0].base_url == "http://bypass:9100/"
    assert cfg.peers[0].cookie_ttl_s == 60


def test_peers_and_legacy_fields_are_mutually_exclusive():
    with pytest.raises(ValueError):
        BypassConfig(base_url="http://a", peers=[{"name": "p1", "base_url": "http://b"}])


@pytest.mark.asyncio
async def test_solve_returns_cookies_and_peer_egress():
    clock = {"now": 1000.0}
    http = FakeHTTP(FakeResp(200, SOLVED))
    cfg = BypassConfig(peers=[{"name": "p1", "base_url": "http://b1", "proxy_url": "http://b1:8888"}])
    cli = NodriverClient(cfg, clock=lambda: clock["now"], http_factory=lambda: http)
    res = await cli.solve("master1.ddnet.org", "https://master1.ddnet.org/x.json")
    assert res.cookies == {"cf_clearance": "abc"}
    assert res.user_agent == "UA"
    assert res.expires == 5000.0
    assert res.peer == "p1"
    assert res.proxy_url == "http://b1:8888"  # replay must leave from that peer
    assert cli.health["peers"]["p1"]["ok"] is True
    assert cli.health["ok"] is True


@pytest.mark.asyncio
async def test_first_peer_wins_second_untouched():
    clock = {"now": 1000.0}
    http1, http2 = FakeHTTP(FakeResp(200, SOLVED)), FakeHTTP(FakeResp(200, SOLVED))
    clients = {"p1": http1, "p2": http2}
    cfg = BypassConfig(peers=[{"name": "p1", "base_url": "http://b1"}, {"name": "p2", "base_url": "http://b2"}])
    cli = NodriverClient(cfg, clock=lambda: clock["now"])
    cli._client = lambda peer: _ready(clients[peer.name])  # noqa: SLF001

    res = await cli.solve("h", "http://u")
    assert res.peer == "p1"
    assert len(http1.calls) == 1
    assert http2.calls == []


@pytest.mark.asyncio
async def test_falls_through_to_next_peer_in_order():
    clock = {"now": 1000.0}
    http1 = FakeHTTP(FakeResp(502))  # peer 1 broken
    http2 = FakeHTTP(FakeResp(200, SOLVED))
    clients = {"p1": http1, "p2": http2}
    cfg = BypassConfig(
        peers=[
            {"name": "p1", "base_url": "http://b1", "proxy_url": "http://b1:8888"},
            {"name": "p2", "base_url": "http://b2", "proxy_url": "http://b2:8888"},
        ],
        down_after_fails=2,
        down_retry_after_s=60,
    )
    cli = NodriverClient(cfg, clock=lambda: clock["now"])
    cli._client = lambda peer: _ready(clients[peer.name])  # noqa: SLF001

    res = await cli.solve("h", "http://u")
    assert res.peer == "p2"
    assert res.proxy_url == "http://b2:8888"
    assert cli.health["peers"]["p1"]["fails"] == 1
    assert cli.health["peers"]["p1"]["consec_fails"] == 1


@pytest.mark.asyncio
async def test_downed_peer_skipped_until_retry_window():
    clock = {"now": 1000.0}
    http1 = FakeHTTP(FakeResp(502), FakeResp(502))
    http2 = FakeHTTP(FakeResp(200, SOLVED), FakeResp(200, SOLVED))
    clients = {"p1": http1, "p2": http2}
    cfg = BypassConfig(
        peers=[{"name": "p1", "base_url": "http://b1"}, {"name": "p2", "base_url": "http://b2"}],
        down_after_fails=1,
        down_retry_after_s=60,
    )
    cli = NodriverClient(cfg, clock=lambda: clock["now"])
    cli._client = lambda peer: _ready(clients[peer.name])  # noqa: SLF001

    await cli.solve("h", "http://u")
    assert cli.health["peers"]["p1"]["down_until"] == 1060.0

    cli.invalidate("h")
    clock["now"] = 1030.0  # still inside the down window
    calls_before = len(http1.calls)
    res = await cli.solve("h", "http://u")
    assert res.peer == "p2"
    assert len(http1.calls) == calls_before  # p1 not retried yet


@pytest.mark.asyncio
async def test_all_peers_failing_raises():
    clock = {"now": 1000.0}
    http = FakeHTTP(FakeResp(500), FakeResp(500))
    cfg = BypassConfig(peers=[{"name": "p1", "base_url": "http://b1"}, {"name": "p2", "base_url": "http://b2"}])
    cli = NodriverClient(cfg, clock=lambda: clock["now"], http_factory=lambda: http)
    with pytest.raises(BypassError) as ei:
        await cli.solve("h", "http://u")
    assert "all bypass peers failed" in str(ei.value)


@pytest.mark.asyncio
async def test_cached_cookie_reused_until_expiry():
    clock = {"now": 1000.0}
    http = FakeHTTP(FakeResp(200, {"cookies": {"cf_clearance": "abc"}, "user_agent": "UA"}))
    cli = make_client(lambda: clock["now"], http)
    await cli.solve("h", "http://u")
    clock["now"] = 2500.0  # within ttl (solved_at=1000 + 1800s)
    res = await cli.solve("h", "http://u")
    assert res.cookies == {"cf_clearance": "abc"}
    assert len(http.calls) == 1  # no second solve
    assert cli.peek("h") is res

    clock["now"] = 3000.0  # expired (2800)
    assert cli.peek("h") is None
    http.queue.append(FakeResp(200, {"cookies": {"cf_clearance": "new"}, "user_agent": "UA"}))
    res = await cli.solve("h", "http://u")
    assert res.cookies == {"cf_clearance": "new"}
    assert len(http.calls) == 2


@pytest.mark.asyncio
async def test_challenge_persisted_drops_cookie_and_blames_peer():
    clock = {"now": 1000.0}
    http = FakeHTTP(FakeResp(200, SOLVED))
    cli = make_client(lambda: clock["now"], http)
    res = await cli.solve("h", "http://u")
    assert cli.peek("h") is not None

    cli.on_challenge_persisted("h", res)
    assert cli.peek("h") is None  # stale clearance dropped
    assert cli.health["peers"]["default"]["consec_fails"] == 1


@pytest.mark.asyncio
async def test_concurrent_solves_single_flight():
    clock = {"now": 1000.0}

    class SlowHTTP:
        def __init__(self):
            self.calls = []

        async def post(self, url, **kwargs):
            self.calls.append(("post", url))
            await asyncio.sleep(0.02)
            return FakeResp(200, {"cookies": {"cf_clearance": "abc"}, "user_agent": "UA"})

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
async def test_check_health_and_probe_ttl():
    clock = {"now": 1000.0}
    http = FakeHTTP(FakeResp(200, {"status": "ok"}))
    cli = make_client(lambda: clock["now"], http)
    peer = cli.peers[0]
    ok, latency, err = await cli.check_health(peer)
    assert ok is True
    assert latency is not None
    assert err is None

    http.queue.append(FakeResp(200, {"status": "ok"}))
    await cli.probe_peers(ttl=30)
    assert cli.health["peers"]["default"]["probe"]["ok"] is True
    await cli.probe_peers(ttl=30)  # inside TTL: no extra request
    assert len(http.queue) == 0


def test_headers_and_base_url_per_peer():
    cfg = BypassConfig(
        peers=[
            {
                "name": "p1",
                "base_url": "http://bypass:9100/",
                "auth_token": "s3cret",
                "access_client_id": "cf-id",
                "access_client_secret": "cf-secret",
            },
            {"name": "p2", "base_url": "http://other:9100"},
        ]
    )
    cli = NodriverClient(cfg, clock=lambda: 0.0, http_factory=lambda: FakeHTTP())
    p1, p2 = cli.peers
    assert cli.base_url(p1) == "http://bypass:9100"  # trailing slash normalised
    h1 = cli._headers(p1)  # noqa: SLF001
    assert h1["Authorization"] == "Bearer s3cret"
    assert h1["CF-Access-Client-Id"] == "cf-id"
    assert h1["CF-Access-Client-Secret"] == "cf-secret"
    h2 = cli._headers(p2)  # noqa: SLF001
    assert "Authorization" not in h2
    assert "CF-Access-Client-Id" not in h2


def test_missing_proxy_url_surfaces_warning():
    cfg = BypassConfig(peers=[{"name": "p1", "base_url": "http://b1"}])
    cli = NodriverClient(cfg, clock=lambda: 0.0, http_factory=lambda: FakeHTTP())
    assert "egress" in cli.health["peers"]["p1"]["warn"]


async def _ready(value):
    return value
