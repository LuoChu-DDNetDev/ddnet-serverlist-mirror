import json
from types import SimpleNamespace

import pytest

from ddnet_mirror.config import UpstreamConfig
from ddnet_mirror.upstream import AggregateUpstreamError, UpstreamClient


class FakeResp:
    def __init__(self, status=200, content=b"{}", headers=None, text=None):
        self.status_code = status
        self.content = content
        self.headers = headers or {}
        self._text = text

    def json(self):
        return json.loads(self.content)

    @property
    def text(self):
        return self._text if self._text is not None else self.content.decode(errors="replace")


class FakeSession:
    def __init__(self, routes):
        self.routes = routes  # url -> callable returning FakeResp, or exception-factory
        self.calls = []
        self.closed = False

    async def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        entry = self.routes[url]
        if isinstance(entry, type) and issubclass(entry, Exception):
            raise entry(f"boom {url}")
        return entry()

    async def close(self):
        self.closed = True


def make_client(urls, routes, retries=0):
    cfg = UpstreamConfig(endpoints=urls, retries=retries)
    sess = FakeSession(routes)
    return UpstreamClient(cfg, session_factory=lambda: sess), sess


CHALLENGE_RESP = lambda: FakeResp(  # noqa: E731
    403,
    content=b"<html>Just a moment... __cf_chl_ jschl</html>",
    headers={"cf-mitigated": "challenge"},
    text="Just a moment... verifying your browser",
)
OK_JSON = b'{"servers": []}'


async def test_success_first_endpoint():
    urls = ["https://a.example/x", "https://b.example/x"]
    routes = {urls[0]: lambda: FakeResp(200, OK_JSON), urls[1]: lambda: FakeResp(200, OK_JSON)}
    cli, sess = make_client(urls, routes)
    res = await cli.fetch()
    assert res.data == OK_JSON
    assert res.url == urls[0]
    assert cli.statuses[urls[0]].ok is True
    assert cli.statuses[urls[0]].latency_ms is not None


async def test_failover_to_next_endpoint():
    urls = ["https://a.example/x", "https://b.example/x"]
    cli, sess = make_client(
        urls,
        {urls[0]: lambda: FakeResp(500), urls[1]: lambda: FakeResp(200, OK_JSON)},
    )
    res = await cli.fetch()
    assert res.url == urls[1]
    assert cli.statuses[urls[0]].ok is False
    assert cli.statuses[urls[1]].ok is True
    assert sess.calls[0][0] == urls[0]
    assert sess.calls[1][0] == urls[1]


async def test_rotation_starts_at_different_endpoint():
    urls = ["https://a.example/x", "https://b.example/x"]
    routes = {urls[0]: lambda: FakeResp(200, OK_JSON), urls[1]: lambda: FakeResp(200, OK_JSON)}
    cli, sess = make_client(urls, routes)
    await cli.fetch()  # starts at a
    res = await cli.fetch()  # should start at b
    assert sess.calls[1][0] == urls[1]
    assert res.url == urls[1]


async def test_transport_error_moves_to_next():
    class Boom(Exception):
        pass

    urls = ["https://a.example/x", "https://b.example/x"]
    cli, sess = make_client(urls, {urls[0]: Boom, urls[1]: lambda: FakeResp(200, OK_JSON)})
    res = await cli.fetch()
    assert res.url == urls[1]
    assert "Boom" in cli.statuses[urls[0]].last_error


async def test_challenge_with_bypass_retries_with_cookies():
    urls = ["https://a.example/x"]
    sess = FakeSession({urls[0]: CHALLENGE_RESP})
    cli = UpstreamClient(UpstreamConfig(endpoints=urls, retries=0), session_factory=lambda: sess)

    solved = 0

    async def solver(host, url):
        nonlocal solved
        solved += 1
        assert host == "a.example"
        return SimpleNamespace(cookies={"cf_clearance": "abc"}, user_agent="UA/1.0")

    # first call returns challenge, second returns success
    async def get(url, **kwargs):
        sess.calls.append((url, kwargs))
        if len(sess.calls) == 1:
            return CHALLENGE_RESP()
        return FakeResp(200, OK_JSON)

    sess.get = get  # override with two-phase behavior
    res = await cli.fetch(bypass_solver=solver)
    assert res.data == OK_JSON
    assert solved == 1
    cookies_sent = sess.calls[1][1].get("cookies")
    assert cookies_sent == {"cf_clearance": "abc"}
    assert sess.calls[1][1].get("headers") == {"User-Agent": "UA/1.0"}


async def test_challenge_without_bypass_fails():
    urls = ["https://a.example/x"]
    cli, _ = make_client(urls, {urls[0]: CHALLENGE_RESP})
    with pytest.raises(AggregateUpstreamError):
        await cli.fetch()


async def test_challenge_persists_after_solve_fails():
    urls = ["https://a.example/x"]

    class AlwaysChallengeSession:
        async def get(self, url, **kwargs):
            return CHALLENGE_RESP()

        async def close(self):
            pass

    cli = UpstreamClient(
        UpstreamConfig(endpoints=urls, retries=0),
        session_factory=AlwaysChallengeSession,
    )

    async def solver(host, url):
        return SimpleNamespace(cookies={"cf_clearance": "abc"}, user_agent="")

    with pytest.raises(AggregateUpstreamError):
        await cli.fetch(bypass_solver=solver)


async def test_all_endpoints_fail_raises_aggregate():
    urls = ["https://a.example/x", "https://b.example/x"]
    cli, _ = make_client(
        urls,
        {urls[0]: lambda: FakeResp(500), urls[1]: lambda: FakeResp(503, b"<html>unavailable</html>")},
    )
    with pytest.raises(AggregateUpstreamError) as ei:
        await cli.fetch()
    assert len(ei.value.errors) == 2


def test_lazy_config_reload():
    cfg = UpstreamConfig(endpoints=["https://a.example/x"], retries=0)
    cli = UpstreamClient(lambda: cfg)
    assert cli.endpoints == ["https://a.example/x"]
    cfg.endpoints = ["https://z.example/x", "https://y.example/x"]
    cfg.retries = 3
    assert cli.endpoints == ["https://z.example/x", "https://y.example/x"]
    assert cli._cfg().retries == 3


async def test_invalid_json_rejected():
    urls = ["https://a.example/x"]
    cli, _ = make_client(urls, {urls[0]: lambda: FakeResp(200, b"not json")})
    with pytest.raises(AggregateUpstreamError):
        await cli.fetch()

async def test_retries_count_as_one_endpoint_failure():
    """down_after_fails counts failed *fetches*, not attempts inside one fetch."""
    urls = ["https://a.example/x"]
    clock = {"now": 1000.0}
    cfg = UpstreamConfig(endpoints=urls, retries=2, down_after_fails=2, down_retry_after_s=60)
    sess = FakeSession({urls[0]: lambda: FakeResp(500)})
    cli = UpstreamClient(cfg, clock=lambda: clock["now"], session_factory=lambda: sess)

    with pytest.raises(AggregateUpstreamError):
        await cli.fetch()
    st = cli.statuses[urls[0]]
    assert st.total == 3  # 1 + 2 retries all attempted
    assert st.consec_fails == 1  # but only one *fetch* failed
    assert st.down_until is None  # so the endpoint stays in rotation

    with pytest.raises(AggregateUpstreamError):
        await cli.fetch()
    assert cli.statuses[urls[0]].consec_fails == 2
    assert cli.statuses[urls[0]].down_until == 1060.0


async def test_total_budget_stops_the_fetch():
    urls = [f"https://m{i}.example/x" for i in range(4)]
    clock = {"now": 1000.0}

    class SlowSession:
        def __init__(self):
            self.calls = []

        async def get(self, url, **kwargs):
            self.calls.append(url)
            clock["now"] += 10.0  # each attempt burns 10s of the budget
            raise ConnectionError("timeout")

        async def close(self):
            pass

    cfg = UpstreamConfig(endpoints=urls, retries=2, timeout_s=10, total_budget_s=25)
    sess = SlowSession()
    cli = UpstreamClient(cfg, clock=lambda: clock["now"], session_factory=lambda: sess)

    with pytest.raises(AggregateUpstreamError) as ei:
        await cli.fetch()
    # Without a budget this would have been 4 endpoints * 3 attempts = 12 calls / 120s.
    assert len(sess.calls) == 3
    assert clock["now"] - 1000.0 <= 30.0
    assert "budget" in str(ei.value)


async def test_attempt_timeout_shrinks_to_remaining_budget():
    url = "https://a.example/x"
    clock = {"now": 1000.0}
    seen: list[float] = []

    class RecordingSession:
        async def get(self, url_, **kwargs):
            seen.append(kwargs.get("timeout"))
            clock["now"] += 18.0
            raise ConnectionError("timeout")

        async def close(self):
            pass

    cfg = UpstreamConfig(endpoints=[url], retries=1, timeout_s=20, total_budget_s=25)
    cli = UpstreamClient(cfg, clock=lambda: clock["now"], session_factory=RecordingSession)
    with pytest.raises(AggregateUpstreamError):
        await cli.fetch()
    assert seen[0] == 20.0  # full timeout while the budget is untouched
    assert seen[1] == 7.0  # only 7s of the 25s budget left


def test_total_budget_must_cover_one_attempt():
    with pytest.raises(ValueError):
        UpstreamConfig(endpoints=["https://a.example/x"], timeout_s=20, total_budget_s=5)


async def test_warm_clearance_skips_the_challenge_round_trip():
    url = "https://a.example/x"
    warm = SimpleNamespace(
        cookies={"cf_clearance": "abc"}, user_agent="UA/1.0", proxy_url="http://b1:8888", peer="p1"
    )
    sess = FakeSession({url: lambda: FakeResp(200, OK_JSON)})
    cli = UpstreamClient(
        UpstreamConfig(endpoints=[url], retries=0),
        session_factory=lambda: sess,
        cookie_provider=lambda host: warm,
    )
    res = await cli.fetch()
    assert res.data == OK_JSON
    assert len(sess.calls) == 1  # no challenge/replay pair
    kwargs = sess.calls[0][1]
    assert kwargs["cookies"] == {"cf_clearance": "abc"}
    assert kwargs["headers"] == {"User-Agent": "UA/1.0"}
    assert kwargs["proxy"] == "http://b1:8888"  # egress the cookie is bound to


async def test_replay_uses_the_solving_peers_egress():
    url = "https://a.example/x"
    sess = FakeSession({})
    solved = SimpleNamespace(
        cookies={"cf_clearance": "new"}, user_agent="UA/2.0", proxy_url="http://b2:8888", peer="p2"
    )

    async def get(u, **kwargs):
        sess.calls.append((u, kwargs))
        return CHALLENGE_RESP() if len(sess.calls) == 1 else FakeResp(200, OK_JSON)

    sess.get = get
    cli = UpstreamClient(UpstreamConfig(endpoints=[url], retries=0), session_factory=lambda: sess)
    res = await cli.fetch(bypass_solver=lambda host, u: _solved(solved))
    assert res.data == OK_JSON
    assert sess.calls[0][1].get("proxy") is None  # bare attempt goes out directly
    assert sess.calls[1][1]["proxy"] == "http://b2:8888"
    assert cli.statuses[url].challenges == 1
    assert cli.statuses[url].challenges_persisted == 0


async def test_persistent_challenge_reports_to_hook():
    url = "https://a.example/x"
    solved = SimpleNamespace(cookies={"cf_clearance": "x"}, user_agent="", proxy_url=None, peer="p1")
    reported: list[tuple[str, object]] = []

    class AlwaysChallengeSession:
        async def get(self, url_, **kwargs):
            return CHALLENGE_RESP()

        async def close(self):
            pass

    cli = UpstreamClient(
        UpstreamConfig(endpoints=[url], retries=0),
        session_factory=AlwaysChallengeSession,
        challenge_hook=lambda host, result: reported.append((host, result)),
    )
    with pytest.raises(AggregateUpstreamError):
        await cli.fetch(bypass_solver=lambda host, u: _solved(solved))
    assert reported == [("a.example", solved)]  # peer p1 blamed, cookie dropped
    assert cli.statuses[url].challenges_persisted == 1


async def test_session_rebuilt_when_impersonate_changes():
    url = "https://a.example/x"
    cfg = UpstreamConfig(endpoints=[url], retries=0, impersonate="chrome")
    built = []

    def factory():
        sess = FakeSession({url: lambda: FakeResp(200, OK_JSON)})
        built.append(sess)
        return sess

    cli = UpstreamClient(lambda: cfg, session_factory=factory)
    await cli.fetch()
    await cli.fetch()
    assert len(built) == 1  # session reused
    cfg.impersonate = "safari"  # e.g. after SIGHUP
    await cli.fetch()
    assert len(built) == 2
    assert built[0].closed is True


async def _solved(value):
    return value


async def test_dead_endpoint_skipped_after_threshold():

    urls = ["https://a.example/x", "https://b.example/x"]
    clock = {"now": 1000.0}
    cfg = UpstreamConfig(endpoints=urls, retries=0, down_after_fails=2, down_retry_after_s=60)

    # a is down: skipped entirely, only b attempted
    sess = FakeSession({urls[1]: lambda: FakeResp(200, OK_JSON)})
    cli2 = UpstreamClient(cfg, clock=lambda: clock["now"], session_factory=lambda: sess)
    cli2._mark_fail(urls[0], ConnectionError("down"))
    cli2._mark_fail(urls[0], ConnectionError("down"))
    assert cli2.statuses[urls[0]].consec_fails == 2
    assert cli2.statuses[urls[0]].down_until == 1060.0
    calls_before = len(sess.calls)
    r = await cli2.fetch()
    assert r.url == urls[1]
    attempted = [c[0] for c in sess.calls[calls_before:]]
    assert attempted == [urls[1]]  # 'a' NOT attempted
    assert cli2.statuses[urls[0]].total == 2  # unchanged


async def test_down_endpoint_reprobed_after_retry_window():
    url = "https://a.example/x"
    clock = {"now": 1000.0}
    cfg = UpstreamConfig(endpoints=[url], retries=0, down_after_fails=2, down_retry_after_s=60)

    class RecoverSession:
        def __init__(self):
            self.calls = []

        async def get(self, url_, **kwargs):
            self.calls.append(url_)
            if clock["now"] >= 1100.0:
                return FakeResp(200, OK_JSON)  # recovered after the retry window
            raise ConnectionError("down")

        async def close(self):
            pass

    sess = RecoverSession()
    cli = UpstreamClient(cfg, clock=lambda: clock["now"], session_factory=lambda: sess)
    cli._mark_fail(url, ConnectionError("down"))
    cli._mark_fail(url, ConnectionError("down"))
    assert cli.statuses[url].down_until == 1060.0

    # within retry window: a is skipped but it is the only endpoint, so the
    # all-down fallback still attempts it once (fails) instead of silently skipping.
    with pytest.raises(AggregateUpstreamError):
        await cli.fetch()
    assert cli.statuses[url].total == 3

    # past retry window -> a re-probed and recovers
    clock["now"] = 1100.0
    r = await cli.fetch()
    assert r.url == url
    assert cli.statuses[url].ok is True
    assert cli.statuses[url].consec_fails == 0
    assert cli.statuses[url].down_until is None
