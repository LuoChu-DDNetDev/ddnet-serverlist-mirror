import pytest

from ddnet_mirror.config import LimitsConfig
from ddnet_mirror.limits import TokenBucketLimiter


class FakeRequest:
    def __init__(self, host="1.2.3.4", headers=None):
        self.client = type("C", (), {"host": host})()
        self.headers = headers or {}


def limiter(clock, **kw):
    cfg = LimitsConfig(**kw)
    return TokenBucketLimiter(lambda: cfg, clock=clock)


def test_burst_then_refill():
    now = {"t": 0.0}
    lim = limiter(lambda: now["t"], rps=2, burst=3)
    assert [lim.allow("a") for _ in range(4)] == [True, True, True, False]
    now["t"] = 0.5  # 2 rps -> one token back
    assert lim.allow("a") is True
    assert lim.allow("a") is False


def test_buckets_are_per_client():
    now = {"t": 0.0}
    lim = limiter(lambda: now["t"], rps=1, burst=1)
    assert lim.allow("a") is True
    assert lim.allow("a") is False
    assert lim.allow("b") is True  # unrelated client unaffected


def test_tracked_clients_are_bounded():
    now = {"t": 0.0}
    lim = limiter(lambda: now["t"], rps=1000, burst=1000, max_tracked_clients=20)
    for i in range(200):
        now["t"] += 0.001
        lim.allow(f"client-{i}")
    assert len(lim._buckets) <= 20  # noqa: SLF001


def test_client_key_prefers_peer_address():
    lim = limiter(lambda: 0.0, trust_forwarded_for=False)
    req = FakeRequest("9.9.9.9", {"x-forwarded-for": "1.1.1.1, 2.2.2.2"})
    assert lim.client_key(req) == "9.9.9.9"  # header not trusted by default


def test_client_key_uses_forwarded_for_when_trusted():
    lim = limiter(lambda: 0.0, trust_forwarded_for=True)
    req = FakeRequest("9.9.9.9", {"x-forwarded-for": "1.1.1.1, 2.2.2.2"})
    assert lim.client_key(req) == "1.1.1.1"


def test_client_key_without_peer():
    lim = limiter(lambda: 0.0)
    req = FakeRequest()
    req.client = None
    assert lim.client_key(req) == "unknown"


@pytest.mark.parametrize("rps", [0.5, 5, 50])
def test_never_exceeds_burst_capacity(rps):
    now = {"t": 0.0}
    lim = limiter(lambda: now["t"], rps=rps, burst=5)
    now["t"] = 10_000.0  # idle for a long time
    granted = sum(1 for _ in range(100) if lim.allow("a"))
    assert granted == 5  # capped at burst, no unbounded credit
