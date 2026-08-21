import asyncio
import os
import time

import httpx
import pytest
import yaml

from ddnet_mirror.cache import CacheStore
from ddnet_mirror.config import ConfigManager
from ddnet_mirror.health import HealthAggregator
from ddnet_mirror.nodriver_client import NodriverClient
from ddnet_mirror.throttler import RefreshOrchestrator
from ddnet_mirror.upstream import UpstreamClient
from ddnet_mirror.web import create_app


def build(
    tmp_path,
    cache_bytes=b'{"servers": []}',
    refresher=None,
    health_token=None,
    limits=None,
    cache_age=0.2,
):
    data = {
        "upstream": {"endpoints": ["https://master1.example/x.json"]},
        "cache": {"path": str(tmp_path / "servers.json")},
        # Off by default so most endpoint tests are not rate limited.
        "limits": limits or {"enabled": False},
    }
    if health_token:
        data["health"] = {"auth_token": health_token}
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    manager = ConfigManager(p)
    store = CacheStore(manager.config.cache.path)
    store.write(cache_bytes)
    now = time.time()
    os.utime(store.path, (now - cache_age, now - cache_age))

    upstream = UpstreamClient(manager.config.upstream)
    bypass = NodriverClient(manager.config.bypass)
    health = HealthAggregator(manager, store, upstream, bypass)
    orch = RefreshOrchestrator(
        manager, store, refresher or (lambda: asyncio.sleep(0))
    )
    return create_app(manager, orch, health), orch


@pytest.mark.asyncio
async def test_servers_endpoints_serve_cached_bytes(tmp_path):
    app, orch = build(tmp_path, b'{"servers": []}')
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        for path in ("/", "/api/v1/servers", "/api/v1/servers/cache"):
            r = await client.get(path)
            assert r.status_code == 200
            assert r.headers["content-type"] == "application/json"
            assert r.content == b'{"servers": []}'


@pytest.mark.asyncio
async def test_cache_endpoint_never_refreshes(tmp_path):
    calls = []

    async def refresher():
        calls.append(1)

    app, orch = build(tmp_path, b"cached", refresher=refresher)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        await client.get("/api/v1/servers/cache")
        await client.get("/api/v1/servers/cache")
    assert calls == []


@pytest.mark.asyncio
async def test_immediate_endpoint_respects_throttle(tmp_path):
    calls = []

    async def refresher():
        calls.append(1)

    app, orch = build(tmp_path, b"cached", refresher=refresher)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        await client.get("/api/v1/servers")  # cache is hot -> no refresh
    assert calls == []


@pytest.mark.asyncio
async def test_etag_second_request_gets_304(tmp_path):
    app, _ = build(tmp_path, b'{"servers": [1]}')
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        first = await client.get("/api/v1/servers")
        etag = first.headers["etag"]
        assert first.headers["cache-control"] == "public, max-age=3"
        assert first.headers["vary"] == "Accept-Encoding"
        assert "last-modified" in first.headers
        again = await client.get("/api/v1/servers", headers={"If-None-Match": etag})
    assert again.status_code == 304
    assert again.content == b""


@pytest.mark.asyncio
async def test_gzip_variant_served_and_has_its_own_etag(tmp_path):
    payload = b'{"servers": [' + b'{"a": 1},' * 500 + b'{"a": 1}]}'
    app, _ = build(tmp_path, payload)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        gz = await client.get("/api/v1/servers", headers={"Accept-Encoding": "gzip"})
        plain = await client.get("/api/v1/servers", headers={"Accept-Encoding": "identity"})
    assert gz.headers["content-encoding"] == "gzip"
    assert int(gz.headers["content-length"]) < len(payload) / 4
    assert gz.content == payload  # httpx decodes it back
    assert "content-encoding" not in plain.headers
    assert plain.content == payload  # upstream bytes, verbatim
    assert gz.headers["etag"] != plain.headers["etag"]  # variants must not collide


@pytest.mark.asyncio
async def test_if_modified_since_gets_304(tmp_path):
    app, _ = build(tmp_path, b"{}")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        first = await client.get("/api/v1/servers/cache")
        again = await client.get(
            "/api/v1/servers/cache", headers={"If-Modified-Since": first.headers["last-modified"]}
        )
    assert again.status_code == 304


@pytest.mark.asyncio
async def test_cache_served_from_memory_without_rereading(tmp_path):
    app, orch = build(tmp_path, b'{"servers": []}')
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        await client.get("/api/v1/servers/cache")
        snap_first = orch.snapshot_cached()
        await client.get("/api/v1/servers/cache")
        snap_second = orch.snapshot_cached()
    assert snap_first is snap_second  # same object: no re-read, no re-compress
    assert snap_first.gzipped is not None


@pytest.mark.asyncio
async def test_health_minimal_without_token(tmp_path):
    app, orch = build(tmp_path)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        r = await client.get("/api/v1/health")
    assert r.status_code == 200
    j = r.json()
    assert j["status"] in ("running", "degraded", "error")
    assert j["cache"]["exists"] is True
    assert "uptime_seconds" in j
    # details that identify the host or leak upstream errors stay private
    assert "process" not in j
    assert "config" not in j
    assert "upstream" not in j


@pytest.mark.asyncio
async def test_health_full_with_token(tmp_path):
    app, orch = build(tmp_path, health_token="s3cret")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        bad = await client.get("/api/v1/health", headers={"Authorization": "Bearer nope"})
        r = await client.get("/api/v1/health", headers={"Authorization": "Bearer s3cret"})
    assert "process" not in bad.json()  # wrong token falls back to minimal
    j = r.json()
    assert isinstance(j["process"]["pid"], int)
    assert j["cache"]["exists"] is True
    assert "master1.example" in j["upstream"]
    assert isinstance(j["upstream"]["master1.example"]["ok"], bool)
    assert "peers" in j["bypass"]
    assert "reloaded_at" in j["config"]
    assert j["refresh"]["gated_requests"] >= 0


@pytest.mark.asyncio
async def test_metrics_endpoint_exposes_counters(tmp_path):
    app, _ = build(tmp_path, b'{"servers": []}')
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        await client.get("/api/v1/servers")
        r = await client.get("/metrics")
    assert r.status_code == 200
    body = r.text
    assert "ddnet_mirror_requests_total" in body
    assert "ddnet_mirror_cache_size_bytes" in body
    assert "ddnet_mirror_throttle_gated_total" in body


@pytest.mark.asyncio
async def test_metrics_requires_token_when_configured(tmp_path):
    app, _ = build(tmp_path, health_token="s3cret")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        denied = await client.get("/metrics")
        ok = await client.get("/metrics", headers={"Authorization": "Bearer s3cret"})
    assert denied.status_code == 401
    assert ok.status_code == 200


@pytest.mark.asyncio
async def test_rate_limit_returns_429(tmp_path):
    app, _ = build(tmp_path, b"{}", limits={"enabled": True, "rps": 1, "burst": 3})
    transport = httpx.ASGITransport(app=app)
    codes = []
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        for _ in range(6):
            codes.append((await client.get("/api/v1/servers/cache")).status_code)
    assert codes[:3] == [200, 200, 200]
    assert 429 in codes[3:]


@pytest.mark.asyncio
async def test_exempt_paths_not_rate_limited(tmp_path):
    app, _ = build(tmp_path, b"{}", limits={"enabled": True, "rps": 1, "burst": 1})
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        codes = [(await client.get("/health")).status_code for _ in range(5)]
    assert codes == [200] * 5


@pytest.mark.asyncio
async def test_concurrency_cap_sheds_with_503(tmp_path):
    gate = asyncio.Event()

    async def slow_refresher():
        await gate.wait()

    app, _ = build(
        tmp_path,
        b"{}",
        refresher=slow_refresher,
        limits={"enabled": True, "rps": 1000, "burst": 1000, "max_concurrency": 1},
        cache_age=30,  # stale, so the request blocks in the refresher
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        first = asyncio.create_task(client.get("/api/v1/servers"))
        await asyncio.sleep(0.05)  # let it occupy the only slot
        shed = await client.get("/api/v1/servers/cache")
        gate.set()
        await first
    assert shed.status_code == 503
    assert shed.headers["retry-after"] == "1"


@pytest.mark.asyncio
async def test_missing_cache_returns_503(tmp_path):
    data = {
        "upstream": {"endpoints": ["https://master1.example/x.json"]},
        "cache": {"path": str(tmp_path / "servers.json")},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    manager = ConfigManager(p)
    store = CacheStore(manager.config.cache.path)  # never written
    orch = RefreshOrchestrator(manager, store, lambda: asyncio.sleep(0))
    app = create_app(manager, orch, None)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        r = await client.get("/api/v1/servers")
    assert r.status_code == 503

@pytest.mark.asyncio
async def test_favicon_returns_assets_file(tmp_path):
    assets = tmp_path / "assets"
    assets.mkdir()
    (assets / "favicon.ico").write_bytes(b"\x00\x00ICO")
    data = {
        "upstream": {"endpoints": ["https://m.example/x.json"]},
        "cache": {"path": str(tmp_path / "s.json")},
    }
    p = tmp_path / "c2.yaml"
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    mgr = ConfigManager(p)
    store2 = CacheStore(mgr.config.cache.path)
    store2.write(b"{}")
    orch = RefreshOrchestrator(mgr, store2, lambda: asyncio.sleep(0))
    fav = create_app(mgr, orch, None, assets_dir=assets)
    transport = httpx.ASGITransport(app=fav)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        r = await client.get("/favicon.ico")
    assert r.status_code == 200
    assert r.headers["content-type"] == "image/x-icon"
    assert r.content == b"\x00\x00ICO"


@pytest.mark.asyncio
async def test_favicon_404_when_missing(tmp_path):
    data = {
        "upstream": {"endpoints": ["https://m.example/x.json"]},
        "cache": {"path": str(tmp_path / "s.json")},
    }
    p = tmp_path / "c3.yaml"
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    mgr = ConfigManager(p)
    store = CacheStore(mgr.config.cache.path)
    store.write(b"{}")
    orch = RefreshOrchestrator(mgr, store, lambda: asyncio.sleep(0))
    fav = create_app(mgr, orch, None, assets_dir=tmp_path / "no-such-assets")
    transport = httpx.ASGITransport(app=fav)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        r = await client.get("/favicon.ico")
    assert r.status_code == 404
