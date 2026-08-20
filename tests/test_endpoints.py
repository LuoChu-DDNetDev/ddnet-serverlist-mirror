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


def build(tmp_path, cache_bytes=b'{"servers": []}', refresher=None):
    data = {
        "upstream": {"endpoints": ["https://master1.example/x.json"]},
        "cache": {"path": str(tmp_path / "servers.json")},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    manager = ConfigManager(p)
    store = CacheStore(manager.config.cache.path)
    store.write(cache_bytes)
    now = time.time()
    os.utime(store.path, (now - 0.2, now - 0.2))  # hot: within 1s throttle window

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
async def test_health_shape(tmp_path):
    app, orch = build(tmp_path)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        r = await client.get("/api/v1/health")
    assert r.status_code == 200
    j = r.json()
    assert j["status"] in ("running", "degraded", "error")
    assert isinstance(j["process"]["pid"], int)
    assert j["cache"]["exists"] is True
    assert "master1.example" in j["upstream"]
    assert isinstance(j["upstream"]["master1.example"]["ok"], bool)
    assert "bypass" in j
    assert "config" in j
    assert "reloaded_at" in j["config"]
    assert "refresh" in j


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