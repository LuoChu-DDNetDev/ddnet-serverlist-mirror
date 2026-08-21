"""Refresher wiring: fusion failures must never cost us the upstream bytes."""

import asyncio
from types import SimpleNamespace

import pytest
import yaml

from ddnet_mirror.cache import CacheStore
from ddnet_mirror.config import ConfigManager, DataSourceConfig
from ddnet_mirror.datasources import build_sources
from ddnet_mirror.main import _build_refresher

UPSTREAM = b'{"servers": [{"addresses": ["a"]}]}'


def wire(tmp_path, datasources=None, data=UPSTREAM):
    cfg = {
        "upstream": {"endpoints": ["https://master1.example/x.json"]},
        "cache": {"path": str(tmp_path / "servers.json")},
        "datasources": datasources or [],
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    manager = ConfigManager(p)
    cache = CacheStore(manager.config.cache.path)
    upstream = SimpleNamespace(
        fetch=lambda bypass_solver=None: _result(
            SimpleNamespace(data=data, url="https://master1.example/x.json", host="master1.example")
        )
    )
    bypass = SimpleNamespace(solve=lambda host, url: _result(None))
    return manager, cache, _build_refresher(manager, cache, upstream, bypass)


async def _result(value):
    return value


@pytest.mark.asyncio
async def test_no_datasources_stores_upstream_bytes_verbatim(tmp_path):
    _, cache, refresher = wire(tmp_path)
    await refresher()
    assert cache.read_raw() == UPSTREAM


@pytest.mark.asyncio
async def test_unbuildable_datasource_does_not_block_the_refresh(tmp_path):
    # `db` is a stub adapter: it builds but always fails to fetch.
    _, cache, refresher = wire(tmp_path, [{"name": "future-db", "type": "db"}])
    await refresher()
    assert cache.read_raw() == UPSTREAM  # upstream data still cached


@pytest.mark.asyncio
async def test_fusion_error_falls_back_to_upstream_bytes(tmp_path):
    # A file source pointing nowhere: apply_datasources skips it, and even if the
    # whole fusion step blew up the upstream bytes must land in the cache.
    _, cache, refresher = wire(
        tmp_path, [{"name": "missing", "type": "file", "path": "/nonexistent/x.json"}]
    )
    await refresher()
    assert cache.read_raw() == UPSTREAM


@pytest.mark.asyncio
async def test_non_json_upstream_still_cached_when_fusion_configured(tmp_path):
    # json.loads would raise here; the refresher must degrade to raw bytes.
    _, cache, refresher = wire(
        tmp_path, [{"name": "x", "type": "file", "path": "/nonexistent/x.json"}], data=b"not json"
    )
    await refresher()
    assert cache.read_raw() == b"not json"


@pytest.mark.asyncio
async def test_working_datasource_is_fused(tmp_path):
    extra = tmp_path / "extra.json"
    extra.write_text('[{"addresses": ["b"]}]', encoding="utf-8")
    manager, cache, refresher = wire(
        tmp_path,
        [
            {
                "name": "extra",
                "type": "file",
                "path": str(extra),
                "strategy": "additional",
                "target_key": "external",
            }
        ],
    )
    await refresher()
    import json

    fused = json.loads(cache.read_raw())
    assert fused["external"] == [{"addresses": ["b"]}]
    assert fused["servers"] == [{"addresses": ["a"]}]


def test_build_sources_skips_disabled_and_unbuildable():
    good = DataSourceConfig(name="ok", type="file", path="/tmp/x.json")
    off = DataSourceConfig(name="off", type="file", path="/tmp/x.json", enabled=False)
    broken = DataSourceConfig.model_construct(name="broken", type="nope", strategy="append")
    built = build_sources([good, off, broken])
    assert [s.name for s in built] == ["ok"]


def test_refresher_is_awaitable_without_running_loop(tmp_path):
    _, cache, refresher = wire(tmp_path)
    asyncio.run(refresher())
    assert cache.read_raw() == UPSTREAM
