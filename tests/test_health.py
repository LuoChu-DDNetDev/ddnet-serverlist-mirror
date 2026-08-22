import time

import yaml

from ddnet_mirror.cache import CacheStore
from ddnet_mirror.config import ConfigManager
from ddnet_mirror.health import HealthAggregator
from ddnet_mirror.nodriver_client import NodriverClient
from ddnet_mirror.upstream import UpstreamClient


def build(tmp_path, write_cache=True):
    data = {
        "upstream": {"endpoints": ["https://master1.example/x.json"]},
        "cache": {"path": str(tmp_path / "servers.json")},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    manager = ConfigManager(p)
    store = CacheStore(manager.config.cache.path)
    if write_cache:
        store.write(b'{"servers": []}')
    upstream = UpstreamClient(manager.config.upstream)
    bypass = NodriverClient(manager.config.bypass)
    return HealthAggregator(manager, store, upstream, bypass), upstream


def test_fresh_start_with_warm_cache_is_running(tmp_path):
    """No endpoint polled yet: a restart inside the throttle window is not degraded."""
    health, upstream = build(tmp_path)
    snap = health.snapshot()
    assert snap["upstream"]["master1.example"]["total"] == 0
    assert snap["status"] == "running"


def test_failed_endpoint_with_cache_is_degraded(tmp_path):
    health, upstream = build(tmp_path)
    url = upstream.endpoints[0]
    upstream._mark_attempt_fail(url, RuntimeError("boom"))  # noqa: SLF001
    snap = health.snapshot()
    assert snap["upstream"]["master1.example"]["total"] == 1
    assert snap["status"] == "degraded"


def test_recovered_endpoint_is_running(tmp_path):
    health, upstream = build(tmp_path)
    upstream._mark_ok(upstream.endpoints[0], 12.0)  # noqa: SLF001
    assert health.snapshot()["status"] == "running"


def test_missing_cache_is_error(tmp_path):
    health, _ = build(tmp_path, write_cache=False)
    snap = health.snapshot()
    assert snap["cache"]["exists"] is False
    assert snap["status"] == "error"


def test_snapshot_reports_config_reload_state(tmp_path):
    health, _ = build(tmp_path)
    snap = health.snapshot()
    assert snap["config"]["status"] == "ok"
    assert snap["config"]["last_error"] is None
    assert snap["config"]["reloaded_at"] <= time.time()
