import asyncio
import os
import time

import pytest
import yaml

from ddnet_mirror.cache import CacheStore
from ddnet_mirror.config import ConfigManager
from ddnet_mirror.throttler import RefreshOrchestrator


def make_manager(tmp_path, cache_bytes=b"{}"):
    data = {
        "upstream": {"endpoints": ["https://master1.example/a.json"]},
        "cache": {"path": str(tmp_path / "servers.json")},
    }
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    manager = ConfigManager(p)
    store = CacheStore(manager.config.cache.path)
    store.write(cache_bytes)
    return manager, store


def set_age(path, age_s):
    now = time.time()
    os.utime(path, (now - age_s, now - age_s))


@pytest.mark.asyncio
async def test_hot_cache_does_not_refresh(tmp_path):
    manager, store = make_manager(tmp_path, b"hot")
    set_age(store.path, 0.3)  # younger than the 1s throttle window
    calls = []

    async def refresher():
        calls.append(1)

    orch = RefreshOrchestrator(manager, store, refresher)
    body = await orch.get_immediate()
    assert body == b"hot"
    assert calls == []


@pytest.mark.asyncio
async def test_concurrent_requests_single_refresh(tmp_path):
    manager, store = make_manager(tmp_path, b"stale")
    set_age(store.path, 5)
    calls = 0

    async def refresher():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        store.write(b"fresh")

    orch = RefreshOrchestrator(manager, store, refresher)
    results = await asyncio.gather(*[orch.get_immediate() for _ in range(10)])
    assert calls == 1  # coalesced into a single upstream fetch
    assert results[0] == b"fresh"


@pytest.mark.asyncio
async def test_stale_cache_triggers_refresh(tmp_path):
    manager, store = make_manager(tmp_path, b"old")
    set_age(store.path, 1.5)  # older than the 1s window
    calls = 0

    async def refresher():
        nonlocal calls
        calls += 1
        store.write(b"new")

    orch = RefreshOrchestrator(manager, store, refresher)
    body = await orch.get_immediate()
    assert calls == 1
    assert body == b"new"


@pytest.mark.asyncio
async def test_refresh_failure_serves_stale(tmp_path):
    manager, store = make_manager(tmp_path, b"stale")
    set_age(store.path, 5)

    async def refresher():
        raise RuntimeError("boom")

    orch = RefreshOrchestrator(manager, store, refresher)
    body = await orch.get_immediate()
    assert body == b"stale"  # degraded: old cache still served
    assert "boom" in (orch.last_refresh_error or "")


def test_tick_idle_backoff_then_restore_base(tmp_path):
    manager, store = make_manager(tmp_path)
    orch = RefreshOrchestrator(manager, store, lambda: None)
    bg = manager.config.background

    # five idle windows -> extended interval
    for _ in range(bg.extend_after_idle_cycles):
        idle, interval = orch._decide(100.0, None, 60.0)
        orch._idle_cycles, orch._current_interval = idle, interval
    assert orch._idle_cycles == 5
    assert orch._current_interval == bg.extended_interval_s

    # further idle windows stay extended
    idle, interval = orch._decide(100.0, None, 300.0)
    orch._idle_cycles, orch._current_interval = idle, interval
    assert orch._current_interval == bg.extended_interval_s

    # an immediate request resets counters and cadence
    orch._mark_immediate(200.0)
    assert orch._idle_cycles == 0
    assert orch._current_interval == bg.base_interval_s

    # activity within the next window keeps idle at 0
    idle, interval = orch._decide(250.0, 200.0, 60.0)
    assert idle == 0
    assert interval == bg.base_interval_s

def test_jitter_bounds():
    import random

    rng = random.Random(7)
    manager, store = make_manager_with_dummy()
    orch = RefreshOrchestrator(manager, store, lambda: None, rng=rng)
    base = manager.config.background.base_interval_s  # 60
    # jitter is +/-8% around the interval
    sleeps = [orch._jittered(base) for _ in range(200)]
    assert all(base * 0.92 <= s <= base * 1.08 for s in sleeps)
    # distribution actually moves, not constant
    assert len(set(round(s, 3) for s in sleeps)) > 50


def make_manager_with_dummy():
    import tempfile
    from pathlib import Path

    d = Path(tempfile.mkdtemp())
    return make_manager(d)
