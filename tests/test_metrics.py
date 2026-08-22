from ddnet_mirror import metrics


def test_render_fills_gauges_from_snapshot():
    snapshot = {
        "status": "running",
        "process": {"pid": 1, "started_at": 0.0, "uptime_seconds": 12.5},
        "cache": {"exists": True, "updated_at": None, "size_bytes": 1234},
        "upstream": {
            "master1.example": {"ok": True, "total": 3, "down_until": None},
            "master2.example": {"ok": False, "total": 2, "down_until": 1060.0},
        },
        "bypass": {
            "peers": {
                "gui-1": {"ok": True, "solves": 2, "fails": 0},
                "gui-2": {"ok": False, "solves": 0, "fails": 1},
            }
        },
        "config": {"status": "ok"},
    }
    body = metrics.render(snapshot).decode()
    assert 'ddnet_mirror_endpoint_up{host="master1.example"} 1.0' in body
    assert 'ddnet_mirror_endpoint_up{host="master2.example"} 0.0' in body
    assert 'ddnet_mirror_endpoint_down_until_seconds{host="master2.example"} 1060.0' in body
    assert 'ddnet_mirror_bypass_peer_up{peer="gui-1"} 1.0' in body
    assert 'ddnet_mirror_bypass_peer_up{peer="gui-2"} 0.0' in body
    assert "ddnet_mirror_cache_size_bytes 1234.0" in body
    assert "ddnet_mirror_uptime_seconds 12.5" in body
    assert "ddnet_mirror_config_ok 1.0" in body


def test_never_contacted_targets_emit_no_sample():
    """Round-robin leaves later masters untouched; 0 would page for an idle one."""
    snapshot = {
        "upstream": {
            "tried.example": {"ok": True, "total": 1, "down_until": None},
            "idle.example": {"ok": False, "total": 0, "down_until": None},
        },
        "bypass": {
            "peers": {
                "unused": {"ok": False, "solves": 0, "fails": 0},
                "probed": {"ok": False, "solves": 0, "fails": 0, "probe": {"ok": True}},
            }
        },
    }
    body = metrics.render(snapshot).decode()
    assert 'ddnet_mirror_endpoint_up{host="tried.example"} 1.0' in body
    assert 'host="idle.example"' not in body
    assert 'peer="unused"' not in body
    # a reachability probe counts even before the first solve
    assert 'ddnet_mirror_bypass_peer_up{peer="probed"} 1.0' in body


def test_render_tolerates_empty_snapshot():
    body = metrics.render({}).decode()
    assert "ddnet_mirror_cache_size_bytes 0.0" in body
    assert "ddnet_mirror_config_ok 0.0" in body


def test_counters_are_labelled_and_increment():
    before = metrics.upstream_fetch_total.labels(host="h", result="ok")._value.get()  # noqa: SLF001
    metrics.upstream_fetch_total.labels(host="h", result="ok").inc()
    after = metrics.upstream_fetch_total.labels(host="h", result="ok")._value.get()  # noqa: SLF001
    assert after == before + 1
