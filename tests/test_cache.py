import gzip
import os
import time

from ddnet_mirror.cache import CacheStore


def test_write_read_roundtrip(tmp_path):
    store = CacheStore(tmp_path / "sub" / "servers.json")
    assert store.exists() is False
    store.write(b'{"servers": []}')
    assert store.exists()
    assert store.read_raw() == b'{"servers": []}'
    assert store.size() == len(b'{"servers": []}')
    assert store.mtime() is not None


def test_write_overwrites_atomically(tmp_path):
    store = CacheStore(tmp_path / "servers.json")
    store.write(b"v1")
    store.write(b"v2-longer-content")
    assert store.read_raw() == b"v2-longer-content"
    # no leftover temp files
    leftovers = [p.name for p in tmp_path.iterdir() if ".tmp" in p.name]
    assert leftovers == []


def test_missing_file(tmp_path):
    store = CacheStore(tmp_path / "nope.json")
    assert store.read_raw() is None
    assert store.mtime() is None
    assert store.size() == 0
    assert store.exists() is False


def test_callable_path_reloads(tmp_path):
    current = tmp_path / "a.json"
    store = CacheStore(lambda: current)
    store.write(b"1")
    assert store.exists() is True
    current = tmp_path / "b.json"  # config reload moved the cache file
    assert store.exists() is False
    store.write(b"2")
    assert (tmp_path / "b.json").read_bytes() == b"2"


def test_snapshot_is_reused_until_the_file_changes(tmp_path):
    store = CacheStore(tmp_path / "servers.json")
    store.write(b'{"servers": []}')
    first = store.snapshot()
    assert store.snapshot() is first  # served from memory
    assert first.gzipped == gzip.compress(first.raw, 6)
    assert first.etag != first.gzip_etag

    store.write(b'{"servers": [1]}')
    second = store.snapshot()
    assert second is not first
    assert second.raw == b'{"servers": [1]}'
    assert second.etag != first.etag


def test_snapshot_picks_up_external_writes(tmp_path):
    path = tmp_path / "servers.json"
    store = CacheStore(path)
    store.write(b"one")
    assert store.snapshot().raw == b"one"

    path.write_bytes(b"external")  # someone else replaced the file
    future = time.time() + 2
    os.utime(path, (future, future))
    assert store.snapshot().raw == b"external"


def test_precompress_can_be_disabled(tmp_path):
    store = CacheStore(tmp_path / "servers.json", precompress=False)
    store.write(b"{}")
    snap = store.snapshot()
    assert snap.gzipped is None
    assert snap.gzip_etag is None