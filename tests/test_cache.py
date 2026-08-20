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