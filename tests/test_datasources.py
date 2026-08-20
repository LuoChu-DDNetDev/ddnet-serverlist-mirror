
import pytest

from ddnet_mirror.config import DataSourceConfig
from ddnet_mirror.datasources import (
    DataSource,
    DataSourceError,
    DbSource,
    FileSource,
    HttpSource,
    apply_datasources,
    apply_strategy,
    build_source,
)


class FakeOk(DataSource):
    async def fetch(self):
        return [{"b": 2}]


class FakeBad(DataSource):
    async def fetch(self):
        raise DataSourceError("offline")


def cfg(**kw):
    defaults = {"name": "x", "type": "http"}
    defaults.update(kw)
    return DataSourceConfig(**defaults)


def test_append_lists():
    out = apply_strategy([{"a": 1}], [{"b": 2}], "append")
    assert out == [{"a": 1}, {"b": 2}]


def test_append_dicts():
    out = apply_strategy({"a": 1}, {"b": 2}, "append")
    assert out == {"a": 1, "b": 2}


def test_merge_dedupes_by_key():
    base = [{"address": "x", "p": 1}, {"address": "y", "p": 2}]
    ext = [{"address": "y", "p": 99}, {"address": "z", "p": 3}]
    out = apply_strategy(base, ext, "merge", "address")
    assert len(out) == 3
    assert out[0] == {"address": "x", "p": 1}
    assert out[1] == {"address": "y", "p": 2}  # base wins on duplicate
    assert out[2] == {"address": "z", "p": 3}


def test_merge_nested_lists_in_dict():
    base = {"servers": [{"address": "x"}]}
    ext = {"servers": [{"address": "x"}, {"address": "y"}]}
    out = apply_strategy(base, ext, "merge", "address")
    assert len(out["servers"]) == 2


def test_override():
    assert apply_strategy([1, 2], [9], "override") == [9]


async def test_failing_source_skipped_other_applied():
    base = [{"a": 1}]
    out = await apply_datasources(base, [FakeOk(cfg(name="good")), FakeBad(cfg(name="bad", type="file"))])
    assert out == [{"a": 1}, {"b": 2}]


async def test_all_sources_fail_leaves_base_intact():
    base = {"servers": [{"a": 1}]}
    out = await apply_datasources(base, [FakeBad(cfg(name="1")), FakeBad(cfg(name="2"))])
    assert out == base


async def test_additional_strategy_attaches_top_level():
    class Ext(DataSource):
        async def fetch(self):
            return {"count": 3}

    src = Ext(cfg(name="meta", strategy="additional", target_key="external"))
    base = {"servers": [{"a": 1}]}
    out = await apply_datasources(base, [src])
    assert out["external"] == {"count": 3}
    assert out["servers"] == [{"a": 1}]


def test_unknown_strategy_raises():
    with pytest.raises(ValueError):
        apply_strategy([], [], "banana")


def test_unknown_source_type_raises():
    with pytest.raises(DataSourceError):
        build_source(cfg(type="nope"))


def test_build_known_sources():
    assert isinstance(build_source(cfg(type="http")), HttpSource)
    assert isinstance(build_source(cfg(type="file")), FileSource)
    assert isinstance(build_source(cfg(type="db")), DbSource)