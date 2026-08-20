from pathlib import Path

import pytest
import yaml

from ddnet_mirror.config import AppConfig, ConfigManager, load_config


def base_yaml():
    return {
        "upstream": {
            "endpoints": [
                "https://master1.example/a.json",
                "https://master2.example/a.json",
            ]
        }
    }


def write(tmp_path: Path, data: dict) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    return p


def test_datasources_null_treated_as_empty(tmp_path):
    data = base_yaml()
    data["datasources"] = None  # YAML `datasources:` with no content
    cfg = load_config(write(tmp_path, data))
    assert cfg.datasources == []


def test_datasources_empty_list(tmp_path):
    data = base_yaml()
    data["datasources"] = []
    cfg = load_config(write(tmp_path, data))
    assert cfg.datasources == []


def test_load_minimal(tmp_path):
    cfg = load_config(write(tmp_path, base_yaml()))
    assert isinstance(cfg, AppConfig)
    assert cfg.server.port == 8080
    assert cfg.upstream.endpoints == [
        "https://master1.example/a.json",
        "https://master2.example/a.json",
    ]
    assert cfg.throttle.min_interval_s == 1.0
    assert cfg.background.base_interval_s == 60.0
    assert cfg.background.extended_interval_s == 300.0
    assert cfg.datasources == []


def test_load_custom_values(tmp_path):
    data = base_yaml()
    data.update(
        {
            "server": {"host": "127.0.0.1", "port": 9000},
            "throttle": {"min_interval_s": 2.5},
            "background": {"extend_after_idle_cycles": 3, "extended_interval_s": 600},
        }
    )
    cfg = load_config(write(tmp_path, data))
    assert cfg.server.port == 9000
    assert cfg.throttle.min_interval_s == 2.5
    assert cfg.background.extend_after_idle_cycles == 3
    assert cfg.background.extended_interval_s == 600


def test_empty_endpoints_rejected(tmp_path):
    with pytest.raises(ValueError):
        load_config(write(tmp_path, {"upstream": {"endpoints": []}}))


def test_bad_yaml_rejected(tmp_path):
    p = tmp_path / "config.yaml"
    p.write_text(": bad [[[", encoding="utf-8")
    with pytest.raises(yaml.YAMLError):
        load_config(p)


def test_reload_swaps_config(tmp_path):
    p = write(tmp_path, base_yaml())
    m = ConfigManager(p)
    assert m.config.server.port == 8080
    data = base_yaml()
    data["server"] = {"port": 9999}
    write(tmp_path, data)
    assert m.reload() is True
    assert m.config.server.port == 9999
    assert m.status == "ok"
    assert m.last_error is None


def test_reload_bad_yaml_keeps_old(tmp_path):
    p = write(tmp_path, base_yaml())
    m = ConfigManager(p)
    port = m.config.server.port
    p.write_text(": bad [[[", encoding="utf-8")
    assert m.reload() is False
    assert m.config.server.port == port  # old config retained
    assert m.status == "error"
    assert m.last_error  # error surfaced for /health