"""Pluggable external JSON data sources and fusion strategies.

A failing source is logged and skipped: external data must never break the
main upstream-serving flow.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from typing import Any

import httpx

from .config import DataSourceConfig

logger = logging.getLogger(__name__)


class DataSourceError(Exception):
    """A single datasource failed; treated as skip, not fatal."""


class DataSource(ABC):
    def __init__(self, cfg: DataSourceConfig) -> None:
        self.cfg = cfg

    @property
    def name(self) -> str:
        return self.cfg.name

    @abstractmethod
    async def fetch(self) -> Any:
        """Return a JSON-decoded value (list or dict). Raise DataSourceError on failure."""


class HttpSource(DataSource):
    def __init__(self, cfg: DataSourceConfig, http_factory=None, timeout: float = 15.0) -> None:
        super().__init__(cfg)
        self._http_factory = http_factory
        self._timeout = timeout

    async def fetch(self) -> Any:
        if not self.cfg.url:
            raise DataSourceError(f"datasource {self.name}: missing url")
        client = self._http_factory() if self._http_factory else httpx.AsyncClient(timeout=self._timeout)
        try:
            resp = await client.get(self.cfg.url)
        except httpx.HTTPError as exc:
            raise DataSourceError(f"datasource {self.name}: {exc}") from exc
        finally:
            if self._http_factory is None:
                await client.aclose()
        if resp.status_code != 200:
            raise DataSourceError(f"datasource {self.name}: HTTP {resp.status_code}")
        return resp.json()


class FileSource(DataSource):
    async def fetch(self) -> Any:
        if not self.cfg.path:
            raise DataSourceError(f"datasource {self.name}: missing path")
        try:
            with open(self.cfg.path, encoding="utf-8") as fh:
                return json.load(fh)
        except OSError as exc:
            raise DataSourceError(f"datasource {self.name}: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise DataSourceError(f"datasource {self.name}: invalid JSON: {exc}") from exc


class DbSource(DataSource):
    """Interface stub for a database-backed datasource (future extension point)."""

    async def fetch(self) -> Any:
        # TODO(extension): implement an adapter (e.g. SQLAlchemy / D1 / asyncpg).
        raise DataSourceError(f"datasource {self.name}: DbSource adapter not implemented yet")


_SOURCE_TYPES: dict[str, type[DataSource]] = {
    "http": HttpSource,
    "file": FileSource,
    "db": DbSource,
}


def build_source(cfg: DataSourceConfig, http_factory=None) -> DataSource:
    cls = _SOURCE_TYPES.get(cfg.type)
    if cls is None:
        raise DataSourceError(f"unknown datasource type: {cfg.type}")
    if cls is HttpSource:
        return cls(cfg, http_factory=http_factory)
    return cls(cfg)


def _dedupe(items: list, key: str) -> list:
    """Dedupe list items by `key` (or by value for scalars), keeping first occurrence."""
    seen: set = set()
    out: list = []
    for item in items:
        k = item.get(key) if isinstance(item, dict) else item
        if k in seen:
            continue
        seen.add(k)
        out.append(item)
    return out


def apply_strategy(base: Any, ext: Any, strategy: str, key: str | None = None) -> Any:
    """Merge external data into base JSON per the configured strategy."""
    key = key or "address"
    if strategy == "append":
        if isinstance(base, list) and isinstance(ext, list):
            return base + ext
        if isinstance(base, dict) and isinstance(ext, dict):
            merged = dict(base)
            merged.update(ext)
            return merged
        if isinstance(base, list):
            return base + [ext]
        if isinstance(ext, list):
            return [base] + ext
        return ext
    if strategy == "merge":
        if isinstance(base, list) and isinstance(ext, list):
            return _dedupe(base + ext, key)
        if isinstance(base, dict) and isinstance(ext, dict):
            merged = dict(base)
            for k, v in ext.items():
                if k in merged and isinstance(merged[k], list) and isinstance(v, list):
                    merged[k] = _dedupe(merged[k] + v, key)
                else:
                    merged[k] = v
            return merged
        return ext
    if strategy == "override":
        return ext
    if strategy == "additional":
        # Attached by the caller under a dedicated top-level key.
        return ext
    raise ValueError(f"unknown strategy: {strategy}")


async def apply_datasources(base: Any, sources: list[DataSource]) -> Any:
    """Apply enabled sources in order; a failing source is logged and skipped."""
    result = base
    for src in sources:
        try:
            data = await src.fetch()
        except Exception as exc:  # noqa: BLE001 - isolation is the contract
            logger.warning("datasource %s failed, skipped: %s", src.name, exc)
            continue
        if src.cfg.strategy == "additional":
            target = src.cfg.target_key or src.name
            if not isinstance(result, dict):
                result = {"servers": result, target: data}
            else:
                result = dict(result)
                result[target] = data
        else:
            result = apply_strategy(result, data, src.cfg.strategy, src.cfg.key)
    return result