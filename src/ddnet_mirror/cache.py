"""CacheStore: atomic file cache of raw upstream bytes.

The cache stores the upstream body byte-for-byte so mirror endpoints can
pass it through unchanged.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Callable
from pathlib import Path


class CacheStore:
    def __init__(self, path: str | Path | Callable[[], str | Path]) -> None:
        # Accept a callable so config hot-reloads can move the cache file too.
        self._path_getter: Callable[[], str | Path] = path if callable(path) else lambda: path

    @property
    def path(self) -> Path:
        return Path(self._path_getter())

    def exists(self) -> bool:
        return self.path.is_file()

    def mtime(self) -> float | None:
        try:
            return self.path.stat().st_mtime
        except OSError:
            return None

    def size(self) -> int:
        try:
            return self.path.stat().st_size
        except OSError:
            return 0

    def read_raw(self) -> bytes | None:
        try:
            return self.path.read_bytes()
        except OSError:
            return None

    def write(self, data: bytes) -> None:
        """Atomic write via temp file + os.replace so readers never see a partial file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=f".{self.path.name}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass