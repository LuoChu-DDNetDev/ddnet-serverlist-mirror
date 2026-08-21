"""CacheStore: atomic file cache of raw upstream bytes, with an in-memory snapshot.

The cache stores the upstream body byte-for-byte so mirror endpoints can pass it
through unchanged. Because the payload is ~1.2 MiB and every client asks for the
same bytes, the store keeps the current content in memory together with its ETag
and a pre-compressed gzip variant: re-reading and re-compressing per request
would dominate the cost of serving.

The in-memory copy is validated against (mtime, size) on every access, so an
external writer (or a config change pointing elsewhere) is still picked up.
"""

from __future__ import annotations

import gzip
import hashlib
import os
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CacheSnapshot:
    raw: bytes
    etag: str
    mtime: float
    size: int
    gzipped: bytes | None = None
    gzip_etag: str | None = None


def _etag(data: bytes, suffix: str = "") -> str:
    return '"' + hashlib.blake2b(data, digest_size=8).hexdigest() + suffix + '"'


class CacheStore:
    def __init__(
        self,
        path: str | Path | Callable[[], str | Path],
        precompress: bool = True,
        gzip_level: int = 6,
    ) -> None:
        # Accept a callable so config hot-reloads can move the cache file too.
        self._path_getter: Callable[[], str | Path] = path if callable(path) else lambda: path
        self._precompress = precompress
        self._gzip_level = gzip_level
        self._snap: CacheSnapshot | None = None
        self._snap_path: Path | None = None

    @property
    def path(self) -> Path:
        return Path(self._path_getter())

    def exists(self) -> bool:
        return self.path.is_file()

    def _stat(self) -> os.stat_result | None:
        try:
            return self.path.stat()
        except OSError:
            return None

    def mtime(self) -> float | None:
        st = self._stat()
        return st.st_mtime if st is not None else None

    def size(self) -> int:
        st = self._stat()
        return st.st_size if st is not None else 0

    def snapshot(self) -> CacheSnapshot | None:
        """Current content, served from memory unless the file changed underneath."""
        st = self._stat()
        if st is None:
            self._snap, self._snap_path = None, None
            return None
        path = self.path
        snap = self._snap
        if (
            snap is not None
            and self._snap_path == path
            and snap.mtime == st.st_mtime
            and snap.size == st.st_size
        ):
            return snap
        try:
            raw = path.read_bytes()
        except OSError:
            return None
        return self._remember(raw, st.st_mtime, path)

    def _remember(self, raw: bytes, mtime: float, path: Path) -> CacheSnapshot:
        gzipped = gzip.compress(raw, self._gzip_level) if self._precompress else None
        snap = CacheSnapshot(
            raw=raw,
            etag=_etag(raw),
            mtime=mtime,
            size=len(raw),
            gzipped=gzipped,
            gzip_etag=_etag(raw, "-gz") if gzipped is not None else None,
        )
        self._snap, self._snap_path = snap, path
        return snap

    def read_raw(self) -> bytes | None:
        snap = self.snapshot()
        return snap.raw if snap is not None else None

    def write(self, data: bytes) -> None:
        """Atomic write via temp file + os.replace so readers never see a partial file."""
        path = self.path
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
        finally:
            try:
                os.unlink(tmp)
            except OSError:
                pass
        st = self._stat()
        if st is not None:  # refresh the snapshot without reading the file back
            self._remember(data, st.st_mtime, path)
