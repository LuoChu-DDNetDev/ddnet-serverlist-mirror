"""Rotating file handler for the mirror service.

Log files are named `<base>_YYYYMMDD_HHMMSS.log` at startup. When one file
grows past `max_bytes` (5MB) or lives longer than `max_age_seconds` (7 days),
it is rotated: the old file is closed and a new file `<base>_YYYYMMDD_HHMMSS_1.log`
(``_2``, ``_3``, ...) is opened, keeping the original startup timestamp.
"""

from __future__ import annotations

import logging
import os
import time

# Sub-second opens inside tests would collide; guard against zero-length intervals.
_MIN_AGE = 1.0


class StartupRotatingFileHandler(logging.FileHandler):
    def __init__(
        self,
        path: str,
        max_bytes: int = 5 * 1024 * 1024,
        max_age_seconds: float = 7 * 24 * 3600,
        encoding: str | None = None,
        delay: bool = True,
        clock=time.time,
    ) -> None:
        self.base_file = path
        self.max_bytes = max_bytes
        self.max_age = max_age_seconds if max_age_seconds >= _MIN_AGE else _MIN_AGE
        self._clock = clock
        self._suffix = 0
        self._opened_at = clock()
        super().__init__(self._current_path(), mode="a", encoding=encoding, delay=delay)

    def _current_path(self) -> str:
        name, ext = os.path.splitext(self.base_file)
        suffix = f"_{self._suffix}" if self._suffix else ""
        return f"{name}{suffix}{ext}"

    def should_rollover(self, record: logging.LogRecord) -> bool:
        if self.max_bytes > 0 and self.stream is not None:
            try:
                self.stream.seek(0, os.SEEK_END)
                if self.stream.tell() + len(record.getMessage()) >= self.max_bytes:
                    return True
            except OSError:
                pass
        if self._clock() - self._opened_at >= self.max_age:
            return True
        return False

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if self.should_rollover(record):
                self.rollover()
        except Exception:  # noqa: BLE001 - never let logging break the app
            self.handleError(record)
        super().emit(record)

    def rollover(self) -> None:
        if self.stream is not None:
            self.stream.close()
            self.stream = None
        self._suffix += 1
        self._opened_at = self._clock()
        self.baseFilename = self._current_path()
        self.stream = self._open()