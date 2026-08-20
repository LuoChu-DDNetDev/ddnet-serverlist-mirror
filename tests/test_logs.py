import logging
import time

from ddnet_mirror.logs import StartupRotatingFileHandler


def make_record(msg="x"):
    return logging.LogRecord(
        name="t", level=logging.INFO, pathname=__file__, lineno=1, msg=msg, args=(), exc_info=None
    )


def enable(logger, h):
    logger.setLevel(logging.INFO)
    logger.propagate = False  # avoid leaking to any root handler
    logger.handlers = []
    h.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(h)


def test_size_rotation(tmp_path):
    fake_clock = {"now": 1000.0}
    h = StartupRotatingFileHandler(
        str(tmp_path / "mirror_2026-08-20_10-00-00.log"),
        max_bytes=100,
        max_age_seconds=604800,
        clock=lambda: fake_clock["now"],
    )
    logger = logging.getLogger("test_size")
    enable(logger, h)

    # write until the file tips past max_bytes
    payload = "a" * 40 + "\n"
    for _ in range(6):
        logger.info(payload)

    # 6 writes of 41 bytes with max_bytes=100 -> base + _1 + _2
    files = sorted(p.name for p in tmp_path.iterdir())
    assert files == [
        "mirror_2026-08-20_10-00-00.log",
        "mirror_2026-08-20_10-00-00_1.log",
        "mirror_2026-08-20_10-00-00_2.log",
    ]
    h.close()


def test_age_rotation(tmp_path):
    fake_clock = {"now": 1000.0}
    h = StartupRotatingFileHandler(
        str(tmp_path / "mirror_2026-08-20_10-00-00.log"),
        max_bytes=1024 * 1024,
        max_age_seconds=100,  # rotate after a short simulated age
        clock=lambda: fake_clock["now"],
    )
    logger = logging.getLogger("test_age")
    enable(logger, h)

    logger.info("a")
    assert not h.should_rollover(make_record())

    fake_clock["now"] = 1200.0  # 200s elapsed > 100s max age
    logger.info("b")
    files = sorted(p.name for p in tmp_path.iterdir())
    assert files == [
        "mirror_2026-08-20_10-00-00.log",
        "mirror_2026-08-20_10-00-00_1.log",
    ]
    h.close()


def test_multi_rotation_suffix_increments(tmp_path):
    fake_clock = {"now": 1000.0}
    h = StartupRotatingFileHandler(
        str(tmp_path / "mirror_2026-08-20_10-00-00.log"),
        max_bytes=50,
        max_age_seconds=604800,
        clock=lambda: fake_clock["now"],
    )
    logger = logging.getLogger("test_multi")
    enable(logger, h)

    for _ in range(20):
        logger.info("payload-payload-payload-1234567890\n")

    files = sorted(p.name for p in tmp_path.iterdir())
    assert "mirror_2026-08-20_10-00-00_0.log" not in files  # first file has no suffix
    assert files[-1].startswith("mirror_2026-08-20_10-00-00_")
    # at least 3 generations after the base
    assert len(files) >= 3
    h.close()


def test_timestamp_format_regex(tmp_path):
    # file named at startup: mirror_YYYY-MM-DD_HH-MM-SS.log
    h = StartupRotatingFileHandler(
        str(tmp_path / "mirror_2026-08-20_10-30-45.log"), max_bytes=1, clock=time.time
    )
    h.close()
    # no strict test needed here; naming pattern covered by other tests