"""Entrypoint for the mirror service (Service A): `ddnet-mirror --config ...`."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import uvicorn

from .cache import CacheStore
from .config import ConfigManager
from .datasources import apply_datasources, build_sources
from .health import HealthAggregator
from .logs import StartupRotatingFileHandler
from .nodriver_client import NodriverClient
from .throttler import RefreshOrchestrator
from .upstream import UpstreamClient
from .web import create_app


def _setup_logging(cfg) -> None:
    level = getattr(logging, str(cfg.logging.level).upper(), logging.INFO)
    handlers = [logging.StreamHandler(sys.stderr)]
    if cfg.logging.dir:
        try:
            log_dir = Path(cfg.logging.dir)
            log_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime(cfg.logging.timestamp_format)
            name = cfg.logging.filename.format(stamp=stamp)
            path = log_dir / name
            handlers.append(
                StartupRotatingFileHandler(
                    str(path),
                    max_bytes=cfg.logging.max_bytes,
                    max_age_seconds=cfg.logging.max_age_seconds,
                )
            )
        except OSError as exc:
            print(f"warning: file logging disabled ({exc})", file=sys.stderr)
    logging.basicConfig(
        level=level,
        handlers=handlers,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _build_refresher(manager, cache, upstream, bypass):
    """Closure that fetches upstream, optionally fuses external sources, persists cache."""
    log = logging.getLogger("ddnet_mirror.refresh")

    async def refresher() -> None:
        result = await upstream.fetch(bypass_solver=bypass.solve)
        payload = result.data  # upstream bytes verbatim unless fusion changes them
        sources = build_sources(manager.config.datasources)
        if sources:
            try:
                parsed = json.loads(result.data)
                fused = await apply_datasources(parsed, sources)
                if fused is not parsed:  # every source failed: keep the original bytes
                    payload = json.dumps(
                        fused, ensure_ascii=False, separators=(",", ":")
                    ).encode()
            except Exception as exc:  # noqa: BLE001 - fusion must never block serving
                log.warning("datasource fusion failed, storing upstream bytes: %s", exc)
                payload = result.data
        cache.write(payload)

    return refresher


def main() -> None:
    parser = argparse.ArgumentParser(prog="ddnet-mirror", description="DDNet server list mirror")
    parser.add_argument("--config", default="config.yaml", help="path to YAML config (default: config.yaml)")
    args = parser.parse_args()

    manager = ConfigManager(args.config)
    _setup_logging(manager.config)
    log = logging.getLogger("ddnet_mirror")

    cache = CacheStore(
        lambda: manager.config.cache.path,
        precompress=manager.config.cache.precompress,
        gzip_level=manager.config.cache.gzip_level,
    )
    # Clients read config lazily so SIGHUP reloads apply to them too.
    bypass = NodriverClient(lambda: manager.config.bypass)
    upstream = UpstreamClient(
        lambda: manager.config.upstream,
        # Reuse a solved clearance (and its egress) instead of getting challenged first.
        cookie_provider=bypass.peek,
        challenge_hook=bypass.on_challenge_persisted,
    )
    orchestrator = RefreshOrchestrator(manager, cache, _build_refresher(manager, cache, upstream, bypass))
    health = HealthAggregator(manager, cache, upstream, bypass)

    app = create_app(manager, orchestrator, health)

    @asynccontextmanager
    async def lifespan(_app):
        loop = asyncio.get_running_loop()
        manager.install_sighup_handler(loop)
        await orchestrator.start()
        await health.start()
        log.info("listening on %s:%s (config: %s)", manager.config.server.host,
            manager.config.server.port, manager.path)
        try:
            yield
        finally:
            # Record the final health state at shutdown.
            log.info("shutting down; final health: %s", health.snapshot())
            await orchestrator.stop()
            await health.stop()
            await upstream.close()
            await bypass.close()

    app.router.lifespan_context = lifespan

    try:
        uvicorn.run(
            app,
            host=manager.config.server.host,
            port=manager.config.server.port,
            log_level=str(manager.config.logging.level).lower(),
            log_config=None,
        )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()