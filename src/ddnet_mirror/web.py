"""FastAPI application factory for the mirror service (Service A)."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Response

from .config import ConfigManager
from .health import HealthAggregator
from .throttler import RefreshOrchestrator

_JSON = "application/json"


def create_app(
    manager: ConfigManager,
    orchestrator: RefreshOrchestrator,
    health: HealthAggregator | None = None,
    assets_dir: str | Path = "assets",
) -> FastAPI:
    app = FastAPI(title="DDNet server list mirror", version="0.1.0")

    def _favicon() -> Response:
        p = Path(assets_dir) / "favicon.ico"
        try:
            data = p.read_bytes()
        except OSError:
            raise HTTPException(status_code=404, detail="no favicon configured") from None
        return Response(content=data, media_type="image/x-icon")

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon() -> Response:
        return _favicon()

    async def _servers_immediate() -> Response:
        data = await orchestrator.get_immediate()
        if data is None:
            raise HTTPException(status_code=503, detail="no cached server list available")
        return Response(content=data, media_type=_JSON)

    async def _servers_cache() -> Response:
        data = await orchestrator.get_cached()
        if data is None:
            raise HTTPException(status_code=503, detail="no cached server list available")
        return Response(content=data, media_type=_JSON)
    
    async def _health_endpoint() -> dict:
            if health is None:
                return {"status": "unknown", "note": "health aggregator not wired"}
            snap = health.snapshot()
            snap["refresh"] = {
                "last_ok_at": orchestrator.last_refresh_ok_ts,
                "last_error": orchestrator.last_refresh_error,
            }
            return snap

    @app.get("/", include_in_schema=False)
    async def root() -> Response:
        return await _servers_immediate()
    
    @app.get("/health")
    async def root_health() -> Response:
            return await _health_endpoint()

    @app.get("/api/v1/servers")
    async def servers_immediate() -> Response:
        return await _servers_immediate()

    @app.get("/api/v1/servers/cache")
    async def servers_cache() -> Response:
        return await _servers_cache()

    @app.get("/api/v1/health")
    async def health_endpoint() -> Response:
        return await _health_endpoint()

    return app
