"""FastAPI application factory for the mirror service (Service A)."""

from __future__ import annotations

import time
from email.utils import formatdate, parsedate_to_datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response

from . import metrics
from .cache import CacheSnapshot
from .config import ConfigManager
from .health import HealthAggregator
from .limits import LimitsMiddleware
from .throttler import RefreshOrchestrator

_JSON = "application/json"


def _accepts_gzip(request: Request) -> bool:
    return "gzip" in request.headers.get("accept-encoding", "").lower()


def _not_modified(request: Request, etag: str, mtime: float) -> bool:
    inm = request.headers.get("if-none-match")
    if inm:
        return any(tag.strip().removeprefix("W/") == etag for tag in inm.split(","))
    ims = request.headers.get("if-modified-since")
    if ims:
        try:
            return parsedate_to_datetime(ims).timestamp() >= int(mtime)
        except (TypeError, ValueError):
            return False
    return False


def serve_snapshot(snap: CacheSnapshot, request: Request, max_age: float) -> Response:
    """Byte-identical upstream body plus validators, so repeat clients cost ~0."""
    body, etag, encoding = snap.raw, snap.etag, "identity"
    headers = {
        "Vary": "Accept-Encoding",
        "Last-Modified": formatdate(snap.mtime, usegmt=True),
        "Cache-Control": f"public, max-age={int(max_age)}",
    }
    if snap.gzipped is not None and snap.gzip_etag is not None and _accepts_gzip(request):
        body, etag, encoding = snap.gzipped, snap.gzip_etag, "gzip"
        headers["Content-Encoding"] = "gzip"
    headers["ETag"] = etag
    endpoint = request.url.path
    if _not_modified(request, etag, snap.mtime):
        metrics.cache_response_total.labels(kind="not_modified").inc()
        return Response(status_code=304, headers=headers)
    metrics.cache_response_total.labels(kind=encoding).inc()
    metrics.response_bytes.labels(endpoint=endpoint, encoding=encoding).inc(len(body))
    return Response(content=body, media_type=_JSON, headers=headers)


def _bearer(request: Request) -> str | None:
    auth = request.headers.get("authorization", "")
    return auth[7:].strip() if auth.lower().startswith("bearer ") else None


def _minimal_health(snap: dict) -> dict:
    """Public health: liveness and cache freshness, nothing identifying."""
    cache = snap.get("cache") or {}
    return {
        "status": snap.get("status"),
        "cache": {
            "exists": cache.get("exists"),
            "updated_at": cache.get("updated_at"),
            "size_bytes": cache.get("size_bytes"),
        },
        "uptime_seconds": (snap.get("process") or {}).get("uptime_seconds"),
    }


def create_app(
    manager: ConfigManager,
    orchestrator: RefreshOrchestrator,
    health: HealthAggregator | None = None,
    assets_dir: str | Path = "assets",
) -> FastAPI:
    app = FastAPI(title="DDNet server list mirror", version="0.1.0")
    app.add_middleware(LimitsMiddleware, manager=manager)

    @app.middleware("http")
    async def _instrument(request: Request, call_next):
        started = time.perf_counter()
        response = await call_next(request)
        endpoint = request.url.path
        metrics.requests_total.labels(endpoint=endpoint, code=response.status_code).inc()
        metrics.request_duration.labels(endpoint=endpoint).observe(time.perf_counter() - started)
        return response

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

    def _respond(snap: CacheSnapshot | None, request: Request) -> Response:
        if snap is None:
            raise HTTPException(status_code=503, detail="no cached server list available")
        return serve_snapshot(snap, request, manager.config.throttle.min_interval_s)

    async def _servers_immediate(request: Request) -> Response:
        return _respond(await orchestrator.snapshot_immediate(), request)

    async def _servers_cache(request: Request) -> Response:
        return _respond(orchestrator.snapshot_cached(), request)

    async def _full_snapshot() -> dict:
        snap = await health.async_snapshot(probe_ttl=manager.config.health.probe_ttl_s)
        snap["refresh"] = {
            "last_ok_at": orchestrator.last_refresh_ok_ts,
            "last_error": orchestrator.last_refresh_error,
            "upstream_attempts": orchestrator.upstream_attempts,
            "gated_requests": orchestrator.gated_requests,
        }
        return snap

    async def _health_endpoint(request: Request) -> dict:
        if health is None:
            return {"status": "unknown", "note": "health aggregator not wired"}
        snap = await _full_snapshot()
        token = manager.config.health.auth_token
        # Without a matching token the details (pid, config path, upstream errors)
        # stay private; liveness stays public.
        if token and _bearer(request) == token:
            return snap
        return _minimal_health(snap)

    @app.get("/", include_in_schema=False)
    async def root(request: Request) -> Response:
        return await _servers_immediate(request)

    @app.get("/health")
    async def root_health(request: Request) -> dict:
        return await _health_endpoint(request)

    @app.get("/api/v1/servers")
    async def servers_immediate(request: Request) -> Response:
        return await _servers_immediate(request)

    @app.get("/api/v1/servers/cache")
    async def servers_cache(request: Request) -> Response:
        return await _servers_cache(request)

    @app.get("/api/v1/health")
    async def health_endpoint(request: Request) -> dict:
        return await _health_endpoint(request)

    metrics_cfg = manager.config.metrics
    if metrics_cfg.enabled:

        @app.get(metrics_cfg.path, include_in_schema=False)
        async def metrics_endpoint(request: Request) -> Response:
            token = manager.config.metrics.auth_token or manager.config.health.auth_token
            if token and _bearer(request) != token:
                raise HTTPException(status_code=401, detail="unauthorized")
            snap = await _full_snapshot() if health is not None else {}
            return Response(content=metrics.render(snap), media_type=metrics.CONTENT_TYPE)

    return app
