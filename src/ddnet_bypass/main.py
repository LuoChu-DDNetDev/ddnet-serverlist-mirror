"""Entrypoint for the nodriver bypass service (Service B): `ddnet-bypass`."""

from __future__ import annotations

import argparse
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from .challenger import BrowserManager, solve_cf

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("ddnet_bypass")


class SolveRequest(BaseModel):
    url: str
    host: str | None = None
    timeout: float = Field(default=30.0, ge=1, le=120)


def create_app(token: str | None = None, headless: bool = False, settle: float = 2.0) -> FastAPI:
    bm = BrowserManager(headless=headless)

    @asynccontextmanager
    async def lifespan(_app):
        yield
        await bm.stop()

    app = FastAPI(title="ddnet-bypass", version="0.1.0", lifespan=lifespan)

    @app.post("/solve")
    async def solve(req: SolveRequest, request: Request) -> dict:
        if token:
            auth = request.headers.get("Authorization", "")
            if auth != f"Bearer {token}":
                raise HTTPException(status_code=401, detail="unauthorized")
        try:
            return await solve_cf(req.url, get_browser=bm.get, timeout=req.timeout, settle=settle)
        except TimeoutError as exc:
            logger.warning("solve timeout: %s", exc)
            raise HTTPException(status_code=504, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            logger.error("solve failed: %s", exc)
            raise HTTPException(status_code=502, detail=f"solve failed: {exc}") from exc

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "service": "ddnet-bypass", "transport": "nodriver"}

    return app


def main() -> None:
    parser = argparse.ArgumentParser(prog="ddnet-bypass", description="nodriver Cloudflare challenge solver")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument("--token", default=None, help="Bearer token required by service A (optional)")
    parser.add_argument("--headless", action="store_true", help="run browser headless (no GUI)")
    args = parser.parse_args()

    app = create_app(token=args.token, headless=args.headless)
    uvicorn.run(app, host=args.host, port=args.port, log_level="info", log_config=None)


if __name__ == "__main__":
    main()