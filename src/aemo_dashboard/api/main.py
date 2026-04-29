"""AEMO Mobile API — FastAPI service serving JSON to the Nem Analyst iPhone app.

Lives at port 8002 on .71. Read-only over the DuckDB at
/Users/davidleitch/aemo_production/data/aemo_readonly.duckdb.

See ./CLAUDE.md and the implementation plan for context.
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from .auth import bearer_token_middleware
from .routers import gauges, generation, meta, outages, prices


def create_app() -> FastAPI:
    app = FastAPI(
        title="AEMO Mobile API",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
    )

    app.add_middleware(GZipMiddleware, minimum_size=1024)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["https://*.itkservices2.com"],
        allow_methods=["GET"],
        allow_headers=["Authorization", "CF-Access-Client-Id", "CF-Access-Client-Secret"],
    )
    app.middleware("http")(bearer_token_middleware)

    app.include_router(meta.router, prefix="/v1")
    app.include_router(prices.router, prefix="/v1")
    app.include_router(gauges.router, prefix="/v1")
    app.include_router(outages.router, prefix="/v1")
    app.include_router(generation.router, prefix="/v1")

    return app


app = create_app()
