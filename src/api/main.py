"""
UC REST API — FastAPI application entry point.

Routers are versioned under /v1/. The existing MCP observability server
is mounted unchanged at /mcp so legacy clients continue to work on port 8002.

Run:
    uvicorn src.api.main:app --host 0.0.0.0 --port 8002
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

logger = logging.getLogger(__name__)

# ── Rate limiter ──────────────────────────────────────────────────────────────

limiter = Limiter(key_func=get_remote_address)


# ── Lifespan: startup orphan cleanup ─────────────────────────────────────────

@asynccontextmanager
async def lifespan(application: FastAPI):
    from src.api.dependencies import get_run_store
    store = get_run_store()
    cleaned = store.cleanup_orphans()
    if cleaned:
        logger.warning("startup: marked %d orphaned runs as failed (server_restart)", cleaned)
    yield


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="UC REST API",
    description="REST API layer for UC1 pipeline, UC2 observability, UC3 search, UC4 recommendations.",
    version="1.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Optional API key auth middleware ─────────────────────────────────────────

@app.middleware("http")
async def api_key_middleware(request: Request, call_next):
    if os.getenv("API_KEY_ENABLED", "false").lower() == "true":
        if request.url.path not in ("/health", "/docs", "/redoc", "/openapi.json"):
            key = request.headers.get("X-API-Key", "")
            expected = os.getenv("API_KEY", "")
            if not expected or key != expected:
                return JSONResponse(
                    status_code=401,
                    content={"error": "unauthorized", "detail": "Invalid or missing X-API-Key header"},
                )
    return await call_next(request)


# ── Health endpoint ───────────────────────────────────────────────────────────

@app.get("/health", tags=["meta"])
def health():
    from src.api.dependencies import get_cache_client, get_hybrid_search, get_recommender

    deps: dict[str, str] = {}

    # Redis
    try:
        cache = get_cache_client()
        stats = cache.get_stats()
        deps["redis"] = "ok" if stats.redis_connected else "degraded"
    except Exception:
        deps["redis"] = "degraded"

    # Postgres
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            dbname=os.getenv("POSTGRES_DB", "uc2"),
            user=os.getenv("POSTGRES_USER", "mip"),
            password=os.getenv("POSTGRES_PASSWORD", "REMOVED_PG_PASSWORD"),
            connect_timeout=2,
        )
        conn.close()
        deps["postgres"] = "ok"
    except Exception:
        deps["postgres"] = "degraded"

    # Prometheus
    try:
        import urllib.request
        prom_url = os.getenv("PROMETHEUS_URL", "http://localhost:9090/-/healthy")
        urllib.request.urlopen(prom_url, timeout=2)
        deps["prometheus"] = "ok"
    except Exception:
        deps["prometheus"] = "degraded"

    # UC3 search
    try:
        search = get_hybrid_search()
        deps["search_index"] = "ok" if search.is_ready() else "not_ready"
    except Exception:
        deps["search_index"] = "degraded"

    # UC4 recommendations
    try:
        rec = get_recommender()
        deps["rec_graph"] = "ok" if rec.is_ready() else "not_ready"
    except Exception:
        deps["rec_graph"] = "degraded"

    overall = "ok" if all(v in ("ok", "not_ready") for v in deps.values()) else "degraded"
    return {"status": overall, "dependencies": deps}


# ── Mount MCP sub-app (preserves /mcp/tools/* paths) ─────────────────────────

try:
    from src.uc2_observability.mcp_server import app as mcp_app
    app.mount("/mcp", mcp_app)
except Exception as exc:
    logger.warning("MCP server could not be mounted: %s", exc)


# ── Register v1 routers ───────────────────────────────────────────────────────

from src.api.routers import pipeline as pipeline_router_module
from src.api.routers import observability as obs_router_module
from src.api.routers import search as search_router_module
from src.api.routers import recommendations as rec_router_module
from src.api.routers import ops as ops_router_module

app.include_router(pipeline_router_module.router, prefix="/v1/pipeline", tags=["pipeline"])
app.include_router(obs_router_module.router, prefix="/v1/observability", tags=["observability"])
app.include_router(search_router_module.router, prefix="/v1/search", tags=["search"])
app.include_router(rec_router_module.router, prefix="/v1/recommendations", tags=["recommendations"])
app.include_router(ops_router_module.router, prefix="/v1/ops", tags=["ops"])
