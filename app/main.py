"""
main.py — FastAPI application entry point.

Startup: init DB schema → init Redis → load symbol masters → start scheduler → start BLPOP loops.
Shutdown: stop background tasks → close Redis → close DB pool.
"""

import asyncio
import logging
import sys

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.database import init_db, close_db
from app.redis_client import init_redis, close_redis
from app.scheduler import create_scheduler

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="NSE VRVP+SVP Trading System",
    version="1.0.0",
    docs_url="/docs" if not settings.is_production else None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

_scheduler = None
_background_tasks: list[asyncio.Task] = []


@app.on_event("startup")
async def startup():
    global _scheduler

    logger.info("=== NSE VRVP+SVP System startup ===")

    # Infrastructure
    await init_db()
    await init_redis()

    # Preload symbol masters (non-fatal if unavailable at boot)
    try:
        from app.symbol_master import load_symbol_masters
        await load_symbol_masters()
    except Exception as exc:
        logger.warning("Symbol master load failed at startup (will retry later): %s", exc)

    # APScheduler (Agent 1, 2, 5-force-exit, 6, 7)
    _scheduler = create_scheduler()
    _scheduler.start()
    logger.info("Scheduler started.")

    # Background BLPOP loops (Agent 4 and Agent 5)
    from app.agents.agent4_risk_guardian import run_risk_guardian
    from app.agents.agent5_order_manager import run_order_manager, run_position_monitor

    _background_tasks.append(asyncio.create_task(run_risk_guardian(), name="agent4_risk_guardian"))
    _background_tasks.append(asyncio.create_task(run_order_manager(), name="agent5_order_manager"))
    _background_tasks.append(asyncio.create_task(run_position_monitor(), name="agent5_position_monitor"))
    logger.info("Background tasks started: %s", [t.get_name() for t in _background_tasks])

    logger.info("=== Startup complete ===")


@app.on_event("shutdown")
async def shutdown():
    logger.info("=== NSE VRVP+SVP System shutdown ===")

    from app.agents.agent4_risk_guardian import stop_risk_guardian
    from app.agents.agent5_order_manager import stop_order_manager
    await stop_risk_guardian()
    await stop_order_manager()

    for task in _background_tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    if _scheduler:
        _scheduler.shutdown(wait=False)

    await close_redis()
    await close_db()
    logger.info("=== Shutdown complete ===")


# ── Routers ───────────────────────────────────────────────────────────────────

from app.agents.agent3_webhook import router as webhook_router
from app.routers.system import router as system_router

app.include_router(webhook_router, prefix="", tags=["webhook"])
app.include_router(system_router, prefix="", tags=["system"])


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        log_level=settings.log_level.lower(),
    )
