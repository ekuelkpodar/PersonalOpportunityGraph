"""
main.py — FastAPI application entry point.
Mounts all routers, configures CORS for frontend dev server on port 5173,
and sets up APScheduler for nightly scoring job.
"""
from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.opportunities import router as opportunities_router
from backend.api.graph import router as graph_router
from backend.api.pipeline import router as pipeline_router
from backend.api.actions import router as actions_router
from backend.api.feedback import router as feedback_router
from backend.api.chat import router as chat_router
from backend.api.websocket import router as ws_router
from backend.api.dashboard import router as dashboard_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Personal Opportunity Graph API",
    description="Graph-based opportunity scoring and action engine",
    version="1.0.0",
)

# ── CORS ──────────────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://localhost:8001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ────────────────────────────────────────────────────────────────────
app.include_router(dashboard_router)
app.include_router(opportunities_router)
app.include_router(graph_router)
app.include_router(pipeline_router)
app.include_router(actions_router)
app.include_router(feedback_router)
app.include_router(chat_router)
app.include_router(ws_router)


# ── Health check ───────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "service": "pog-api"}


# ── APScheduler: nightly scoring at 2am ───────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from backend.config import SCORING_JOB_HOUR, SCORING_JOB_MINUTE

    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        _run_nightly_scoring,
        "cron",
        hour=SCORING_JOB_HOUR,
        minute=SCORING_JOB_MINUTE,
        id="nightly_scoring",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Nightly scoring job scheduled at {SCORING_JOB_HOUR:02d}:{SCORING_JOB_MINUTE:02d}")


async def _run_nightly_scoring():
    """Async wrapper for the nightly scoring job."""
    import asyncio
    loop = asyncio.get_event_loop()
    try:
        logger.info("Starting nightly scoring job...")
        from backend.graph.scorer import run_scoring_job
        await loop.run_in_executor(None, run_scoring_job)
        logger.info("Nightly scoring job complete.")
    except Exception as e:
        logger.error(f"Nightly scoring job failed: {e}")


if __name__ == "__main__":
    import uvicorn
    from backend.config import API_HOST, API_PORT
    uvicorn.run("backend.main:app", host=API_HOST, port=API_PORT, reload=True)
