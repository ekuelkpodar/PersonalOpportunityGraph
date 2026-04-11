"""
pipeline.py — Pipeline control API endpoints.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from backend.config import PIPELINE_PROGRESS_DB
from backend.pipeline.embedder import get_embedding_stats
from backend.models import PipelineStatusResponse, SourceStatsModel

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


class RunPipelineRequest(BaseModel):
    force_reprocess: bool = False


@router.post("/run")
async def run_pipeline(request: RunPipelineRequest):
    """Start the ingestion pipeline in the background."""
    from backend.pipeline.orchestrator import start_pipeline, is_running
    if is_running():
        raise HTTPException(status_code=409, detail="Pipeline is already running")
    await start_pipeline(force_reprocess=request.force_reprocess)
    return {"status": "started", "message": "Pipeline started. Connect to /ws/pipeline for live progress."}


@router.get("/status", response_model=PipelineStatusResponse)
async def get_pipeline_status():
    """Return current pipeline status and stats."""
    from backend.pipeline.orchestrator import is_running, get_last_run_stats

    running    = is_running()
    last_stats = get_last_run_stats()
    embed_stats = get_embedding_stats()

    source_stats: Dict[str, Any] = {}
    if last_stats:
        raw = last_stats.get("stats", {})
        for src in ["feedspot", "xlist", "clutch", "facebook", "skool"]:
            s = raw.get(src, {})
            source_stats[src] = SourceStatsModel(
                source_name=src,
                file_count=s.get("files", 0),
                rows_processed=s.get("rows", 0),
                nodes_created=s.get("nodes", 0),
                dupes_skipped=s.get("dupes", 0),
                status="done" if last_stats.get("status") == "done" else "pending",
            ).dict()

    log_tail = _get_log_tail()

    return PipelineStatusResponse(
        is_running=running,
        sources_stats=source_stats,
        embedding_progress={
            "embedded_counts": embed_stats,
            "total_embedded": sum(embed_stats.values()),
        },
        last_run=last_stats.get("finished_at") if last_stats else None,
        log_tail=log_tail,
    )


@router.post("/score/run")
async def run_scoring():
    """Manually trigger the opportunity scoring job."""
    try:
        from backend.graph.scorer import run_scoring_job
        run_scoring_job()
        return {"status": "done", "message": "Scoring complete"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/source-stats")
async def get_source_stats():
    """Return per-file processing stats from SQLite."""
    try:
        conn = sqlite3.connect(PIPELINE_PROGRESS_DB)
        rows = conn.execute(
            "SELECT filepath, source, rows, nodes, processed_at FROM processed_files "
            "ORDER BY processed_at DESC LIMIT 500"
        ).fetchall()
        conn.close()
        return [
            {"filepath": r[0], "source": r[1], "rows": r[2],
             "nodes": r[3], "processed_at": r[4]}
            for r in rows
        ]
    except Exception:
        return []


def _get_log_tail(lines: int = 100) -> list:
    """Read last N lines from SQLite pipeline_runs log."""
    try:
        conn = sqlite3.connect(PIPELINE_PROGRESS_DB)
        rows = conn.execute(
            "SELECT started_at, finished_at, status, stats FROM pipeline_runs "
            "ORDER BY id DESC LIMIT ?",
            (lines,)
        ).fetchall()
        conn.close()
        return [
            f"[{r[0]}] Status={r[1]} | Finished={r[2] or 'running'}"
            for r in rows
        ]
    except Exception:
        return []
