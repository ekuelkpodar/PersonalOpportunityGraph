"""
actions.py — Action Engine API endpoints.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional

router = APIRouter(prefix="/api/actions", tags=["actions"])


class DraftUpdateRequest(BaseModel):
    node_id: str
    venture_context: str
    new_draft: str


@router.get("/node/{node_id}")
async def get_action_for_node(
    node_id: str,
    venture_context: str = Query("applied_insights"),
):
    """Get the Next Best Action for a node."""
    from backend.graph.neo4j_client import run_query
    from backend.action.engine import get_next_best_action

    rows = run_query(
        "MATCH (n {id: $id}) RETURN n, labels(n) AS labels LIMIT 1",
        {"id": node_id}
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Node not found")

    node = dict(rows[0]["n"])
    node["_labels"] = rows[0]["labels"]
    score_prop = f"opportunity_score_{venture_context}"

    breakdown = {
        "opportunity_score": float(node.get(score_prop) or 0.0),
        "relevance":      float(node.get(f"score_relevance_{venture_context}") or 0.0),
        "reachability":   float(node.get(f"score_reachability_{venture_context}") or 0.0),
        "influence":      float(node.get(f"score_influence_{venture_context}") or 0.0),
        "responsiveness": float(node.get(f"score_responsiveness_{venture_context}") or 0.0),
        "confidence":     float(node.get("confidence_score") or 0.0),
        "novelty":        float(node.get(f"score_novelty_{venture_context}") or 0.0),
    }

    return get_next_best_action(node, venture_context, breakdown)


@router.put("/draft")
async def update_draft(request: DraftUpdateRequest):
    """Save a manually edited message draft."""
    import sqlite3
    from backend.config import DRAFT_CACHE_DB
    from backend.action.drafts import _content_hash
    from backend.graph.neo4j_client import run_query

    rows = run_query(
        "MATCH (n {id: $id}) RETURN n LIMIT 1",
        {"id": request.node_id}
    )
    node = dict(rows[0]["n"]) if rows else {"id": request.node_id}

    c_hash = _content_hash(node, request.venture_context)
    conn = sqlite3.connect(DRAFT_CACHE_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS draft_cache (
            node_id TEXT NOT NULL, venture_context TEXT NOT NULL,
            content_hash TEXT NOT NULL, draft_text TEXT NOT NULL,
            generated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (node_id, venture_context)
        )
    """)
    conn.execute(
        "INSERT OR REPLACE INTO draft_cache (node_id, venture_context, content_hash, draft_text) "
        "VALUES (?, ?, ?, ?)",
        (request.node_id, request.venture_context, c_hash, request.new_draft)
    )
    conn.commit()
    conn.close()
    return {"status": "saved"}


@router.post("/batch-generate")
async def batch_generate(
    venture_context: str = Query("applied_insights"),
    limit: int = Query(100),
):
    """Batch pre-generate action drafts for top scoring nodes."""
    from backend.action.drafts import batch_generate_drafts
    count = batch_generate_drafts(venture_context=venture_context, limit=limit)
    return {"status": "done", "drafts_generated": count}
