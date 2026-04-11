"""
feedback.py — Interaction logging and feedback loop API endpoints.
"""
from __future__ import annotations

from typing import List

from fastapi import APIRouter, Query

from backend.models import InteractionLogRequest, InteractionRecord
from backend.feedback.loop import (
    log_interaction, get_interactions_for_node,
    get_interaction_summary, get_conversion_rate,
)

router = APIRouter(prefix="/api/feedback", tags=["feedback"])


@router.post("/interaction")
async def log_interaction_endpoint(request: InteractionLogRequest):
    """Log a user interaction with a node."""
    interaction_id = log_interaction(
        node_id=request.node_id,
        venture_context=request.venture_context,
        intent_mode=request.intent_mode,
        action_taken=request.action_taken,
        action_type=request.action_type,
        channel_used=request.channel_used,
        outcome=request.outcome,
        notes=request.notes,
    )
    return {"status": "logged", "interaction_id": interaction_id}


@router.get("/node/{node_id}")
async def get_node_interactions(node_id: str):
    """Return all interactions for a specific node."""
    return get_interactions_for_node(node_id)


@router.get("/summary")
async def get_summary(venture_context: str = Query(None)):
    """Return interaction outcome summary."""
    return get_interaction_summary(venture_context)


@router.get("/conversion-rate")
async def get_conversion(venture_context: str = Query("applied_insights")):
    """Return conversion rate for a venture context."""
    rate = get_conversion_rate(venture_context)
    return {"venture_context": venture_context, "conversion_rate": rate}
