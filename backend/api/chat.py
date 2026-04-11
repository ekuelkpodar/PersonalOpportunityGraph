"""
chat.py — RAG chat API endpoints.
"""
from __future__ import annotations

from fastapi import APIRouter, Query
from typing import List

from backend.models import ChatMessageRequest, ChatMessageResponse, CitedNodeModel
from backend.rag.agent import answer_query, get_query_history, get_suggestions

router = APIRouter(prefix="/api/chat", tags=["chat"])


@router.post("/query", response_model=ChatMessageResponse)
async def query_graph(request: ChatMessageRequest):
    """Answer a question about the opportunity graph."""
    result = answer_query(
        query=request.query,
        venture_context=request.venture_context,
        intent_mode=request.intent_mode,
        history=request.history,
    )

    cited_nodes = [
        CitedNodeModel(
            node_id=c["node_id"],
            name=c["name"],
            node_type=c["node_type"],
            relevance_reason=c["relevance_reason"],
        )
        for c in result.get("cited_nodes", [])
    ]

    return ChatMessageResponse(
        answer_text=result["answer_text"],
        reasoning_path=result["reasoning_path"],
        cited_nodes=cited_nodes,
        confidence=result["confidence"],
        venture_context=result["venture_context"],
        query_id=result["query_id"],
    )


@router.get("/history")
async def get_history(limit: int = Query(50)):
    """Return recent query history."""
    return get_query_history(limit=limit)


@router.get("/suggestions")
async def get_query_suggestions(
    venture_context: str = Query("applied_insights"),
):
    """Return pre-built query suggestions for a venture context."""
    return {"suggestions": get_suggestions(venture_context)}
