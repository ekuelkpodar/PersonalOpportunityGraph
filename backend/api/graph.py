"""
graph.py — Graph Explorer API endpoints.
"""
from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Query

from backend.graph.ego_network import get_ego_subgraph, get_node_subgraph
from backend.graph.neo4j_client import get_node, get_node_neighbors, get_graph_stats
from backend.models import GraphDataResponse

router = APIRouter(prefix="/api/graph", tags=["graph"])


@router.get("/ego", response_model=GraphDataResponse)
async def get_ego_graph(
    venture_context: str = Query("applied_insights"),
    intent_mode: str     = Query("Exploit"),
    top_n: int           = Query(50),
    min_score: float     = Query(0.0),
    node_types: str      = Query(""),   # comma-separated
):
    """
    Return ego-centered subgraph for visualization.
    Lazy-load: ego + top_n nodes by opportunity score.
    """
    types = [t.strip() for t in node_types.split(",") if t.strip()] if node_types else None
    return get_ego_subgraph(
        venture_context=venture_context,
        top_n=top_n,
        min_score=min_score,
        node_types=types,
        intent_mode=intent_mode,
    )


@router.get("/node/{node_id}", response_model=GraphDataResponse)
async def get_node_graph(node_id: str):
    """Return 1-hop neighborhood subgraph centered on a node."""
    return get_node_subgraph(node_id)


@router.get("/node/{node_id}/detail")
async def get_node_detail(
    node_id: str,
    venture_context: str = Query("applied_insights"),
):
    """
    Return full node properties + score breakdown + neighbors.
    """
    node = get_node(node_id)
    if not node:
        return {"error": "Node not found"}

    neighbors = get_node_neighbors(node_id)
    node["neighbors"] = neighbors[:50]
    node["venture_context"] = venture_context

    # Score breakdown for the requested venture context
    score_prop = f"opportunity_score_{venture_context}"
    node["opportunity_score"] = float(node.get(score_prop) or 0.0)
    node["score_breakdown"] = {
        "relevance":      float(node.get(f"score_relevance_{venture_context}") or 0.0),
        "reachability":   float(node.get(f"score_reachability_{venture_context}") or 0.0),
        "influence":      float(node.get(f"score_influence_{venture_context}") or 0.0),
        "responsiveness": float(node.get(f"score_responsiveness_{venture_context}") or 0.0),
        "confidence":     float(node.get("confidence_score") or 0.0),
        "novelty":        float(node.get(f"score_novelty_{venture_context}") or 0.0),
    }

    # All venture scores for comparison
    from backend.config import EGO_VENTURE_CONTEXTS
    node["all_venture_scores"] = {
        vc: float(node.get(f"opportunity_score_{vc}") or 0.0)
        for vc in EGO_VENTURE_CONTEXTS
    }

    # Similar nodes via Qdrant
    try:
        from backend.graph.qdrant_client import get_vector, search_similar
        vec = get_vector(node_id)
        if vec:
            similar = search_similar(vec, top_k=5)
            node["similar_nodes"] = similar
        else:
            node["similar_nodes"] = []
    except Exception:
        node["similar_nodes"] = []

    # Interaction history
    from backend.feedback.loop import get_interactions_for_node
    node["interaction_history"] = get_interactions_for_node(node_id)

    return node


@router.get("/stats")
async def get_stats():
    """Return graph statistics."""
    return get_graph_stats()
