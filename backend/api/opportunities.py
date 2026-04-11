"""
opportunities.py — Opportunity feed API endpoints.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

from backend.config import EGO_VENTURE_CONTEXTS, OPPORTUNITY_SCORE_MIN_ACTION
from backend.graph.neo4j_client import run_query
from backend.graph.scorer import apply_intent_multipliers
from backend.graph.weak_ties import detect_unexpected_opportunities
from backend.feedback.loop import get_feedback_adjusted_score
from backend.action.engine import get_next_best_action
from backend.models import (
    NodeSummaryModel, OpportunityFeedRequest, OpportunityFeedResponse,
    ScoreBreakdownModel, UnexpectedOpportunityModel,
)

router = APIRouter(prefix="/api/opportunities", tags=["opportunities"])


def _node_to_summary(
    row: Dict,
    venture_context: str,
    intent_mode: str,
) -> NodeSummaryModel:
    """Convert a Neo4j node row to NodeSummaryModel."""
    node_id   = row.get("node_id") or row.get("id", "")
    node_type = (row.get("labels") or ["Person"])[0]
    score_prop = f"opportunity_score_{venture_context}"

    base_score = float(row.get(score_prop) or 0.0)

    breakdown = ScoreBreakdownModel(
        relevance      = float(row.get(f"score_relevance_{venture_context}") or 0.0),
        reachability   = float(row.get(f"score_reachability_{venture_context}") or 0.0),
        influence      = float(row.get(f"score_influence_{venture_context}") or 0.0),
        responsiveness = float(row.get(f"score_responsiveness_{venture_context}") or 0.0),
        confidence     = float(row.get(f"score_confidence_{venture_context}") or 0.0),
        novelty        = float(row.get(f"score_novelty_{venture_context}") or 0.0),
    )

    # Apply intent mode multipliers
    adjusted_score = apply_intent_multipliers(breakdown.dict(), intent_mode)

    # Apply feedback adjustment
    final_score = get_feedback_adjusted_score(adjusted_score, node_id, venture_context)

    return NodeSummaryModel(
        id=node_id,
        name=row.get("name") or row.get("n_name", ""),
        node_type=node_type,
        source=row.get("source") or [],
        topic_cluster=row.get("topic_cluster"),
        location=row.get("location"),
        opportunity_score=final_score,
        score_breakdown=breakdown,
        warmth_score=float(row.get("warmth_score") or 0.0),
        confidence_score=float(row.get("confidence_score") or 0.0),
        is_trending=bool(row.get("is_trending", False)),
        new_to_network=bool(row.get("new_to_network", False)),
        is_weak_tie=bool(row.get("is_weak_tie", False)),
        bridged_clusters=list(row.get("bridged_clusters") or []),
    )


@router.post("/feed", response_model=OpportunityFeedResponse)
async def get_opportunity_feed(request: OpportunityFeedRequest):
    """
    Paginated opportunity feed ranked by score.
    Supports filtering by node type, topic cluster, warmth tier, source, location.
    """
    venture_context = request.venture_context
    intent_mode     = request.intent_mode
    score_prop      = f"opportunity_score_{venture_context}"

    where_clauses = ["NOT n:Ego", f"n.`{score_prop}` IS NOT NULL",
                     f"n.`{score_prop}` >= $min_score"]
    params: Dict[str, Any] = {
        "min_score": request.min_score,
        "skip":      request.page * request.page_size,
        "limit":     request.page_size,
    }

    if request.node_types:
        type_filter = " OR ".join(f"n:{t}" for t in request.node_types)
        where_clauses.append(f"({type_filter})")

    if request.topic_clusters:
        where_clauses.append("n.topic_cluster IN $topic_clusters")
        params["topic_clusters"] = request.topic_clusters

    if request.location:
        where_clauses.append("toLower(n.location) CONTAINS toLower($location)")
        params["location"] = request.location

    if request.warmth_tiers:
        # Map tier names to score ranges
        warmth_conds = []
        for tier in request.warmth_tiers:
            if tier == "hot":
                warmth_conds.append("n.warmth_score >= 1.0")
            elif tier == "warm":
                warmth_conds.append("(n.warmth_score >= 0.5 AND n.warmth_score < 1.0)")
            elif tier == "cool":
                warmth_conds.append("(n.warmth_score >= 0.1 AND n.warmth_score < 0.5)")
            elif tier == "cold":
                warmth_conds.append("(n.warmth_score < 0.1 OR n.warmth_score IS NULL)")
        if warmth_conds:
            where_clauses.append(f"({' OR '.join(warmth_conds)})")

    where_str = " AND ".join(where_clauses)

    count_rows = run_query(
        f"MATCH (n) WHERE {where_str} RETURN count(n) AS total",
        params
    )
    total = count_rows[0]["total"] if count_rows else 0

    rows = run_query(
        f"""
        MATCH (n)
        WHERE {where_str}
        RETURN n.id AS node_id,
               n.name AS name,
               labels(n) AS labels,
               n.`{score_prop}` AS {score_prop.replace('.', '_')},
               n.`score_relevance_{venture_context}` AS score_relevance_{venture_context},
               n.`score_reachability_{venture_context}` AS score_reachability_{venture_context},
               n.`score_influence_{venture_context}` AS score_influence_{venture_context},
               n.`score_responsiveness_{venture_context}` AS score_responsiveness_{venture_context},
               n.`score_confidence_{venture_context}` AS score_confidence_{venture_context},
               n.`score_novelty_{venture_context}` AS score_novelty_{venture_context},
               n.warmth_score AS warmth_score,
               n.confidence_score AS confidence_score,
               n.source AS source,
               n.topic_cluster AS topic_cluster,
               n.location AS location,
               n.is_trending AS is_trending,
               n.new_to_network AS new_to_network,
               n.is_weak_tie AS is_weak_tie,
               n.bridged_clusters AS bridged_clusters
        ORDER BY n.`{score_prop}` DESC
        SKIP $skip LIMIT $limit
        """,
        params
    )

    items = []
    for row in rows:
        flat = {k: v for k, v in row.items()}
        flat[score_prop]             = row.get(score_prop.replace(".", "_"))
        flat["opportunity_score_" + venture_context] = flat.get(score_prop.replace(".", "_"))
        summary = _node_to_summary(flat, venture_context, intent_mode)
        items.append(summary)

    return OpportunityFeedResponse(
        items=items,
        total=total,
        page=request.page,
        page_size=request.page_size,
        venture_context=venture_context,
        intent_mode=intent_mode,
    )


@router.get("/unexpected", response_model=List[UnexpectedOpportunityModel])
async def get_unexpected_opportunities(
    venture_context: str = Query("applied_insights"),
    limit: int = Query(20),
):
    """Return Unexpected Opportunities (weak tie nodes)."""
    nodes = detect_unexpected_opportunities(venture_context)[:limit]
    result = []
    for node in nodes:
        score_prop = f"opportunity_score_{venture_context}"
        rows = run_query(
            f"MATCH (n {{id: $id}}) RETURN n.`{score_prop}` AS score, "
            f"n.warmth_score AS warmth, n.confidence_score AS confidence, "
            f"n.source AS source, n.topic_cluster AS topic, n.name AS name, "
            f"labels(n) AS labels",
            {"id": node["node_id"]}
        )
        if not rows:
            continue
        r = rows[0]
        summary = NodeSummaryModel(
            id=node["node_id"],
            name=r.get("name", ""),
            node_type=(r.get("labels") or ["Person"])[0],
            source=list(r.get("source") or []),
            topic_cluster=r.get("topic"),
            opportunity_score=float(r.get("score") or 0.0),
            warmth_score=float(r.get("warmth") or 0.0),
            confidence_score=float(r.get("confidence") or 0.0),
            is_weak_tie=True,
            bridged_clusters=node.get("bridged_clusters", []),
        )
        result.append(UnexpectedOpportunityModel(
            node=summary,
            bridged_clusters=node.get("bridged_clusters", []),
            betweenness=node.get("betweenness", 0.0),
            path_length=node.get("path_length", 99),
        ))
    return result


@router.get("/node/{node_id}/action")
async def get_node_action(
    node_id: str,
    venture_context: str = Query("applied_insights"),
):
    """Get Next Best Action for a specific node."""
    rows = run_query(
        "MATCH (n {id: $id}) RETURN n, labels(n) AS labels LIMIT 1",
        {"id": node_id}
    )
    if not rows:
        return {"error": "Node not found"}

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

    action = get_next_best_action(node, venture_context, breakdown)
    return action
