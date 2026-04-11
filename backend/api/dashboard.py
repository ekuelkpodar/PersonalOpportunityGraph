"""
dashboard.py — Dashboard stats API endpoint.
"""
from __future__ import annotations

from fastapi import APIRouter

from backend.graph.neo4j_client import get_graph_stats, run_query
from backend.feedback.loop import get_interaction_summary
from backend.config import EGO_VENTURE_CONTEXTS
from backend.models import DashboardStatsResponse, NodeSummaryModel, ScoreBreakdownModel

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/stats", response_model=DashboardStatsResponse)
async def get_dashboard_stats():
    """Return all dashboard statistics."""
    stats = get_graph_stats()

    # Top 5 opportunities per venture context
    top_opps = {}
    for vc in EGO_VENTURE_CONTEXTS:
        score_prop = f"opportunity_score_{vc}"
        rows = run_query(
            f"""
            MATCH (n)
            WHERE NOT n:Ego AND n.`{score_prop}` IS NOT NULL
            RETURN n.id AS node_id, n.name AS name, labels(n) AS labels,
                   n.`{score_prop}` AS opp_score,
                   n.warmth_score AS warmth, n.confidence_score AS confidence,
                   n.source AS source, n.topic_cluster AS topic_cluster,
                   n.is_trending AS is_trending, n.is_weak_tie AS is_weak_tie,
                   n.`score_relevance_{vc}` AS relevance,
                   n.`score_reachability_{vc}` AS reachability
            ORDER BY n.`{score_prop}` DESC LIMIT 5
            """
        )
        items = []
        for row in rows:
            items.append(NodeSummaryModel(
                id=row["node_id"],
                name=row.get("name", ""),
                node_type=(row.get("labels") or ["Person"])[0],
                source=list(row.get("source") or []),
                topic_cluster=row.get("topic_cluster"),
                opportunity_score=float(row.get("opp_score") or 0.0),
                score_breakdown=ScoreBreakdownModel(
                    relevance=float(row.get("relevance") or 0.0),
                    reachability=float(row.get("reachability") or 0.0),
                ),
                warmth_score=float(row.get("warmth") or 0.0),
                confidence_score=float(row.get("confidence") or 0.0),
                is_trending=bool(row.get("is_trending", False)),
                is_weak_tie=bool(row.get("is_weak_tie", False)),
            ))
        top_opps[vc] = items

    # Unexpected opps count
    weak_tie_rows = run_query(
        "MATCH (n) WHERE n.is_weak_tie = true RETURN count(n) AS cnt"
    )
    unexpected_count = weak_tie_rows[0]["cnt"] if weak_tie_rows else 0

    # Trending nodes
    trend_rows = run_query(
        """
        MATCH (n)
        WHERE n.is_trending = true AND NOT n:Ego
        RETURN n.id AS node_id, n.name AS name, labels(n) AS labels,
               n.warmth_score AS warmth, n.confidence_score AS confidence,
               n.source AS source, n.topic_cluster AS topic_cluster
        LIMIT 10
        """
    )
    trending = [
        NodeSummaryModel(
            id=r["node_id"],
            name=r.get("name", ""),
            node_type=(r.get("labels") or ["Person"])[0],
            source=list(r.get("source") or []),
            warmth_score=float(r.get("warmth") or 0.0),
            confidence_score=float(r.get("confidence") or 0.0),
            is_trending=True,
        )
        for r in trend_rows
    ]

    # Interaction summary
    interaction_summary = get_interaction_summary()

    # Confidence distribution
    conf_rows = run_query(
        """
        MATCH (n) WHERE NOT n:Ego AND n.confidence_score IS NOT NULL
        WITH CASE
          WHEN n.confidence_score >= 0.7 THEN 'high'
          WHEN n.confidence_score >= 0.3 THEN 'medium'
          ELSE 'low'
        END AS tier
        RETURN tier, count(*) AS cnt
        """
    )
    conf_dist = {r["tier"]: r["cnt"] for r in conf_rows}

    # Warmth distribution
    warmth_rows = run_query(
        """
        MATCH (n) WHERE NOT n:Ego AND n.warmth_score IS NOT NULL
        WITH CASE
          WHEN n.warmth_score >= 1.0 THEN 'hot'
          WHEN n.warmth_score >= 0.5 THEN 'warm'
          WHEN n.warmth_score >= 0.1 THEN 'cool'
          ELSE 'cold'
        END AS tier
        RETURN tier, count(*) AS cnt
        """
    )
    warmth_dist = {r["tier"]: r["cnt"] for r in warmth_rows}

    return DashboardStatsResponse(
        total_nodes=sum([stats.get("person", 0), stats.get("company", 0),
                         stats.get("publisher", 0), stats.get("community", 0)]),
        total_edges=stats.get("edges", 0),
        total_persons=stats.get("person", 0),
        total_companies=stats.get("company", 0),
        total_publishers=stats.get("publisher", 0),
        total_communities=stats.get("community", 0),
        top_opportunities=top_opps,
        unexpected_opps_count=unexpected_count,
        trending_nodes=trending,
        interaction_summary=interaction_summary,
        confidence_distribution=conf_dist,
        warmth_distribution=warmth_dist,
        node_type_distribution={
            "Person":    stats.get("person", 0),
            "Company":   stats.get("company", 0),
            "Publisher": stats.get("publisher", 0),
            "Community": stats.get("community", 0),
        },
    )
