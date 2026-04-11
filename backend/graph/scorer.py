"""
scorer.py — 6-component opportunity scoring formula + intent mode multipliers.

opportunity_score (per venture context) =
  0.30 × relevance_score
+ 0.25 × reachability_score
+ 0.15 × influence_score (PageRank normalized)
+ 0.15 × responsiveness_score
+ 0.10 × confidence_score
+ 0.05 × novelty_score (betweenness × inverse similarity)

Intent mode multipliers are applied at query time (not stored).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.config import (
    SCORE_W_RELEVANCE, SCORE_W_REACHABILITY, SCORE_W_INFLUENCE,
    SCORE_W_RESPONSIVENESS, SCORE_W_CONFIDENCE, SCORE_W_NOVELTY,
    INTENT_MULTIPLIERS, EGO_ID, EGO_VENTURE_CONTEXTS,
    WEAK_TIE_BETWEENNESS_MIN, WEAK_TIE_COSINE_SIM_MAX,
    WARMTH_SKOOL_DM,
)
from backend.graph.neo4j_client import run_query, run_write
from backend.graph.qdrant_client import cosine_sim_to_ego
from backend.graph.reachability import compute_reachability


def compute_relevance(node_id: str, venture_context: str,
                       node_type: Optional[str] = None) -> float:
    """Cosine similarity between node embedding and ego venture variant."""
    return cosine_sim_to_ego(node_id, venture_context, node_type)


def compute_influence(node: Dict[str, Any]) -> float:
    """Normalized PageRank (0-1)."""
    return float(node.get("pagerank_norm") or 0.0)


def compute_responsiveness(node: Dict[str, Any]) -> float:
    """
    Responsiveness based on warmth and community activity.
    has DM = 1.0, active community member = 0.6, cold = 0.2
    """
    warmth = float(node.get("warmth_score") or 0.0)
    if warmth >= WARMTH_SKOOL_DM:
        return 1.0
    if warmth >= 0.5:
        return 0.6
    return 0.2


def compute_novelty(node: Dict[str, Any], relevance: float) -> float:
    """
    novelty_score = betweenness_norm × (1 - cosine_similarity)
    High betweenness + low similarity = bridge node we haven't explored.
    """
    betweenness = float(node.get("betweenness_norm") or 0.0)
    inverse_sim = max(0.0, 1.0 - relevance)
    return round(min(betweenness * inverse_sim, 1.0), 4)


def score_node(
    node: Dict[str, Any],
    venture_context: str,
) -> Dict[str, float]:
    """
    Compute all 6 score components for a single node.
    Returns breakdown dict + final opportunity_score.
    """
    node_id   = node.get("id") or node.get("node_id", "")
    node_type = (node.get("_labels") or node.get("labels") or ["Person"])[0]

    relevance      = compute_relevance(node_id, venture_context, node_type)
    reachability   = compute_reachability(node_id)
    influence      = compute_influence(node)
    responsiveness = compute_responsiveness(node)
    confidence     = float(node.get("confidence_score") or 0.0)
    novelty        = compute_novelty(node, relevance)

    score = (
        relevance      * SCORE_W_RELEVANCE
      + reachability   * SCORE_W_REACHABILITY
      + influence      * SCORE_W_INFLUENCE
      + responsiveness * SCORE_W_RESPONSIVENESS
      + confidence     * SCORE_W_CONFIDENCE
      + novelty        * SCORE_W_NOVELTY
    )

    return {
        "relevance":      round(relevance,      4),
        "reachability":   round(reachability,   4),
        "influence":      round(influence,      4),
        "responsiveness": round(responsiveness, 4),
        "confidence":     round(confidence,     4),
        "novelty":        round(novelty,        4),
        "opportunity_score": round(min(score, 1.0), 4),
    }


def apply_intent_multipliers(
    breakdown: Dict[str, float],
    intent_mode: str,
) -> float:
    """
    Apply intent mode multipliers to score components and recompute final score.
    Returns the adjusted opportunity_score.
    """
    multipliers = INTENT_MULTIPLIERS.get(intent_mode, {})
    if not multipliers:
        return breakdown["opportunity_score"]

    adjusted = (
        breakdown["relevance"]      * multipliers.get("relevance", 1.0)      * SCORE_W_RELEVANCE
      + breakdown["reachability"]   * multipliers.get("reachability", 1.0)   * SCORE_W_REACHABILITY
      + breakdown["influence"]      * multipliers.get("influence", 1.0)      * SCORE_W_INFLUENCE
      + breakdown["responsiveness"] * multipliers.get("responsiveness", 1.0) * SCORE_W_RESPONSIVENESS
      + breakdown["confidence"]     * multipliers.get("confidence", 1.0)     * SCORE_W_CONFIDENCE
      + breakdown["novelty"]        * multipliers.get("novelty", 1.0)        * SCORE_W_NOVELTY
    )
    # Normalize by sum of adjusted weights to keep score in 0-1
    weight_sum = sum(
        SCORE_W_RELEVANCE      * multipliers.get("relevance", 1.0)
      + SCORE_W_REACHABILITY   * multipliers.get("reachability", 1.0)
      + SCORE_W_INFLUENCE      * multipliers.get("influence", 1.0)
      + SCORE_W_RESPONSIVENESS * multipliers.get("responsiveness", 1.0)
      + SCORE_W_CONFIDENCE     * multipliers.get("confidence", 1.0)
      + SCORE_W_NOVELTY        * multipliers.get("novelty", 1.0)
        for _ in [1]
    )
    if weight_sum > 0:
        adjusted = adjusted / weight_sum

    return round(min(adjusted, 1.0), 4)


def run_scoring_job() -> None:
    """
    Nightly scoring job: score all nodes for all 4 venture contexts.
    Writes scores back to Neo4j as node properties.
    """
    from backend.graph.gds import run_all_gds
    from backend.graph.temporal import update_temporal_signals
    from backend.graph.weak_ties import mark_weak_ties_in_neo4j

    # Run GDS algorithms first
    run_all_gds()

    # Fetch all non-ego nodes
    rows = run_query(
        """
        MATCH (n)
        WHERE NOT n:Ego
        RETURN n, labels(n) AS labels
        LIMIT 50000
        """
    )

    for row in rows:
        node = dict(row["n"])
        node["_labels"] = row["labels"]
        node_id   = node.get("id")
        if not node_id:
            continue

        update_props: Dict[str, Any] = {}

        for venture in EGO_VENTURE_CONTEXTS.keys():
            breakdown = score_node(node, venture)
            score_val = breakdown["opportunity_score"]

            update_props[f"opportunity_score_{venture}"]    = score_val
            update_props[f"score_relevance_{venture}"]      = breakdown["relevance"]
            update_props[f"score_reachability_{venture}"]   = breakdown["reachability"]
            update_props[f"score_influence_{venture}"]      = breakdown["influence"]
            update_props[f"score_responsiveness_{venture}"] = breakdown["responsiveness"]
            update_props[f"score_confidence_{venture}"]     = breakdown["confidence"]
            update_props[f"score_novelty_{venture}"]        = breakdown["novelty"]

        # Build SET clause dynamically
        set_parts = ", ".join(f"n.`{k}` = ${k.replace('-', '_')}" for k in update_props)
        safe_params = {k.replace("-", "_"): v for k, v in update_props.items()}
        safe_params["node_id"] = node_id

        try:
            run_write(
                f"MATCH (n {{id: $node_id}}) SET {set_parts}",
                safe_params
            )
        except Exception:
            pass

    # Update temporal signals
    update_temporal_signals()

    # Detect weak ties for primary venture context
    mark_weak_ties_in_neo4j("applied_insights")
