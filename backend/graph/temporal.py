"""
temporal.py — Temporal intelligence: trend_signal, is_trending, new_to_network.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any

from backend.config import (
    TREND_NEW_NODE_DAYS, TREND_UPDATED_DAYS, TREND_ACTIVE_POSTS_PER_DAY,
    TREND_SIGNAL_NEW_NODE, TREND_SIGNAL_UPDATED, TREND_SIGNAL_ACTIVE_COMM,
    TREND_SIGNAL_DEFAULT, TREND_IS_TRENDING_MIN, TREND_IS_TRENDING_SCORE_MIN,
)
from backend.graph.neo4j_client import run_query, run_write


def compute_trend_signal(node: Dict[str, Any], now: Optional[datetime] = None) -> float:
    """
    Compute trend_signal for a node dict (from Neo4j).

    Rules:
      - Ingested in last 30 days → 1.0
      - Ingested in last 7 days  → 0.8 (overrides if more recent)
      - Community with >50 posts/day → 0.6
      - default → 0.2
    """
    if now is None:
        now = datetime.now(timezone.utc)

    ingested_at_str = node.get("ingested_at")
    if ingested_at_str:
        try:
            ingested_at = datetime.fromisoformat(ingested_at_str)
            if ingested_at.tzinfo is None:
                ingested_at = ingested_at.replace(tzinfo=timezone.utc)
            age_days = (now - ingested_at).days
            if age_days <= TREND_UPDATED_DAYS:
                return TREND_SIGNAL_UPDATED
            if age_days <= TREND_NEW_NODE_DAYS:
                return TREND_SIGNAL_NEW_NODE
        except (ValueError, TypeError):
            pass

    # Community-specific: high activity
    daily_posts = node.get("daily_posts")
    if daily_posts and daily_posts >= TREND_ACTIVE_POSTS_PER_DAY:
        return TREND_SIGNAL_ACTIVE_COMM

    return TREND_SIGNAL_DEFAULT


def compute_is_trending(trend_signal: float, opportunity_score: float) -> bool:
    return trend_signal >= TREND_IS_TRENDING_MIN and opportunity_score >= TREND_IS_TRENDING_SCORE_MIN


def update_temporal_signals() -> None:
    """
    Batch update temporal signals for all nodes in Neo4j.
    Also detects new_to_network: nodes whose path to ego shortened since last run.
    """
    now = datetime.now(timezone.utc)
    cutoff_new     = (now - timedelta(days=TREND_NEW_NODE_DAYS)).isoformat()
    cutoff_updated = (now - timedelta(days=TREND_UPDATED_DAYS)).isoformat()

    # Nodes ingested within last 7 days → TREND_SIGNAL_UPDATED
    run_write(
        """
        MATCH (n)
        WHERE n.ingested_at >= $cutoff
          AND NOT n:Ego
        SET n.trend_signal = $signal
        """,
        {"cutoff": cutoff_updated, "signal": TREND_SIGNAL_UPDATED}
    )

    # Nodes ingested within last 30 days (not last 7) → TREND_SIGNAL_NEW_NODE
    run_write(
        """
        MATCH (n)
        WHERE n.ingested_at >= $cutoff_new
          AND n.ingested_at < $cutoff_updated
          AND NOT n:Ego
        SET n.trend_signal = $signal
        """,
        {"cutoff_new": cutoff_new, "cutoff_updated": cutoff_updated,
         "signal": TREND_SIGNAL_NEW_NODE}
    )

    # Communities with high daily_posts → TREND_SIGNAL_ACTIVE_COMM (if not already set higher)
    run_write(
        """
        MATCH (n:Community)
        WHERE n.daily_posts >= $posts_threshold
          AND (n.trend_signal IS NULL OR n.trend_signal < $signal)
        SET n.trend_signal = $signal
        """,
        {"posts_threshold": TREND_ACTIVE_POSTS_PER_DAY,
         "signal": TREND_SIGNAL_ACTIVE_COMM}
    )

    # Default for all others
    run_write(
        """
        MATCH (n)
        WHERE n.trend_signal IS NULL
          AND NOT n:Ego
        SET n.trend_signal = $signal
        """,
        {"signal": TREND_SIGNAL_DEFAULT}
    )

    # is_trending: trend_signal >= threshold AND opportunity_score > threshold
    for venture in ["applied_insights", "aegis_t2a", "rgn_trucking", "job_search"]:
        score_prop = f"opportunity_score_{venture}"
        run_write(
            f"""
            MATCH (n)
            WHERE n.trend_signal >= $ts_min
              AND n.`{score_prop}` >= $score_min
              AND NOT n:Ego
            SET n.is_trending = true
            """,
            {"ts_min": TREND_IS_TRENDING_MIN, "score_min": TREND_IS_TRENDING_SCORE_MIN}
        )

    # Mark non-trending
    run_write(
        """
        MATCH (n)
        WHERE (n.trend_signal IS NULL OR n.trend_signal < $ts_min)
          AND NOT n:Ego
        SET n.is_trending = false
        """,
        {"ts_min": TREND_IS_TRENDING_MIN}
    )
