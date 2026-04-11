"""
feedback/loop.py — Interaction logging + feedback-adjusted score computation.

Interactions table:
  id, person_id, venture_context, intent_mode, action_taken, action_type,
  channel_used, outcome, notes, timestamp

Feedback loop:
  - "converted" or "replied" → find top-20 similar nodes (Qdrant), boost +0.08
  - "not_relevant"           → find top-20 similar nodes, penalize -0.05
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.config import (
    INTERACTIONS_DB,
    FEEDBACK_POSITIVE_BOOST,
    FEEDBACK_NEGATIVE_PENALTY,
    FEEDBACK_SIMILAR_TOP_K,
)


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(INTERACTIONS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS interactions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            node_id         TEXT NOT NULL,
            venture_context TEXT NOT NULL,
            intent_mode     TEXT NOT NULL,
            action_taken    TEXT DEFAULT '',
            action_type     TEXT NOT NULL,
            channel_used    TEXT NOT NULL,
            outcome         TEXT NOT NULL,
            notes           TEXT DEFAULT '',
            timestamp       TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS feedback_adjustments (
            node_id         TEXT NOT NULL,
            venture_context TEXT NOT NULL,
            adjustment      REAL DEFAULT 0.0,
            updated_at      TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (node_id, venture_context)
        )
    """)
    conn.commit()
    return conn


def log_interaction(
    node_id: str,
    venture_context: str,
    intent_mode: str,
    action_taken: str,
    action_type: str,
    channel_used: str,
    outcome: str,
    notes: str = "",
) -> int:
    """
    Log an interaction and apply feedback adjustments.
    Returns the new interaction ID.
    """
    conn = _get_conn()
    cursor = conn.execute(
        """
        INSERT INTO interactions
          (node_id, venture_context, intent_mode, action_taken,
           action_type, channel_used, outcome, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (node_id, venture_context, intent_mode, action_taken,
         action_type, channel_used, outcome, notes)
    )
    interaction_id = cursor.lastrowid
    conn.commit()
    conn.close()

    # Apply feedback boost/penalty to similar nodes
    _apply_feedback(node_id, venture_context, outcome)

    return interaction_id


def _apply_feedback(node_id: str, venture_context: str, outcome: str) -> None:
    """
    Find top-K similar nodes and apply score adjustment.
    """
    positive_outcomes = {"converted", "replied", "meeting"}
    negative_outcomes = {"not_relevant"}

    if outcome in positive_outcomes:
        delta = FEEDBACK_POSITIVE_BOOST
    elif outcome in negative_outcomes:
        delta = -FEEDBACK_NEGATIVE_PENALTY
    else:
        return

    # Find similar nodes via Qdrant
    try:
        from backend.graph.qdrant_client import get_vector, search_similar
        node_vec = get_vector(node_id)
        if not node_vec:
            return

        similar = search_similar(
            query_vector=node_vec,
            top_k=FEEDBACK_SIMILAR_TOP_K,
        )

        conn = _get_conn()
        for item in similar:
            sim_id = item.get("node_id")
            if not sim_id or sim_id == node_id:
                continue
            conn.execute(
                """
                INSERT INTO feedback_adjustments (node_id, venture_context, adjustment)
                VALUES (?, ?, ?)
                ON CONFLICT(node_id, venture_context) DO UPDATE
                  SET adjustment = adjustment + excluded.adjustment,
                      updated_at = datetime('now')
                """,
                (sim_id, venture_context, delta)
            )
        conn.commit()
        conn.close()
    except Exception:
        pass


def get_feedback_adjustment(node_id: str, venture_context: str) -> float:
    """Return the accumulated feedback adjustment for a node."""
    conn = _get_conn()
    row = conn.execute(
        "SELECT adjustment FROM feedback_adjustments WHERE node_id = ? AND venture_context = ?",
        (node_id, venture_context)
    ).fetchone()
    conn.close()
    return float(row[0]) if row else 0.0


def get_feedback_adjusted_score(
    base_score: float,
    node_id: str,
    venture_context: str,
) -> float:
    """Return base_score + feedback adjustment, clamped to 0-1."""
    adjustment = get_feedback_adjustment(node_id, venture_context)
    return round(max(0.0, min(1.0, base_score + adjustment)), 4)


def get_interactions_for_node(node_id: str) -> List[Dict]:
    """Return all interactions for a node, newest first."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM interactions WHERE node_id = ? ORDER BY timestamp DESC",
        (node_id,)
    ).fetchall()
    cols = ["id", "node_id", "venture_context", "intent_mode",
            "action_taken", "action_type", "channel_used",
            "outcome", "notes", "timestamp"]
    conn.close()
    return [dict(zip(cols, row)) for row in rows]


def get_interaction_summary(venture_context: Optional[str] = None) -> Dict[str, Any]:
    """
    Return outcome counts per venture context.
    """
    conn = _get_conn()
    if venture_context:
        rows = conn.execute(
            "SELECT outcome, count(*) as cnt FROM interactions "
            "WHERE venture_context = ? GROUP BY outcome",
            (venture_context,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT venture_context, outcome, count(*) as cnt FROM interactions "
            "GROUP BY venture_context, outcome"
        ).fetchall()
    conn.close()

    summary: Dict[str, Any] = {}
    for row in rows:
        if venture_context:
            outcome, cnt = row
            summary[outcome] = cnt
        else:
            vc, outcome, cnt = row
            if vc not in summary:
                summary[vc] = {}
            summary[vc][outcome] = cnt
    return summary


def get_conversion_rate(venture_context: str) -> float:
    """Return converted / (total contacted) for a venture context."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT outcome, count(*) as cnt FROM interactions "
        "WHERE venture_context = ? GROUP BY outcome",
        (venture_context,)
    ).fetchall()
    conn.close()

    totals = {row[0]: row[1] for row in rows}
    total = sum(totals.values())
    if total == 0:
        return 0.0
    converted = totals.get("converted", 0)
    return round(converted / total, 4)
