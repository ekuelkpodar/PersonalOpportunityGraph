"""
engine.py — Rule-based Next Best Action selector.

Rules (evaluated in order, first match wins):
  IF warmth_score == 1.0 AND relevance > 0.6:
    → DM / Skool / High
  ELIF shared_community_count > 0 AND reachability > 0.5:
    → DM / Skool or X / High
  ELIF path_length == 2 AND warm_edge_on_path:
    → AskIntro / Skool / Medium
  ELIF influence_score > 0.7 AND relevance > 0.4:
    → EngageContent / X / Medium
  ELIF company node AND clutch_category matches venture_context:
    → PitchService / Email or LinkedIn / Medium
  ELSE:
    → EngageContent / X / Low
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.config import (
    EGO_ID, WARMTH_SKOOL_DM,
)
from backend.graph.neo4j_client import (
    get_shared_community_count, has_warm_edge_on_path, get_shortest_path,
)
from backend.models import NextBestActionModel


# Venture context → relevant Clutch categories
_VENTURE_CLUTCH_MATCH = {
    "applied_insights": ["DigitalMarketing", "Development", "ITServices"],
    "aegis_t2a":        ["Development", "ITServices", "BusinessServices"],
    "rgn_trucking":     ["BusinessServices"],
    "job_search":       ["Development", "ITServices"],
}


def select_action(
    node: Dict[str, Any],
    venture_context: str,
    score_breakdown: Dict[str, float],
    routing_path: List[str],
) -> Tuple[str, str, str]:
    """
    Select action_type, channel, and priority based on rules.
    Returns (action_type, channel, priority).
    """
    node_type      = _get_node_type(node)
    warmth         = float(node.get("warmth_score") or 0.0)
    relevance      = score_breakdown.get("relevance", 0.0)
    reachability   = score_breakdown.get("reachability", 0.0)
    influence      = score_breakdown.get("influence", 0.0)
    path_len       = len(routing_path) - 1 if routing_path else 99
    clutch_cat     = node.get("clutch_category") or ""
    has_skool_dm   = bool(node.get("skool_dm_url"))
    has_x          = bool(node.get("x_url") or node.get("x_handle"))
    has_email      = bool(node.get("email"))
    has_linkedin   = bool(node.get("linkedin_url"))

    # Check warm edge on path
    warm_on_path = has_warm_edge_on_path(routing_path) if routing_path else False

    # Shared communities
    shared_comm = get_shared_community_count(node.get("id", ""))

    # Rule 1: Warm DM contact with high relevance
    if warmth >= WARMTH_SKOOL_DM and relevance > 0.6:
        channel = "Skool" if has_skool_dm else "X"
        return "DM", channel, "High"

    # Rule 2: Shared community + high reachability
    if shared_comm > 0 and reachability > 0.5:
        channel = "Skool" if has_skool_dm else "X"
        return "DM", channel, "High"

    # Rule 3: 2-hop path with warm edge → ask for intro
    if path_len == 2 and warm_on_path:
        return "AskIntro", "Skool", "Medium"

    # Rule 4: High influence + moderate relevance → engage content
    if influence > 0.7 and relevance > 0.4:
        return "EngageContent", "X", "Medium"

    # Rule 5: Company node with matching venture category
    if node_type == "Company":
        relevant_cats = _VENTURE_CLUTCH_MATCH.get(venture_context, [])
        if any(cat.lower() in clutch_cat.lower() for cat in relevant_cats):
            channel = "Email" if has_email else "LinkedIn"
            return "PitchService", channel, "Medium"

    # Default
    return "EngageContent", "X", "Low"


def _get_node_type(node: Dict[str, Any]) -> str:
    labels = node.get("_labels") or node.get("labels") or []
    if labels:
        return labels[0]
    return "Person"


def build_reason(
    node: Dict[str, Any],
    venture_context: str,
    action_type: str,
    score_breakdown: Dict[str, float],
) -> str:
    """
    Generate 2-sentence reason: why this person, why now.
    Rule-based, not LLM.
    """
    name    = node.get("name", "this person")
    score   = score_breakdown.get("opportunity_score", 0.0)
    top_sig = _top_signal(score_breakdown)
    venture_label = _venture_label(venture_context)

    why_person = (
        f"{name} is a strong match for your {venture_label} goals "
        f"with a {score:.0%} opportunity score driven by {top_sig}."
    )

    if action_type == "DM":
        why_now = "You have a direct channel via Skool — now is the right time to reach out while the connection is warm."
    elif action_type == "AskIntro":
        why_now = "You share a mutual warm contact who can make the introduction and boost response odds significantly."
    elif action_type == "EngageContent":
        why_now = "Engaging with their content builds visibility before a direct outreach, increasing conversion probability."
    elif action_type == "PitchService":
        why_now = "Their service profile aligns with your offering — a targeted pitch can open a short sales cycle."
    else:
        why_now = "Staying on their radar through light engagement is the right first move before a direct ask."

    return f"{why_person} {why_now}"


def build_expected_outcome(action_type: str, venture_context: str) -> str:
    """Return what a successful interaction looks like."""
    venture_label = _venture_label(venture_context)
    outcomes = {
        "DM":            f"A response and short conversation that qualifies them as a {venture_label} opportunity or warm referral.",
        "AskIntro":      "An introduction message and a scheduled call within 1-2 weeks.",
        "EngageContent": "They notice your engagement, follow back, and open a DM thread.",
        "PitchService":  f"A discovery call to explore a {venture_label} engagement.",
        "Collaborate":   "An agreement to co-create content or a joint project proposal.",
    }
    return outcomes.get(action_type, "A positive response and an open line of communication.")


def _top_signal(breakdown: Dict[str, float]) -> str:
    """Return the name of the highest non-score component."""
    components = {
        "relevance":      breakdown.get("relevance", 0),
        "reachability":   breakdown.get("reachability", 0),
        "influence":      breakdown.get("influence", 0),
        "responsiveness": breakdown.get("responsiveness", 0),
    }
    if not components:
        return "strong profile match"
    top = max(components, key=lambda k: components[k])
    labels = {
        "relevance":      "semantic relevance to your goals",
        "reachability":   "strong network proximity",
        "influence":      "high network influence",
        "responsiveness": "high outreach warmth",
    }
    return labels.get(top, "strong profile match")


def _venture_label(venture_context: str) -> str:
    labels = {
        "applied_insights": "Applied Insights",
        "aegis_t2a":        "AEGIS-T2A",
        "rgn_trucking":     "RGN Trucking",
        "job_search":       "job search",
    }
    return labels.get(venture_context, venture_context)


def get_next_best_action(
    node: Dict[str, Any],
    venture_context: str,
    score_breakdown: Dict[str, float],
    routing_path: Optional[List[str]] = None,
) -> NextBestActionModel:
    """
    Produce a full NextBestActionModel for a node.
    Message draft is generated by drafts.py (cached).
    """
    from backend.action.routing import build_routing_path_names
    from backend.action.drafts import get_or_generate_draft

    if routing_path is None:
        from backend.graph.reachability import get_routing_path
        routing_path = get_routing_path(node.get("id", ""))

    action_type, channel, priority = select_action(
        node, venture_context, score_breakdown, routing_path
    )

    reason           = build_reason(node, venture_context, action_type, score_breakdown)
    expected_outcome = build_expected_outcome(action_type, venture_context)
    path_names       = build_routing_path_names(routing_path)

    # Draft (from cache or LLM)
    draft = get_or_generate_draft(
        node=node,
        venture_context=venture_context,
        action_type=action_type,
        routing_path=routing_path,
    )

    return NextBestActionModel(
        action_type=action_type,
        channel=channel,
        reason=reason,
        message_draft=draft,
        expected_outcome=expected_outcome,
        priority=priority,
        routing_path=path_names,
    )
