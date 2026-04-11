"""
reachability.py — Reachability score computation.

reachability_score =
  (1.0 / shortest_path_length)  × 0.50
+ (warm_edge_on_path ? 1.0 : 0) × 0.30
+ (shared_community / 10)       × 0.20
"""
from __future__ import annotations

from typing import Optional

from backend.config import REACH_W_PATH_LENGTH, REACH_W_WARM_EDGE, REACH_W_SHARED_COMM
from backend.graph.neo4j_client import (
    get_shortest_path, get_shared_community_count, has_warm_edge_on_path,
)
from backend.config import EGO_ID


def compute_reachability(node_id: str) -> float:
    """
    Compute reachability_score for a node relative to ego.
    Returns float 0.0 - 1.0.
    """
    path = get_shortest_path(EGO_ID, node_id)

    if not path:
        return 0.0

    path_len = len(path) - 1  # number of hops
    if path_len <= 0:
        return 1.0

    # Component 1: path length (shorter = higher)
    path_score = 1.0 / path_len

    # Component 2: warm edge on path
    warm = has_warm_edge_on_path(path)
    warm_score = 1.0 if warm else 0.0

    # Component 3: shared community count (capped at 10)
    shared = get_shared_community_count(node_id)
    comm_score = min(shared / 10.0, 1.0)

    total = (
        path_score  * REACH_W_PATH_LENGTH
      + warm_score  * REACH_W_WARM_EDGE
      + comm_score  * REACH_W_SHARED_COMM
    )

    return round(min(total, 1.0), 4)


def get_path_length(node_id: str) -> int:
    """Return shortest hop count from ego to node. Returns 99 if unreachable."""
    path = get_shortest_path(EGO_ID, node_id)
    if not path:
        return 99
    return len(path) - 1


def get_routing_path(node_id: str) -> list:
    """Return ordered list of node IDs from ego to target."""
    path = get_shortest_path(EGO_ID, node_id)
    return path or []
