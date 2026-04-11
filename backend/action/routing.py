"""
routing.py — Warm path builder and routing path name resolver.
"""
from __future__ import annotations

from typing import List, Optional

from backend.config import EGO_ID, EGO_NAME
from backend.graph.neo4j_client import run_query, get_shortest_path


def build_routing_path_names(path_ids: List[str]) -> List[str]:
    """
    Resolve a list of node IDs to human-readable names.
    Returns list of 'Name (Type)' strings.
    """
    if not path_ids:
        return []

    rows = run_query(
        """
        MATCH (n)
        WHERE n.id IN $ids
        RETURN n.id AS node_id, n.name AS name, labels(n) AS labels
        """,
        {"ids": path_ids}
    )

    id_to_name = {}
    for row in rows:
        node_id = row["node_id"]
        name    = row["name"] or node_id
        label   = row["labels"][0] if row["labels"] else "Node"
        if node_id == EGO_ID:
            id_to_name[node_id] = EGO_NAME
        else:
            id_to_name[node_id] = f"{name} ({label})"

    return [id_to_name.get(nid, nid) for nid in path_ids]


def find_warm_path(target_id: str) -> List[str]:
    """
    Find the shortest path from ego to target that uses a warm edge if possible.
    Falls back to plain shortest path.
    """
    # Try path through warm contacts first
    warm_rows = run_query(
        """
        MATCH (ego:Ego {id: $ego_id})-[:WARM_CONTACT]->(warm:Person)
        WITH ego, warm
        MATCH p = shortestPath((warm)-[*..4]-(target {id: $target_id}))
        WHERE NOT target:Ego
        RETURN [ego.id] + [node IN nodes(p) | node.id] AS path_ids
        ORDER BY length(p) ASC
        LIMIT 1
        """,
        {"ego_id": EGO_ID, "target_id": target_id}
    )

    if warm_rows and warm_rows[0].get("path_ids"):
        return warm_rows[0]["path_ids"]

    # Fallback to plain shortest path
    path = get_shortest_path(EGO_ID, target_id)
    return path or []
