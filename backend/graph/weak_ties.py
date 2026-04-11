"""
weak_ties.py — Unexpected Opportunity detection.

A node qualifies as an Unexpected Opportunity if:
  betweenness_centrality (normalized) > 0.6
  AND cosine_similarity_to_ego < 0.35
  AND shortest_path_to_ego <= 3
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.config import (
    WEAK_TIE_BETWEENNESS_MIN,
    WEAK_TIE_COSINE_SIM_MAX,
    WEAK_TIE_MAX_HOPS,
    EGO_ID,
)
from backend.graph.neo4j_client import run_query, run_write, get_shortest_path
from backend.graph.qdrant_client import cosine_sim_to_ego


def detect_unexpected_opportunities(
    venture_context: str = "applied_insights",
) -> List[Dict[str, Any]]:
    """
    Return list of unexpected opportunity nodes with their bridge cluster info.
    Uses betweenness_norm and precomputed path distances.
    """
    # Get candidates with high betweenness
    rows = run_query(
        """
        MATCH (n)
        WHERE n.betweenness_norm >= $b_min
          AND NOT n:Ego
        RETURN n.id AS node_id, n.name AS name,
               labels(n) AS labels,
               n.betweenness_norm AS betweenness,
               n.louvain_community AS community,
               n.topic_cluster AS topic_cluster
        LIMIT 500
        """,
        {"b_min": WEAK_TIE_BETWEENNESS_MIN}
    )

    result = []
    for row in rows:
        node_id = row.get("node_id")
        if not node_id:
            continue

        # Check cosine similarity to ego
        sim = cosine_sim_to_ego(node_id, venture_context)
        if sim >= WEAK_TIE_COSINE_SIM_MAX:
            continue

        # Check path length to ego
        path = get_shortest_path(EGO_ID, node_id)
        if not path or len(path) - 1 > WEAK_TIE_MAX_HOPS:
            continue

        path_len = len(path) - 1

        # Find bridged clusters via neighbors
        bridged = _find_bridged_clusters(node_id)

        result.append({
            "node_id":         node_id,
            "name":            row.get("name", ""),
            "node_type":       row.get("labels", ["Unknown"])[0],
            "betweenness":     row.get("betweenness", 0.0),
            "cosine_sim":      sim,
            "path_length":     path_len,
            "louvain_community": row.get("community"),
            "topic_cluster":   row.get("topic_cluster"),
            "bridged_clusters": bridged,
        })

    # Sort by betweenness descending
    result.sort(key=lambda x: x["betweenness"], reverse=True)
    return result


def _find_bridged_clusters(node_id: str) -> List[str]:
    """
    Find distinct Louvain communities in the 1-hop neighborhood of a node.
    These represent the clusters it bridges.
    """
    rows = run_query(
        """
        MATCH (n {id: $id})-[]-(m)
        WHERE m.louvain_community IS NOT NULL
        RETURN DISTINCT m.louvain_community AS community,
               m.topic_cluster AS topic_cluster
        LIMIT 20
        """,
        {"id": node_id}
    )
    clusters = []
    seen_communities = set()
    for row in rows:
        comm = row.get("community")
        topic = row.get("topic_cluster")
        if comm not in seen_communities:
            seen_communities.add(comm)
            label = topic or f"Cluster {comm}"
            clusters.append(label)
    return clusters


def mark_weak_ties_in_neo4j(venture_context: str = "applied_insights") -> int:
    """
    Flag unexpected opportunity nodes in Neo4j.
    Returns count of nodes flagged.
    """
    nodes = detect_unexpected_opportunities(venture_context)
    for node in nodes:
        run_write(
            """
            MATCH (n {id: $id})
            SET n.is_weak_tie = true,
                n.bridged_clusters = $clusters
            """,
            {"id": node["node_id"], "clusters": node["bridged_clusters"]}
        )
    # Clear flag on non-qualifying nodes
    ids = [n["node_id"] for n in nodes]
    if ids:
        run_write(
            "MATCH (n) WHERE NOT n.id IN $ids AND NOT n:Ego "
            "SET n.is_weak_tie = false",
            {"ids": ids}
        )
    return len(nodes)
