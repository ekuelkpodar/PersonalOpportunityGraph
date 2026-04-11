"""
gds.py — Neo4j Graph Data Science operations.
PageRank, Louvain community detection, betweenness centrality, node2vec.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.graph.neo4j_client import run_query, run_write, get_driver


_GRAPH_NAME = "pog_graph"


def _drop_graph_if_exists() -> None:
    try:
        run_query(
            "CALL gds.graph.exists($name) YIELD exists",
            {"name": _GRAPH_NAME}
        )
        run_write(
            "CALL gds.graph.drop($name, false) YIELD graphName",
            {"name": _GRAPH_NAME}
        )
    except Exception:
        pass


def project_graph() -> None:
    """
    Project the in-memory GDS graph over all node/edge types.
    Uses a Cypher projection for flexibility.
    """
    _drop_graph_if_exists()
    try:
        run_write(
            """
            CALL gds.graph.project(
                $name,
                ['Person', 'Company', 'Publisher', 'Community', 'Ego'],
                {
                    HAS_AUTHOR:      {orientation: 'UNDIRECTED', properties: {weight: {defaultValue: 1.0}}},
                    WORKS_AT:        {orientation: 'UNDIRECTED', properties: {weight: {defaultValue: 0.7}}},
                    AFFILIATED_WITH: {orientation: 'UNDIRECTED', properties: {weight: {defaultValue: 0.5}}},
                    MEMBER_OF:       {orientation: 'UNDIRECTED', properties: {weight: {defaultValue: 1.0}}},
                    WARM_CONTACT:    {orientation: 'UNDIRECTED', properties: {weight: {defaultValue: 1.0}}},
                    IN_COMMUNITY:    {orientation: 'UNDIRECTED', properties: {weight: {defaultValue: 0.8}}}
                },
                {nodeProperties: ['warmth_score', 'confidence_score']}
            )
            """,
            {"name": _GRAPH_NAME}
        )
    except Exception as e:
        raise RuntimeError(f"Failed to project GDS graph: {e}") from e


def run_pagerank(write_back: bool = True) -> Dict[str, float]:
    """
    Run PageRank on the projected graph.
    Returns {node_id: pagerank_score} if write_back=False.
    """
    if write_back:
        run_write(
            """
            CALL gds.pageRank.write($name, {
                writeProperty: 'pagerank',
                maxIterations: 20,
                dampingFactor: 0.85,
                relationshipWeightProperty: 'weight'
            }) YIELD nodePropertiesWritten
            """,
            {"name": _GRAPH_NAME}
        )
        return {}
    else:
        rows = run_query(
            """
            CALL gds.pageRank.stream($name, {
                maxIterations: 20,
                dampingFactor: 0.85,
                relationshipWeightProperty: 'weight'
            })
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).id AS node_id, score
            """,
            {"name": _GRAPH_NAME}
        )
        return {row["node_id"]: row["score"] for row in rows if row["node_id"]}


def run_louvain(write_back: bool = True) -> Dict[str, int]:
    """
    Run Louvain community detection.
    Returns {node_id: community_id}.
    """
    if write_back:
        run_write(
            """
            CALL gds.louvain.write($name, {
                writeProperty: 'louvain_community',
                relationshipWeightProperty: 'weight'
            }) YIELD nodePropertiesWritten
            """,
            {"name": _GRAPH_NAME}
        )
        return {}
    else:
        rows = run_query(
            """
            CALL gds.louvain.stream($name, {
                relationshipWeightProperty: 'weight'
            })
            YIELD nodeId, communityId
            RETURN gds.util.asNode(nodeId).id AS node_id, communityId AS community_id
            """,
            {"name": _GRAPH_NAME}
        )
        return {row["node_id"]: row["community_id"]
                for row in rows if row["node_id"]}


def run_betweenness(write_back: bool = True) -> Dict[str, float]:
    """
    Run betweenness centrality (approximate).
    Returns {node_id: betweenness_score}.
    """
    if write_back:
        run_write(
            """
            CALL gds.betweenness.write($name, {
                writeProperty: 'betweenness',
                samplingSize: 100
            }) YIELD nodePropertiesWritten
            """,
            {"name": _GRAPH_NAME}
        )
        return {}
    else:
        rows = run_query(
            """
            CALL gds.betweenness.stream($name, {samplingSize: 100})
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).id AS node_id, score
            """,
            {"name": _GRAPH_NAME}
        )
        return {row["node_id"]: row["score"] for row in rows if row["node_id"]}


def run_node2vec(embedding_dim: int = 128, write_back: bool = True) -> None:
    """Run node2vec to compute 128-dim structural embeddings."""
    if write_back:
        run_write(
            """
            CALL gds.node2vec.write($name, {
                writeProperty: 'node2vec_embedding',
                embeddingDimension: $dim,
                walkLength: 80,
                walksPerNode: 10,
                windowSize: 10,
                relationshipWeightProperty: 'weight'
            }) YIELD nodeCount
            """,
            {"name": _GRAPH_NAME, "dim": embedding_dim}
        )


def normalize_pagerank() -> None:
    """Normalize pagerank values to 0-1 range."""
    rows = run_query("MATCH (n) WHERE n.pagerank IS NOT NULL "
                     "RETURN max(n.pagerank) AS max_pr")
    if not rows or not rows[0]["max_pr"]:
        return
    max_pr = rows[0]["max_pr"]
    if max_pr > 0:
        run_write(
            "MATCH (n) WHERE n.pagerank IS NOT NULL "
            "SET n.pagerank_norm = n.pagerank / $max_pr",
            {"max_pr": max_pr}
        )


def normalize_betweenness() -> None:
    """Normalize betweenness centrality to 0-1 range."""
    rows = run_query("MATCH (n) WHERE n.betweenness IS NOT NULL "
                     "RETURN max(n.betweenness) AS max_b")
    if not rows or not rows[0]["max_b"]:
        return
    max_b = rows[0]["max_b"]
    if max_b > 0:
        run_write(
            "MATCH (n) WHERE n.betweenness IS NOT NULL "
            "SET n.betweenness_norm = n.betweenness / $max_b",
            {"max_b": max_b}
        )


def run_all_gds() -> None:
    """Project graph and run all GDS algorithms with write-back."""
    project_graph()
    run_pagerank(write_back=True)
    normalize_pagerank()
    run_louvain(write_back=True)
    run_betweenness(write_back=True)
    normalize_betweenness()
    try:
        run_node2vec(write_back=True)
    except Exception:
        pass  # node2vec may fail on small graphs
