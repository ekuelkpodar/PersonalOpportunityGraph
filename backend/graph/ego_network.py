"""
ego_network.py — Ego network extraction: 1-hop and 2-hop subgraphs.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from backend.config import EGO_ID
from backend.graph.neo4j_client import run_query
from backend.models import GraphNodeModel, GraphEdgeModel, GraphDataResponse


def get_ego_subgraph(
    venture_context: str = "applied_insights",
    top_n: int = 50,
    min_score: float = 0.0,
    node_types: Optional[List[str]] = None,
    intent_mode: str = "Exploit",
) -> GraphDataResponse:
    """
    Return ego + top N nodes by opportunity score as a graph payload.
    Includes all edges between the returned nodes.
    """
    score_prop = f"opportunity_score_{venture_context}"

    type_filter = ""
    if node_types:
        labels = "|".join(node_types)
        type_filter = f"AND (n:{labels})"

    rows = run_query(
        f"""
        MATCH (n)
        WHERE NOT n:Ego
          AND n.`{score_prop}` >= $min_score
          {type_filter}
        RETURN n, labels(n) AS labels
        ORDER BY n.`{score_prop}` DESC
        LIMIT $top_n
        """,
        {"min_score": min_score, "top_n": top_n}
    )

    # Add ego itself
    ego_rows = run_query(
        "MATCH (e:Ego {id: $id}) RETURN e, labels(e) AS labels",
        {"id": EGO_ID}
    )

    all_rows = ego_rows + rows
    node_ids = set()
    nodes: List[GraphNodeModel] = []

    for row in all_rows:
        props = dict(row["n"] if "n" in row else row["e"])
        labels = row.get("labels", [])
        node_id = props.get("id")
        if not node_id or node_id in node_ids:
            continue
        node_ids.add(node_id)

        node_type = labels[0] if labels else "Unknown"
        score = float(props.get(score_prop) or 0.0)

        nodes.append(GraphNodeModel(
            id=node_id,
            name=props.get("name", ""),
            node_type=node_type,
            opportunity_score=score,
            confidence_score=float(props.get("confidence_score") or 0.0),
            warmth_score=float(props.get("warmth_score") or 0.0),
            topic_cluster=props.get("topic_cluster"),
            is_trending=bool(props.get("is_trending", False)),
            is_weak_tie=bool(props.get("is_weak_tie", False)),
            louvain_community=props.get("louvain_community"),
        ))

    # Fetch edges between the returned nodes
    id_list = list(node_ids)
    edge_rows = run_query(
        """
        MATCH (a)-[r]->(b)
        WHERE a.id IN $ids AND b.id IN $ids
        RETURN a.id AS src, b.id AS tgt, type(r) AS rel_type, r.weight AS weight
        LIMIT 2000
        """,
        {"ids": id_list}
    )

    edges: List[GraphEdgeModel] = []
    for row in edge_rows:
        edges.append(GraphEdgeModel(
            source=row["src"],
            target=row["tgt"],
            rel_type=row["rel_type"],
            weight=float(row["weight"] or 1.0),
        ))

    return GraphDataResponse(
        nodes=nodes,
        edges=edges,
        total_nodes=len(nodes),
        total_edges=len(edges),
    )


def get_node_subgraph(node_id: str) -> GraphDataResponse:
    """Return 1-hop neighborhood subgraph centered on a node."""
    rows = run_query(
        """
        MATCH (center {id: $id})-[r]-(neighbor)
        RETURN center, labels(center) AS center_labels,
               neighbor, labels(neighbor) AS neighbor_labels,
               type(r) AS rel_type, r.weight AS weight
        LIMIT 200
        """,
        {"id": node_id}
    )

    node_ids = set()
    nodes: List[GraphNodeModel] = []
    edges: List[GraphEdgeModel] = []

    # Add center node
    if rows:
        center_props = dict(rows[0]["center"])
        center_id    = center_props.get("id", node_id)
        node_ids.add(center_id)
        nodes.append(GraphNodeModel(
            id=center_id,
            name=center_props.get("name", ""),
            node_type=rows[0]["center_labels"][0] if rows[0]["center_labels"] else "Unknown",
            opportunity_score=0.0,
            confidence_score=float(center_props.get("confidence_score") or 0.0),
            warmth_score=float(center_props.get("warmth_score") or 0.0),
        ))

    for row in rows:
        neighbor_props = dict(row["neighbor"])
        neighbor_id    = neighbor_props.get("id")
        if not neighbor_id:
            continue
        if neighbor_id not in node_ids:
            node_ids.add(neighbor_id)
            ntype = row["neighbor_labels"][0] if row["neighbor_labels"] else "Unknown"
            nodes.append(GraphNodeModel(
                id=neighbor_id,
                name=neighbor_props.get("name", ""),
                node_type=ntype,
                opportunity_score=float(neighbor_props.get("opportunity_score_applied_insights") or 0.0),
                confidence_score=float(neighbor_props.get("confidence_score") or 0.0),
                warmth_score=float(neighbor_props.get("warmth_score") or 0.0),
                topic_cluster=neighbor_props.get("topic_cluster"),
                is_trending=bool(neighbor_props.get("is_trending", False)),
            ))

        # Add edge (use center as source if rel was outgoing, otherwise reverse)
        center_id_for_edge = rows[0]["center"].get("id", node_id) if rows else node_id
        edges.append(GraphEdgeModel(
            source=center_id_for_edge,
            target=neighbor_id,
            rel_type=row.get("rel_type", "CONNECTED"),
            weight=float(row.get("weight") or 1.0),
        ))

    return GraphDataResponse(
        nodes=nodes,
        edges=edges,
        total_nodes=len(nodes),
        total_edges=len(edges),
    )
