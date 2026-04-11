"""
neo4j_client.py — Neo4j driver wrapper with helper query methods.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from neo4j import GraphDatabase, Driver

from backend.config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, EGO_ID


_driver: Optional[Driver] = None


def get_driver() -> Driver:
    global _driver
    if _driver is None or not _driver.verify_connectivity():
        _driver = GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
    return _driver


def run_query(cypher: str, params: Optional[Dict] = None) -> List[Dict]:
    """Run a read query and return list of record dicts."""
    driver = get_driver()
    with driver.session() as session:
        result = session.run(cypher, **(params or {}))
        return [dict(r) for r in result]


def run_write(cypher: str, params: Optional[Dict] = None) -> None:
    """Run a write query."""
    driver = get_driver()
    with driver.session() as session:
        session.run(cypher, **(params or {}))


def get_node(node_id: str) -> Optional[Dict]:
    """Fetch a node by id (any label)."""
    rows = run_query(
        "MATCH (n {id: $id}) RETURN n, labels(n) AS labels LIMIT 1",
        {"id": node_id}
    )
    if not rows:
        return None
    rec = rows[0]
    props = dict(rec["n"])
    props["_labels"] = rec["labels"]
    return props


def get_node_neighbors(node_id: str, max_hops: int = 1) -> List[Dict]:
    """Return 1-hop neighbors of a node."""
    rows = run_query(
        """
        MATCH (n {id: $id})-[r]-(m)
        RETURN m, labels(m) AS labels, type(r) AS rel_type, r.weight AS weight
        LIMIT 200
        """,
        {"id": node_id}
    )
    result = []
    for row in rows:
        props = dict(row["m"])
        props["_labels"] = row["labels"]
        props["_rel_type"] = row["rel_type"]
        props["_weight"] = row["weight"]
        result.append(props)
    return result


def get_shortest_path(from_id: str, to_id: str) -> Optional[List[str]]:
    """Return shortest path as list of node IDs."""
    rows = run_query(
        """
        MATCH p = shortestPath((a {id: $from_id})-[*..6]-(b {id: $to_id}))
        RETURN [node IN nodes(p) | node.id] AS path_ids
        LIMIT 1
        """,
        {"from_id": from_id, "to_id": to_id}
    )
    if rows:
        return rows[0]["path_ids"]
    return None


def get_all_shortest_paths_from_ego(to_id: str) -> Optional[List[str]]:
    return get_shortest_path(EGO_ID, to_id)


def get_graph_stats() -> Dict[str, int]:
    """Return count of each node type and total edges."""
    stats = {}
    for label in ["Person", "Company", "Publisher", "Community", "Ego"]:
        rows = run_query(f"MATCH (n:{label}) RETURN count(n) AS cnt")
        stats[label.lower()] = rows[0]["cnt"] if rows else 0
    rows = run_query("MATCH ()-[r]->() RETURN count(r) AS cnt")
    stats["edges"] = rows[0]["cnt"] if rows else 0
    return stats


def set_node_property(node_id: str, key: str, value: Any) -> None:
    run_write(
        f"MATCH (n {{id: $id}}) SET n.`{key}` = $value",
        {"id": node_id, "value": value}
    )


def set_node_properties(node_id: str, props: Dict[str, Any]) -> None:
    if not props:
        return
    set_parts = ", ".join(f"n.`{k}` = ${k}" for k in props)
    params = {"id": node_id, **props}
    run_write(f"MATCH (n {{id: $id}}) SET {set_parts}", params)


def get_nodes_by_type(node_type: str, limit: int = 1000,
                       skip: int = 0) -> List[Dict]:
    rows = run_query(
        f"MATCH (n:{node_type}) RETURN n, labels(n) AS labels "
        f"SKIP $skip LIMIT $limit",
        {"skip": skip, "limit": limit}
    )
    result = []
    for row in rows:
        props = dict(row["n"])
        props["_labels"] = row["labels"]
        result.append(props)
    return result


def get_top_nodes_by_score(venture_context: str,
                            limit: int = 50,
                            min_score: float = 0.0) -> List[Dict]:
    score_prop = f"opportunity_score_{venture_context}"
    rows = run_query(
        f"""
        MATCH (n)
        WHERE n.`{score_prop}` IS NOT NULL
          AND n.`{score_prop}` >= $min_score
          AND NOT n:Ego
        RETURN n, labels(n) AS labels
        ORDER BY n.`{score_prop}` DESC
        LIMIT $limit
        """,
        {"min_score": min_score, "limit": limit}
    )
    result = []
    for row in rows:
        props = dict(row["n"])
        props["_labels"] = row["labels"]
        result.append(props)
    return result


def get_shared_community_count(node_id: str) -> int:
    """Count communities shared between ego and a given node."""
    rows = run_query(
        """
        MATCH (ego:Ego {id: $ego_id})-[:MEMBER_OF]->(c:Community)<-[:MEMBER_OF|IN_COMMUNITY]-(n {id: $node_id})
        RETURN count(c) AS cnt
        """,
        {"ego_id": EGO_ID, "node_id": node_id}
    )
    return rows[0]["cnt"] if rows else 0


def has_warm_edge_on_path(path_ids: List[str]) -> bool:
    """Check if any node in path has a WARM_CONTACT edge from ego."""
    if len(path_ids) < 2:
        return False
    for node_id in path_ids[1:]:
        rows = run_query(
            "MATCH (:Ego {id: $ego_id})-[:WARM_CONTACT]->(n {id: $node_id}) RETURN 1 LIMIT 1",
            {"ego_id": EGO_ID, "node_id": node_id}
        )
        if rows:
            return True
    return False
