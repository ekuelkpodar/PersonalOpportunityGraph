"""
retriever.py — Hybrid retrieval: Qdrant top-K → Neo4j hop filter → score re-rank.

Pipeline:
  1. Embed query via Ollama nomic-embed-text
  2. Qdrant top-50 semantic search
  3. Filter: keep only nodes within MAX_HOP_FILTER hops of ego in Neo4j
  4. Re-rank by opportunity_score for the given venture context
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import httpx

from backend.config import (
    OLLAMA_BASE_URL, OLLAMA_EMBED_MODEL,
    TOP_K_QDRANT_RETRIEVAL, MAX_HOP_FILTER, EGO_ID,
)
from backend.graph.qdrant_client import search_all_collections
from backend.graph.neo4j_client import run_query, get_shortest_path


def embed_query(query_text: str) -> Optional[List[float]]:
    """Embed a query string via Ollama nomic-embed-text."""
    try:
        resp = httpx.post(
            f"{OLLAMA_BASE_URL}/api/embeddings",
            json={"model": OLLAMA_EMBED_MODEL, "prompt": query_text},
            timeout=60.0,
        )
        resp.raise_for_status()
        return resp.json().get("embedding")
    except Exception:
        return None


def retrieve(
    query_text: str,
    venture_context: str = "applied_insights",
    top_k: int = 10,
    max_hops: int = MAX_HOP_FILTER,
) -> List[Dict[str, Any]]:
    """
    Hybrid retrieval for RAG.
    Returns list of ranked node dicts with relevance_reason.
    """
    # Step 1: Embed query
    query_vec = embed_query(query_text)
    if not query_vec:
        return []

    # Step 2: Qdrant top-50
    candidates = search_all_collections(query_vec, top_k=TOP_K_QDRANT_RETRIEVAL)

    if not candidates:
        return []

    # Step 3: Neo4j hop filter
    node_ids = [c["node_id"] for c in candidates if c.get("node_id")]
    if not node_ids:
        return []

    # Bulk fetch node data + scores from Neo4j
    score_prop = f"opportunity_score_{venture_context}"
    rows = run_query(
        f"""
        MATCH (n)
        WHERE n.id IN $ids
          AND NOT n:Ego
        RETURN n.id AS node_id, n.name AS name, labels(n) AS labels,
               n.`{score_prop}` AS opp_score,
               n.warmth_score AS warmth,
               n.confidence_score AS confidence,
               n.topic_cluster AS topic_cluster,
               n.bio_raw AS bio_raw,
               n.description AS description,
               n.louvain_community AS louvain_community
        """,
        {"ids": node_ids}
    )

    node_map: Dict[str, Dict] = {row["node_id"]: dict(row) for row in rows}

    # Filter by hop distance to ego
    filtered = []
    for candidate in candidates:
        node_id = candidate.get("node_id")
        if not node_id or node_id not in node_map:
            continue

        path = get_shortest_path(EGO_ID, node_id)
        if path is None:
            hop_count = max_hops + 1  # unreachable
        else:
            hop_count = len(path) - 1

        if hop_count > max_hops:
            continue

        node_data = node_map[node_id]
        node_data["vector_score"] = candidate.get("score", 0.0)
        node_data["hop_count"]    = hop_count
        node_data["path_ids"]     = path or []
        filtered.append(node_data)

    # Step 4: Re-rank by opportunity_score
    filtered.sort(
        key=lambda x: (float(x.get("opp_score") or 0.0), x.get("vector_score", 0.0)),
        reverse=True,
    )

    result = []
    for node in filtered[:top_k]:
        node_type = (node.get("labels") or ["Person"])[0]
        bio = node.get("bio_raw") or node.get("description") or ""
        reason = _build_relevance_reason(query_text, node, node_type)

        result.append({
            "node_id":         node["node_id"],
            "name":            node.get("name", ""),
            "node_type":       node_type,
            "opp_score":       float(node.get("opp_score") or 0.0),
            "vector_score":    node.get("vector_score", 0.0),
            "hop_count":       node.get("hop_count", 99),
            "path_ids":        node.get("path_ids", []),
            "topic_cluster":   node.get("topic_cluster"),
            "warmth":          float(node.get("warmth") or 0.0),
            "confidence":      float(node.get("confidence") or 0.0),
            "relevance_reason": reason,
        })

    return result


def _build_relevance_reason(query: str, node: Dict, node_type: str) -> str:
    """Build a short relevance reason string."""
    name  = node.get("name", "This node")
    score = float(node.get("opp_score") or 0.0)
    hops  = node.get("hop_count", "?")
    topic = node.get("topic_cluster") or ""

    reason_parts = []
    if score > 0.6:
        reason_parts.append(f"high opportunity score ({score:.0%})")
    if hops <= 2:
        reason_parts.append(f"{hops}-hop connection to you")
    if topic:
        reason_parts.append(f"topic: {topic}")

    if not reason_parts:
        reason_parts.append("semantic match to your query")

    return f"{name} ({node_type}) — " + ", ".join(reason_parts)
