"""
qdrant_client.py — Qdrant wrapper for semantic search.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, ScoredPoint

from backend.config import (
    QDRANT_HOST, QDRANT_PORT,
    QDRANT_COLLECTION_PROFILES,
    QDRANT_COLLECTION_COMPANIES,
    QDRANT_COLLECTION_COMMUNITIES,
)


_client: Optional[QdrantClient] = None


def get_client() -> QdrantClient:
    global _client
    if _client is None:
        _client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    return _client


def _collection_for_type(node_type: Optional[str] = None) -> str:
    if node_type == "Company":
        return QDRANT_COLLECTION_COMPANIES
    if node_type == "Community":
        return QDRANT_COLLECTION_COMMUNITIES
    return QDRANT_COLLECTION_PROFILES


def search_similar(
    query_vector: List[float],
    node_type: Optional[str] = None,
    top_k: int = 50,
    score_threshold: float = 0.0,
    filter_dict: Optional[Dict[str, Any]] = None,
) -> List[Dict]:
    """
    Search for similar nodes by vector.
    Returns list of {node_id, score, payload}.
    """
    client = get_client()
    collection = _collection_for_type(node_type)

    qdrant_filter = None
    if filter_dict:
        conditions = []
        for key, value in filter_dict.items():
            conditions.append(
                FieldCondition(key=key, match=MatchValue(value=value))
            )
        if conditions:
            qdrant_filter = Filter(must=conditions)

    try:
        results = client.search(
            collection_name=collection,
            query_vector=query_vector,
            limit=top_k,
            score_threshold=score_threshold,
            query_filter=qdrant_filter,
            with_payload=True,
        )
    except Exception:
        return []

    return [
        {
            "node_id": r.payload.get("node_id"),
            "score":   r.score,
            "payload": r.payload,
        }
        for r in results
        if r.payload
    ]


def get_vector(node_id: str, node_type: Optional[str] = None) -> Optional[List[float]]:
    """Retrieve the stored vector for a node_id."""
    from backend.pipeline.loader import _node_id_to_int
    client = get_client()
    collection = _collection_for_type(node_type)
    try:
        point_id = _node_id_to_int(node_id)
        results = client.retrieve(
            collection_name=collection,
            ids=[point_id],
            with_vectors=True,
        )
        if results:
            return results[0].vector
    except Exception:
        pass
    return None


def search_all_collections(
    query_vector: List[float],
    top_k: int = 50,
) -> List[Dict]:
    """Search across all 3 collections and merge results."""
    results = []
    for collection in [QDRANT_COLLECTION_PROFILES,
                       QDRANT_COLLECTION_COMPANIES,
                       QDRANT_COLLECTION_COMMUNITIES]:
        client = get_client()
        try:
            hits = client.search(
                collection_name=collection,
                query_vector=query_vector,
                limit=top_k,
                with_payload=True,
            )
            for h in hits:
                if h.payload:
                    results.append({
                        "node_id": h.payload.get("node_id"),
                        "score":   h.score,
                        "payload": h.payload,
                    })
        except Exception:
            pass
    # Sort by score descending
    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_k]


def cosine_sim_to_ego(node_id: str, venture_context: str,
                       node_type: Optional[str] = None) -> float:
    """
    Compute cosine similarity between a node's vector and ego's venture variant.
    Returns 0.0 if vectors not available.
    """
    from backend.pipeline.embedder import embed_ego_variants
    from backend.config import EGO_VENTURE_CONTEXTS

    ego_vectors = embed_ego_variants()
    ego_vec = ego_vectors.get(venture_context)
    if not ego_vec:
        return 0.0

    node_vec = get_vector(node_id, node_type)
    if not node_vec:
        return 0.0

    return _cosine(ego_vec, node_vec)


def _cosine(a: List[float], b: List[float]) -> float:
    """Compute cosine similarity between two equal-length vectors."""
    if len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na  = sum(x * x for x in a) ** 0.5
    nb  = sum(x * x for x in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)
