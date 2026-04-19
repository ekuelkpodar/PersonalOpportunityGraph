"""
embedder.py — Batch Ollama nomic-embed-text embeddings with resume capability.

Checkpoints are stored in SQLite pipeline_progress.db.
Batch size: 50 nodes per call.
Exponential backoff on failure: 2s, 4s, 8s, 16s, 32s (max 5 retries).
"""
from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

from backend.config import (
    OLLAMA_BASE_URL,
    OLLAMA_EMBED_MODEL,
    EMBED_BATCH_SIZE,
    EMBED_MAX_RETRIES,
    EMBED_INITIAL_BACKOFF,
    EMBED_BACKOFF_MULTIPLIER,
    PIPELINE_PROGRESS_DB,
    EGO_VENTURE_CONTEXTS,
)


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(PIPELINE_PROGRESS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS embedding_checkpoint (
            node_id     TEXT PRIMARY KEY,
            node_type   TEXT NOT NULL,
            embedded_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


def _is_embedded(conn: sqlite3.Connection, node_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM embedding_checkpoint WHERE node_id = ?", (node_id,)
    ).fetchone()
    return row is not None


def _mark_embedded(conn: sqlite3.Connection, node_id: str, node_type: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO embedding_checkpoint (node_id, node_type) VALUES (?, ?)",
        (node_id, node_type)
    )
    conn.commit()


def _embed_texts_ollama(texts: List[str]) -> List[Optional[List[float]]]:
    """
    Call Ollama /api/embed for a batch of texts (Ollama >= 0.1.26).
    Returns list of embedding vectors (or None on failure per text).
    """
    if not texts:
        return []

    backoff = EMBED_INITIAL_BACKOFF
    for attempt in range(EMBED_MAX_RETRIES):
        try:
            resp = httpx.post(
                f"{OLLAMA_BASE_URL}/api/embed",
                json={"model": OLLAMA_EMBED_MODEL, "input": texts},
                timeout=120.0,
            )
            resp.raise_for_status()
            data = resp.json()
            embeddings = data.get("embeddings", [])
            # Pad with None if fewer embeddings returned than inputs
            results: List[Optional[List[float]]] = list(embeddings)
            while len(results) < len(texts):
                results.append(None)
            return results
        except Exception:
            if attempt < EMBED_MAX_RETRIES - 1:
                time.sleep(backoff)
                backoff *= EMBED_BACKOFF_MULTIPLIER

    return [None] * len(texts)


def _build_text_for_node(node: Any, node_type: str) -> str:
    """Build a text representation of a node for embedding."""
    parts = []

    if node_type == "Person":
        if hasattr(node, "name") and node.name:
            parts.append(node.name)
        if hasattr(node, "bio_raw") and node.bio_raw:
            parts.append(node.bio_raw[:500])
        if hasattr(node, "x_handle") and node.x_handle:
            parts.append(f"@{node.x_handle}")
        if hasattr(node, "scoble_lists"):
            parts.extend(node.scoble_lists[:5])
        if hasattr(node, "location") and node.location:
            parts.append(node.location)

    elif node_type == "Company":
        if hasattr(node, "name") and node.name:
            parts.append(node.name)
        if hasattr(node, "description") and node.description:
            parts.append(node.description[:500])
        if hasattr(node, "services_raw"):
            parts.extend(node.services_raw[:5])
        if hasattr(node, "clutch_category") and node.clutch_category:
            parts.append(node.clutch_category)
        if hasattr(node, "scoble_category") and node.scoble_category:
            parts.append(node.scoble_category)
        if hasattr(node, "location") and node.location:
            parts.append(node.location)

    elif node_type == "Publisher":
        if hasattr(node, "name") and node.name:
            parts.append(node.name)
        if hasattr(node, "description") and node.description:
            parts.append(node.description[:500])
        if hasattr(node, "category") and node.category:
            parts.append(node.category)
        if hasattr(node, "category_type") and node.category_type:
            parts.append(node.category_type)

    elif node_type == "Community":
        if hasattr(node, "name") and node.name:
            parts.append(node.name)
        if hasattr(node, "category") and node.category:
            parts.append(node.category)
        if hasattr(node, "platform") and node.platform:
            parts.append(node.platform)
        if hasattr(node, "topic_cluster") and node.topic_cluster:
            parts.append(node.topic_cluster)

    return " ".join(p for p in parts if p)


def embed_nodes(
    nodes: List[Any],
    node_type: str,
    progress_callback=None,
) -> Dict[str, List[float]]:
    """
    Embed a list of nodes. Returns {node_id: embedding_vector}.
    Skips nodes already checkpointed. Processes in batches of EMBED_BATCH_SIZE.
    """
    conn = _get_conn()
    results: Dict[str, List[float]] = {}
    to_embed: List[Tuple[str, str]] = []  # [(node_id, text)]

    for node in nodes:
        node_id = node.id
        if _is_embedded(conn, node_id):
            continue
        text = _build_text_for_node(node, node_type)
        if text.strip():
            to_embed.append((node_id, text))

    total = len(to_embed)
    embedded_count = 0

    for batch_start in range(0, total, EMBED_BATCH_SIZE):
        batch = to_embed[batch_start:batch_start + EMBED_BATCH_SIZE]
        ids   = [item[0] for item in batch]
        texts = [item[1] for item in batch]

        vectors = _embed_texts_ollama(texts)

        for node_id, vector in zip(ids, vectors):
            if vector is not None:
                results[node_id] = vector
                _mark_embedded(conn, node_id, node_type)

        embedded_count += len(batch)

        if progress_callback:
            progress_callback(embedded_count, total, node_type)

    conn.close()
    return results


def embed_ego_variants() -> Dict[str, List[float]]:
    """
    Embed all 4 venture-specific ego text variants.
    Returns {venture_context_key: embedding_vector}.
    """
    results: Dict[str, List[float]] = {}
    for key, text in EGO_VENTURE_CONTEXTS.items():
        vectors = _embed_texts_ollama([text])
        if vectors and vectors[0] is not None:
            results[key] = vectors[0]
    return results


def get_embedding_stats() -> Dict[str, int]:
    """Return count of embedded nodes per type."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT node_type, COUNT(*) FROM embedding_checkpoint GROUP BY node_type"
    ).fetchall()
    conn.close()
    return {row[0]: row[1] for row in rows}
