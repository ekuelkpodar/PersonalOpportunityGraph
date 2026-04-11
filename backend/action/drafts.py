"""
drafts.py — Message draft generation via Ollama llama3 + SQLite cache.

Draft is cached keyed by (node_id, venture_context).
Regenerated only if node data changes (tracked by content hash).
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any, Dict, List, Optional

import httpx

from backend.config import (
    DRAFT_CACHE_DB,
    OLLAMA_BASE_URL,
    OLLAMA_GENERATE_MODEL,
    OPPORTUNITY_SCORE_MIN_ACTION,
    EGO_NAME,
    EGO_VENTURES,
    EGO_SKILLS,
    EGO_VENTURE_CONTEXTS,
)


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DRAFT_CACHE_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS draft_cache (
            node_id         TEXT NOT NULL,
            venture_context TEXT NOT NULL,
            content_hash    TEXT NOT NULL,
            draft_text      TEXT NOT NULL,
            generated_at    TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (node_id, venture_context)
        )
    """)
    conn.commit()
    return conn


def _content_hash(node: Dict, venture_context: str) -> str:
    key = json.dumps({
        "id":           node.get("id"),
        "name":         node.get("name"),
        "bio_raw":      node.get("bio_raw"),
        "description":  node.get("description"),
        "venture":      venture_context,
    }, sort_keys=True)
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _get_cached(conn: sqlite3.Connection, node_id: str,
                venture_context: str, content_hash: str) -> Optional[str]:
    row = conn.execute(
        "SELECT draft_text, content_hash FROM draft_cache "
        "WHERE node_id = ? AND venture_context = ?",
        (node_id, venture_context)
    ).fetchone()
    if row and row[1] == content_hash:
        return row[0]
    return None


def _save_cache(conn: sqlite3.Connection, node_id: str,
                venture_context: str, content_hash: str, draft: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO draft_cache "
        "(node_id, venture_context, content_hash, draft_text) VALUES (?, ?, ?, ?)",
        (node_id, venture_context, content_hash, draft)
    )
    conn.commit()


def _build_prompt(node: Dict, venture_context: str,
                   action_type: str, routing_path: List[str]) -> str:
    name        = node.get("name", "this person")
    bio         = node.get("bio_raw") or node.get("description") or ""
    node_type   = (node.get("_labels") or ["Person"])[0]
    venture_text = EGO_VENTURE_CONTEXTS.get(venture_context, "")

    path_str = " → ".join(routing_path) if routing_path else "direct"

    action_instructions = {
        "DM": "Write a warm, concise direct message (3-5 sentences) introducing yourself and expressing genuine interest in their work. Do not pitch anything yet.",
        "AskIntro": "Write a brief message asking a mutual contact to introduce you. Reference the shared connection naturally.",
        "EngageContent": "Write a thoughtful 2-3 sentence reply or comment on their content that adds value and introduces yourself subtly.",
        "PitchService": "Write a short, direct outreach message (4-5 sentences) pitching a specific service that matches their needs. Include one concrete value proposition.",
        "Collaborate": "Write a collaboration proposal message that references shared interests and proposes a specific joint project.",
    }

    instruction = action_instructions.get(action_type, action_instructions["DM"])

    prompt = f"""You are writing an outreach message on behalf of {EGO_NAME}, an AI professional based in Atlanta, GA.

{EGO_NAME}'s context for this outreach:
{venture_text}

Target person/company: {name}
Type: {node_type}
Bio/Description: {bio[:400] if bio else 'Not available'}

Routing path to target: {path_str}

Task: {instruction}

Requirements:
- 3-5 sentences maximum
- Personalized to their actual bio/work
- No generic phrases like "I came across your profile"
- First sentence should be specific to them
- Include a clear call to action in the last sentence
- Tone: professional but warm

Write only the message text, no subject line, no greeting label:"""

    return prompt


def _generate_draft_ollama(prompt: str) -> str:
    """Call Ollama llama3 to generate a message draft."""
    try:
        resp = httpx.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model":  OLLAMA_GENERATE_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.7,
                    "num_predict": 200,
                },
            },
            timeout=120.0,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("response", "").strip()
    except Exception as e:
        return (
            f"Hi {{}}, I noticed your work and wanted to connect. "
            f"I'm working on AI automation solutions and think there might be synergy. "
            f"Would love a quick 15-minute chat if you're open to it."
        )


def get_or_generate_draft(
    node: Dict,
    venture_context: str,
    action_type: str,
    routing_path: Optional[List[str]] = None,
) -> str:
    """
    Return cached draft or generate a new one via Ollama.
    """
    node_id      = node.get("id", "")
    c_hash       = _content_hash(node, venture_context)
    path         = routing_path or []

    conn = _get_conn()
    cached = _get_cached(conn, node_id, venture_context, c_hash)
    if cached:
        conn.close()
        return cached

    prompt = _build_prompt(node, venture_context, action_type, path)
    draft  = _generate_draft_ollama(prompt)

    if not draft or len(draft) < 20:
        name = node.get("name", "there")
        draft = (
            f"Hi {name}, your work caught my attention and I wanted to connect. "
            f"I'm building {EGO_VENTURES[0]} and see strong alignment with what you're doing. "
            f"Would you be open to a quick call to explore potential collaboration?"
        )

    _save_cache(conn, node_id, venture_context, c_hash, draft)
    conn.close()
    return draft


def batch_generate_drafts(
    venture_context: str = "applied_insights",
    limit: int = 200,
) -> int:
    """
    Pre-generate drafts for top scoring nodes across all venture contexts.
    Returns count of drafts generated.
    """
    from backend.graph.neo4j_client import run_query

    total = 0
    for vc in EGO_VENTURE_CONTEXTS.keys():
        score_prop = f"opportunity_score_{vc}"
        rows = run_query(
            f"""
            MATCH (n)
            WHERE n.`{score_prop}` >= $min_score
              AND NOT n:Ego
            RETURN n, labels(n) AS labels
            ORDER BY n.`{score_prop}` DESC
            LIMIT $limit
            """,
            {"min_score": OPPORTUNITY_SCORE_MIN_ACTION, "limit": limit}
        )

        for row in rows:
            node = dict(row["n"])
            node["_labels"] = row["labels"]

            get_or_generate_draft(
                node=node,
                venture_context=vc,
                action_type="DM",
                routing_path=[],
            )
            total += 1

    return total
