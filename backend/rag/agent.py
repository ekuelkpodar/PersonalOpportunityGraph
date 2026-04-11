"""
agent.py — LangGraph RAG agent with graph path reasoning.

Every response includes:
  - answer_text
  - reasoning_path (node IDs: Ego → ... → target)
  - cited_nodes [{node_id, name, node_type, relevance_reason}]
  - confidence: "high" | "medium" | "low"
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any, Dict, List, Optional

import httpx

from backend.config import (
    OLLAMA_BASE_URL, OLLAMA_GENERATE_MODEL,
    INTERACTIONS_DB, EGO_VENTURE_CONTEXTS, EGO_NAME,
)
from backend.rag.retriever import retrieve
from backend.graph.neo4j_client import run_query
from backend.action.routing import build_routing_path_names


# Predefined query suggestions per venture context
QUERY_SUGGESTIONS = {
    "applied_insights": [
        "Who should I reach out to about AI automation services?",
        "Find founders who are actively building with LLMs",
        "Which communities have the most AI automation practitioners?",
        "Who are the top agency owners I should connect with?",
    ],
    "aegis_t2a": [
        "Who should I reach out to about AEGIS-T2A?",
        "Find enterprise AI governance experts in my network",
        "Who are the bridge nodes between my cluster and enterprise security?",
        "Which Clutch agencies offer AI/ML automation services?",
    ],
    "rgn_trucking": [
        "Who can help with federal contracting opportunities?",
        "Find trucking and logistics operators in my network",
        "Which Facebook groups are most active for freight and logistics?",
        "Who should I talk to about USASpending prime contracting?",
    ],
    "job_search": [
        "Who is hiring senior AI engineers in my network?",
        "Find AI architects I'm 2 hops from",
        "Which companies in my graph are building LLM infrastructure?",
        "Who are influential AI/ML practitioners I should connect with?",
    ],
}


def _get_query_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(INTERACTIONS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS query_history (
            id              TEXT PRIMARY KEY,
            query           TEXT NOT NULL,
            venture_context TEXT NOT NULL,
            intent_mode     TEXT NOT NULL,
            answer          TEXT NOT NULL,
            cited_nodes     TEXT NOT NULL,
            reasoning_path  TEXT NOT NULL,
            confidence      TEXT NOT NULL,
            timestamp       TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


def _save_query(conn: sqlite3.Connection, query_id: str, query: str,
                venture_context: str, intent_mode: str,
                answer: str, cited_nodes: List, reasoning_path: List,
                confidence: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO query_history "
        "(id, query, venture_context, intent_mode, answer, cited_nodes, reasoning_path, confidence) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (query_id, query, venture_context, intent_mode, answer,
         json.dumps(cited_nodes), json.dumps(reasoning_path), confidence)
    )
    conn.commit()


def _build_synthesis_prompt(
    query: str,
    venture_context: str,
    retrieved_nodes: List[Dict],
    reasoning_path: List[str],
) -> str:
    venture_text = EGO_VENTURE_CONTEXTS.get(venture_context, "")
    node_summaries = []
    for node in retrieved_nodes[:10]:
        name   = node.get("name", "Unknown")
        ntype  = node.get("node_type", "Person")
        hops   = node.get("hop_count", "?")
        reason = node.get("relevance_reason", "")
        score  = node.get("opp_score", 0.0)
        node_summaries.append(
            f"- {name} ({ntype}): {reason} | Score: {score:.0%} | Hops from you: {hops}"
        )
    nodes_text = "\n".join(node_summaries)

    path_text = " → ".join(reasoning_path) if reasoning_path else "direct"

    prompt = f"""You are {EGO_NAME}'s personal opportunity graph assistant. Answer the query using ONLY the nodes retrieved from the graph.

Your current venture context: {venture_text}

Query: {query}

Retrieved opportunities:
{nodes_text}

Network path to top result: {path_text}

Instructions:
1. Answer in 3-5 sentences, directly addressing the query
2. Reference specific names and nodes from the retrieved list
3. Explain WHY each recommendation is relevant to the venture context
4. Be concrete — give names, not vague descriptions
5. End with one specific next step

Answer:"""
    return prompt


def _call_ollama_generate(prompt: str) -> str:
    """Call Ollama llama3 for synthesis."""
    try:
        resp = httpx.post(
            f"{OLLAMA_BASE_URL}/api/generate",
            json={
                "model":  OLLAMA_GENERATE_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.5, "num_predict": 400},
            },
            timeout=180.0,
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip()
    except Exception as e:
        return f"Unable to generate synthesis: {e}. Please check Ollama is running."


def _assess_confidence(retrieved_nodes: List[Dict]) -> str:
    if not retrieved_nodes:
        return "low"
    top = retrieved_nodes[0]
    score = float(top.get("opp_score") or 0.0)
    hops  = top.get("hop_count", 99)
    if score > 0.7 and hops <= 2:
        return "high"
    if score > 0.4 or hops <= 3:
        return "medium"
    return "low"


def answer_query(
    query: str,
    venture_context: str = "applied_insights",
    intent_mode: str = "Exploit",
    history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """
    Main RAG entrypoint.
    Returns {answer_text, reasoning_path, cited_nodes, confidence, query_id}.
    """
    query_id = str(uuid.uuid4())[:8]

    # Retrieve relevant nodes
    retrieved = retrieve(query, venture_context, top_k=10)

    # Build reasoning path from top result
    reasoning_path: List[str] = []
    if retrieved:
        top_node = retrieved[0]
        path_ids  = top_node.get("path_ids", [])
        reasoning_path = build_routing_path_names(path_ids)

    # Build cited nodes list
    cited_nodes = []
    for node in retrieved:
        cited_nodes.append({
            "node_id":         node["node_id"],
            "name":            node.get("name", ""),
            "node_type":       node.get("node_type", "Person"),
            "relevance_reason": node.get("relevance_reason", ""),
        })

    confidence = _assess_confidence(retrieved)

    # Generate answer text
    if retrieved:
        prompt      = _build_synthesis_prompt(query, venture_context, retrieved, reasoning_path)
        answer_text = _call_ollama_generate(prompt)
    else:
        answer_text = (
            f"No strong matches found for '{query}' in your {venture_context} context. "
            "Try running the ingestion pipeline or broadening your query."
        )
        confidence = "low"

    # Persist query
    conn = _get_query_conn()
    _save_query(conn, query_id, query, venture_context, intent_mode,
                answer_text, cited_nodes, reasoning_path, confidence)
    conn.close()

    return {
        "query_id":      query_id,
        "answer_text":   answer_text,
        "reasoning_path": reasoning_path,
        "cited_nodes":   cited_nodes,
        "confidence":    confidence,
        "venture_context": venture_context,
    }


def get_query_history(limit: int = 50) -> List[Dict]:
    """Return recent query history."""
    conn = _get_query_conn()
    rows = conn.execute(
        "SELECT id, query, venture_context, intent_mode, answer, "
        "       cited_nodes, reasoning_path, confidence, timestamp "
        "FROM query_history ORDER BY timestamp DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    cols = ["query_id", "query", "venture_context", "intent_mode", "answer",
            "cited_nodes", "reasoning_path", "confidence", "timestamp"]
    result = []
    for row in rows:
        d = dict(zip(cols, row))
        d["cited_nodes"]    = json.loads(d["cited_nodes"])
        d["reasoning_path"] = json.loads(d["reasoning_path"])
        result.append(d)
    return result


def get_suggestions(venture_context: str) -> List[str]:
    """Return predefined query suggestions for a venture context."""
    return QUERY_SUGGESTIONS.get(venture_context, QUERY_SUGGESTIONS["applied_insights"])
