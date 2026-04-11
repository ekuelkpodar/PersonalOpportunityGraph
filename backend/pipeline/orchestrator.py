"""
orchestrator.py — LangGraph pipeline orchestration with WebSocket progress streaming.

Pipeline stages:
  1. scan      — discover all files
  2. ingest    — parse + dedup each source
  3. embed     — batch Ollama embeddings
  4. load      — Neo4j MERGE + Qdrant upsert
  5. score     — run opportunity scoring
  6. actions   — pre-generate Next Best Action drafts

Progress is streamed via an asyncio Queue consumed by the WebSocket endpoint.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import traceback
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional

from backend.config import PIPELINE_PROGRESS_DB
from backend.pipeline.scanner import scan_all, count_files
from backend.pipeline.dedup import (
    PersonDedup, CompanyDedup, PublisherDedup, CommunityDedup,
)
from backend.pipeline.embedder import embed_nodes, embed_ego_variants, get_embedding_stats
from backend.pipeline.loader import (
    setup_neo4j_constraints, load_ego_node,
    load_persons, load_companies, load_publishers, load_communities,
    load_edges, upsert_embeddings, upsert_ego_embeddings,
)
from backend.pipeline.sources.feedspot import parse_feedspot_file
from backend.pipeline.sources.xlist import parse_xlist_file
from backend.pipeline.sources.clutch import parse_clutch_file
from backend.pipeline.sources.facebook import parse_facebook_csv, parse_facebook_xlsx
from backend.pipeline.sources.skool import parse_skool_communities, parse_skool_dms
from backend.models import EdgeRecord


# ── Progress tracking ─────────────────────────────────────────────────────────

def _get_progress_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(PIPELINE_PROGRESS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at   TEXT,
            finished_at  TEXT,
            status       TEXT DEFAULT 'running',
            stats        TEXT DEFAULT '{}'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            filepath   TEXT PRIMARY KEY,
            source     TEXT,
            rows       INTEGER DEFAULT 0,
            nodes      INTEGER DEFAULT 0,
            processed_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    return conn


def _is_file_processed(conn: sqlite3.Connection, filepath: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM processed_files WHERE filepath = ?", (filepath,)
    ).fetchone()
    return row is not None


def _mark_file_processed(conn: sqlite3.Connection, filepath: str,
                          source: str, rows: int, nodes: int) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO processed_files (filepath, source, rows, nodes) "
        "VALUES (?, ?, ?, ?)",
        (filepath, source, rows, nodes)
    )
    conn.commit()


# ── Progress event format ─────────────────────────────────────────────────────

def _evt(source: str, stage: str, message: str,
         pct: float = 0.0, data: Optional[Dict] = None) -> Dict:
    return {
        "source":  source,
        "stage":   stage,
        "message": message,
        "pct":     round(pct, 1),
        "data":    data or {},
        "ts":      datetime.now(timezone.utc).isoformat(),
    }


# ── Main pipeline ─────────────────────────────────────────────────────────────

class PipelineOrchestrator:
    """
    Runs the full ingestion pipeline.
    Accepts an asyncio.Queue for live progress events.
    """

    def __init__(self, progress_queue: Optional[asyncio.Queue] = None,
                 force_reprocess: bool = False):
        self.queue = progress_queue
        self.force = force_reprocess
        self._stats: Dict[str, Any] = {}

    async def _emit(self, source: str, stage: str, message: str,
                    pct: float = 0.0, data: Optional[Dict] = None) -> None:
        evt = _evt(source, stage, message, pct, data)
        if self.queue:
            await self.queue.put(json.dumps(evt))

    async def run(self) -> Dict[str, Any]:
        """Execute the full pipeline. Returns final stats dict."""
        conn = _get_progress_conn()
        run_id = conn.execute(
            "INSERT INTO pipeline_runs (started_at, status) VALUES (datetime('now'), 'running')"
        ).lastrowid
        conn.commit()

        try:
            await self._emit("pipeline", "start", "Pipeline started", 0.0)

            # ── Stage 1: Setup ───────────────────────────────────────────────
            await self._emit("pipeline", "setup", "Setting up Neo4j constraints", 2.0)
            setup_neo4j_constraints()
            load_ego_node()
            await self._emit("pipeline", "setup", "Ego node loaded", 5.0)

            # ── Stage 2: Scan ────────────────────────────────────────────────
            await self._emit("pipeline", "scan", "Scanning source files", 8.0)
            manifest = scan_all()
            total_files = count_files(manifest)
            await self._emit("pipeline", "scan", f"Found {total_files} files", 10.0,
                             {"total_files": total_files})

            # ── Stage 3: Ingest ──────────────────────────────────────────────
            persons_dedup    = PersonDedup(conn)
            companies_dedup  = CompanyDedup(conn)
            publishers_dedup = PublisherDedup(conn)
            communities_dedup= CommunityDedup(conn)
            all_edges: List[EdgeRecord] = []

            feedspot_stats  = await self._ingest_feedspot(
                manifest["feedspot"], conn, publishers_dedup, persons_dedup, all_edges)
            await self._emit("feedspot", "done",
                             f"Feedspot: {feedspot_stats['nodes']} nodes", 30.0,
                             feedspot_stats)

            xlist_stats = await self._ingest_xlist(
                manifest["xlist"], conn, persons_dedup, companies_dedup, all_edges)
            await self._emit("xlist", "done",
                             f"XList: {xlist_stats['nodes']} nodes", 45.0,
                             xlist_stats)

            clutch_stats = await self._ingest_clutch(
                manifest["clutch"], conn, companies_dedup)
            await self._emit("clutch", "done",
                             f"Clutch: {clutch_stats['nodes']} nodes", 55.0,
                             clutch_stats)

            fb_stats = await self._ingest_facebook(
                manifest["facebook"], conn, communities_dedup)
            await self._emit("facebook", "done",
                             f"Facebook: {fb_stats['nodes']} nodes", 63.0,
                             fb_stats)

            skool_stats = await self._ingest_skool(
                manifest["skool"], conn, persons_dedup, communities_dedup, all_edges)
            await self._emit("skool", "done",
                             f"Skool: {skool_stats['nodes']} nodes", 70.0,
                             skool_stats)

            # ── Stage 4: Load Neo4j ──────────────────────────────────────────
            await self._emit("pipeline", "load", "Loading nodes into Neo4j", 72.0)

            all_persons    = persons_dedup.all_nodes()
            all_companies  = companies_dedup.all_nodes()
            all_publishers = publishers_dedup.all_nodes()
            all_communities= communities_dedup.all_nodes()

            n_persons    = load_persons(all_persons)
            n_companies  = load_companies(all_companies)
            n_publishers = load_publishers(all_publishers)
            n_communities= load_communities(all_communities)
            n_edges      = load_edges(all_edges)

            total_nodes = n_persons + n_companies + n_publishers + n_communities
            await self._emit("pipeline", "load",
                             f"Loaded {total_nodes} nodes, {n_edges} edges", 80.0,
                             {"nodes": total_nodes, "edges": n_edges})

            # ── Stage 5: Embed ───────────────────────────────────────────────
            await self._emit("pipeline", "embed", "Generating embeddings", 82.0)

            def make_progress_cb(source_name: str):
                async def cb(done, total, ntype):
                    pct = 82.0 + (done / max(total, 1)) * 10.0
                    # Can't await in sync callback — put on event loop
                    if self.queue:
                        try:
                            self.queue.put_nowait(json.dumps(
                                _evt(source_name, "embed",
                                     f"Embedded {done}/{total} {ntype}", pct)
                            ))
                        except asyncio.QueueFull:
                            pass
                return cb

            ego_embeddings = embed_ego_variants()
            upsert_ego_embeddings(ego_embeddings)

            person_embs    = embed_nodes(all_persons,     "Person")
            company_embs   = embed_nodes(all_companies,   "Company")
            publisher_embs = embed_nodes(all_publishers,  "Publisher")
            community_embs = embed_nodes(all_communities, "Community")

            upsert_embeddings(all_persons,    person_embs,    "Person")
            upsert_embeddings(all_companies,  company_embs,   "Company")
            upsert_embeddings(all_publishers, publisher_embs, "Publisher")
            upsert_embeddings(all_communities,community_embs, "Community")

            await self._emit("pipeline", "embed", "Embeddings complete", 92.0)

            # ── Stage 6: Score ───────────────────────────────────────────────
            await self._emit("pipeline", "score", "Running opportunity scoring", 93.0)
            try:
                from backend.graph.scorer import run_scoring_job
                run_scoring_job()
                await self._emit("pipeline", "score", "Scoring complete", 96.0)
            except Exception as e:
                await self._emit("pipeline", "score",
                                 f"Scoring skipped (run manually): {e}", 96.0)

            # ── Stage 7: Pre-generate action drafts ──────────────────────────
            await self._emit("pipeline", "actions", "Pre-generating action drafts", 97.0)
            try:
                from backend.action.drafts import batch_generate_drafts
                batch_generate_drafts(limit=200)
                await self._emit("pipeline", "actions", "Action drafts ready", 99.0)
            except Exception as e:
                await self._emit("pipeline", "actions",
                                 f"Drafts skipped: {e}", 99.0)

            # ── Done ─────────────────────────────────────────────────────────
            final_stats = {
                "persons":    n_persons,
                "companies":  n_companies,
                "publishers": n_publishers,
                "communities":n_communities,
                "edges":      n_edges,
                "feedspot":   feedspot_stats,
                "xlist":      xlist_stats,
                "clutch":     clutch_stats,
                "facebook":   fb_stats,
                "skool":      skool_stats,
            }

            conn.execute(
                "UPDATE pipeline_runs SET finished_at=datetime('now'), status='done', stats=? WHERE id=?",
                (json.dumps(final_stats), run_id)
            )
            conn.commit()

            await self._emit("pipeline", "done", "Pipeline complete", 100.0, final_stats)
            return final_stats

        except Exception as e:
            tb = traceback.format_exc()
            conn.execute(
                "UPDATE pipeline_runs SET finished_at=datetime('now'), status='error', stats=? WHERE id=?",
                (json.dumps({"error": str(e), "traceback": tb}), run_id)
            )
            conn.commit()
            await self._emit("pipeline", "error", f"Pipeline error: {e}", 0.0,
                             {"error": str(e)})
            raise
        finally:
            conn.close()

    # ── Source ingestion helpers ───────────────────────────────────────────────

    async def _ingest_feedspot(self, files, conn, publishers_dedup,
                                persons_dedup, edges):
        stats = {"files": 0, "rows": 0, "nodes": 0, "dupes": 0}
        for filepath in files:
            if not self.force and _is_file_processed(conn, filepath):
                continue
            rows, nodes, dupes = 0, 0, 0
            for pub, person, edge in parse_feedspot_file(filepath):
                rows += 1
                _, dup = publishers_dedup.add(pub)
                if dup:
                    dupes += 1
                else:
                    nodes += 1
                if person:
                    persons_dedup.add(person)
                if edge:
                    edges.append(edge)
            _mark_file_processed(conn, filepath, "feedspot", rows, nodes)
            stats["files"] += 1
            stats["rows"]  += rows
            stats["nodes"] += nodes
            stats["dupes"] += dupes
        return stats

    async def _ingest_xlist(self, xlist_manifest, conn, persons_dedup,
                             companies_dedup, edges):
        stats = {"files": 0, "rows": 0, "nodes": 0, "dupes": 0}
        all_files = xlist_manifest.get("person", []) + xlist_manifest.get("company", [])
        for filepath in all_files:
            if not self.force and _is_file_processed(conn, filepath):
                continue
            rows, nodes, dupes = 0, 0, 0
            for node_type, node, node_edges in parse_xlist_file(filepath):
                rows += 1
                if node_type == "person":
                    _, dup = persons_dedup.add(node)
                else:
                    _, dup = companies_dedup.add(node)
                if dup:
                    dupes += 1
                else:
                    nodes += 1
                edges.extend(node_edges)
            _mark_file_processed(conn, filepath, "xlist", rows, nodes)
            stats["files"] += 1
            stats["rows"]  += rows
            stats["nodes"] += nodes
            stats["dupes"] += dupes
        return stats

    async def _ingest_clutch(self, clutch_manifest, conn, companies_dedup):
        stats = {"files": 0, "rows": 0, "nodes": 0, "dupes": 0}
        for category, files in clutch_manifest.items():
            for filepath in files:
                if not self.force and _is_file_processed(conn, filepath):
                    continue
                rows, nodes, dupes = 0, 0, 0
                for node in parse_clutch_file(filepath):
                    rows += 1
                    _, dup = companies_dedup.add(node)
                    if dup:
                        dupes += 1
                    else:
                        nodes += 1
                _mark_file_processed(conn, filepath, f"clutch_{category}", rows, nodes)
                stats["files"] += 1
                stats["rows"]  += rows
                stats["nodes"] += nodes
                stats["dupes"] += dupes
        return stats

    async def _ingest_facebook(self, fb_manifest, conn, communities_dedup):
        stats = {"files": 0, "rows": 0, "nodes": 0, "dupes": 0}
        for filepath in fb_manifest.get("csv", []):
            if not self.force and _is_file_processed(conn, filepath):
                continue
            rows, nodes, dupes = 0, 0, 0
            for node in parse_facebook_csv(filepath):
                rows += 1
                _, dup = communities_dedup.add(node)
                if dup:
                    dupes += 1
                else:
                    nodes += 1
            _mark_file_processed(conn, filepath, "facebook", rows, nodes)
            stats["files"] += 1
            stats["rows"]  += rows
            stats["nodes"] += nodes
            stats["dupes"] += dupes

        for filepath in fb_manifest.get("xlsx", []):
            if not self.force and _is_file_processed(conn, filepath):
                continue
            rows, nodes, dupes = 0, 0, 0
            for node in parse_facebook_xlsx(filepath):
                rows += 1
                _, dup = communities_dedup.add(node)
                if dup:
                    dupes += 1
                else:
                    nodes += 1
            _mark_file_processed(conn, filepath, "facebook_xlsx", rows, nodes)
            stats["files"] += 1
            stats["rows"]  += rows
            stats["nodes"] += nodes
            stats["dupes"] += dupes

        return stats

    async def _ingest_skool(self, skool_manifest, conn, persons_dedup,
                             communities_dedup, edges):
        stats = {"files": 0, "rows": 0, "nodes": 0, "dupes": 0}

        communities_path = skool_manifest.get("communities")
        if communities_path:
            if self.force or not _is_file_processed(conn, communities_path):
                rows, nodes, dupes = 0, 0, 0
                for community, edge in parse_skool_communities(communities_path):
                    rows += 1
                    _, dup = communities_dedup.add(community)
                    if dup:
                        dupes += 1
                    else:
                        nodes += 1
                    edges.append(edge)
                _mark_file_processed(conn, communities_path, "skool_communities", rows, nodes)
                stats["files"] += 1
                stats["rows"]  += rows
                stats["nodes"] += nodes
                stats["dupes"] += dupes

        dms_path = skool_manifest.get("dms")
        if dms_path:
            if self.force or not _is_file_processed(conn, dms_path):
                rows, nodes, dupes = 0, 0, 0
                for person, edge in parse_skool_dms(dms_path):
                    rows += 1
                    _, dup = persons_dedup.add(person)
                    if dup:
                        dupes += 1
                    else:
                        nodes += 1
                    edges.append(edge)
                _mark_file_processed(conn, dms_path, "skool_dms", rows, nodes)
                stats["files"] += 1
                stats["rows"]  += rows
                stats["nodes"] += nodes
                stats["dupes"] += dupes

        return stats


# ── Global pipeline state (singleton for the running process) ─────────────────

_pipeline_running = False
_pipeline_task: Optional[asyncio.Task] = None
_progress_queue: Optional[asyncio.Queue] = None


def is_running() -> bool:
    return _pipeline_running


async def start_pipeline(force_reprocess: bool = False) -> asyncio.Queue:
    """Start the pipeline in the background. Returns a progress queue."""
    global _pipeline_running, _pipeline_task, _progress_queue

    if _pipeline_running:
        raise RuntimeError("Pipeline is already running")

    _progress_queue = asyncio.Queue(maxsize=500)
    orchestrator = PipelineOrchestrator(
        progress_queue=_progress_queue,
        force_reprocess=force_reprocess,
    )

    async def _run():
        global _pipeline_running
        _pipeline_running = True
        try:
            await orchestrator.run()
        finally:
            _pipeline_running = False
            if _progress_queue:
                await _progress_queue.put(json.dumps({"stage": "terminal"}))

    _pipeline_task = asyncio.create_task(_run())
    return _progress_queue


def get_progress_queue() -> Optional[asyncio.Queue]:
    return _progress_queue


def get_last_run_stats() -> Optional[Dict]:
    """Return stats from the most recent completed pipeline run."""
    try:
        conn = _get_progress_conn()
        row = conn.execute(
            "SELECT stats, started_at, finished_at, status "
            "FROM pipeline_runs ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            return {
                "stats":       json.loads(row[0] or "{}"),
                "started_at":  row[1],
                "finished_at": row[2],
                "status":      row[3],
            }
    except Exception:
        pass
    return None
