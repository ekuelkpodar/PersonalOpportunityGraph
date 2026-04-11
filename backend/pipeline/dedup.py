"""
dedup.py — Deduplication engine.

Strategy:
  1. SHA-256 primary key: exact match on id field
  2. rapidfuzz Jaro-Winkler fuzzy fallback:
       Person threshold: 0.92
       Company threshold: 0.88
  3. Confidence score updated when records are merged

Maintains in-memory indexes during a pipeline run.
Persists dedup log to SQLite for auditing.
"""
from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Optional, Tuple

from backend.config import (
    PIPELINE_PROGRESS_DB,
    DEDUP_PERSON_THRESHOLD,
    DEDUP_COMPANY_THRESHOLD,
)
from backend.models import PersonNode, CompanyNode, PublisherNode, CommunityNode
from backend.utils import clean_text, jaro_winkler_sim, compute_confidence


def _init_dedup_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dedup_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            node_type    TEXT NOT NULL,
            kept_id      TEXT NOT NULL,
            merged_id    TEXT NOT NULL,
            similarity   REAL,
            match_field  TEXT,
            timestamp    TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()


class PersonDedup:
    """
    Deduplicates PersonNode objects.
    Index: {id → node} (exact) + list of (id, name, handle) for fuzzy.
    """

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._by_id:     Dict[str, PersonNode] = {}
        self._by_handle: Dict[str, str]        = {}  # handle → node_id
        self._by_email:  Dict[str, str]        = {}  # email  → node_id
        self._name_index: List[Tuple[str, str]] = []  # [(normalized_name, node_id)]
        _init_dedup_table(conn)

    def add(self, node: PersonNode) -> Tuple[PersonNode, bool]:
        """
        Add a PersonNode. Returns (canonical_node, was_duplicate).
        If duplicate found, merges new node into existing.
        """
        # 1. Exact id match
        if node.id in self._by_id:
            existing = self._by_id[node.id]
            _merge_person(existing, node)
            return existing, True

        # 2. Exact handle match
        if node.x_handle:
            h = node.x_handle.lower()
            if h in self._by_handle:
                existing = self._by_id[self._by_handle[h]]
                _log_dedup(self._conn, "Person", existing.id, node.id, 1.0, "x_handle")
                _merge_person(existing, node)
                return existing, True

        # 3. Exact email match
        if node.email:
            e = node.email.lower()
            if e in self._by_email:
                existing = self._by_id[self._by_email[e]]
                _log_dedup(self._conn, "Person", existing.id, node.id, 1.0, "email")
                _merge_person(existing, node)
                return existing, True

        # 4. Fuzzy name match
        name_norm = clean_text(node.name)
        if name_norm:
            for existing_name, existing_id in self._name_index:
                sim = jaro_winkler_sim(name_norm, existing_name)
                if sim >= DEDUP_PERSON_THRESHOLD:
                    existing = self._by_id[existing_id]
                    _log_dedup(self._conn, "Person", existing_id, node.id, sim, "name_fuzzy")
                    _merge_person(existing, node)
                    return existing, True

        # New node — index it
        self._by_id[node.id] = node
        if node.x_handle:
            self._by_handle[node.x_handle.lower()] = node.id
        if node.email:
            self._by_email[node.email.lower()] = node.id
        if name_norm:
            self._name_index.append((name_norm, node.id))

        return node, False

    def all_nodes(self) -> List[PersonNode]:
        return list(self._by_id.values())


class CompanyDedup:
    """
    Deduplicates CompanyNode objects.
    """

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._by_id:    Dict[str, CompanyNode] = {}
        self._by_clutch: Dict[str, str]        = {}
        self._by_xurl:   Dict[str, str]        = {}
        self._name_index: List[Tuple[str, str]] = []
        _init_dedup_table(conn)

    def add(self, node: CompanyNode) -> Tuple[CompanyNode, bool]:
        if node.id in self._by_id:
            existing = self._by_id[node.id]
            _merge_company(existing, node)
            return existing, True

        if node.clutch_url:
            u = node.clutch_url.lower()
            if u in self._by_clutch:
                existing = self._by_id[self._by_clutch[u]]
                _log_dedup(self._conn, "Company", existing.id, node.id, 1.0, "clutch_url")
                _merge_company(existing, node)
                return existing, True

        if node.x_url:
            u = node.x_url.lower()
            if u in self._by_xurl:
                existing = self._by_id[self._by_xurl[u]]
                _log_dedup(self._conn, "Company", existing.id, node.id, 1.0, "x_url")
                _merge_company(existing, node)
                return existing, True

        name_norm = clean_text(node.name)
        if name_norm:
            for existing_name, existing_id in self._name_index:
                sim = jaro_winkler_sim(name_norm, existing_name)
                if sim >= DEDUP_COMPANY_THRESHOLD:
                    existing = self._by_id[existing_id]
                    _log_dedup(self._conn, "Company", existing_id, node.id, sim, "name_fuzzy")
                    _merge_company(existing, node)
                    return existing, True

        self._by_id[node.id] = node
        if node.clutch_url:
            self._by_clutch[node.clutch_url.lower()] = node.id
        if node.x_url:
            self._by_xurl[node.x_url.lower()] = node.id
        if name_norm:
            self._name_index.append((name_norm, node.id))

        return node, False

    def all_nodes(self) -> List[CompanyNode]:
        return list(self._by_id.values())


class PublisherDedup:
    """Deduplicates PublisherNode by site_url or name."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._by_id:   Dict[str, PublisherNode] = {}
        self._by_url:  Dict[str, str]            = {}
        self._name_index: List[Tuple[str, str]]  = []
        _init_dedup_table(conn)

    def add(self, node: PublisherNode) -> Tuple[PublisherNode, bool]:
        if node.id in self._by_id:
            existing = self._by_id[node.id]
            _merge_publisher(existing, node)
            return existing, True

        if node.site_url:
            u = node.site_url.lower()
            if u in self._by_url:
                existing = self._by_id[self._by_url[u]]
                _log_dedup(self._conn, "Publisher", existing.id, node.id, 1.0, "site_url")
                _merge_publisher(existing, node)
                return existing, True

        name_norm = clean_text(node.name)
        if name_norm:
            for existing_name, existing_id in self._name_index:
                sim = jaro_winkler_sim(name_norm, existing_name)
                if sim >= 0.90:
                    existing = self._by_id[existing_id]
                    _log_dedup(self._conn, "Publisher", existing_id, node.id, sim, "name_fuzzy")
                    _merge_publisher(existing, node)
                    return existing, True

        self._by_id[node.id] = node
        if node.site_url:
            self._by_url[node.site_url.lower()] = node.id
        if name_norm:
            self._name_index.append((name_norm, node.id))

        return node, False

    def all_nodes(self) -> List[PublisherNode]:
        return list(self._by_id.values())


class CommunityDedup:
    """Deduplicates CommunityNode by url or name+platform."""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._by_id:  Dict[str, CommunityNode] = {}
        self._by_url: Dict[str, str]           = {}
        self._name_index: List[Tuple[str, str]] = []
        _init_dedup_table(conn)

    def add(self, node: CommunityNode) -> Tuple[CommunityNode, bool]:
        if node.id in self._by_id:
            existing = self._by_id[node.id]
            _merge_community(existing, node)
            return existing, True

        if node.url:
            u = node.url.lower()
            if u in self._by_url:
                existing = self._by_id[self._by_url[u]]
                _log_dedup(self._conn, "Community", existing.id, node.id, 1.0, "url")
                _merge_community(existing, node)
                return existing, True

        name_norm = clean_text(node.name)
        if name_norm:
            for existing_name, existing_id in self._name_index:
                sim = jaro_winkler_sim(name_norm, existing_name)
                if sim >= 0.90:
                    existing = self._by_id[existing_id]
                    _log_dedup(self._conn, "Community", existing_id, node.id, sim, "name_fuzzy")
                    _merge_community(existing, node)
                    return existing, True

        self._by_id[node.id] = node
        if node.url:
            self._by_url[node.url.lower()] = node.id
        if name_norm:
            self._name_index.append((name_norm, node.id))

        return node, False

    def all_nodes(self) -> List[CommunityNode]:
        return list(self._by_id.values())


# ── Merge helpers ─────────────────────────────────────────────────────────────

def _merge_person(existing: PersonNode, incoming: PersonNode) -> None:
    """Merge incoming fields into existing node (non-destructive)."""
    for src in incoming.source:
        if src not in existing.source:
            existing.source.append(src)
    for lst in incoming.scoble_lists:
        if lst not in existing.scoble_lists:
            existing.scoble_lists.append(lst)
    existing.x_handle    = existing.x_handle    or incoming.x_handle
    existing.x_url       = existing.x_url       or incoming.x_url
    existing.email       = existing.email        or incoming.email
    existing.linkedin_url= existing.linkedin_url or incoming.linkedin_url
    existing.bio_raw     = existing.bio_raw      or incoming.bio_raw
    existing.location    = existing.location     or incoming.location
    existing.fb_followers= existing.fb_followers or incoming.fb_followers
    existing.tw_followers= existing.tw_followers or incoming.tw_followers
    existing.ig_followers= existing.ig_followers or incoming.ig_followers
    existing.skool_dm_url= existing.skool_dm_url or incoming.skool_dm_url
    existing.skool_last_msg = existing.skool_last_msg or incoming.skool_last_msg
    # Take highest warmth
    existing.warmth_score = max(existing.warmth_score, incoming.warmth_score)
    # Recompute confidence with updated source count
    existing.confidence_score = compute_confidence(
        len(existing.source),
        [existing.name, existing.email, existing.x_handle,
         existing.linkedin_url, existing.location, existing.bio_raw],
    )


def _merge_company(existing: CompanyNode, incoming: CompanyNode) -> None:
    for src in incoming.source:
        if src not in existing.source:
            existing.source.append(src)
    existing.clutch_url      = existing.clutch_url      or incoming.clutch_url
    existing.x_url           = existing.x_url           or incoming.x_url
    existing.site_url        = existing.site_url        or incoming.site_url
    existing.location        = existing.location        or incoming.location
    existing.min_project_size= existing.min_project_size or incoming.min_project_size
    existing.hourly_rate     = existing.hourly_rate      or incoming.hourly_rate
    existing.team_size       = existing.team_size        or incoming.team_size
    existing.description     = existing.description      or incoming.description
    existing.scoble_category = existing.scoble_category  or incoming.scoble_category
    existing.clutch_category = existing.clutch_category  or incoming.clutch_category
    for s in incoming.services_raw:
        if s not in existing.services_raw:
            existing.services_raw.append(s)
    existing.primary_service = existing.primary_service or incoming.primary_service
    existing.confidence_score = compute_confidence(
        len(existing.source),
        [existing.name, existing.clutch_url, existing.location, existing.description],
    )


def _merge_publisher(existing: PublisherNode, incoming: PublisherNode) -> None:
    existing.description   = existing.description   or incoming.description
    existing.location      = existing.location      or incoming.location
    existing.domain_authority = existing.domain_authority or incoming.domain_authority
    existing.fb_followers  = existing.fb_followers  or incoming.fb_followers
    existing.tw_followers  = existing.tw_followers  or incoming.tw_followers
    existing.ig_followers  = existing.ig_followers  or incoming.ig_followers
    existing.email         = existing.email         or incoming.email
    existing.confidence_score = compute_confidence(
        1,
        [existing.name, existing.site_url, existing.description, existing.location],
    )


def _merge_community(existing: CommunityNode, incoming: CommunityNode) -> None:
    existing.visibility    = existing.visibility   or incoming.visibility
    existing.member_count  = existing.member_count or incoming.member_count
    existing.daily_posts   = existing.daily_posts  or incoming.daily_posts
    existing.category      = existing.category     or incoming.category
    existing.topic_cluster = existing.topic_cluster or incoming.topic_cluster
    existing.confidence_score = compute_confidence(
        1,
        [existing.name, existing.url, existing.category, existing.member_count],
    )


def _log_dedup(conn: sqlite3.Connection, node_type: str,
               kept_id: str, merged_id: str,
               similarity: float, match_field: str) -> None:
    try:
        conn.execute(
            "INSERT INTO dedup_log (node_type, kept_id, merged_id, similarity, match_field) "
            "VALUES (?, ?, ?, ?, ?)",
            (node_type, kept_id, merged_id, similarity, match_field)
        )
        conn.commit()
    except Exception:
        pass
