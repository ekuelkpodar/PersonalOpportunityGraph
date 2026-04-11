"""
skool.py — Normalizer for Skool CSV files.

SkoolCommunities.csv — 749 communities the ego has joined.
  Expected columns (inferred dynamically): community name, url, category,
  member count, and any metadata columns.

SkoolDM.csv — 317 direct messages / contacts.
  Expected columns: person name, skool profile url, last message, date, etc.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

from backend.models import CommunityNode, PersonNode, EdgeRecord
from backend.config import EGO_ID
from backend.utils import (
    clean_text, clean_url, clean_handle, community_id, person_id,
    parse_member_count, compute_confidence, infer_topic_cluster,
    compute_warmth, utcnow_str, safe_get,
)

csv.field_size_limit(10_000_000)


def _detect_community_columns(header: List[str]) -> dict:
    """
    Map header names to column indices for SkoolCommunities.csv.
    Returns a dict of semantic_key → col_index.
    """
    header_lower = [h.strip().lower() for h in header]
    mapping = {}

    name_candidates   = ["name", "community", "community name", "title", "group"]
    url_candidates    = ["url", "link", "profile url", "community url"]
    cat_candidates    = ["category", "type", "topic"]
    member_candidates = ["members", "member count", "size"]

    def find(candidates):
        for c in candidates:
            for i, h in enumerate(header_lower):
                if c in h:
                    return i
        return None

    mapping["name"]    = find(name_candidates)
    mapping["url"]     = find(url_candidates)
    mapping["cat"]     = find(cat_candidates)
    mapping["members"] = find(member_candidates)

    # Fallback: positional guesses if headers are not descriptive
    if mapping["name"] is None:
        mapping["name"] = 0
    if mapping["url"] is None:
        mapping["url"] = 1 if len(header_lower) > 1 else None

    return mapping


def _detect_dm_columns(header: List[str]) -> dict:
    """Map header names for SkoolDM.csv."""
    header_lower = [h.strip().lower() for h in header]
    mapping = {}

    name_candidates = ["name", "person", "contact", "user", "display name"]
    url_candidates  = ["url", "link", "profile", "skool url"]
    msg_candidates  = ["message", "last message", "msg", "note", "last msg"]

    def find(candidates):
        for c in candidates:
            for i, h in enumerate(header_lower):
                if c in h:
                    return i
        return None

    mapping["name"] = find(name_candidates)
    mapping["url"]  = find(url_candidates)
    mapping["msg"]  = find(msg_candidates)

    if mapping["name"] is None:
        mapping["name"] = 0
    if mapping["url"] is None:
        mapping["url"] = 1 if len(header_lower) > 1 else None

    return mapping


def parse_skool_communities(filepath: str) -> Iterator[Tuple[CommunityNode, EdgeRecord]]:
    """
    Yield (CommunityNode, MEMBER_OF edge from ego) for each Skool community.
    """
    now = utcnow_str()

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return

        cols = _detect_community_columns(header)

        for row in reader:
            if not row or all(c.strip() == "" for c in row):
                continue
            try:
                name     = clean_text(safe_get(row, cols["name"])) if cols["name"] is not None else ""
                url      = clean_url(safe_get(row, cols["url"])) if cols["url"] is not None else ""
                category = clean_text(safe_get(row, cols["cat"])) if cols["cat"] is not None else ""
                raw_members = safe_get(row, cols["members"]) if cols["members"] is not None else None
                members  = parse_member_count(raw_members)

                if not name and not url:
                    continue

                c_id  = community_id(url=url or None, name=name, platform="skool")
                topic = infer_topic_cluster(f"{name} {category}")
                conf  = compute_confidence(1, [name, url, category, members])

                community = CommunityNode(
                    id=c_id,
                    name=name or "",
                    platform="skool",
                    source="skool",
                    url=url or None,
                    member_count=members,
                    category=category or None,
                    topic_cluster=topic,
                    joined=True,
                    confidence_score=conf,
                    ingested_at=now,
                )

                edge = EdgeRecord(
                    source_id=EGO_ID,
                    target_id=c_id,
                    rel_type="MEMBER_OF",
                    weight=1.0,
                )

                yield community, edge

            except Exception:
                continue


def parse_skool_dms(filepath: str) -> Iterator[Tuple[PersonNode, EdgeRecord]]:
    """
    Yield (PersonNode, WARM_CONTACT edge from ego) for each Skool DM contact.
    """
    now = utcnow_str()

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return

        cols = _detect_dm_columns(header)

        for row in reader:
            if not row or all(c.strip() == "" for c in row):
                continue
            try:
                name      = clean_text(safe_get(row, cols["name"])) if cols["name"] is not None else ""
                url       = clean_url(safe_get(row, cols["url"])) if cols["url"] is not None else ""
                last_msg  = clean_text(safe_get(row, cols["msg"])) if cols["msg"] is not None else ""

                if not name and not url:
                    continue

                p_id = person_id(name=name, src="skool_dm")
                conf = compute_confidence(1, [name, url, last_msg], has_conflict=False)
                topic = infer_topic_cluster(last_msg)

                person = PersonNode(
                    id=p_id,
                    name=name or "",
                    source=["skool_dm"],
                    skool_dm_url=url or None,
                    skool_last_msg=last_msg or None,
                    warmth_score=compute_warmth(["skool_dm"], is_skool_dm=True),
                    confidence_score=conf,
                    ingested_at=now,
                    topic_cluster=topic,
                )

                edge = EdgeRecord(
                    source_id=EGO_ID,
                    target_id=p_id,
                    rel_type="WARM_CONTACT",
                    weight=1.0,
                )

                yield person, edge

            except Exception:
                continue
