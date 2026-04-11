"""
facebook.py — Normalizer for Facebook Groups files.

Three schema types:
  1. CSS-scraped CSVs (FacebookTrucking.csv, bisnesses-1.csv, businessOwners.csv,
     business owners.csv):
       col0=group_url, col1=group_name, col2=clean_url,
       col3=metadata("Public · 36K members · 50+ posts a day"), col4="Join"
  2. facebook.csv (different layout):
       col0=url, col1=img(skip), col2=name, col3=metadata
  3. XLSX files:
       oracle_facebook_groups.xlsx: #, Group Name, Facebook URL, Category,
                                    Visibility, Members, Source Search
       SQL_Facebook_Groups_Cleaned.xlsx: #, Group Name, Category, Members,
                                          Privacy, Daily Posts, Facebook URL
       facebook-3.xlsx: treat as oracle-style if same columns, else generic
"""
from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Iterator, Optional, List

import openpyxl

from backend.models import CommunityNode
from backend.utils import (
    clean_text, clean_url, community_id, parse_member_count, parse_daily_posts,
    parse_visibility, compute_confidence, infer_topic_cluster, utcnow_str,
    safe_get,
)

csv.field_size_limit(10_000_000)


def _build_community(
    name: str,
    url: Optional[str],
    visibility: Optional[str],
    member_count: Optional[int],
    daily_posts: Optional[int],
    category: Optional[str],
    source: str,
    now: str,
) -> Optional[CommunityNode]:
    if not name and not url:
        return None

    c_id  = community_id(url=url or None, name=name, platform="facebook")
    topic = infer_topic_cluster(f"{name} {category or ''}")
    fields = [name, url, visibility, member_count, daily_posts, category]
    conf   = compute_confidence(1, fields)

    return CommunityNode(
        id=c_id,
        name=name or "",
        platform="facebook",
        source=source,
        url=url or None,
        visibility=visibility,
        member_count=member_count,
        daily_posts=daily_posts,
        category=category,
        topic_cluster=topic,
        joined=False,
        confidence_score=conf,
        ingested_at=now,
    )


def _parse_css_csv_row(row: List, source: str, now: str) -> Optional[CommunityNode]:
    """
    CSS-scraped CSV: col0=group_url, col1=group_name, col2=clean_url,
    col3=metadata, col4='Join'
    """
    group_url = clean_url(safe_get(row, 0))
    name      = clean_text(safe_get(row, 1))
    clean_u   = clean_url(safe_get(row, 2))
    metadata  = clean_text(safe_get(row, 3))

    url = clean_u or group_url

    members    = parse_member_count(metadata)
    daily      = parse_daily_posts(metadata)
    visibility = parse_visibility(metadata)

    return _build_community(name, url, visibility, members, daily, None, source, now)


def _parse_alt_csv_row(row: List, source: str, now: str) -> Optional[CommunityNode]:
    """
    facebook.csv: col0=url, col1=img(skip), col2=name, col3=metadata
    """
    url      = clean_url(safe_get(row, 0))
    name     = clean_text(safe_get(row, 2))
    metadata = clean_text(safe_get(row, 3))

    members    = parse_member_count(metadata)
    daily      = parse_daily_posts(metadata)
    visibility = parse_visibility(metadata)

    return _build_community(name, url, visibility, members, daily, None, source, now)


def parse_facebook_csv(filepath: str) -> Iterator[CommunityNode]:
    """Detect schema and parse a Facebook CSV file."""
    filename = Path(filepath).name
    is_alt   = filename.lower() == "facebook.csv"
    source   = "facebook"
    now      = utcnow_str()

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        try:
            _header = next(reader)
        except StopIteration:
            return

        for row in reader:
            if not row or all(c.strip() == "" for c in row):
                continue
            try:
                if is_alt:
                    node = _parse_alt_csv_row(row, source, now)
                else:
                    node = _parse_css_csv_row(row, source, now)
                if node:
                    yield node
            except Exception:
                continue


def parse_facebook_xlsx(filepath: str) -> Iterator[CommunityNode]:
    """Parse structured XLSX Facebook group files."""
    filename = Path(filepath).name.lower()
    source   = "facebook"
    now      = utcnow_str()

    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        wb.close()
        return

    # Skip header row
    header = [str(c).strip().lower() if c else "" for c in rows[0]]
    data_rows = rows[1:]

    if "oracle" in filename:
        # oracle_facebook_groups.xlsx:
        # #, Group Name, Facebook URL, Category, Visibility, Members, Source Search
        for row in data_rows:
            try:
                name       = clean_text(row[1] if len(row) > 1 else None)
                url        = clean_url(row[2] if len(row) > 2 else None)
                category   = clean_text(row[3] if len(row) > 3 else None)
                visibility = clean_text(row[4] if len(row) > 4 else None)
                members    = parse_member_count(row[5] if len(row) > 5 else None)
                node = _build_community(
                    name, url, visibility or None, members, None,
                    category or None, source, now
                )
                if node:
                    yield node
            except Exception:
                continue

    elif "sql_facebook" in filename or "cleaned" in filename:
        # SQL_Facebook_Groups_Cleaned.xlsx:
        # #, Group Name, Category, Members, Privacy, Daily Posts, Facebook URL
        for row in data_rows:
            try:
                name       = clean_text(row[1] if len(row) > 1 else None)
                category   = clean_text(row[2] if len(row) > 2 else None)
                members    = parse_member_count(row[3] if len(row) > 3 else None)
                privacy    = clean_text(row[4] if len(row) > 4 else None)
                daily      = parse_daily_posts(str(row[5]) if len(row) > 5 and row[5] else None)
                url        = clean_url(row[6] if len(row) > 6 else None)
                node = _build_community(
                    name, url, privacy or None, members, daily,
                    category or None, source, now
                )
                if node:
                    yield node
            except Exception:
                continue

    else:
        # Generic XLSX — try oracle-style first (most common)
        for row in data_rows:
            try:
                name     = clean_text(row[1] if len(row) > 1 else None)
                url      = clean_url(row[2] if len(row) > 2 else None)
                category = clean_text(row[3] if len(row) > 3 else None)
                members  = parse_member_count(row[5] if len(row) > 5 else None)
                node = _build_community(
                    name, url, None, members, None,
                    category or None, source, now
                )
                if node:
                    yield node
            except Exception:
                continue

    wb.close()
