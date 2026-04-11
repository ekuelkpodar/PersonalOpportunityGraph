"""
xlist.py — Normalizer for Scoble XList CSV files.

Column structure (all files):
  col0=x_profile_url, col1=avatar_img(skip), col2=display_name, col3=@handle,
  col4="Block"(skip), col5="Follow"(skip), col6="Click to Follow..."(skip),
  col7=bio_start, col8-N=bio_overflow+@org_mentions+linked_urls

File names with '#' are handled via pathlib.Path — no string splitting on '#'.
"""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

from backend.models import PersonNode, CompanyNode, EdgeRecord
from backend.utils import (
    clean_text, clean_url, clean_handle, concat_bio_cols,
    person_id, company_id, extract_org_mentions, extract_urls,
    compute_confidence, compute_warmth, infer_topic_cluster, utcnow_str,
    safe_get,
)

csv.field_size_limit(10_000_000)

# Files that contain Company nodes (instead of Person)
_COMPANY_FILENAMES = {
    "AIcompanies-1.csv",
    "AIcompanies-2.csv",
    "AI Developer Tools.csv",
    "AI Enterprise or Work.csv",
    "Infrastructure.csv",
    "VC Firms.csv",
}


def _scoble_list_name(filepath: str) -> str:
    """Return the Scoble list name from the filename (without extension)."""
    return Path(filepath).stem


def _is_company_file(filepath: str) -> bool:
    return Path(filepath).name in _COMPANY_FILENAMES


def _parse_row_as_person(row: List, list_name: str, now: str) -> Optional[PersonNode]:
    x_url    = clean_url(safe_get(row, 0))
    name     = clean_text(safe_get(row, 2))
    handle   = clean_handle(safe_get(row, 3))

    # Bio: col7 is bio_start; cols 8+ are overflow + @mentions + urls
    bio_parts = []
    if safe_get(row, 7):
        bio_parts.append(clean_text(safe_get(row, 7)))
    overflow = concat_bio_cols(row, 8)
    if overflow:
        bio_parts.append(overflow)
    bio_raw = " ".join(bio_parts).strip()

    if not name and not handle and not x_url:
        return None

    # Derive x_handle from handle col or profile URL
    if not handle and x_url:
        parts = x_url.rstrip("/").split("/")
        if parts:
            candidate = parts[-1].lstrip("@")
            if candidate and candidate not in ("intent", "follow"):
                handle = candidate

    p_id = person_id(x_handle=handle or None, name=name, src="xlist")

    fields = [name, handle, x_url, bio_raw]
    conf   = compute_confidence(1, fields)

    orgs   = extract_org_mentions(bio_raw)
    urls   = extract_urls(bio_raw)
    topic  = infer_topic_cluster(bio_raw)

    node = PersonNode(
        id=p_id,
        name=name or handle or "",
        source=["scoble"],
        x_handle=handle or None,
        x_url=x_url or None,
        bio_raw=bio_raw or None,
        scoble_lists=[list_name],
        warmth_score=compute_warmth(["scoble"]),
        confidence_score=conf,
        ingested_at=now,
        topic_cluster=topic,
    )

    return node


def _parse_row_as_company(row: List, list_name: str, now: str) -> Optional[Tuple[CompanyNode, List[EdgeRecord]]]:
    x_url  = clean_url(safe_get(row, 0))
    name   = clean_text(safe_get(row, 2))
    handle = clean_handle(safe_get(row, 3))

    bio_parts = []
    if safe_get(row, 7):
        bio_parts.append(clean_text(safe_get(row, 7)))
    overflow = concat_bio_cols(row, 8)
    if overflow:
        bio_parts.append(overflow)
    bio_raw = " ".join(bio_parts).strip()

    if not name and not x_url:
        return None

    c_id  = company_id(x_url=x_url or None, name=name)
    topic = infer_topic_cluster(bio_raw)
    fields = [name, x_url, handle, bio_raw]
    conf   = compute_confidence(1, fields)

    company = CompanyNode(
        id=c_id,
        name=name or handle or "",
        source=["scoble"],
        x_url=x_url or None,
        description=bio_raw or None,
        scoble_category=list_name,
        confidence_score=conf,
        ingested_at=now,
        topic_cluster=topic,
    )

    return company, []


def parse_xlist_file(
    filepath: str,
) -> Iterator[Tuple[str, object, List[EdgeRecord]]]:
    """
    Yield (node_type, node, edges) for each valid row.
    node_type is 'person' or 'company'.
    """
    is_company = _is_company_file(filepath)
    list_name  = _scoble_list_name(filepath)
    now        = utcnow_str()

    # Use pathlib to open — safe with '#' in filenames
    path = Path(filepath)
    with path.open("r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        try:
            _header = next(reader)  # skip header row
        except StopIteration:
            return

        for row in reader:
            if not row or all(c.strip() == "" for c in row):
                continue
            try:
                if is_company:
                    result = _parse_row_as_company(row, list_name, now)
                    if result:
                        company, edges = result
                        yield "company", company, edges
                else:
                    node = _parse_row_as_person(row, list_name, now)
                    if node:
                        yield "person", node, []
            except Exception:
                continue
