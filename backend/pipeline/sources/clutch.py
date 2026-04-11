"""
clutch.py — Normalizer for Clutch agency CSV files.

Schema (all subdirectories):
  col0=clutch_url, col1=logo(skip), col2=agency_name, col3=url_dup(skip),
  col4=min_project_size, col5=hourly_rate, col6=team_size, col7=location,
  cols8-14=service_percentages ("25% SEO", "15% PPC" etc), col15=description

Category is auto-detected from subdirectory name.
"""
from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Iterator, List, Optional

from backend.models import CompanyNode
from backend.utils import (
    clean_text, clean_url, company_id, parse_services_list,
    normalize_project_size, normalize_hourly_rate,
    compute_confidence, infer_topic_cluster, utcnow_str, safe_get,
    clutch_category_from_subdir,
)

csv.field_size_limit(10_000_000)


def _category_from_filepath(filepath: str) -> str:
    """Infer Clutch category from the immediate parent directory name."""
    parent = Path(filepath).parent.name
    return clutch_category_from_subdir(parent)


def _parse_row(row: List, category: str, now: str) -> Optional[CompanyNode]:
    """Parse a single Clutch CSV row into a CompanyNode."""
    clutch_url   = clean_url(safe_get(row, 0))
    # col1 = logo (skip)
    agency_name  = clean_text(safe_get(row, 2))
    # col3 = url_dup (skip)
    min_proj     = normalize_project_size(safe_get(row, 4))
    hourly       = normalize_hourly_rate(safe_get(row, 5))
    team_size    = clean_text(safe_get(row, 6))
    location     = clean_text(safe_get(row, 7))

    # Service percentage columns: 8-14
    service_cols = [safe_get(row, i) for i in range(8, 15)]
    services_raw, primary_service = parse_services_list(service_cols)

    description  = clean_text(safe_get(row, 15))

    if not agency_name and not clutch_url:
        return None

    c_id  = company_id(clutch_url=clutch_url or None, name=agency_name)
    topic = infer_topic_cluster(
        f"{agency_name} {description} {category} {' '.join(services_raw)}"
    )
    fields = [clutch_url, agency_name, min_proj, hourly, team_size,
              location, description] + services_raw
    conf   = compute_confidence(1, fields)

    return CompanyNode(
        id=c_id,
        name=agency_name or "",
        source=["clutch"],
        clutch_url=clutch_url or None,
        location=location or None,
        min_project_size=min_proj,
        hourly_rate=hourly,
        team_size=team_size or None,
        services_raw=services_raw,
        primary_service=primary_service,
        description=description or None,
        clutch_category=category,
        confidence_score=conf,
        ingested_at=now,
        topic_cluster=topic,
    )


def parse_clutch_file(filepath: str) -> Iterator[CompanyNode]:
    """Yield CompanyNode for each valid row in a Clutch CSV file."""
    category = _category_from_filepath(filepath)
    now      = utcnow_str()

    path = Path(filepath)
    with path.open("r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        try:
            _header = next(reader)
        except StopIteration:
            return

        for row in reader:
            if not row or all(c.strip() == "" for c in row):
                continue
            try:
                node = _parse_row(row, category, now)
                if node:
                    yield node
            except Exception:
                continue
