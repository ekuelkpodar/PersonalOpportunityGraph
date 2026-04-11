"""
feedspot.py — Normalizer for Feedspot blog CSV files.

Schema variants:
  V1 (blogs_1-10, 13):  Id, Category, Site URL, Site Name, Author Name,
      Primary Email, Author Designation, Author Twitter, Author Linkedin,
      Notes, Location, Facebook Followers, Facebook Url, Twitter Followers,
      Twitter Url, Instagram Followers, Instagram URL, Description
  V2 (blogs_11-12, 14-16): same + Domain Authority col, different field names.
"""
from __future__ import annotations

import csv
import os
import re
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

from backend.config import FEEDSPOT_V1_INDICES, FEEDSPOT_V2_INDICES
from backend.models import PersonNode, PublisherNode, EdgeRecord
from backend.utils import (
    clean_text, clean_url, clean_handle, make_id, person_id, publisher_id,
    parse_follower_count, parse_domain_authority, compute_reach_score,
    infer_topic_cluster, compute_confidence, compute_warmth, utcnow_str,
    safe_str, safe_get,
)

csv.field_size_limit(10_000_000)


def _get_schema_version(filepath: str) -> int:
    """Determine V1 or V2 by examining the filename index."""
    stem = Path(filepath).stem  # e.g. 'blogs_11'
    m = re.search(r"_(\d+)$", stem)
    if m:
        idx = int(m.group(1))
        if idx in FEEDSPOT_V1_INDICES:
            return 1
        if idx in FEEDSPOT_V2_INDICES:
            return 2
    # fallback: read header
    return 1


def _parse_row_v1(row: List, category: str, now: str
                  ) -> Tuple[Optional[PublisherNode], Optional[PersonNode], Optional[EdgeRecord]]:
    """Parse a V1 row. Columns are positional after header skip."""
    # col: 0=Id, 1=Category, 2=Site URL, 3=Site Name, 4=Author Name,
    #      5=Primary Email, 6=Author Designation, 7=Author Twitter,
    #      8=Author Linkedin, 9=Notes, 10=Location, 11=FB Followers,
    #      12=FB Url, 13=TW Followers, 14=TW Url, 15=IG Followers,
    #      16=IG URL, 17=Description

    site_url    = clean_url(safe_get(row, 2))
    site_name   = clean_text(safe_get(row, 3))
    author_name = clean_text(safe_get(row, 4))
    email       = clean_text(safe_get(row, 5)).lower() if safe_get(row, 5) else None
    designation = clean_text(safe_get(row, 6))
    tw_handle   = clean_handle(safe_get(row, 7))
    linkedin_url= clean_url(safe_get(row, 8))
    location    = clean_text(safe_get(row, 10))
    fb_followers= parse_follower_count(safe_get(row, 11))
    fb_url      = clean_url(safe_get(row, 12))
    tw_followers= parse_follower_count(safe_get(row, 13))
    tw_url      = clean_url(safe_get(row, 14))
    ig_followers= parse_follower_count(safe_get(row, 15))
    ig_url      = clean_url(safe_get(row, 16))
    description = clean_text(safe_get(row, 17))

    if not site_name and not site_url:
        return None, None, None

    pub_id = publisher_id(site_url or None, site_name or None)
    reach  = compute_reach_score(fb_followers, tw_followers, ig_followers)
    text_for_cluster = f"{site_name} {description} {category}"
    topic  = infer_topic_cluster(text_for_cluster)

    pub_fields = [site_url, site_name, description, location, fb_followers,
                  tw_followers, ig_followers, email]
    pub_conf   = compute_confidence(1, pub_fields)

    publisher = PublisherNode(
        id=pub_id,
        name=site_name or site_url,
        source="feedspot",
        site_url=site_url or None,
        category=category,
        category_type=_infer_category_type(site_name, description),
        topic_cluster=topic,
        description=description or None,
        location=location or None,
        fb_followers=fb_followers,
        tw_followers=tw_followers,
        ig_followers=ig_followers,
        reach_score=reach,
        email=email or None,
        fb_url=fb_url or None,
        tw_url=tw_url or None,
        ig_url=ig_url or None,
        confidence_score=pub_conf,
        ingested_at=now,
    )

    person: Optional[PersonNode] = None
    edge:   Optional[EdgeRecord] = None

    if author_name:
        p_id = person_id(
            x_handle=tw_handle or None,
            email=email if not email else None,
            name=author_name,
            src="feedspot"
        )
        per_fields = [author_name, email, tw_handle, linkedin_url, location, designation]
        per_conf   = compute_confidence(1, per_fields)

        person = PersonNode(
            id=p_id,
            name=author_name,
            source=["feedspot"],
            x_handle=tw_handle or None,
            x_url=f"https://x.com/{tw_handle}" if tw_handle else None,
            email=email or None,
            linkedin_url=linkedin_url or None,
            location=location or None,
            warmth_score=compute_warmth(["feedspot"]),
            confidence_score=per_conf,
            ingested_at=now,
            topic_cluster=topic,
        )

        edge = EdgeRecord(
            source_id=pub_id,
            target_id=p_id,
            rel_type="HAS_AUTHOR",
            weight=1.0,
        )

    return publisher, person, edge


def _parse_row_v2(row: List, category: str, now: str
                  ) -> Tuple[Optional[PublisherNode], Optional[PersonNode], Optional[EdgeRecord]]:
    """Parse a V2 row which has an extra Domain Authority column."""
    # V2 header: Sr.no, Category, Site URL, Site Name, Author Name,
    #   Primary Email, Designation, Twitter, Linkedin, Notes, Location,
    #   Domain Authority, Facebook Followers, Facebook Url, Twitter Followers,
    #   Twitter Url, Instagram Followers, Instagram URL, Description

    site_url     = clean_url(safe_get(row, 2))
    site_name    = clean_text(safe_get(row, 3))
    author_name  = clean_text(safe_get(row, 4))
    email        = clean_text(safe_get(row, 5)).lower() if safe_get(row, 5) else None
    designation  = clean_text(safe_get(row, 6))
    tw_handle    = clean_handle(safe_get(row, 7))
    linkedin_url = clean_url(safe_get(row, 8))
    location     = clean_text(safe_get(row, 10))
    domain_auth  = parse_domain_authority(safe_get(row, 11))
    fb_followers = parse_follower_count(safe_get(row, 12))
    fb_url       = clean_url(safe_get(row, 13))
    tw_followers = parse_follower_count(safe_get(row, 14))
    tw_url       = clean_url(safe_get(row, 15))
    ig_followers = parse_follower_count(safe_get(row, 16))
    ig_url       = clean_url(safe_get(row, 17))
    description  = clean_text(safe_get(row, 18))

    if not site_name and not site_url:
        return None, None, None

    pub_id = publisher_id(site_url or None, site_name or None)
    reach  = compute_reach_score(fb_followers, tw_followers, ig_followers)
    text_for_cluster = f"{site_name} {description} {category}"
    topic  = infer_topic_cluster(text_for_cluster)

    pub_fields = [site_url, site_name, description, location, fb_followers,
                  tw_followers, ig_followers, email, domain_auth]
    pub_conf   = compute_confidence(1, pub_fields)

    publisher = PublisherNode(
        id=pub_id,
        name=site_name or site_url,
        source="feedspot",
        site_url=site_url or None,
        category=category,
        category_type=_infer_category_type(site_name, description),
        topic_cluster=topic,
        description=description or None,
        location=location or None,
        domain_authority=domain_auth,
        fb_followers=fb_followers,
        tw_followers=tw_followers,
        ig_followers=ig_followers,
        reach_score=reach,
        email=email or None,
        fb_url=fb_url or None,
        tw_url=tw_url or None,
        ig_url=ig_url or None,
        confidence_score=pub_conf,
        ingested_at=now,
    )

    person: Optional[PersonNode] = None
    edge:   Optional[EdgeRecord] = None

    if author_name:
        p_id = person_id(
            x_handle=tw_handle or None,
            email=email if not email else None,
            name=author_name,
            src="feedspot"
        )
        per_fields = [author_name, email, tw_handle, linkedin_url, location, designation]
        per_conf   = compute_confidence(1, per_fields)

        person = PersonNode(
            id=p_id,
            name=author_name,
            source=["feedspot"],
            x_handle=tw_handle or None,
            x_url=f"https://x.com/{tw_handle}" if tw_handle else None,
            email=email or None,
            linkedin_url=linkedin_url or None,
            location=location or None,
            warmth_score=compute_warmth(["feedspot"]),
            confidence_score=per_conf,
            ingested_at=now,
            topic_cluster=topic,
        )

        edge = EdgeRecord(
            source_id=pub_id,
            target_id=p_id,
            rel_type="HAS_AUTHOR",
            weight=1.0,
        )

    return publisher, person, edge


def _infer_category_type(name: str, description: str) -> str:
    """Infer Blog/Podcast/YouTube/Magazine from name and description."""
    combined = (name + " " + description).lower()
    if "podcast" in combined:
        return "Podcast"
    if "youtube" in combined or "channel" in combined:
        return "YouTube"
    if "magazine" in combined or "journal" in combined:
        return "Magazine"
    return "Blog"


def parse_feedspot_file(filepath: str) -> Iterator[Tuple[PublisherNode, Optional[PersonNode], Optional[EdgeRecord]]]:
    """
    Yield (publisher, person_or_None, edge_or_None) for each valid row.
    """
    version = _get_schema_version(filepath)
    now = utcnow_str()

    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return

        # Infer category from file contents (first column header usually 'Category')
        category_col_idx = 1  # Always column 1 in both schemas

        for row in reader:
            if not row or all(c.strip() == "" for c in row):
                continue
            category = clean_text(safe_get(row, category_col_idx)) or "General"

            try:
                if version == 2:
                    pub, person, edge = _parse_row_v2(row, category, now)
                else:
                    pub, person, edge = _parse_row_v1(row, category, now)
            except Exception:
                continue

            if pub:
                yield pub, person, edge
