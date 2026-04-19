"""
utils.py — Shared utilities: ID generation, text cleaning, follower parsing,
topic clustering, confidence scoring, warmth scoring, member count parsing,
service percentage parsing.
"""
from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from backend.config import (
    TOPIC_CLUSTERS,
    WARMTH_SKOOL_DM,
    WARMTH_SKOOL,
    WARMTH_SCOBLE,
    WARMTH_FEEDSPOT,
    WARMTH_COLD,
    CONF_W_SOURCES,
    CONF_W_COMPLETENESS,
    CONF_W_AGREEMENT,
    CONF_MIN_SOURCES,
)


# ── ID Generation ─────────────────────────────────────────────────────────────

def make_id(key: str) -> str:
    """SHA-256 hash of a canonical key string, returned as hex."""
    normalized = key.strip().lower()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:32]


def person_id(x_handle: Optional[str] = None,
              email: Optional[str] = None,
              name: Optional[str] = None,
              src: Optional[str] = None) -> str:
    if x_handle:
        key = f"xhandle:{clean_handle(x_handle)}"
    elif email:
        key = f"email:{email.strip().lower()}"
    elif name and src:
        key = f"person:{clean_text(name)}:{src}"
    elif name:
        key = f"person:{clean_text(name)}"
    else:
        key = f"person:unknown:{make_id(str(datetime.now()))}"
    return make_id(key)


def company_id(clutch_url: Optional[str] = None,
               x_url: Optional[str] = None,
               name: Optional[str] = None) -> str:
    if clutch_url:
        key = f"clutch:{clutch_url.strip().lower()}"
    elif x_url:
        key = f"xurl:{x_url.strip().lower()}"
    elif name:
        key = f"company:{clean_text(name)}"
    else:
        key = f"company:unknown:{make_id(str(datetime.now()))}"
    return make_id(key)


def publisher_id(site_url: Optional[str] = None,
                 name: Optional[str] = None) -> str:
    if site_url:
        key = f"publisher:{site_url.strip().lower()}"
    elif name:
        key = f"publisher:{clean_text(name)}"
    else:
        key = f"publisher:unknown:{make_id(str(datetime.now()))}"
    return make_id(key)


def community_id(url: Optional[str] = None,
                 name: Optional[str] = None,
                 platform: Optional[str] = None) -> str:
    if url:
        key = f"community:{url.strip().lower()}"
    elif name and platform:
        key = f"community:{clean_text(name)}:{platform}"
    elif name:
        key = f"community:{clean_text(name)}"
    else:
        key = f"community:unknown:{make_id(str(datetime.now()))}"
    return make_id(key)


# ── Text Cleaning ────────────────────────────────────────────────────────────

def clean_text(text: Any) -> str:
    """Normalize to lowercase stripped string; handle non-string inputs."""
    if text is None:
        return ""
    s = str(text)
    s = unicodedata.normalize("NFKC", s)
    s = s.strip()
    return s


def clean_handle(handle: Any) -> str:
    """Normalize an @handle — strip @, lowercase, strip whitespace."""
    if handle is None:
        return ""
    s = str(handle).strip().lower()
    if s.startswith("@"):
        s = s[1:]
    return s


def clean_url(url: Any) -> str:
    """Strip trailing slashes, lowercase, remove utm params."""
    if not url:
        return ""
    s = str(url).strip().lower()
    s = re.sub(r"\?.*", "", s)   # drop query string
    s = s.rstrip("/")
    return s


def concat_bio_cols(row: List[Any], start_col: int) -> str:
    """Concatenate bio overflow columns into a single string."""
    parts = []
    for val in row[start_col:]:
        v = clean_text(val)
        if v and v not in ("nan", "none", ""):
            parts.append(v)
    return " ".join(parts)


def truncate(text: str, max_chars: int = 2000) -> str:
    """Truncate text to max_chars characters."""
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "…"


# ── Follower / Count Parsing ──────────────────────────────────────────────────

_SUFFIX_MAP = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}


def parse_follower_count(raw: Any) -> Optional[int]:
    """
    Parse follower counts like '36K', '1.2M', '5,423', '5000'.
    Returns None if unparseable.
    """
    if raw is None:
        return None
    s = str(raw).strip().lower().replace(",", "").replace(" ", "")
    if not s or s in ("nan", "none", "-", ""):
        return None
    m = re.match(r"^([\d.]+)([kmb])?$", s)
    if not m:
        return None
    try:
        num = float(m.group(1))
        suffix = m.group(2)
        if suffix:
            num *= _SUFFIX_MAP[suffix]
        return int(num)
    except (ValueError, KeyError):
        return None


def parse_member_count(raw: Any) -> Optional[int]:
    """
    Parse Facebook/Skool member counts from metadata strings like
    'Public · 36K members · 50+ posts a day' or plain numbers.
    """
    if raw is None:
        return None
    s = str(raw)
    m = re.search(r"([\d,.]+[KkMm]?)\s*members?", s, re.IGNORECASE)
    if m:
        return parse_follower_count(m.group(1))
    # try bare number
    s_stripped = re.sub(r"[^\d.KkMm]", "", s)
    return parse_follower_count(s_stripped) if s_stripped else None


def parse_daily_posts(raw: Any) -> Optional[int]:
    """Extract daily posts from metadata like '50+ posts a day'."""
    if raw is None:
        return None
    s = str(raw)
    m = re.search(r"([\d]+)\+?\s*posts?\s*a?\s*day", s, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def parse_visibility(raw: Any) -> Optional[str]:
    """Extract 'Public' or 'Private' from metadata."""
    if raw is None:
        return None
    s = str(raw).lower()
    if "public" in s:
        return "Public"
    if "private" in s or "closed" in s:
        return "Private"
    return None


# ── Service Percentage Parsing ────────────────────────────────────────────────

def parse_service_percent(raw: Any) -> Tuple[Optional[str], Optional[float]]:
    """
    Parse Clutch service strings like '25% SEO', '15% PPC'.
    Returns (service_label, weight_0_to_1) or (None, None).
    """
    if not raw:
        return None, None
    s = str(raw).strip()
    m = re.match(r"^(\d+(?:\.\d+)?)\s*%\s*(.+)$", s)
    if m:
        pct = float(m.group(1))
        label = m.group(2).strip()
        return label, pct / 100.0
    return None, None


def parse_services_list(cols: List[Any]) -> Tuple[List[str], Optional[str]]:
    """
    Parse cols 8-14 from a Clutch row into a services_raw list and primary_service.
    Returns (services_raw, primary_service).
    """
    services: List[str] = []
    best_label: Optional[str] = None
    best_pct: float = 0.0

    for col in cols:
        label, weight = parse_service_percent(col)
        if label and weight is not None:
            services.append(f"{int(weight*100)}% {label}")
            if weight > best_pct:
                best_pct = weight
                best_label = label

    return services, best_label


# ── Domain Authority Parsing ──────────────────────────────────────────────────

def parse_domain_authority(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    try:
        return int(float(str(raw).strip()))
    except (ValueError, TypeError):
        return None


# ── Reach Score (log10-normalized for Publishers) ─────────────────────────────

def compute_reach_score(fb: Optional[int],
                         tw: Optional[int],
                         ig: Optional[int]) -> float:
    """
    Log10-normalize total social followers to 0-1 range.
    Reference ceiling: 10M followers → 1.0
    """
    total = (fb or 0) + (tw or 0) + (ig or 0)
    if total <= 0:
        return 0.0
    log_val = math.log10(total + 1)
    log_max = math.log10(10_000_001)
    return min(log_val / log_max, 1.0)


# ── Warmth Scoring ───────────────────────────────────────────────────────────

def compute_warmth(sources: List[str], is_skool_dm: bool = False) -> float:
    """
    Return the highest warmth tier present in source list.
    """
    if is_skool_dm:
        return WARMTH_SKOOL_DM
    src_set = {s.lower() for s in sources}
    if "skool_dm" in src_set:
        return WARMTH_SKOOL_DM
    if "skool" in src_set:
        return WARMTH_SKOOL
    if "scoble" in src_set or "xlist" in src_set:
        return WARMTH_SCOBLE
    if "feedspot" in src_set:
        return WARMTH_FEEDSPOT
    return WARMTH_COLD


# ── Confidence Scoring ────────────────────────────────────────────────────────

def compute_confidence(num_sources: int,
                        field_values: List[Any],
                        has_conflict: bool = False) -> float:
    """
    confidence_score =
        (num_sources / 3, capped at 1.0)  × 0.40
      + (non-empty fields / total fields)  × 0.35
      + (no conflict → 1.0, else 0.0)      × 0.25
    """
    source_ratio = min(num_sources / CONF_MIN_SOURCES, 1.0)
    non_empty = sum(
        1 for v in field_values
        if v is not None and str(v).strip() not in ("", "nan", "none", "N/A")
    )
    completeness = non_empty / max(len(field_values), 1)
    agreement = 0.0 if has_conflict else 1.0

    score = (source_ratio * CONF_W_SOURCES
             + completeness * CONF_W_COMPLETENESS
             + agreement * CONF_W_AGREEMENT)
    return round(min(score, 1.0), 4)


# ── Category Type Inference ──────────────────────────────────────────────────

def infer_category_type(category: str, name: str = "", description: str = "") -> str:
    """Infer Publisher category_type from category string and optional name/description."""
    cat_lower = (category or "").lower()
    if "podcast" in cat_lower:
        return "Podcast"
    if "youtube" in cat_lower or "channel" in cat_lower:
        return "YouTube"
    if "magazine" in cat_lower:
        return "Magazine"
    if "newsletter" in cat_lower:
        return "Newsletter"
    combined = (name + " " + description).lower()
    if "podcast" in combined:
        return "Podcast"
    if "youtube" in combined or "channel" in combined:
        return "YouTube"
    if "magazine" in combined or "journal" in combined:
        return "Magazine"
    return "Blog"


# ── Topic Clustering ─────────────────────────────────────────────────────────

def infer_topic_cluster(text: str) -> Optional[str]:
    """
    Score each topic cluster by keyword hits in text.
    Returns the best-matching cluster name or None.
    """
    if not text:
        return None
    text_lower = text.lower()
    scores: Dict[str, int] = {}
    for cluster, keywords in TOPIC_CLUSTERS.items():
        hits = sum(1 for kw in keywords if kw in text_lower)
        if hits:
            scores[cluster] = hits
    if not scores:
        return None
    return max(scores, key=lambda k: scores[k])


# ── Timestamp ────────────────────────────────────────────────────────────────

def utcnow_str() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Safe CSV value extraction ─────────────────────────────────────────────────

def safe_get(row: List[Any], idx: int, default: Any = None) -> Any:
    """Return row[idx] if it exists and is not blank/nan, else default."""
    try:
        val = row[idx]
    except IndexError:
        return default
    if val is None:
        return default
    s = str(val).strip()
    if s.lower() in ("", "nan", "none", "n/a", "-"):
        return default
    return val


def safe_str(row: List[Any], idx: int) -> str:
    v = safe_get(row, idx)
    return clean_text(v) if v is not None else ""


def safe_int(row: List[Any], idx: int) -> Optional[int]:
    v = safe_get(row, idx)
    if v is None:
        return None
    try:
        return int(float(str(v)))
    except (ValueError, TypeError):
        return None


# ── Clutch category from subdirectory name ────────────────────────────────────

def clutch_category_from_subdir(subdir_name: str) -> str:
    """Map the Clutch subdirectory name to a canonical category."""
    mapping = {
        "DigitalMarketing Clutch": "DigitalMarketing",
        "Development Clutch":      "Development",
        "Design Clutch":           "Design",
        "BusinessServicesClutch":  "BusinessServices",
        "ITservicesClutch":        "ITServices",
    }
    return mapping.get(subdir_name, subdir_name)


# ── Normalize project size / hourly rate strings ─────────────────────────────

def normalize_project_size(raw: Any) -> Optional[str]:
    if not raw:
        return None
    s = str(raw).strip()
    if s.lower() in ("", "nan", "none"):
        return None
    return s


def normalize_hourly_rate(raw: Any) -> Optional[str]:
    if not raw:
        return None
    s = str(raw).strip()
    if s.lower() in ("", "nan", "none"):
        return None
    return s


# ── Extract @mentions and URLs from bio text ──────────────────────────────────

def extract_org_mentions(bio: str) -> List[str]:
    """Return a list of @org_mentions found in the bio."""
    return re.findall(r"@([\w]+)", bio)


def extract_urls(text: str) -> List[str]:
    """Return a list of URLs found in the text."""
    return re.findall(r"https?://[^\s,]+", text)


# ── Jaro-Winkler via rapidfuzz ────────────────────────────────────────────────

def jaro_winkler_sim(a: str, b: str) -> float:
    """Compute Jaro-Winkler similarity using rapidfuzz."""
    try:
        from rapidfuzz.distance import JaroWinkler
        return JaroWinkler.similarity(a.lower(), b.lower())
    except ImportError:
        # fallback: exact match
        return 1.0 if a.lower() == b.lower() else 0.0
