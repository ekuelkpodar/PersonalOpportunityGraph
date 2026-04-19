"""
scanner.py — Discovers all source files across all 5 data directories.
Returns a structured manifest of files grouped by source type.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List

from backend.config import (
    FEEDSPOT_DIR,
    XLIST_DIR,
    CLUTCH_SUBDIRS,
    FACEBOOK_DIR,
    SKOOL_DIR,
)


def _glob_csvs(directory: str) -> List[str]:
    """Return sorted list of all .csv files in a directory (non-recursive)."""
    d = Path(directory)
    if not d.exists():
        return []
    return sorted(str(p) for p in d.glob("*.csv") if p.is_file())


def _glob_xlsx(directory: str) -> List[str]:
    """Return sorted list of all .xlsx files in a directory (non-recursive)."""
    d = Path(directory)
    if not d.exists():
        return []
    return sorted(str(p) for p in d.glob("*.xlsx") if p.is_file())


def scan_feedspot() -> List[str]:
    return sorted(_glob_csvs(FEEDSPOT_DIR) + _glob_xlsx(FEEDSPOT_DIR))


def scan_xlist() -> Dict[str, List[str]]:
    """Returns {'person': [...], 'company': [...]}."""
    all_csvs = _glob_csvs(XLIST_DIR)

    company_names = {
        "AIcompanies-1.csv", "AIcompanies-2.csv",
        "AI Developer Tools.csv", "AI Enterprise or Work.csv",
        "Infrastructure.csv", "VC Firms.csv",
    }

    person_files: List[str] = []
    company_files: List[str] = []

    for path in all_csvs:
        filename = Path(path).name
        if filename in company_names:
            company_files.append(path)
        else:
            person_files.append(path)

    return {"person": person_files, "company": company_files}


def scan_clutch() -> Dict[str, List[str]]:
    """Returns dict keyed by category name → list of csv paths."""
    result: Dict[str, List[str]] = {}
    for category, subdir in CLUTCH_SUBDIRS.items():
        csvs = _glob_csvs(subdir)
        # exclude python helper scripts that may have been scraped
        csvs = [f for f in csvs if not f.endswith(".py")]
        result[category] = csvs
    return result


def scan_facebook() -> Dict[str, List[str]]:
    """Returns {'csv': [...csv paths], 'xlsx': [...xlsx paths]}."""
    csvs = _glob_csvs(FACEBOOK_DIR)
    xlsxs = _glob_xlsx(FACEBOOK_DIR)
    return {"csv": csvs, "xlsx": xlsxs}


def scan_skool() -> Dict[str, str]:
    """Returns {'communities': path, 'dms': path}."""
    result: Dict[str, str] = {}
    d = Path(SKOOL_DIR)
    for f in d.iterdir():
        if f.name == "SkoolCommunities.csv":
            result["communities"] = str(f)
        elif f.name == "SkoolDM.csv":
            result["dms"] = str(f)
    return result


def scan_all() -> Dict[str, any]:
    """Scan all sources and return a full manifest."""
    return {
        "feedspot":  scan_feedspot(),
        "xlist":     scan_xlist(),
        "clutch":    scan_clutch(),
        "facebook":  scan_facebook(),
        "skool":     scan_skool(),
    }


def count_files(manifest: Dict) -> int:
    """Count total data files in a manifest returned by scan_all."""
    total = 0
    total += len(manifest.get("feedspot", []))
    xlist = manifest.get("xlist", {})
    total += len(xlist.get("person", [])) + len(xlist.get("company", []))
    for files in manifest.get("clutch", {}).values():
        total += len(files)
    fb = manifest.get("facebook", {})
    total += len(fb.get("csv", [])) + len(fb.get("xlsx", []))
    skool = manifest.get("skool", {})
    total += len(skool)
    return total
