#!/usr/bin/env python3
"""Phase 3: seed high-confidence manual overrides for unmatched precincts.

For historical years where vintage VTD catalogs lack usable place names
(e.g. Greene VTD00 NAME00 is '3000'), match election precinct labels to
official Census VTD20 NAME20, then back-solve a vintage src_vtdst through
the full block-chain transfer table.

Also seeds override_dst_vtd20 for matched_no_transfer rows when a unique
official destination can be inferred from the precinct label.

Writes/updates Data/crosswalks/tn_crosswalk_manual_overrides.csv
(enabled=1, review_status=phase3_auto_*).
"""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
XWALK = DATA / "crosswalks"
OVERRIDES = XWALK / "tn_crosswalk_manual_overrides.csv"
VTD20 = DATA / "tn_vtd_2020.geojson"
COUNTY = DATA / "tl_2020_47_county20.geojson"

CHAIN_BY_VINTAGE = {
    2000: XWALK / "tn_vtd00_to_vtd20_block_chain.csv",
    2010: XWALK / "tn_vtd10_to_vtd20_block_chain.csv",
}

OVERRIDE_FIELDS = [
    "enabled",
    "year",
    "county_norm",
    "from_precinct_norm",
    "current_match_method",
    "current_top_suggested_dst_vtd20",
    "override_src_vtdst",
    "override_dst_vtd20",
    "override_reason",
    "review_status",
    "review_notes",
]


def norm_text(value: str) -> str:
    s = (value or "").strip().upper()
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm_county(value: str) -> str:
    return re.sub(r"\s+COUNTY$", "", norm_text(value)).strip()


def pick_vintage(year: int) -> int:
    if year <= 2009:
        return 2000
    if year <= 2019:
        return 2010
    return 2020


def simplify(value: str) -> str:
    s = norm_text(value)
    s = re.sub(
        r"\b(PRECINCT|PCT|DISTRICT|DIST|WARD|VTD|BOX|CORP|CORPORATION|CITY|COUNTY)\b",
        " ",
        s,
    )
    s = re.sub(r"\s+", " ", s).strip()
    return s


def county_name_by_fips() -> Dict[str, str]:
    payload = __import__("json").loads(COUNTY.read_text(encoding="utf-8"))
    out = {}
    for feat in payload.get("features", []):
        props = feat.get("properties") or {}
        fp = str(props.get("COUNTYFP20") or "").zfill(3)
        name = norm_county(props.get("NAME20") or "")
        if fp and name:
            out[fp] = name
    return out


def load_vtd20_names() -> Dict[str, Dict[str, str]]:
    """county_norm -> {name_norm -> vtdst20} and simplified index."""
    import json

    fp_map = county_name_by_fips()
    exact: Dict[str, Dict[str, str]] = defaultdict(dict)
    simple: Dict[str, Dict[str, str]] = defaultdict(dict)
    payload = json.loads(VTD20.read_text(encoding="utf-8"))
    for feat in payload.get("features", []):
        props = feat.get("properties") or {}
        county = fp_map.get(str(props.get("COUNTYFP20") or "").zfill(3), "")
        vtd = str(props.get("VTDST20") or "").strip().zfill(6)
        name = norm_text(props.get("NAME20") or "")
        if not county or not vtd or not name:
            continue
        exact[county][name] = vtd
        simple[county][simplify(name)] = vtd
    return {"exact": exact, "simple": simple}


def resolve_dst_from_name(
    county: str,
    precinct: str,
    catalogs: Dict[str, Dict[str, Dict[str, str]]],
) -> Tuple[str, str, float]:
    p = norm_text(precinct)
    if not p:
        return "", "", 0.0
    exact = catalogs["exact"].get(county, {})
    if p in exact:
        return exact[p], "exact_vtd20_name", 1.0
    simp = simplify(p)
    simple = catalogs["simple"].get(county, {})
    if simp and simp in simple:
        return simple[simp], "simple_vtd20_name", 0.99

    # Unique containment / fuzzy against Census names.
    scored: List[Tuple[float, str, str]] = []
    for name, vtd in exact.items():
        if simp and (simp in name or name in simp) and min(len(simp), len(name)) >= 4:
            scored.append((0.95, vtd, name))
            continue
        score = SequenceMatcher(None, simp or p, simplify(name) or name).ratio()
        if score >= 0.9:
            scored.append((score, vtd, name))
    if not scored:
        return "", "", 0.0
    scored.sort(reverse=True)
    best_score, best_vtd, _ = scored[0]
    # Require uniqueness of top VTD.
    top_vtds = {v for s, v, _ in scored if s >= best_score - 1e-9}
    if len(top_vtds) != 1:
        return "", "", 0.0
    return best_vtd, "fuzzy_vtd20_name", round(best_score, 4)


def load_chain_dst_to_src(vintage: int) -> Dict[str, Dict[str, List[Tuple[str, float]]]]:
    """county_norm -> dst_vtd20 -> [(src_vtdst, weight), ...]"""
    path = CHAIN_BY_VINTAGE.get(vintage)
    if not path or not path.exists():
        return {}
    fp_map = county_name_by_fips()
    out: Dict[str, Dict[str, List[Tuple[str, float]]]] = defaultdict(lambda: defaultdict(list))
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            county = fp_map.get(str(row.get("src_countyfp") or "").zfill(3), "")
            src = str(row.get("src_vtdst") or "").strip()
            dst = str(row.get("dst_vtdst") or "").strip().zfill(6)
            try:
                w = float(row.get("src_weight") or 0.0)
            except ValueError:
                w = 0.0
            if county and src and dst and w > 0:
                out[county][dst].append((src, w))
    # Collapse duplicate src weights.
    collapsed: Dict[str, Dict[str, List[Tuple[str, float]]]] = {}
    for county, dst_map in out.items():
        collapsed[county] = {}
        for dst, pairs in dst_map.items():
            agg: Dict[str, float] = defaultdict(float)
            for src, w in pairs:
                agg[src] += w
            collapsed[county][dst] = sorted(agg.items(), key=lambda kv: (-kv[1], kv[0]))
    return collapsed


def best_src_for_dst(
    county: str,
    dst: str,
    chain: Dict[str, Dict[str, List[Tuple[str, float]]]],
) -> Tuple[str, float]:
    pairs = chain.get(county, {}).get(dst, [])
    if not pairs:
        return "", 0.0
    src, weight = pairs[0]
    total = sum(w for _, w in pairs)
    share = weight / total if total else 0.0
    # Prefer a dominant source (>=40% of inbound weight) or unique source.
    if len(pairs) == 1 or share >= 0.4:
        return src, share
    return "", 0.0


def load_existing_overrides() -> Dict[Tuple[str, str, str], dict]:
    out: Dict[Tuple[str, str, str], dict] = {}
    if not OVERRIDES.exists():
        return out
    with OVERRIDES.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            key = (
                str(row.get("year") or "").strip(),
                norm_county(row.get("county_norm") or ""),
                norm_text(row.get("from_precinct_norm") or ""),
            )
            out[key] = row
    return out


def iter_unmatched() -> Iterable[dict]:
    for path in sorted(XWALK.glob("tn_precinct_to_vtd20_blockweighted_*_unmatched.csv")):
        if re.search(r"blockweighted_\d{4}__", path.name):
            continue
        m = re.search(r"blockweighted_(\d{4})_unmatched\.csv$", path.name)
        if not m:
            continue
        year = int(m.group(1))
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                yield {
                    "year": year,
                    "county_norm": norm_county(row.get("county_norm") or ""),
                    "from_precinct_norm": norm_text(row.get("precinct_norm") or row.get("from_precinct_norm") or ""),
                    "match_method": str(row.get("match_method") or "").strip(),
                }


def main() -> None:
    catalogs = load_vtd20_names()
    chain_2000 = load_chain_dst_to_src(2000)
    chain_2010 = load_chain_dst_to_src(2010)
    existing = load_existing_overrides()

    seeded = 0
    skipped_enabled = 0
    skipped_ambiguous = 0
    by_year: Dict[int, int] = defaultdict(int)
    by_county: Dict[str, int] = defaultdict(int)

    for row in iter_unmatched():
        year = int(row["year"])
        county = row["county_norm"]
        precinct = row["from_precinct_norm"]
        method = row["match_method"]
        if not county or not precinct:
            continue
        key = (str(year), county, precinct)
        prior = existing.get(key)
        if prior and str(prior.get("enabled") or "").strip() in {"1", "TRUE", "True", "true"}:
            if prior.get("override_src_vtdst") or prior.get("override_dst_vtd20"):
                skipped_enabled += 1
                continue

        dst, how, score = resolve_dst_from_name(county, precinct, catalogs)
        if not dst:
            skipped_ambiguous += 1
            continue

        # Prefer direct official VTD20 destination overrides. Vintage src back-solve is
        # unsafe when VTD00/10 units are numeric mega-polygons that split across many
        # VTD20s (Greene-style); dst-only keeps election labels on the named Census VTD.
        existing[key] = {
            "enabled": "1",
            "year": str(year),
            "county_norm": county,
            "from_precinct_norm": precinct,
            "current_match_method": method,
            "current_top_suggested_dst_vtd20": dst,
            "override_src_vtdst": "",
            "override_dst_vtd20": dst,
            "override_reason": f"phase3 {how} score={score} dst_only",
            "review_status": "phase3_auto_dst",
            "review_notes": "seeded by Scripts/seed_tn_phase3_manual_overrides.py",
        }

        seeded += 1
        by_year[year] += 1
        by_county[county] += 1

    # Write full overrides file (preserve non-seeded rows).
    rows = list(existing.values())
    rows.sort(key=lambda r: (int(r.get("year") or 0), r.get("county_norm") or "", r.get("from_precinct_norm") or ""))
    with OVERRIDES.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OVERRIDE_FIELDS, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in OVERRIDE_FIELDS})

    print(
        {
            "seeded": seeded,
            "skipped_already_enabled": skipped_enabled,
            "skipped_ambiguous": skipped_ambiguous,
            "by_year": dict(sorted(by_year.items())),
            "by_county_top": sorted(by_county.items(), key=lambda kv: (-kv[1], kv[0]))[:15],
            "overrides_total": len(rows),
            "output": str(OVERRIDES.relative_to(ROOT)),
        }
    )


if __name__ == "__main__":
    main()
