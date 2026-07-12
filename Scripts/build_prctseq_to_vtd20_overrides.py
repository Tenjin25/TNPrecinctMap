#!/usr/bin/env python3
"""Build PRCTSEQ -> official VTD20 override rows for current-decade TN sources.

This script writes Data/crosswalks/tn_prctseq_to_vtd20_overrides.csv using:
  - Shelby-specific reviewed geometry output
  - Davidson 2024 source PRCTSEQ/label pairs matched to official VTD20 names

The output intentionally supports weighted rows so counties with merged current
precincts can fan out one PRCTSEQ into multiple official VTD20 targets later.
"""

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"
OUT_CSV = XWALK_DIR / "tn_prctseq_to_vtd20_overrides.csv"
OUT_SUMMARY = XWALK_DIR / "tn_prctseq_to_vtd20_overrides_summary.json"
CURRENT_2024_CSV = DATA_DIR / "20241105__tn__general__precinct.csv"
SHELBY_REVIEW_CSV = DATA_DIR / "reports" / "tn08_shelby_geometry_review_2024_president.csv"
BLOCKASSIGN_NAMES_CSV = XWALK_DIR / "tn_blockassign_vtd_with_names.csv"


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def norm_text(s: str) -> str:
    s = norm_space(s).upper()
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def read_rows(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        yield from csv.DictReader(f)


def load_county_fp_map() -> Dict[str, str]:
    county_geojson = DATA_DIR / "tl_2020_47_county20.geojson"
    payload = json.loads(county_geojson.read_text(encoding="utf-8"))
    out: Dict[str, str] = {}
    for feat in payload.get("features", []):
        props = feat.get("properties", {}) or {}
        name = norm_text(str(props.get("NAME20", "")))
        fp = str(props.get("COUNTYFP20", "")).zfill(3)
        if name and fp:
            out[name] = fp
    return out


def parse_prctseq_int(value: str) -> int:
    s = norm_space(value)
    return int(s) if s.isdigit() else 0


def canonical_davidson_label(value: str) -> str:
    s = norm_space(value).upper()
    m = re.fullmatch(r"0*(\d+)-0*(\d+)", s)
    if m:
        return f"{int(m.group(1))}-{int(m.group(2))}"
    m = re.fullmatch(r"0*(\d+)\s+0*(\d+)", s)
    if m:
        return f"{int(m.group(1))}-{int(m.group(2))}"
    return s


def load_blockassign_name_rows(county_fp: str) -> List[dict]:
    rows: List[dict] = []
    for row in read_rows(BLOCKASSIGN_NAMES_CSV):
        if str(row.get("county_fips", "")).zfill(3) != county_fp:
            continue
        vtd20 = str(row.get("vtd_code", "")).zfill(6)
        name = norm_space(str(row.get("vtd_name", "")))
        if vtd20 and name:
            rows.append({"vtd20": vtd20, "name": name})
    return rows


def build_shelby_rows(county_fp: str) -> List[dict]:
    priority = {"core_tn08": 0, "boundary_split": 1, "sliver_only": 2}
    best_by_prctseq: Dict[int, dict] = {}

    for row in read_rows(SHELBY_REVIEW_CSV):
        prctseq = parse_prctseq_int(str(row.get("prctseq", "")))
        vtd20 = str(row.get("code", "")).zfill(6)
        if not (prctseq and vtd20.isdigit()):
            continue
        status = norm_space(str(row.get("geometry_status", "")))
        try:
            share = float(row.get("district8_share") or 0.0)
        except ValueError:
            share = 0.0
        candidate = {
            "county_fp": county_fp,
            "county_norm": "SHELBY",
            "prctseq": prctseq,
            "vtd20": vtd20,
            "vtd_name": norm_space(str(row.get("vtd_name", ""))),
            "weight": 1.0,
            "source": "shelby_geometry_review",
            "confidence": status or "reviewed",
            "_priority": priority.get(status, 9),
            "_share": share,
        }
        current = best_by_prctseq.get(prctseq)
        if current is None or (
            candidate["_priority"],
            -candidate["_share"],
            candidate["vtd20"],
        ) < (
            current["_priority"],
            -current["_share"],
            current["vtd20"],
        ):
            best_by_prctseq[prctseq] = candidate

    out = []
    for prctseq in sorted(best_by_prctseq):
        row = dict(best_by_prctseq[prctseq])
        row.pop("_priority", None)
        row.pop("_share", None)
        out.append(row)
    return out


def build_davidson_rows(county_fp: str) -> List[dict]:
    official_lookup: Dict[str, List[dict]] = defaultdict(list)
    for row in load_blockassign_name_rows(county_fp):
        key = canonical_davidson_label(row["name"])
        official_lookup[key].append(row)

    seen_pairs = set()
    out: List[dict] = []
    for row in read_rows(CURRENT_2024_CSV):
        if norm_text(str(row.get("COUNTY", ""))) != "DAVIDSON":
            continue
        prctseq = parse_prctseq_int(str(row.get("PRCTSEQ", "")))
        precinct = canonical_davidson_label(str(row.get("PRECINCT", "")))
        if not (prctseq and precinct):
            continue
        pair = (prctseq, precinct)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        matches = official_lookup.get(precinct, [])
        if len(matches) != 1:
            continue
        match = matches[0]
        out.append(
            {
                "county_fp": county_fp,
                "county_norm": "DAVIDSON",
                "prctseq": prctseq,
                "vtd20": match["vtd20"],
                "vtd_name": match["name"],
                "weight": 1.0,
                "source": "davidson_current_label_exact",
                "confidence": "exact_name",
            }
        )

    out.sort(key=lambda r: int(r["prctseq"]))
    return out


def write_csv(rows: List[dict]) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["county_fp", "county_norm", "prctseq", "vtd20", "vtd_name", "weight", "source", "confidence"],
        )
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    county_fp_map = load_county_fp_map()
    shelby_rows = build_shelby_rows(county_fp_map["SHELBY"])
    davidson_rows = build_davidson_rows(county_fp_map["DAVIDSON"])
    rows = shelby_rows + davidson_rows
    rows.sort(key=lambda r: (r["county_fp"], int(r["prctseq"])))
    write_csv(rows)

    summary = {
        "output_csv": str(OUT_CSV),
        "row_count": len(rows),
        "county_counts": {
            "SHELBY": len(shelby_rows),
            "DAVIDSON": len(davidson_rows),
        },
    }
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
