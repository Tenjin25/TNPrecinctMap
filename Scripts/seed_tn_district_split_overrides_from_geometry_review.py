#!/usr/bin/env python3
"""Seed district split overrides from the GeoPandas geometry review.

Drops pieces classified as ``sliver`` (<1% area) and renormalizes the remaining
district weights for that precinct/scope/lines_year. Real secondary splits are
left alone unless a reviewer later edits the override CSV by hand.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
REPORT_DIR = DATA_DIR / "reports" / "district_crosswalk_comparison" / "split_geometry_review"
PIECES_CSV = REPORT_DIR / "split_geometry_review_pieces.csv"
OUT_CSV = DATA_DIR / "crosswalks" / "tn_district_split_overrides.csv"
OUT_SUMMARY = DATA_DIR / "crosswalks" / "tn_district_split_overrides_seed_summary.json"

FIELDNAMES = [
    "scope",
    "lines_year",
    "precinct_key",
    "county_norm",
    "prec_id",
    "name20",
    "district_num",
    "area_weight",
    "note",
    "source",
]


def numeric(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def normalize_key(value: str) -> str:
    return " ".join(str(value or "").strip().upper().split())


def main() -> None:
    if not PIECES_CSV.exists():
        raise FileNotFoundError(
            f"Missing {PIECES_CSV}. Run export_tn_district_split_geometry_review.py first."
        )

    with PIECES_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        pieces = list(csv.DictReader(handle))

    grouped: Dict[Tuple[str, str, str], List[dict]] = defaultdict(list)
    for row in pieces:
        key = (
            str(row.get("scope") or "").strip(),
            str(row.get("lines_year") or "").strip(),
            normalize_key(row.get("precinct_key") or ""),
        )
        if all(key):
            grouped[key].append(row)

    out_rows: List[dict] = []
    seeded_precincts = 0
    dropped_slivers = 0

    for (scope, lines_year, precinct_key), rows in sorted(grouped.items()):
        slivers = [r for r in rows if str(r.get("geometry_status") or "").strip() == "sliver"]
        keepers = [r for r in rows if str(r.get("geometry_status") or "").strip() != "sliver"]
        if not slivers or not keepers:
            continue

        total = sum(numeric(r.get("area_weight")) for r in keepers)
        if total <= 0:
            continue

        seeded_precincts += 1
        dropped_slivers += len(slivers)
        dropped_note = ",".join(
            f"{str(r.get('district_num')).strip()}:{numeric(r.get('area_weight')):.6f}"
            for r in sorted(slivers, key=lambda x: numeric(x.get("area_weight")), reverse=True)
        )
        sample = keepers[0]
        county_norm = normalize_key(str(sample.get("county_norm") or "").strip())
        if not county_norm and " - " in precinct_key:
            county_norm = precinct_key.split(" - ", 1)[0]
        prec_id = str(sample.get("prec_id") or "").strip()
        if not prec_id and " - " in precinct_key:
            prec_id = precinct_key.split(" - ", 1)[1].strip()
        prec_id = prec_id.zfill(6)
        for keeper in sorted(keepers, key=lambda r: numeric(r.get("area_weight")), reverse=True):
            weight = numeric(keeper.get("area_weight")) / total
            out_rows.append(
                {
                    "scope": scope,
                    "lines_year": lines_year,
                    "precinct_key": precinct_key,
                    "county_norm": county_norm,
                    "prec_id": prec_id,
                    "name20": str(sample.get("name20") or "").strip(),
                    "district_num": str(keeper.get("district_num") or "").strip(),
                    "area_weight": f"{weight:.8f}",
                    "note": f"drop_slivers[{dropped_note}]; renormalize remaining area weights",
                    "source": "split_geometry_review_sliver_prune",
                }
            )

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(out_rows)

    summary = {
        "pieces_csv": str(PIECES_CSV.relative_to(DATA_DIR)),
        "output_csv": str(OUT_CSV.relative_to(DATA_DIR)),
        "seeded_precincts": seeded_precincts,
        "override_rows": len(out_rows),
        "dropped_sliver_pieces": dropped_slivers,
    }
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
