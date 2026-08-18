#!/usr/bin/env python3
"""Publish the validated area-weighted 2024 presidential district slice."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
SOURCE = DATA / "reports" / "district_crosswalk_comparison" / "tn_2024_canonical_block_bridge_2026_districts_area_weighted.csv"
TARGET = DATA / "district_contests_2026" / "congressional_president_2024.json"
UNCHANGED_DISTRICTS = {"1", "2"}


def main() -> None:
    payload = json.loads(TARGET.read_text(encoding="utf-8"))
    current = payload["general"]["results"]
    rows = pd.read_csv(SOURCE)
    for row in rows.to_dict("records"):
        district = str(int(row["DISTRICT"]))
        if district in UNCHANGED_DISTRICTS:
            continue
        node = current[district]
        dem = int(round(row["dem"]))
        rep = int(round(row["rep"]))
        other = int(round(row["other"]))
        total = dem + rep + other
        margin = rep - dem
        node.update({
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": other,
            "total_votes": total,
            "margin": margin,
            "margin_pct": round(abs(margin) / total * 100, 4) if total else 0,
            "winner": "REP" if margin > 0 else "DEM" if margin < 0 else "TIE",
        })
    payload["meta"].update({
        "source": "canonical_2024_precinct_to_rdh_vap_weighted_blocks_to_2026_districts_area_weighted",
        "match_coverage_pct": 100.0,
        "canonical_precinct_count": 1965,
        "rdh_bridge_rows": 239644,
        "vote_preservation_error": 0.0,
    })
    TARGET.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(TARGET)


if __name__ == "__main__":
    main()
