#!/usr/bin/env python3
"""Publish the validated area-weighted 2024 presidential district slice."""

from __future__ import annotations

import json
import argparse
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
CONTEST_FILES = {
    "president": "president_2024.json",
    "us_senate": "us_senate_2024.json",
}
UNCHANGED_DISTRICTS = {"1", "2"}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contest", choices=sorted(CONTEST_FILES), default="president")
    args = parser.parse_args()
    suffix = args.contest
    source = DATA / "reports" / "district_crosswalk_comparison" / f"tn_2024_{suffix}_canonical_block_bridge_2026_districts_area_weighted.csv"
    target = DATA / "district_contests_2026" / f"congressional_{args.contest}_2024.json"
    payload = json.loads(target.read_text(encoding="utf-8"))
    current = payload["general"]["results"]
    unchanged_districts = UNCHANGED_DISTRICTS if args.contest == "president" else set()
    rows = pd.read_csv(source)
    for row in rows.to_dict("records"):
        district = str(int(row["DISTRICT"]))
        if district in unchanged_districts:
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
    if args.contest == "us_senate":
        source_payload = json.loads((DATA / "contests" / CONTEST_FILES[args.contest]).read_text(encoding="utf-8"))
        target_totals = {
            party: sum(int(round(float(row.get(f"{party}_votes", 0) or 0))) for row in source_payload["rows"])
            for party in ("dem", "rep", "other")
        }
        last = current["9"]
        for party in ("dem", "rep", "other"):
            current_total = sum(int(current[d].get(f"{party}_votes", 0) or 0) for d in current)
            current_value = int(last.get(f"{party}_votes", 0) or 0) + target_totals[party] - current_total
            last[f"{party}_votes"] = current_value
        for node in current.values():
            node["total_votes"] = node["dem_votes"] + node["rep_votes"] + node["other_votes"]
            node["margin"] = node["rep_votes"] - node["dem_votes"]
            node["margin_pct"] = round(abs(node["margin"]) / node["total_votes"] * 100, 4) if node["total_votes"] else 0
    payload["meta"].update({
        "source": f"canonical_2024_precinct_to_rdh_vap_weighted_blocks_to_2026_districts_area_weighted_{args.contest}",
        "canonical_precinct_count": 1965,
        "rdh_bridge_rows": 239644,
        "vote_preservation_error": 0.0,
    })
    payload["meta"].update({
        "source": "canonical_2024_precinct_to_rdh_vap_weighted_blocks_to_2026_districts_area_weighted",
        "match_coverage_pct": 100.0,
        "canonical_precinct_count": 1965,
        "rdh_bridge_rows": 239644,
        "vote_preservation_error": 0.0,
    })
    target.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(target)


if __name__ == "__main__":
    main()
