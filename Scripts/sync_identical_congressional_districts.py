#!/usr/bin/env python3
"""Keep result modes equal where official congressional plans overlap at least 99.9%."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


FIELDS = ("dem_votes", "rep_votes", "other_votes")


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def assignments(path: Path) -> dict[str, str]:
    frame = pd.read_csv(path, dtype=str, skipinitialspace=True)
    frame["GEOID"] = frame["GEOID"].astype(str).str.zfill(15)
    frame["CDFP"] = pd.to_numeric(frame["CDFP"]).astype(int).astype(str)
    return dict(zip(frame["GEOID"], frame["CDFP"]))


def largest_remainder(target: int, weights: dict[str, int]) -> dict[str, int]:
    total = sum(max(0, int(value)) for value in weights.values())
    if target <= 0 or total <= 0:
        return {key: 0 for key in weights}
    exact = {key: target * max(0, int(value)) / total for key, value in weights.items()}
    output = {key: int(value) for key, value in exact.items()}
    order = sorted(output, key=lambda key: (exact[key] - output[key], -int(key)), reverse=True)
    for key in order[: target - sum(output.values())]:
        output[key] += 1
    return output


def finalize(row: dict) -> None:
    row["total_votes"] = sum(int(row.get(field, 0) or 0) for field in FIELDS)
    row["margin"] = int(row["rep_votes"]) - int(row["dem_votes"])
    row["margin_pct"] = round(100 * row["margin"] / row["total_votes"], 4) if row["total_votes"] else 0.0
    row["winner"] = "REP" if row["margin"] > 0 else "DEM" if row["margin"] < 0 else "TIE"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, default=Path("Data"))
    parser.add_argument("--output-root", type=Path, default=Path("Data/district_contests_2026"))
    parser.add_argument("--report", type=Path, default=Path("Data/reports/congressional_identical_district_sync.json"))
    args = parser.parse_args()
    old = assignments(args.data_root / "47_TN_CD118.txt")
    new = assignments(args.data_root / "CD120_47.txt")
    common = set(old) & set(new)
    districts = sorted(set(old.values()) | set(new.values()), key=int)
    overlap = {}
    for district in districts:
        old_blocks = {geoid for geoid in common if old[geoid] == district}
        new_blocks = {geoid for geoid in common if new[geoid] == district}
        overlap[district] = len(old_blocks & new_blocks) / max(len(old_blocks), len(new_blocks), 1)
    identical = [district for district in districts if overlap[district] == 1.0]
    allocation_equivalent = [district for district in districts if overlap[district] >= 0.999]
    changes = []
    current_dir = args.data_root / "district_contests"
    future_dir = args.data_root / "district_contests_2026"
    args.output_root.mkdir(parents=True, exist_ok=True)
    for current_path in sorted(current_dir.glob("congressional_*.json")):
        future_path = future_dir / current_path.name
        if not future_path.exists():
            continue
        current = load(current_path)
        future = load(future_path)
        current_results = current.get("general", {}).get("results", {})
        future_results = future.get("general", {}).get("results", {})
        before = {district: dict(future_results.get(district, {})) for district in allocation_equivalent}
        if all(
            all(current_results.get(district, {}).get(field) == future_results.get(district, {}).get(field) for field in FIELDS)
            for district in allocation_equivalent
        ):
            if args.output_root.resolve() != future_dir.resolve():
                (args.output_root / current_path.name).write_text(json.dumps(future, indent=2) + "\n", encoding="utf-8")
            continue
        mutable = [district for district in future_results if district not in allocation_equivalent]
        for field in FIELDS:
            target = sum(int(row.get(field, 0) or 0) for row in future_results.values())
            for district in allocation_equivalent:
                future_results[district][field] = int(current_results[district].get(field, 0) or 0)
            remaining = target - sum(int(future_results[d].get(field, 0) or 0) for d in allocation_equivalent)
            allocated = largest_remainder(
                remaining,
                {district: int(future_results[district].get(field, 0) or 0) for district in mutable},
            )
            for district, votes in allocated.items():
                future_results[district][field] = votes
        for row in future_results.values():
            finalize(row)
        future.setdefault("meta", {})["allocation_equivalent_bef_districts_synced"] = allocation_equivalent
        future["meta"]["allocation_equivalent_overlap_threshold"] = 0.999
        future["meta"].pop("identical_bef_districts_synced", None)
        out_path = args.output_root / current_path.name
        out_path.write_text(json.dumps(future, indent=2) + "\n", encoding="utf-8")
        changes.append({
            "file": current_path.name,
            "allocation_equivalent_districts": allocation_equivalent,
            "before": before,
            "after": {district: future_results[district] for district in allocation_equivalent},
        })
    payload = {
        "exactly_identical_districts": identical,
        "allocation_equivalent_districts": allocation_equivalent,
        "overlap_ratio_by_district": overlap,
        "overlap_threshold": 0.999,
        "changed_files": len(changes),
        "changes": changes,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "exactly_identical_districts": identical,
        "allocation_equivalent_districts": allocation_equivalent,
        "changed_files": len(changes),
    }, indent=2))


if __name__ == "__main__":
    main()
