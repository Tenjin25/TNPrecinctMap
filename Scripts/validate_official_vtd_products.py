#!/usr/bin/env python3
"""Validate official historical VTD products and congressional plan continuity."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

import geopandas as gpd
import pandas as pd


FIELDS = ("dem_votes", "rep_votes", "other_votes")


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def contest_totals(path: Path) -> dict[str, int]:
    node = load(path)
    return {
        field: sum(int(row.get(field, 0) or 0) for row in node.get("rows", []))
        for field in FIELDS
    }


def read_assignment(path: Path) -> dict[str, str]:
    frame = pd.read_csv(path, dtype=str, skipinitialspace=True)
    frame["GEOID"] = frame["GEOID"].astype(str).str.zfill(15)
    frame["CDFP"] = pd.to_numeric(frame["CDFP"]).astype(int).astype(str)
    return dict(zip(frame["GEOID"], frame["CDFP"]))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, default=Path("Data"))
    parser.add_argument("--block-dir", type=Path)
    parser.add_argument("--output", type=Path, default=Path("Data/reports/official_vtd_products_validation.json"))
    args = parser.parse_args()
    errors = []

    products = load(args.data_root / "reports" / "official_vtd_products_summary.json")
    geometry_checks = []
    for item in products.get("geometry_files", []):
        path = args.data_root / "historical_vtd" / item["file"]
        node = load(path)
        actual = len(node.get("features", []))
        geometry_checks.append({"year": item["year"], "expected": item["features"], "actual": actual})
        if actual != int(item["features"]):
            errors.append(f"{path}: expected {item['features']} features, found {actual}")

    standard_manifest = {
        (int(row["year"]), row["contest_type"]): row["file"]
        for row in load(args.data_root / "contests" / "manifest.json").get("files", [])
    }
    historical_manifest = load(args.data_root / "historical_precinct_contests" / "manifest.json")
    contest_checks = []
    for item in historical_manifest.get("files", []):
        key = (int(item["year"]), item["contest_type"])
        historical_path = args.data_root / "historical_precinct_contests" / item["file"]
        standard_path = args.data_root / "contests" / standard_manifest[key]
        historical = contest_totals(historical_path)
        standard = contest_totals(standard_path)
        contest_checks.append({"year": key[0], "contest_type": key[1], "historical": historical, "canonical": standard})
        if historical != standard:
            errors.append(f"Historical precinct totals differ from canonical contest totals: {key}")

    weight_sums = defaultdict(float)
    with (args.data_root / "crosswalks" / "tn_official_precinct_to_modern_districts.csv").open(
        encoding="utf-8-sig", newline=""
    ) as handle:
        for row in csv.DictReader(handle):
            key = (row["year"], row["official_feature_id"], row["scope"], row["lines_year"])
            weight_sums[key] += float(row["weight"])
    bad_weights = [
        {"year": key[0], "feature_id": key[1], "scope": key[2], "lines_year": key[3], "sum": total}
        for key, total in weight_sums.items() if abs(total - 1.0) > 1e-8
    ]
    if bad_weights:
        errors.append(f"{len(bad_weights)} official-feature district weight groups do not sum to one")

    cd22 = read_assignment(args.data_root / "47_TN_CD118.txt")
    cd26 = read_assignment(args.data_root / "CD120_47.txt")
    common = set(cd22) & set(cd26)
    comparison = {}
    changed_geoids = set()
    for district in ("1", "2"):
        membership22 = {geoid for geoid in common if cd22[geoid] == district}
        membership26 = {geoid for geoid in common if cd26[geoid] == district}
        changed = membership22 ^ membership26
        changed_geoids.update(changed)
        comparison[f"cd_{district}"] = {
            "blocks_2022": len(membership22),
            "blocks_2026": len(membership26),
            "changed_blocks": len(changed),
            "identical": not changed,
            "overlap_ratio": len(membership22 & membership26) / max(len(membership22), len(membership26), 1),
        }
        comparison[f"cd_{district}"]["allocation_equivalent"] = comparison[f"cd_{district}"]["overlap_ratio"] >= 0.999
    if args.block_dir:
        block_path = args.block_dir / "tl_2020_47_tabblock20.zip"
        if block_path.exists() and changed_geoids:
            blocks = gpd.read_file(block_path, columns=["GEOID20", "POP20"])
            population = dict(zip(blocks["GEOID20"].astype(str), pd.to_numeric(blocks["POP20"], errors="coerce").fillna(0)))
            for district in ("1", "2"):
                changed = {
                    geoid for geoid in common
                    if (cd22[geoid] == district) != (cd26[geoid] == district)
                }
                comparison[f"cd_{district}"]["changed_block_population_2020"] = int(sum(population.get(g, 0) for g in changed))
            transfers = []
            for source, target in sorted({(cd22[g], cd26[g]) for g in changed_geoids}, key=lambda pair: (int(pair[0]), int(pair[1]))):
                moved = {g for g in changed_geoids if cd22[g] == source and cd26[g] == target}
                transfers.append({
                    "from": source,
                    "to": target,
                    "blocks": len(moved),
                    "population_2020": int(sum(population.get(g, 0) for g in moved)),
                })
            comparison["cd_2"]["transfers"] = transfers

    result_mode_comparison = {}
    current_dir = args.data_root / "district_contests"
    future_dir = args.data_root / "district_contests_2026"
    for district in ("1", "2"):
        differing = []
        for current_path in sorted(current_dir.glob("congressional_*.json")):
            future_path = future_dir / current_path.name
            if not future_path.exists():
                continue
            current_row = load(current_path).get("general", {}).get("results", {}).get(district, {})
            future_row = load(future_path).get("general", {}).get("results", {}).get(district, {})
            if any(current_row.get(field) != future_row.get(field) for field in FIELDS):
                differing.append(current_path.name)
        result_mode_comparison[f"cd_{district}"] = {
            "differing_contest_files": len(differing),
            "files": differing,
        }
    for district in ("1", "2"):
        if comparison[f"cd_{district}"]["allocation_equivalent"] and result_mode_comparison[f"cd_{district}"]["differing_contest_files"]:
            errors.append(f"CD-{district.zfill(2)} meets the 99.9% overlap rule but differs between result modes")

    payload = {
        "valid": not errors,
        "errors": errors,
        "geometry_checks": geometry_checks,
        "contest_total_checks": contest_checks,
        "district_weight_groups": len(weight_sums),
        "bad_district_weight_groups": bad_weights,
        "congressional_2022_vs_2026": comparison,
        "congressional_result_mode_comparison": result_mode_comparison,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "valid": payload["valid"],
        "geometry_files": len(geometry_checks),
        "contest_files": len(contest_checks),
        "district_weight_groups": len(weight_sums),
        "cd_comparison": comparison,
    }, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
