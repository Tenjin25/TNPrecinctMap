#!/usr/bin/env python3
"""Independently audit TN district allocations from reconciled precinct results."""

from __future__ import annotations

import argparse
import json
import shutil
from collections import defaultdict
from pathlib import Path

import geopandas as gpd


FIELDS = ("dem_votes", "rep_votes", "other_votes")
SCOPE_FILES = {
    "congressional": ("tl_2022_47_cd118.geojson", "CD118FP"),
    "state_house": ("tl_2022_47_sldl.geojson", "SLDLST"),
    "state_senate": ("tl_2022_47_sldu.geojson", "SLDUST"),
}


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def allocate(target: int, weights: dict[str, float]) -> dict[str, int]:
    keys = sorted(weights, key=lambda value: int(value))
    if not keys:
        return {}
    clean = {key: max(0.0, float(weights[key])) for key in keys}
    total = sum(clean.values())
    if total <= 0:
        clean = {key: 1.0 for key in keys}
        total = float(len(keys))
    exact = {key: target * clean[key] / total for key in keys}
    out = {key: int(exact[key]) for key in keys}
    remainder = target - sum(out.values())
    order = sorted(keys, key=lambda key: (exact[key] - out[key], -int(key)), reverse=True)
    for key in order[:remainder]:
        out[key] += 1
    return out


def spatial_weights(data_dir: Path, scope: str, lines_2026: bool = False):
    precincts = gpd.read_file(data_dir / "tn_voting_precincts.geojson")[["county_norm", "prec_id", "geometry"]]
    if lines_2026:
        district_path, district_col = data_dir / "tl_2026_47_cd2026.geojson", "DISTRICT"
    else:
        name, district_col = SCOPE_FILES[scope]
        district_path = data_dir / name
    districts = gpd.read_file(district_path)[[district_col, "geometry"]]
    precincts = precincts.to_crs(5070)
    districts = districts.to_crs(5070)
    precincts["prec_area"] = precincts.geometry.area
    joined = gpd.overlay(precincts, districts, how="intersection")
    joined["weight"] = joined.geometry.area / joined["prec_area"]
    raw = defaultdict(lambda: defaultdict(float))
    county = defaultdict(lambda: defaultdict(float))
    for row in joined.itertuples(index=False):
        county_norm = str(row.county_norm).strip().upper()
        precinct = str(row.prec_id).strip().zfill(6)
        district = str(getattr(row, district_col)).strip()
        district = str(int(district)) if district.isdigit() else district
        weight = max(0.0, float(row.weight))
        if county_norm and precinct and district and weight > 0:
            raw[(county_norm, precinct)][district] += weight
    weights = {}
    for key, dmap in raw.items():
        total = sum(dmap.values())
        weights[key] = {district: value / total for district, value in dmap.items()}
    counties = gpd.read_file(data_dir / "tl_2020_47_county20.geojson")[["NAME20", "geometry"]].to_crs(5070)
    counties["county_area"] = counties.geometry.area
    county_joined = gpd.overlay(counties, districts, how="intersection")
    county_joined["weight"] = county_joined.geometry.area / county_joined["county_area"]
    for row in county_joined.itertuples(index=False):
        county_norm = str(row.NAME20).strip().upper()
        district = str(getattr(row, district_col)).strip()
        district = str(int(district)) if district.isdigit() else district
        if county_norm and district and float(row.weight) > 0:
            county[county_norm][district] += float(row.weight)
    county_weights = {}
    for key, dmap in county.items():
        total = sum(dmap.values())
        county_weights[key] = {district: value / total for district, value in dmap.items()}
    return weights, county_weights


def independently_rebuild(contest: dict, targets: dict, weights: dict, county_weights: dict):
    observed = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for row in contest.get("rows", []):
        label = str(row.get("county", ""))
        if " - " not in label:
            continue
        county, precinct = label.split(" - ", 1)
        county = county.strip().upper()
        precinct = precinct.strip().zfill(6) if precinct.strip().isdigit() else ""
        allocs = weights.get((county, precinct), {}) if precinct else {}
        for district, share in allocs.items():
            for field in FIELDS:
                observed[county][field][district] += int(row.get(field, 0) or 0) * share

    district_totals = defaultdict(lambda: {field: 0 for field in FIELDS})
    methods = defaultdict(int)
    for county, target in targets.items():
        fallback = county_weights.get(county, {})
        for field in FIELDS:
            field_target = int(target.get(field, 0) or 0)
            dmap = observed[county][field]
            mapped = sum(dmap.values())
            districts = set(dmap) | set(fallback)
            if not districts and field_target:
                methods["unresolved"] += field_target
                continue
            if mapped > field_target and mapped > 0:
                method = "party_specific_contraction"
                final_float = {
                    district: field_target * float(dmap.get(district, 0.0)) / mapped
                    for district in districts
                }
            elif mapped < field_target:
                residual = field_target - mapped
                residual_weights = {
                    district: max(
                        0.0,
                        float(dmap.get(district, 0.0)),
                        float(fallback.get(district, 0.0)),
                    )
                    for district in districts
                }
                weight_total = sum(residual_weights.values())
                if weight_total <= 0:
                    residual_weights = {district: 1.0 for district in districts}
                    weight_total = float(len(districts))
                method = "observed_plus_county_geographic_residual"
                final_float = {
                    district: float(dmap.get(district, 0.0))
                    + residual * residual_weights[district] / weight_total
                    for district in districts
                }
            else:
                method = "observed_exact"
                final_float = {district: float(dmap.get(district, 0.0)) for district in districts}
            allocation = allocate(field_target, final_float)
            for district, votes in allocation.items():
                district_totals[district][field] += votes
            methods[method] += field_target
    return district_totals, dict(methods)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("Data"))
    parser.add_argument("--staged-root", type=Path, required=True)
    parser.add_argument("--staged-2026", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--corrected-root", type=Path)
    args = parser.parse_args()
    if args.corrected_root:
        for name in ("district_contests", "district_contests_2026"):
            source = args.staged_root / name if name == "district_contests" else args.staged_2026
            if source and source.exists():
                shutil.copytree(source, args.corrected_root / name, dirs_exist_ok=True)
    audit_path = args.staged_root / "audit" / "tn_election_reconciliation_audit.json"
    if not audit_path.exists():
        audit_path = args.staged_root / "reports" / "tn_election_reconciliation_audit.json"
    audit = load(audit_path)
    targets_by_contest = defaultdict(dict)
    for row in audit.get("county_rows", []):
        targets_by_contest[(row["contest_type"], int(row["year"]))][row["county"]] = row["certified_targets"]

    reviews = []
    weight_cache = {}
    jobs = [(scope, args.staged_root / "district_contests", False) for scope in SCOPE_FILES]
    if args.staged_2026:
        jobs.append(("congressional", args.staged_2026, True))
    for scope, district_dir, lines_2026 in jobs:
        cache_key = (scope, lines_2026)
        weight_cache[cache_key] = spatial_weights(args.data_dir, scope, lines_2026)
        weights, county_weights = weight_cache[cache_key]
        for district_path in sorted(district_dir.glob(f"{scope}_*.json")):
            if district_path.name in {"manifest.json", "calibration_overrides.json"}:
                continue
            district_payload = load(district_path)
            contest_type = district_payload.get("contest_type")
            year = int(district_payload.get("year", 0) or 0)
            contest_path = args.staged_root / "contests" / f"{contest_type}_{year}.json"
            targets = targets_by_contest.get((contest_type, year))
            if not contest_path.exists() or not targets:
                continue
            independent, methods = independently_rebuild(load(contest_path), targets, weights, county_weights)
            generated = district_payload.get("general", {}).get("results", {})
            discrepancies = []
            flips = []
            independent_zero_districts = []
            for district in sorted(set(independent) | set(generated), key=int):
                expected = independent.get(district, {field: 0 for field in FIELDS})
                actual = generated.get(district, {field: 0 for field in FIELDS})
                delta = {field: int(actual.get(field, 0) or 0) - int(expected.get(field, 0) or 0) for field in FIELDS}
                expected_margin = int(expected.get("rep_votes", 0)) - int(expected.get("dem_votes", 0))
                actual_margin = int(actual.get("rep_votes", 0) or 0) - int(actual.get("dem_votes", 0) or 0)
                expected_total = sum(int(expected.get(field, 0) or 0) for field in FIELDS)
                expected_winner = "REP" if expected_margin > 0 else ("DEM" if expected_margin < 0 else "TIE")
                actual_winner = "REP" if actual_margin > 0 else ("DEM" if actual_margin < 0 else "TIE")
                if any(abs(value) > 2 for value in delta.values()):
                    discrepancies.append({"district": district, "delta": delta, "expected": expected, "actual": {field: actual.get(field, 0) for field in FIELDS}})
                if expected_total <= 0 and sum(int(actual.get(field, 0) or 0) for field in FIELDS) > 0:
                    independent_zero_districts.append(district)
                elif expected_winner != actual_winner:
                    flips.append({"district": district, "independent": expected_winner, "generated": actual_winner})
            reviews.append({
                "lines": 2026 if lines_2026 else 2022,
                "scope": scope,
                "contest_type": contest_type,
                "year": year,
                "file": district_path.name,
                "allocation_methods": methods,
                "discrepancies_gt_2_votes": discrepancies,
                "winner_disagreements": flips,
                "independent_zero_districts": independent_zero_districts,
            })
            if args.corrected_root:
                corrected_payload = dict(district_payload)
                corrected_payload["meta"] = dict(district_payload.get("meta", {}))
                corrected_payload["meta"]["geographic_audit_method"] = "independent_county_constrained_spatial_precinct_overlay"
                corrected_results = {}
                for district in sorted(independent, key=int):
                    source_row = dict(generated.get(district, {}))
                    for field in FIELDS:
                        source_row[field] = int(independent[district][field])
                    source_row["total_votes"] = sum(source_row[field] for field in FIELDS)
                    source_row["margin"] = source_row["rep_votes"] - source_row["dem_votes"]
                    source_row["margin_pct"] = round(
                        source_row["margin"] / source_row["total_votes"] * 100.0
                        if source_row["total_votes"] else 0.0,
                        4,
                    )
                    source_row["winner"] = "REP" if source_row["margin"] > 0 else ("DEM" if source_row["margin"] < 0 else "TIE")
                    corrected_results[district] = source_row
                corrected_payload["general"] = {"results": corrected_results}
                out_name = "district_contests_2026" if lines_2026 else "district_contests"
                out_path = args.corrected_root / out_name / district_path.name
                out_path.write_text(json.dumps(corrected_payload, indent=2), encoding="utf-8")
    summary = {
        "files_checked": len(reviews),
        "files_with_discrepancies": sum(bool(row["discrepancies_gt_2_votes"]) for row in reviews),
        "district_discrepancies": sum(len(row["discrepancies_gt_2_votes"]) for row in reviews),
        "winner_disagreements": sum(len(row["winner_disagreements"]) for row in reviews),
        "independent_zero_districts": sum(len(row["independent_zero_districts"]) for row in reviews),
    }
    payload = {"methodology": "independent_county_constrained_spatial_precinct_overlay", "summary": summary, "reviews": reviews}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
