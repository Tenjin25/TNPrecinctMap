#!/usr/bin/env python3
"""Rebuild older statewide district results directly from official VTD features.

Matched geographic precinct votes use official-VTD -> NHGIS block -> Census BEF
weights. Residual unmatched and administrative votes stay within their certified
county and are distributed over its complete official block-population footprint.
County and statewide party totals are preserved exactly.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path


FIELDS = ("dem_votes", "rep_votes", "other_votes")
DEFAULT_YEARS = (2008, 2012, 2014)


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def largest_remainder(target: int, weights: dict[str, float]) -> dict[str, int]:
    keys = sorted(weights, key=int)
    total = sum(max(0.0, float(weights[key])) for key in keys)
    if target <= 0 or total <= 0:
        return {key: 0 for key in keys}
    exact = {key: target * max(0.0, float(weights[key])) / total for key in keys}
    out = {key: int(exact[key]) for key in keys}
    order = sorted(keys, key=lambda key: (exact[key] - out[key], -int(key)), reverse=True)
    for key in order[: target - sum(out.values())]:
        out[key] += 1
    return out


def finalize(row: dict) -> None:
    row["total_votes"] = sum(int(row.get(field, 0) or 0) for field in FIELDS)
    row["margin"] = int(row["rep_votes"]) - int(row["dem_votes"])
    row["margin_pct"] = (
        round(100.0 * row["margin"] / row["total_votes"], 4)
        if row["total_votes"] else 0.0
    )
    row["winner"] = "REP" if row["margin"] > 0 else "DEM" if row["margin"] < 0 else "TIE"


def load_weights(path: Path) -> tuple[dict, dict]:
    out = defaultdict(list)
    county_footprints = defaultdict(Counter)
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            key = (
                int(row["year"]), row["scope"], int(row["lines_year"]),
                str(int(row["official_feature_id"])),
            )
            district = str(int(row["district"]))
            weight = float(row["weight"])
            out[key].append((district, weight))
            county_key = (int(row["year"]), row["scope"], int(row["lines_year"]), row["county_norm"])
            county_footprints[county_key][district] += weight * float(row.get("feature_mass") or 1.0)
    return dict(out), dict(county_footprints)


def rebuild_file(
    source_path: Path,
    published_path: Path,
    output_path: Path,
    weights: dict,
    county_footprints: dict,
    scope: str,
    lines_year: int,
) -> dict:
    source = read_json(source_path)
    published = read_json(published_path)
    year = int(source["year"])
    contest = source["contest_type"]
    county_totals = defaultdict(Counter)
    county_geo = defaultdict(lambda: defaultdict(Counter))
    matched_rows = unmatched_rows = nongeo_rows = 0

    for row in source.get("rows", []):
        label = str(row.get("county", ""))
        if " - " not in label:
            continue
        county, code = label.split(" - ", 1)
        for field in FIELDS:
            county_totals[county][field] += int(row.get(field, 0) or 0)
        feature_id = ""
        prefix = f"H{year}-"
        if code.startswith(prefix) and code[len(prefix):].isdigit():
            feature_id = str(int(code[len(prefix):]))
        if not feature_id:
            if code.startswith("NG-"):
                nongeo_rows += 1
            else:
                unmatched_rows += 1
            continue
        allocs = weights.get((year, scope, lines_year, feature_id), [])
        if not allocs:
            unmatched_rows += 1
            continue
        matched_rows += 1
        for field in FIELDS:
            votes = int(row.get(field, 0) or 0)
            for district, weight in allocs:
                county_geo[county][field][district] += votes * weight

    county_output = defaultdict(lambda: defaultdict(Counter))
    for county, targets in county_totals.items():
        combined = Counter()
        for field in FIELDS:
            combined.update(county_geo[county][field])
        all_districts = set(combined)
        for field in FIELDS:
            all_districts.update(county_geo[county][field])
        neutral_footprint = county_footprints.get((year, scope, lines_year, county), {})
        all_districts.update(neutral_footprint)
        if not all_districts:
            raise RuntimeError(f"No official block district footprint for {county} {year} {scope}")
        for field in FIELDS:
            geographic = county_geo[county][field]
            matched_total = sum(geographic.values())
            target = int(targets[field])
            # Preserve safely matched precinct votes in their direct districts.
            # Spread only the unresolved remainder across the county's complete
            # official block footprint. Using the matched subset alone can erase
            # districts when a county (notably 2012 Knox) has few matched labels.
            allocation_basis = Counter(geographic)
            residual = max(0.0, target - matched_total)
            neutral_total = sum(neutral_footprint.values())
            if residual > 0 and neutral_total > 0:
                for district, mass in neutral_footprint.items():
                    allocation_basis[district] += residual * float(mass) / neutral_total
            elif not allocation_basis:
                allocation_basis.update(combined or neutral_footprint)
            allocated = largest_remainder(target, dict(allocation_basis))
            for district, votes in allocated.items():
                county_output[county][district][field] += votes

    district_totals = defaultdict(Counter)
    for county, districts in county_output.items():
        for district, values in districts.items():
            district_totals[district].update(values)

    old_results = published.get("general", {}).get("results", {})
    results = {}
    for district in sorted(set(old_results) | set(district_totals), key=int):
        row = dict(old_results.get(district, {}))
        for field in FIELDS:
            row[field] = int(district_totals[district][field])
        finalize(row)
        results[district] = row
    published["general"] = {"results": results}
    published.setdefault("meta", {})["official_vtd_direct_method"] = (
        "Tennessee Comptroller election-date VTDs translated through NHGIS blocks "
        "to official Census BEFs; residual county votes allocated over the complete "
        "official county block-population footprint; county and statewide totals preserved exactly"
    )
    published["meta"]["official_vtd_source"] = source.get("meta", {}).get("geography", "")
    published["meta"]["district_lines_year"] = lines_year
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(published, indent=2) + "\n", encoding="utf-8")

    county_errors = []
    for county, targets in county_totals.items():
        for field in FIELDS:
            actual = sum(values[field] for values in county_output[county].values())
            if actual != targets[field]:
                county_errors.append({"county": county, "field": field, "target": targets[field], "actual": actual})
    target_totals = {field: sum(values[field] for values in county_totals.values()) for field in FIELDS}
    output_totals = {field: sum(values[field] for values in district_totals.values()) for field in FIELDS}
    return {
        "file": output_path.name,
        "scope": scope,
        "year": year,
        "contest_type": contest,
        "lines_year": lines_year,
        "matched_rows": matched_rows,
        "unmatched_rows": unmatched_rows,
        "non_geographic_rows": nongeo_rows,
        "county_errors": county_errors,
        "target_totals": target_totals,
        "output_totals": output_totals,
        "before_hd28": old_results.get("28") if scope == "state_house" and year == 2012 else None,
        "after_hd28": results.get("28") if scope == "state_house" and year == 2012 else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, default=Path("Data"))
    parser.add_argument("--output-root", type=Path, default=Path("Data"))
    parser.add_argument("--years", nargs="*", type=int, default=list(DEFAULT_YEARS))
    args = parser.parse_args()
    weights, county_footprints = load_weights(
        args.data_root / "crosswalks" / "tn_official_precinct_to_modern_districts.csv"
    )
    reports = []
    manifest = read_json(args.data_root / "historical_precinct_contests" / "manifest.json")
    selected = {
        (int(row["year"]), row["contest_type"]): row["file"]
        for row in manifest.get("files", []) if int(row["year"]) in set(args.years)
    }
    for (year, contest), source_name in sorted(selected.items()):
        source_path = args.data_root / "historical_precinct_contests" / source_name
        for scope in ("congressional", "state_house", "state_senate"):
            name = f"{scope}_{contest}_{year}.json"
            published = args.data_root / "district_contests" / name
            if published.exists():
                reports.append(rebuild_file(
                    source_path, published,
                    args.output_root / "district_contests" / name,
                    weights, county_footprints, scope, 2022,
                ))
        name = f"congressional_{contest}_{year}.json"
        published = args.data_root / "district_contests_2026" / name
        if published.exists():
            reports.append(rebuild_file(
                source_path, published,
                args.output_root / "district_contests_2026" / name,
                weights, county_footprints, "congressional", 2026,
            ))
    payload = {"generated_by": Path(__file__).name, "files": reports}
    report_path = args.output_root / "reports" / "official_vtd_direct_district_rebuild.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    errors = [error for row in reports for error in row["county_errors"]]
    mismatches = [row for row in reports if row["target_totals"] != row["output_totals"]]
    hd28 = [row for row in reports if row.get("after_hd28")]
    print(json.dumps({
        "files": len(reports),
        "county_errors": len(errors),
        "statewide_mismatches": len(mismatches),
        "hd28": hd28,
    }, indent=2))
    if errors or mismatches:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
