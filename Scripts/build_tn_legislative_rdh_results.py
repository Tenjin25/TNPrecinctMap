#!/usr/bin/env python3
"""Build statewide-election results on Tennessee's enacted 2022 legislative plan.

The input is the Redistricting Data Hub 2020-block disaggregation.  Blocks are
assigned from explicit GSL##/GSU## vote fields when those fields exist; blocks
with no legislative votes (and older files without those fields) use a
representative point in the enacted TIGER geometry.  Party totals are then
reconciled exactly to the certified statewide totals with largest remainder.

2012 is deliberately unsupported: it has no local RDH block base in this repo.
Direct official/precinct-with-district output files are preserved by default.
"""

from __future__ import annotations

import argparse
import io
import json
import re
import zipfile
from collections import defaultdict
from pathlib import Path

import shapefile
from shapely.geometry import shape
from shapely.strtree import STRtree


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
FIELDS = ("dem_votes", "rep_votes", "other_votes")
OFFICES = {"PRE": "president", "GOV": "governor", "USS": "us_senate"}
SCOPES = {
    "state_house": ("tl_2022_47_sldl.geojson", "SLDLST", "GSL"),
    "state_senate": ("tl_2022_47_sldu.geojson", "SLDUST", "GSU"),
}
YEARS = (2016, 2018, 2020, 2022, 2024)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def certified_targets(contest: str, year: int) -> dict[str, int]:
    payload = load_json(DATA / "contests" / f"{contest}_{year}.json")
    rows = payload.get("rows", [])
    return {field: sum(int(row.get(field, 0) or 0) for row in rows) for field in FIELDS}


def largest_remainder(target: int, values: dict[str, float], districts: list[str]) -> dict[str, int]:
    total = sum(max(0.0, values.get(d, 0.0)) for d in districts)
    weights = {d: max(0.0, values.get(d, 0.0)) for d in districts}
    if total <= 0:
        weights = {d: 1.0 for d in districts}
        total = float(len(districts))
    exact = {d: target * weights[d] / total for d in districts}
    out = {d: int(exact[d]) for d in districts}
    remainder = target - sum(out.values())
    order = sorted(districts, key=lambda d: (exact[d] - out[d], -int(d)), reverse=True)
    for district in order[:remainder]:
        out[district] += 1
    return out


def district_index(path: Path, field: str):
    payload = load_json(path)
    geometries, ids = [], []
    for feature in payload["features"]:
        district = str(int(str(feature["properties"][field])))
        geometries.append(shape(feature["geometry"]))
        ids.append(district)
    return STRtree(geometries), geometries, ids


def contained_district(block_geom, tree: STRtree, geometries, ids) -> str:
    point = block_geom.representative_point()
    candidates = tree.query(point)
    for index in candidates:
        if geometries[index].covers(point):
            return ids[index]
    # Floating-point edge fallback: choose the nearest enacted district.
    index = tree.nearest(point)
    return ids[index]


def party_field(column: str, year: int):
    match = re.match(rf"^G{str(year)[2:]}(PRE|GOV|USS)([A-Z])", column)
    if not match:
        return None
    contest = OFFICES[match.group(1)]
    party = match.group(2)
    field = "dem_votes" if party == "D" else ("rep_votes" if party == "R" else "other_votes")
    return contest, field


def explicit_district(record: dict, prefix: str) -> str:
    totals: dict[str, float] = defaultdict(float)
    for key, value in record.items():
        match = re.match(rf"^{prefix}(\d{{2}})[A-Z]", key)
        if match:
            totals[str(int(match.group(1)))] += float(value or 0)
    if not totals or max(totals.values()) <= 0:
        return ""
    return max(totals, key=lambda district: (totals[district], -int(district)))


def read_block_rows(year: int):
    path = DATA / f"tn_{year}_gen_2020_blocks.zip"
    with zipfile.ZipFile(path) as archive:
        dbf_name = next(name for name in archive.namelist() if name.lower().endswith(".dbf"))
        shp_name = next(name for name in archive.namelist() if name.lower().endswith(".shp"))
        reader = shapefile.Reader(
            shp=io.BytesIO(archive.read(shp_name)),
            dbf=io.BytesIO(archive.read(dbf_name)),
        )
        fields = [item[0] for item in reader.fields[1:]]
        try:
            for shape_record in reader.iterShapeRecords():
                yield dict(zip(fields, shape_record.record)), shape_record.shape
        finally:
            reader.close()


def source_rank(meta: dict) -> int:
    if meta.get("official_direct_district_totals"):
        return 5
    direct = float(meta.get("direct_precinct_vote_pct", 0) or 0)
    if direct >= 99.99:
        return 4
    source = str(meta.get("source", ""))
    if "rdh" in source.lower():
        return 3
    if "precinct" in source.lower():
        return 2
    return 1


def result_row(votes: dict[str, int]) -> dict:
    dem, rep, other = (int(votes[field]) for field in FIELDS)
    total = dem + rep + other
    margin = rep - dem
    return {
        "dem_votes": dem, "rep_votes": rep, "other_votes": other,
        "total_votes": total, "dem_candidate": "", "rep_candidate": "",
        "margin": margin,
        "margin_pct": round(margin / total * 100.0 if total else 0.0, 4),
        "winner": "REP" if margin > 0 else ("DEM" if margin < 0 else "TIE"),
    }


def compare(before: dict, after: dict) -> dict:
    changed, flips, margins, totals, details = [], [], [], [], []
    abs_margin_errors = []
    for district in sorted(after, key=int):
        old, new = before.get(district, {}), after[district]
        if any(int(old.get(f, 0) or 0) != int(new[f]) for f in FIELDS):
            changed.append(district)
            details.append({
                "district": district,
                "before_votes": {f: int(old.get(f, 0) or 0) for f in FIELDS},
                "after_votes": {f: int(new[f]) for f in FIELDS},
                "before_margin_pct": old.get("margin_pct"),
                "after_margin_pct": new.get("margin_pct"),
            })
        if old and old.get("winner") != new.get("winner"):
            flips.append(district)
        delta = float(new.get("margin_pct", 0)) - float(old.get("margin_pct", 0) or 0)
        if old:
            abs_margin_errors.append(abs(delta))
        if old and abs(delta) >= 1:
            margins.append({"district": district, "change_pp": round(delta, 4)})
        total_delta = int(new["total_votes"]) - int(old.get("total_votes", 0) or 0)
        if old and total_delta:
            totals.append({"district": district, "change": total_delta})
    return {
        "changed_districts": changed, "district_changes": details, "flips": flips,
        "margin_changes_ge_1pp": margins, "vote_total_changes": totals,
        "comparison_margin_mae_pp": round(sum(abs_margin_errors) / len(abs_margin_errors), 4) if abs_margin_errors else None,
        "comparison_margin_max_error_pp": round(max(abs_margin_errors), 4) if abs_margin_errors else None,
    }


def refresh_manifest(output_dir: Path) -> None:
    path = output_dir / "manifest.json"
    if not path.exists():
        return
    payload = load_json(path)
    for entry in payload.get("files", []):
        result_path = output_dir / str(entry.get("file", ""))
        if not result_path.exists():
            continue
        result = load_json(result_path)
        rows = result.get("general", {}).get("results", {}).values()
        entry["districts"] = len(result.get("general", {}).get("results", {}))
        entry["dem_total"] = sum(int(row.get("dem_votes", 0) or 0) for row in rows)
        rows = result.get("general", {}).get("results", {}).values()
        entry["rep_total"] = sum(int(row.get("rep_votes", 0) or 0) for row in rows)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=DATA / "district_contests")
    parser.add_argument("--audit", type=Path, default=DATA / "reports" / "tn_legislative_rdh_audit.json")
    parser.add_argument("--force", action="store_true", help="Replace a higher-ranked existing source")
    parser.add_argument("--baseline-dir", type=Path, help="Optional before-state used for the audit comparison")
    parser.add_argument("--compare-only", action="store_true", help="Refresh the audit from existing outputs without rebuilding blocks")
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.compare_only:
        if not args.baseline_dir:
            parser.error("--compare-only requires --baseline-dir")
        prior = load_json(args.audit) if args.audit.exists() else {"methodology": "RDH 2020-block disaggregation using modified VAP", "years": {}}
        files = []
        for out in sorted(args.output_dir.glob("state_house_*.json")) + sorted(args.output_dir.glob("state_senate_*.json")):
            match = re.match(r"^(state_house|state_senate)_(president|governor|us_senate)_(2016|2018|2020|2022|2024)\.json$", out.name)
            baseline = args.baseline_dir / out.name
            if not match or not baseline.exists():
                continue
            current_payload, old_payload = load_json(out), load_json(baseline)
            current = current_payload.get("general", {}).get("results", {})
            old = old_payload.get("general", {}).get("results", {})
            if "rdh" not in str(current_payload.get("meta", {}).get("source", "")).lower():
                # Retained higher-ranked source: compare the staged RDH result only
                # when a full build is run; compare-only documents preservation.
                files.append({"file": out.name, "published": False, "existing_source_rank": source_rank(old_payload.get("meta", {})), "new_source_rank": 3, "retained_source": current_payload.get("meta", {}).get("source"), **compare(old, current)})
                continue
            files.append({"file": out.name, "published": True, "existing_source_rank": source_rank(old_payload.get("meta", {})), "new_source_rank": 3, **compare(old, current)})
        prior["files"] = files
        prior["unresolved_anomalies"] = [
            "2012 is maintained by build_tn_2012_rdh_results.py using the election-vintage 2010 block/VTD base",
            "2022 direct precinct-with-district results retained despite large RDH comparison deltas",
            "2024 State House direct precinct-with-district results retained; RDH used only as an error benchmark",
        ]
        args.audit.write_text(json.dumps(prior, indent=2) + "\n", encoding="utf-8")
        refresh_manifest(args.output_dir)
        print(json.dumps({"files_compared": len(files)}, indent=2))
        return
    indexes = {
        scope: (*district_index(DATA / file_name, field), prefix)
        for scope, (file_name, field, prefix) in SCOPES.items()
    }
    audit = {"methodology": "RDH 2020-block disaggregation using modified VAP", "years": {}, "files": []}
    containment_cache: dict[str, dict[str, str]] = {scope: {} for scope in SCOPES}

    for year in YEARS:
        accum = {scope: defaultdict(lambda: defaultdict(float)) for scope in SCOPES}
        methods = {scope: defaultdict(int) for scope in SCOPES}
        contest_columns = None
        for record, raw_geometry in read_block_rows(year):
            if contest_columns is None:
                contest_columns = {key: party_field(key, year) for key in record}
                contest_columns = {key: value for key, value in contest_columns.items() if value}
            for scope in SCOPES:
                tree, geometries, ids, prefix = indexes[scope]
                district = explicit_district(record, prefix)
                method = "explicit_legislative_vote_field" if district else "enacted_tiger_representative_point"
                if not district:
                    geoid = str(record.get("GEOID20", ""))
                    district = containment_cache[scope].get(geoid, "")
                    if not district:
                        district = contained_district(shape(raw_geometry.__geo_interface__), tree, geometries, ids)
                        containment_cache[scope][geoid] = district
                methods[scope][method] += 1
                for column, (contest, field) in contest_columns.items():
                    accum[scope][(contest, field)][district] += float(record.get(column, 0) or 0)

        audit["years"][str(year)] = {scope: dict(methods[scope]) for scope in SCOPES}
        for scope, (_file_name, _field, _prefix) in SCOPES.items():
            districts = [str(value) for value in range(1, 100 if scope == "state_house" else 34)]
            contests = sorted({key[0] for key in accum[scope]})
            for contest in contests:
                targets = certified_targets(contest, year)
                allocations = {
                    field: largest_remainder(targets[field], accum[scope][(contest, field)], districts)
                    for field in FIELDS
                }
                results = {d: result_row({f: allocations[f][d] for f in FIELDS}) for d in districts}
                out = args.output_dir / f"{scope}_{contest}_{year}.json"
                old_payload = load_json(out) if out.exists() else {}
                old_results = old_payload.get("general", {}).get("results", {})
                old_rank = source_rank(old_payload.get("meta", {}))
                expected_districts = 99 if scope == "state_house" else 33
                # A nominally direct source that omits enacted districts fails
                # the district-count validation and cannot outrank a complete
                # block-derived result.
                if len(old_results) != expected_districts:
                    old_rank = min(old_rank, 2)
                new_rank = 3
                publish = args.force or old_rank < new_rank
                payload = {
                    "scope": scope, "contest_type": contest, "year": year,
                    "meta": {
                        "source": "RDH_2020_block_modified_VAP",
                        "estimated": True,
                        "source_quality_rank": 3,
                        "block_assignment": "explicit_GSL_GSU_then_enacted_TIGER_representative_point",
                        "reconciliation": "party_specific_statewide_largest_remainder",
                        "certified_statewide_targets": targets,
                        "districts": len(districts),
                    },
                    "general": {"results": results},
                }
                if publish:
                    out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
                audit["files"].append({
                    "file": out.name, "published": publish,
                    "existing_source_rank": old_rank, "new_source_rank": new_rank,
                    "statewide_targets": targets,
                    "statewide_after": {f: sum(results[d][f] for d in districts) for f in FIELDS},
                    **compare(old_results, results),
                })

    args.audit.parent.mkdir(parents=True, exist_ok=True)
    audit["unresolved_anomalies"] = [
        "2012 is maintained by build_tn_2012_rdh_results.py using the election-vintage 2010 block/VTD base",
        "2022 direct precinct-with-district results retained despite large RDH comparison deltas",
        "2024 State House direct precinct-with-district results retained; RDH used only as an error benchmark",
    ]
    args.audit.write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    refresh_manifest(args.output_dir)
    print(json.dumps({"files_audited": len(audit["files"]), "files_published": sum(row["published"] for row in audit["files"])}, indent=2))


if __name__ == "__main__":
    main()
