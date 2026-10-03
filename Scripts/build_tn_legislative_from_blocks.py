#!/usr/bin/env python3
"""Aggregate RDH block-disaggregated statewide results to Tennessee districts."""

from __future__ import annotations

import argparse
import io
import json
import re
import zipfile
from pathlib import Path

import pandas as pd
import geopandas as gpd
import pyogrio


FIELDS = ("dem_votes", "rep_votes", "other_votes")
CONTESTS = {
    2016: {"president": "G16PRE"},
    2018: {"governor": "G18GOV", "us_senate": "G18USS"},
    2020: {"president": "G20PRE", "us_senate": "G20USS"},
    2022: {"governor": "G22GOV"},
    2024: {"president": "G24PRE", "us_senate": "G24USS"},
}


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def largest_remainder(target: int, weights: dict[str, float]) -> dict[str, int]:
    keys = sorted(weights, key=int)
    total = sum(max(0.0, float(weights[key])) for key in keys)
    if total <= 0:
        return {key: 0 for key in keys}
    exact = {key: target * max(0.0, float(weights[key])) / total for key in keys}
    out = {key: int(exact[key]) for key in keys}
    order = sorted(keys, key=lambda key: (exact[key] - out[key], -int(key)), reverse=True)
    for key in order[: target - sum(out.values())]:
        out[key] += 1
    return out


def shapefile_uri(path: Path) -> str:
    with zipfile.ZipFile(path) as archive:
        names = [name for name in archive.namelist() if name.lower().endswith(".shp")]
    if len(names) != 1:
        raise RuntimeError(f"Expected one shapefile in {path}, found {names}")
    return f"zip://{path.resolve()}!{names[0]}"


def available_fields(path: Path, prefer_csv: bool) -> list[str]:
    if prefer_csv:
        with zipfile.ZipFile(path) as archive:
            csv_name = next(name for name in archive.namelist() if name.lower().endswith(".csv"))
            with archive.open(csv_name) as source:
                return list(pd.read_csv(source, nrows=0).columns)
    return list(pyogrio.read_info(shapefile_uri(path))["fields"])


def read_block_votes(path: Path, columns: list[str], prefer_csv: bool) -> pd.DataFrame:
    if prefer_csv:
        with zipfile.ZipFile(path) as archive:
            csv_name = next(name for name in archive.namelist() if name.lower().endswith(".csv"))
            with archive.open(csv_name) as source:
                return pd.read_csv(source, usecols=columns, dtype={"GEOID20": str})
    return pyogrio.read_dataframe(
        shapefile_uri(path), columns=columns, read_geometry=False
    )


def current_plan_assignment(
    data_dir: Path, published_data_dir: Path, scope: str, lines_year: int
) -> tuple[pd.DataFrame, dict]:
    """Build a current-plan block assignment from district races plus TIGER geometry.

    The repository's Census BlockAssign archive is dated December 2020 and therefore
    contains the prior legislative plan.  The RDH 2022/2024 files encode the current
    district in their GSL/GSU/GCON contest columns. Those columns are authoritative
    when they contain votes; representative-point containment fills zero-vote blocks.
    """
    if scope == "state_house":
        election_years = (2024,)
        race_prefix = "GSL"
        tiger_name = "tl_2022_47_sldl.geojson"
        district_field = "SLDLST"
    elif scope == "state_senate":
        election_years = (2022, 2024)
        race_prefix = "GSU"
        tiger_name = "tl_2022_47_sldu.geojson"
        district_field = "SLDUST"
    elif lines_year == 2022:
        election_years = (2024,)
        race_prefix = "GCON"
        tiger_name = "tl_2022_47_cd118.geojson"
        district_field = "CD118FP"
    else:
        election_years = ()
        race_prefix = ""
        tiger_name = "tl_2026_47_cd2026.geojson"
        district_field = "DISTRICT"

    assignments = None
    block_geometry = None
    diagnostic = {
        "race_prefix": race_prefix or None,
        "election_years": list(election_years),
        "lines_year": lines_year,
    }
    for year in election_years:
        path = data_dir / f"tn_{year}_gen_2020_blocks.zip"
        fields = available_fields(path, False)
        race_columns = [column for column in fields if re.match(rf"^{race_prefix}\d\d", column)]
        frame = pyogrio.read_dataframe(
            shapefile_uri(path), columns=["GEOID20", *race_columns]
        )
        frame["GEOID20"] = frame["GEOID20"].astype(str).str.zfill(15)
        grouped = {
            district: [
                column
                for column in race_columns
                if column[len(race_prefix):len(race_prefix) + 2] == district
            ]
            for district in sorted({
                column[len(race_prefix):len(race_prefix) + 2]
                for column in race_columns
            })
        }
        district_votes = pd.DataFrame({
            district: frame[columns].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)
            for district, columns in grouped.items()
        })
        nonzero_count = district_votes.gt(0).sum(axis=1)
        if int((nonzero_count > 1).sum()):
            raise RuntimeError(f"{year} {race_prefix} has blocks assigned to multiple districts")
        inferred = district_votes.idxmax(axis=1).where(nonzero_count == 1)
        inferred.index = frame["GEOID20"]
        assignments = inferred if assignments is None else assignments.combine_first(inferred)
        if block_geometry is None:
            block_geometry = frame[["GEOID20", "geometry"]].copy()

    if block_geometry is None:
        path = data_dir / "tn_2024_gen_2020_blocks.zip"
        block_geometry = pyogrio.read_dataframe(shapefile_uri(path), columns=["GEOID20"])
        block_geometry["GEOID20"] = block_geometry["GEOID20"].astype(str).str.zfill(15)
    if assignments is None:
        assignments = pd.Series(index=block_geometry["GEOID20"], dtype="object")
    block_geometry["district"] = block_geometry["GEOID20"].map(assignments)
    points = block_geometry[["GEOID20", "district", "geometry"]].copy()
    points.geometry = points.geometry.representative_point()
    districts = pyogrio.read_dataframe(
        published_data_dir / tiger_name, columns=[district_field]
    ).to_crs(points.crs)
    spatial = gpd.sjoin(
        points, districts[[district_field, "geometry"]], how="left", predicate="within"
    )
    if spatial[district_field].isna().any():
        raise RuntimeError(f"TIGER assignment missed {int(spatial[district_field].isna().sum())} blocks")
    explicit = spatial["district"].notna()
    explicit_number = pd.to_numeric(spatial["district"], errors="coerce")
    spatial_number = pd.to_numeric(spatial[district_field], errors="coerce")
    diagnostic.update({
        "blocks": int(len(spatial)),
        "explicit_race_assignment_blocks": int(explicit.sum()),
        "geometry_fallback_blocks": int((~explicit).sum()),
        "explicit_geometry_agreements": int((explicit & explicit_number.eq(spatial_number)).sum()),
        "explicit_geometry_disagreements": int((explicit & explicit_number.ne(spatial_number)).sum()),
    })
    spatial["district"] = spatial["district"].fillna(spatial[district_field])
    spatial["district"] = pd.to_numeric(spatial["district"]).astype(int).astype(str)
    return spatial[["GEOID20", "district"]].drop_duplicates("GEOID20"), diagnostic


def party_field(column: str, prefix: str) -> str:
    suffix = column[len(prefix):]
    party = suffix[:1]
    if party == "D":
        return "dem_votes"
    if party == "R":
        return "rep_votes"
    return "other_votes"


def contest_targets(contest_payload: dict) -> dict[str, int]:
    return {
        field: sum(int(row.get(field, 0) or 0) for row in contest_payload.get("rows", []))
        for field in FIELDS
    }


def finalize(row: dict) -> None:
    row["total_votes"] = sum(int(row[field]) for field in FIELDS)
    row["margin"] = int(row["rep_votes"]) - int(row["dem_votes"])
    row["margin_pct"] = round(100.0 * row["margin"] / row["total_votes"], 4) if row["total_votes"] else 0.0
    row["winner"] = "REP" if row["margin"] > 0 else "DEM" if row["margin"] < 0 else "TIE"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--published-data-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--scope", choices=("state_house", "state_senate", "congressional"), default="state_house"
    )
    parser.add_argument("--lines-year", type=int, choices=(2022, 2026), default=2022)
    args = parser.parse_args()

    assignment, assignment_diagnostic = current_plan_assignment(
        args.data_dir, args.published_data_dir, args.scope, args.lines_year
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    report = []

    for year, contests in CONTESTS.items():
        csv_zip = args.data_dir / f"tn_{year}_gen_2020_blocks_csv.zip"
        shape_zip = args.data_dir / f"tn_{year}_gen_2020_blocks.zip"
        prefer_csv = csv_zip.exists()
        source_path = csv_zip if prefer_csv else shape_zip
        if not source_path.exists():
            continue
        fields = available_fields(source_path, prefer_csv)
        vote_columns = [
            column for column in fields
            if any(column.startswith(prefix) for prefix in contests.values())
        ]
        blocks = read_block_votes(source_path, ["GEOID20", *vote_columns], prefer_csv)
        blocks["GEOID20"] = blocks["GEOID20"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(15)
        joined = blocks.merge(assignment, on="GEOID20", how="inner", validate="many_to_one")

        for contest_type, prefix in contests.items():
            selected = [column for column in vote_columns if column.startswith(prefix)]
            raw_by_field = {field: {} for field in FIELDS}
            for field in FIELDS:
                columns = [column for column in selected if party_field(column, prefix) == field]
                series = joined[columns].apply(pd.to_numeric, errors="coerce").fillna(0).sum(axis=1)
                grouped = series.groupby(joined["district"]).sum()
                raw_by_field[field] = {str(district): float(value) for district, value in grouped.items()}

            contest_path = args.published_data_dir / "contests" / f"{contest_type}_{year}.json"
            district_subdir = (
                "district_contests_2026"
                if args.scope == "congressional" and args.lines_year == 2026
                else "district_contests"
            )
            district_path = (
                args.published_data_dir
                / district_subdir
                / f"{args.scope}_{contest_type}_{year}.json"
            )
            if not contest_path.exists() or not district_path.exists():
                continue
            contest_payload = load_json(contest_path)
            district_payload = load_json(district_path)
            targets = contest_targets(contest_payload)
            allocations = {
                field: largest_remainder(targets[field], raw_by_field[field]) for field in FIELDS
            }
            results = district_payload.get("general", {}).get("results", {})
            if args.scope == "congressional" and args.lines_year == 2026:
                anchors = {district for district in ("1", "2") if district in results}
                for field in FIELDS:
                    anchor_votes = {
                        district: int(results[district].get(field, 0) or 0)
                        for district in anchors
                    }
                    remaining_target = targets[field] - sum(anchor_votes.values())
                    if remaining_target < 0:
                        raise RuntimeError(f"2026 congressional anchors exceed {field} target")
                    adjustable_weights = {
                        district: weight
                        for district, weight in raw_by_field[field].items()
                        if district not in anchors
                    }
                    allocations[field] = {
                        **anchor_votes,
                        **largest_remainder(remaining_target, adjustable_weights),
                    }
            all_districts = sorted(set(results) | set(allocations["dem_votes"]), key=int)
            corrected = {}
            for district in all_districts:
                row = dict(results.get(district, {}))
                for field in FIELDS:
                    row[field] = int(allocations[field].get(district, 0))
                finalize(row)
                corrected[district] = row
            district_payload["general"] = {"results": corrected}
            district_payload.setdefault("meta", {})["block_disaggregated_method"] = (
                "RDH statewide votes disaggregated to 2020 Census blocks; district assignments "
                "inferred from 2022/2024 district contest columns when applicable, with enacted "
                f"{args.lines_year} TIGER representative-point assignment as fallback; "
                "party-specifically reconciled to certified statewide totals"
            )
            district_payload["meta"]["block_disaggregated_source"] = source_path.name
            district_payload["meta"]["block_assignment_audit"] = assignment_diagnostic
            if args.scope == "congressional" and args.lines_year == 2026:
                district_payload["meta"]["preserved_districts"] = ["1", "2"]
            out_path = args.output_dir / district_path.name
            out_path.write_text(json.dumps(district_payload, indent=2) + "\n", encoding="utf-8")
            report.append({
                "scope": args.scope,
                "contest_type": contest_type,
                "year": year,
                "file": out_path.name,
                "blocks_in_source": int(len(blocks)),
                "blocks_joined": int(len(joined)),
                "block_join_pct": round(100.0 * len(joined) / len(blocks), 6) if len(blocks) else 0.0,
                "targets": targets,
                "output_totals": {
                    field: sum(row[field] for row in corrected.values()) for field in FIELDS
                },
            })

    print(json.dumps({"files": report}, indent=2))


if __name__ == "__main__":
    main()
