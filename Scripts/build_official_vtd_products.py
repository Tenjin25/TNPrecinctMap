#!/usr/bin/env python3
"""Build frontend, lineage, and modern-district products from official TN VTDs.

The Tennessee Comptroller election-date polygons remain the precinct authority.
NHGIS block crosswalks translate historical blocks to 2020 blocks; official Census
block equivalency files then assign those blocks to enacted district plans.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import geopandas as gpd
import pandas as pd

import integrate_official_vtd_archive as official


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
YEARS = official.PRIORITY
FIELDS = ("dem_votes", "rep_votes", "other_votes")
LAYER_BY_YEAR = {year: f"VTD_{year}_November_State_General" for year in YEARS}
ELECTION_FILE_BY_YEAR = {
    2008: "20081104__tn__general__precinct.csv",
    2012: "20121106__tn__general__precinct.csv",
    2014: "20141104__tn__general__precinct.csv",
    2016: "20161108__tn__general__precinct.csv",
    2018: "20181106__tn__general__precinct.csv",
    2020: "20201103__tn__general__precinct.csv",
    2022: "20221108__tn__general__precinct.csv",
    2024: "20241105__tn__general__precinct.csv",
}
OFFICE_MAP = {
    "PRESIDENT": "president",
    "UNITED STATES SENATE": "us_senate",
    "US SENATE": "us_senate",
    "U S SENATE": "us_senate",
    "GOVERNOR": "governor",
}


def block_transfer_to_2020(vintage: int) -> dict[str, list[tuple[str, float]]]:
    chain = DATA / "crosswalks" / "block_chain"
    if vintage == 2020:
        path = chain / "hop_tabblock20_to_vtd20_blockassign.csv"
        out = {}
        with path.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                block = row["block_geoid"]
                out[block] = [(block, 1.0)]
        return out
    first = defaultdict(list)
    path = chain / (
        "hop_tabblock00_to_tabblock10_nhgis.csv"
        if vintage == 2000 else "hop_tabblock10_to_tabblock20_nhgis.csv"
    )
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            source = row[f"block_geoid_{vintage}"]
            target = row["block_geoid_2010" if vintage == 2000 else "block_geoid_2020"]
            first[source].append((target, float(row["xwalk_weight"])))
    if vintage == 2010:
        return dict(first)
    second = defaultdict(list)
    with (chain / "hop_tabblock10_to_tabblock20_nhgis.csv").open(
        encoding="utf-8-sig", newline=""
    ) as handle:
        for row in csv.DictReader(handle):
            second[row["block_geoid_2010"]].append(
                (row["block_geoid_2020"], float(row["xwalk_weight"]))
            )
    return {
        source: [
            (block20, weight1 * weight2)
            for block10, weight1 in links
            for block20, weight2 in second.get(block10, [])
        ]
        for source, links in first.items()
    }


def feature_block20_mass(
    gdf: gpd.GeoDataFrame, block_dir: Path, vintage: int
) -> tuple[dict[int, Counter], dict]:
    blocks = gpd.read_file(official.block_path(block_dir, vintage))
    suffix = str(vintage)[-2:]
    geoid_col = next(
        c for c in (f"GEOID{suffix}", f"BLKIDFP{suffix}", "GEOID20", "GEOID10", "GEOID00", "GEOID")
        if c in blocks.columns
    )
    pop_col = next(
        (c for c in (f"POP{suffix}", "POP20", "POP10", "POP00") if c in blocks.columns),
        None,
    )
    columns = [geoid_col] + ([pop_col] if pop_col else []) + ["geometry"]
    blocks = blocks[columns].to_crs(gdf.crs)
    blocks["mass"] = (
        pd.to_numeric(blocks[pop_col], errors="coerce").fillna(0).astype(float)
        if pop_col else 0.0
    )
    zero = blocks["mass"] <= 0
    blocks.loc[zero, "mass"] = blocks.loc[zero].geometry.area.clip(lower=1)
    points = blocks.copy()
    points.geometry = points.geometry.representative_point()
    polygon_index = gpd.GeoDataFrame(
        {"feature_id": gdf.index, "polygon_area": gdf.geometry.area},
        geometry=gdf.geometry,
        crs=gdf.crs,
    )
    joined = gpd.sjoin(
        points,
        polygon_index,
        predicate="within",
        how="inner",
    )
    # A few official vintages contain overlapping polygons.  Assign each Census
    # block once, preferring the smaller containing precinct at an overlap.
    joined = joined.sort_values([geoid_col, "polygon_area", "feature_id"]).drop_duplicates(
        geoid_col, keep="first"
    )
    transfer = block_transfer_to_2020(vintage)
    result: dict[int, Counter] = defaultdict(Counter)
    missing = 0
    for _, row in joined.iterrows():
        links = transfer.get(str(row[geoid_col]), [])
        if not links:
            missing += 1
            continue
        for block20, share in links:
            result[int(row["feature_id"])][block20] += float(row["mass"]) * float(share)
    return result, {
        "vintage": vintage,
        "source_blocks": int(len(blocks)),
        "joined_blocks": int(len(joined)),
        "unlinked_blocks": int(missing),
        "features": int(len(result)),
    }


def load_assignments() -> dict[tuple[str, int], dict[str, str]]:
    specs = {
        ("state_house", 2022): (DATA / "47_TN_SLDL22.txt", "SLDLST"),
        ("state_senate", 2022): (DATA / "47_TN_SLDU22.txt", "SLDUST"),
        ("congressional", 2022): (DATA / "47_TN_CD118.txt", "CDFP"),
        ("congressional", 2026): (DATA / "CD120_47.txt", "CDFP"),
    }
    output = {}
    for key, (path, field) in specs.items():
        frame = pd.read_csv(path, dtype=str, skipinitialspace=True)
        frame["GEOID"] = frame["GEOID"].astype(str).str.zfill(15)
        frame[field] = pd.to_numeric(frame[field]).astype(int).astype(str)
        if len(frame) != frame["GEOID"].nunique():
            raise RuntimeError(f"Duplicate GEOID assignments in {path}")
        output[key] = dict(zip(frame["GEOID"], frame[field]))
    return output


def feature_district_weights(
    feature_blocks: dict[int, Counter], assignments: dict[tuple[str, int], dict[str, str]]
) -> dict[tuple[int, str, int], dict[str, float]]:
    result = {}
    for feature_id, blocks in feature_blocks.items():
        for (scope, lines_year), assignment in assignments.items():
            counts = Counter()
            for block20, mass in blocks.items():
                district = assignment.get(block20)
                if district:
                    counts[district] += float(mass)
            total = sum(counts.values())
            weights = {district: mass / total for district, mass in counts.items()} if total else {}
            if weights and max(weights.values()) >= 0.999:
                winner = max(weights, key=weights.get)
                weights = {winner: 1.0}
            result[(feature_id, scope, lines_year)] = weights
    return result


def slug(value: object) -> str:
    return re.sub(r"[^A-Z0-9]+", "_", official.norm(value)).strip("_") or "UNKNOWN"


def feature_code(year: int, feature_id: int) -> str:
    return f"H{year}-{feature_id:04d}"


def party_field(value: object) -> str:
    party = official.norm(value)
    if party in {"D", "DEM", "DEMOCRAT", "DEMOCRATIC"}:
        return "dem_votes"
    if party in {"R", "REP", "REPUBLICAN"}:
        return "rep_votes"
    return "other_votes"


def contest_type(value: object) -> str:
    office = official.norm(value)
    for label, kind in OFFICE_MAP.items():
        if office == label or office.startswith(label + " "):
            return kind
    return ""


def build_historical_contests(
    year: int,
    sources: list[Path],
    matched: list[dict],
    unmatched: list[dict],
    nongeo: list[dict],
    out_dir: Path,
) -> list[dict]:
    matched_key = {
        (row["county"], row["precinct_norm"]): feature_code(year, int(row["feature_id"]))
        for row in matched
    }
    unmatched_keys = {(row["county"], row["precinct_norm"]) for row in unmatched}
    nongeo_keys = {(row["county"], row["precinct_norm"]) for row in nongeo}
    totals = defaultdict(lambda: {field: 0 for field in FIELDS})
    candidates = defaultdict(Counter)
    for source in sources:
        with source.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                kind = contest_type(row.get("office"))
                if not kind:
                    continue
                county = official.norm_county(row.get("county"))
                precinct = official.norm(row.get("precinct"))
                key = (county, precinct)
                if key in matched_key:
                    code = matched_key[key]
                elif key in nongeo_keys or official.NON_GEO.search(str(row.get("precinct") or "")):
                    code = f"NG-{slug(precinct)}"
                else:
                    code = f"UNM-{slug(precinct)}"
                    unmatched_keys.add(key)
                label = f"{county} - {code}"
                field = party_field(row.get("party"))
                votes = int(round(float(row.get("votes") or 0)))
                totals[(kind, label)][field] += votes
                candidate = str(row.get("candidate") or "").strip()
                if candidate:
                    candidates[(kind, field)][candidate] += votes
    manifests = []
    for kind in sorted({key[0] for key in totals}):
        dem_candidate = candidates[(kind, "dem_votes")].most_common(1)
        rep_candidate = candidates[(kind, "rep_votes")].most_common(1)
        rows = []
        for (row_kind, label), values in sorted(totals.items()):
            if row_kind != kind:
                continue
            dem = int(values["dem_votes"])
            rep = int(values["rep_votes"])
            other = int(values["other_votes"])
            total = dem + rep + other
            margin = rep - dem
            rows.append({
                "county": label,
                "dem_votes": dem,
                "rep_votes": rep,
                "other_votes": other,
                "total_votes": total,
                "dem_candidate": dem_candidate[0][0] if dem_candidate else "",
                "rep_candidate": rep_candidate[0][0] if rep_candidate else "",
                "margin": margin,
                "margin_pct": round(100 * margin / total, 4) if total else 0.0,
                "winner": "REP" if margin > 0 else "DEM" if margin < 0 else "TIE",
                "color": "",
            })
        path = out_dir / f"{kind}_{year}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "contest_type": kind,
            "year": year,
            "meta": {
                "sources": [source.name for source in sources],
                "geography": LAYER_BY_YEAR[year],
                "authority": "Tennessee Comptroller of the Treasury and county election commissions",
                "unmatched_labels": len(unmatched_keys),
                "non_geographic_labels": len(nongeo_keys),
            },
            "rows": rows,
        }, indent=2) + "\n", encoding="utf-8")
        manifests.append({
            "year": year,
            "contest_type": kind,
            "file": path.name,
            "rows": len(rows),
            "total_votes": sum(row["total_votes"] for row in rows),
        })
    return manifests


def export_geometry(
    year: int, gdf: gpd.GeoDataFrame, catalog: pd.DataFrame, path: Path
) -> None:
    frame = gdf.to_crs(4326).copy()
    frame.geometry = frame.geometry.simplify(0.00015, preserve_topology=True)
    properties = []
    for idx in frame.index:
        row = catalog.loc[idx]
        properties.append({
            "county_nam": row["county"],
            "county_norm": row["county"],
            "prec_id": feature_code(year, int(idx)),
            "precinct_full_name": str(row["raw_name"] or "").strip(),
            "official_feature_id": int(idx),
            "official_vtd_code": str(row["code"] or ""),
            "election_year": year,
        })
    out = gpd.GeoDataFrame(properties, geometry=frame.geometry, crs=4326)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(out.to_json(drop_id=True, separators=(",", ":")), encoding="utf-8")


def build_lineage(
    feature_blocks_by_year: dict[int, dict[int, Counter]],
    catalogs: dict[int, pd.DataFrame],
) -> list[dict]:
    rows = []
    for from_year, to_year in zip(YEARS, YEARS[1:]):
        source = feature_blocks_by_year[from_year]
        target = feature_blocks_by_year[to_year]
        target_by_block = defaultdict(Counter)
        for feature_id, blocks in target.items():
            for block, mass in blocks.items():
                target_by_block[block][feature_id] += float(mass)
        for from_feature, blocks in source.items():
            overlap = Counter()
            for block, source_mass in blocks.items():
                candidates = target_by_block.get(block, {})
                target_total = sum(candidates.values())
                if target_total <= 0:
                    continue
                for to_feature, target_mass in candidates.items():
                    overlap[to_feature] += float(source_mass) * float(target_mass) / target_total
            total = sum(overlap.values())
            if total <= 0:
                continue
            weights = {feature: mass / total for feature, mass in overlap.items()}
            for to_feature, weight in sorted(weights.items(), key=lambda item: item[1], reverse=True):
                if weight < 0.001:
                    continue
                source_row = catalogs[from_year].loc[from_feature]
                target_row = catalogs[to_year].loc[to_feature]
                rows.append({
                    "from_year": from_year,
                    "to_year": to_year,
                    "from_feature_id": from_feature,
                    "to_feature_id": to_feature,
                    "from_county": source_row["county"],
                    "to_county": target_row["county"],
                    "from_name": source_row["raw_name"],
                    "to_name": target_row["raw_name"],
                    "weight": round(weight, 12),
                    "relationship": "stable" if weight >= 0.999 else "dominant" if weight >= 0.5 else "split",
                })
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--block-dir", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=DATA)
    args = parser.parse_args()

    fips_names, counties = official.county_lookup()
    assignments = load_assignments()
    feature_blocks_by_year = {}
    catalogs = {}
    direct_rows = []
    contest_manifest = []
    geometry_manifest = []
    diagnostics = []

    for year in YEARS:
        layer = LAYER_BY_YEAR[year]
        raw = official.read_layer(args.archive, layer)
        gdf, repair_stats = official.repair(raw)
        catalog = official.official_catalog(gdf, counties, fips_names)
        catalogs[year] = catalog
        source = args.output_root / ELECTION_FILE_BY_YEAR[year]
        matched, unmatched, nongeo = official.match_results(official.result_precincts(source), catalog)
        vintage = 2000 if year <= 2008 else 2010 if year <= 2018 else 2020
        feature_blocks, block_stats = feature_block20_mass(gdf, args.block_dir, vintage)
        feature_blocks_by_year[year] = feature_blocks
        district_weights = feature_district_weights(feature_blocks, assignments)
        # Keep district weights for every official feature, including features that
        # do not have a safely matched results label.  Those unmatched geometries
        # provide a neutral county/block footprint for residual-vote allocation;
        # they are not treated as precinct-result matches.
        matched_by_feature = {int(row["feature_id"]): row for row in matched}
        for (feature_id, scope, lines_year), weights in district_weights.items():
            result_match = matched_by_feature.get(int(feature_id))
            catalog_row = catalog.loc[int(feature_id)]
            feature_mass = sum(feature_blocks.get(int(feature_id), {}).values())
            for district, weight in sorted(weights.items(), key=lambda item: int(item[0])):
                direct_rows.append({
                    "year": year,
                    "county_norm": catalog_row["county"],
                    "from_precinct_norm": (
                        result_match["precinct_norm"] if result_match
                        else official.norm(catalog_row["raw_name"])
                    ),
                    "official_feature_id": int(feature_id),
                    "scope": scope,
                    "lines_year": lines_year,
                    "district": district,
                    "weight": round(weight, 12),
                    "feature_mass": round(feature_mass, 12),
                    "match_method": (
                        result_match["match_method"] if result_match
                        else "official_geometry_unmatched_result"
                    ),
                    "match_score": result_match["confidence"] if result_match else 0.0,
                })
        geometry_path = args.output_root / "historical_vtd" / f"tn_official_vtd_{year}.geojson"
        export_geometry(year, gdf, catalog, geometry_path)
        geometry_manifest.append({
            "year": year,
            "file": geometry_path.name,
            "features": int(len(gdf)),
            "bytes": geometry_path.stat().st_size,
        })
        canonical = args.output_root / "canonical_precinct_csvs" / ELECTION_FILE_BY_YEAR[year]
        contest_sources = [canonical if canonical.exists() else source]
        if year == 2022:
            governor = args.output_root / "canonical_precinct_csvs" / "20221108__tn__general__governor__precinct.csv"
            if governor.exists():
                contest_sources.append(governor)
        contest_manifest.extend(build_historical_contests(
            year, contest_sources, matched, unmatched, nongeo,
            args.output_root / "historical_precinct_contests",
        ))
        diagnostics.append({
            "year": year,
            "layer": layer,
            "matched": len(matched),
            "unmatched": len(unmatched),
            "non_geographic": len(nongeo),
            **repair_stats,
            **block_stats,
        })

    official.write_csv(
        args.output_root / "crosswalks" / "tn_official_precinct_to_modern_districts.csv",
        direct_rows,
    )
    lineage_rows = build_lineage(feature_blocks_by_year, catalogs)
    official.write_csv(args.output_root / "crosswalks" / "tn_official_vtd_lineage.csv", lineage_rows)
    (args.output_root / "historical_vtd" / "manifest.json").write_text(
        json.dumps({"files": geometry_manifest}, indent=2) + "\n", encoding="utf-8"
    )
    (args.output_root / "historical_precinct_contests" / "manifest.json").write_text(
        json.dumps({"files": sorted(contest_manifest, key=lambda row: (row["year"], row["contest_type"]))}, indent=2) + "\n",
        encoding="utf-8",
    )
    report = {
        "generated_by": Path(__file__).name,
        "authoritative_source": "Tennessee Comptroller of the Treasury and county election commissions",
        "nhgis_role": "historical block translation only",
        "district_assignment_sources": ["47_TN_SLDL22.txt", "47_TN_SLDU22.txt", "47_TN_CD118.txt", "CD120_47.txt"],
        "years": diagnostics,
        "geometry_files": geometry_manifest,
        "historical_contest_files": len(contest_manifest),
        "direct_district_rows": len(direct_rows),
        "lineage_rows": len(lineage_rows),
    }
    reports = args.output_root / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    (reports / "official_vtd_products_summary.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
