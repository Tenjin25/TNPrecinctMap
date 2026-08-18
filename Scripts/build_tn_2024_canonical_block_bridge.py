#!/usr/bin/env python3
"""Bridge canonical 2024 precinct votes onto RDH 2020 blocks and 2026 districts."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
OUT = DATA / "reports" / "district_crosswalk_comparison"
OUT.mkdir(parents=True, exist_ok=True)
SUMMARY = OUT / "tn_2024_canonical_block_bridge_summary.json"
DISTRICT_OUT = OUT / "tn_2024_canonical_block_bridge_2026_districts.csv"
BRIDGE_OUT = OUT / "tn_2024_canonical_to_rdh_block_bridge.csv"


def numeric(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def main() -> None:
    block_url = "zip://Data/tn_2024_gen_2020_blocks.zip!tn_2024_gen_2020_blocks/tn_2024_gen_2020_blocks.shp"
    blocks = gpd.read_file(block_url, columns=["GEOID20", "COUNTYFP", "VAP_MOD", "geometry"]).to_crs(5070)
    blocks["block_area"] = blocks.geometry.area
    blocks["VAP_MOD"] = pd.to_numeric(blocks["VAP_MOD"], errors="coerce").fillna(0)

    canonical = gpd.read_file(DATA / "tn_voting_precincts.geojson", columns=["county_norm", "prec_id", "geometry"]).to_crs(5070)
    canonical["prec_id"] = canonical["prec_id"].astype(str).str.zfill(6)
    votes = json.loads((DATA / "contests" / "president_2024.json").read_text(encoding="utf-8"))["rows"]
    vote_map = {}
    for row in votes:
        county, prec_id = row["county"].split(" - ", 1)
        vote_map[(county.strip().upper(), prec_id.strip().zfill(6))] = {
            "dem": numeric(row.get("dem_votes")),
            "rep": numeric(row.get("rep_votes")),
            "other": numeric(row.get("other_votes")),
        }
    canonical["vote_key"] = list(zip(canonical["county_norm"].str.upper(), canonical["prec_id"]))
    for party in ("dem", "rep", "other"):
        canonical[party] = canonical["vote_key"].map(lambda key: vote_map.get(key, {}).get(party, 0.0))

    # Canonical precinct -> RDH block intersections.
    pairs = gpd.sjoin(
        blocks[["GEOID20", "VAP_MOD", "block_area", "geometry"]],
        canonical[["county_norm", "prec_id", "dem", "rep", "other", "geometry"]],
        how="inner", predicate="intersects",
    )
    canonical_geom = canonical.geometry.loc[pairs["index_right"]].array
    pairs["intersection_area"] = shapely.area(shapely.intersection(pairs.geometry.array, canonical_geom))
    pairs = pairs[pairs.intersection_area > 0].copy()
    pairs["area_fraction"] = pairs["intersection_area"] / pairs["block_area"]
    pairs["allocation_weight"] = pairs["VAP_MOD"] * pairs["area_fraction"]
    pairs.loc[pairs["allocation_weight"] <= 0, "allocation_weight"] = pairs.loc[pairs["allocation_weight"] <= 0, "area_fraction"]
    pairs["canonical_key"] = list(zip(pairs["county_norm"].str.upper(), pairs["prec_id"].astype(str).str.zfill(6)))
    pairs["weight_total"] = pairs.groupby("canonical_key")["allocation_weight"].transform("sum")
    pairs["canonical_weight"] = pairs["allocation_weight"] / pairs["weight_total"]
    for party in ("dem", "rep", "other"):
        pairs[f"synthetic_{party}"] = pairs[party] * pairs["canonical_weight"]

    bridge_cols = ["GEOID20", "county_norm", "prec_id", "canonical_weight", "intersection_area", "area_fraction"]
    pairs[bridge_cols].to_csv(BRIDGE_OUT, index=False, float_format="%.8f")

    # RDH block -> 2026 district assignment. Interior blocks are assigned by
    # centroid; boundary-block area weighting remains available in the audit
    # script, but is intentionally separated from this vote-preservation test.
    districts = gpd.read_file(DATA / "tl_2026_47_cd2026.geojson", columns=["DISTRICT", "geometry"]).to_crs(5070)
    block_centroids = blocks[["GEOID20", "geometry"]].copy()
    block_centroids.geometry = block_centroids.geometry.centroid
    district_pairs = gpd.sjoin(block_centroids, districts, how="inner", predicate="within")
    district_pairs["district_weight"] = 1.0
    district_pairs = district_pairs[["GEOID20", "DISTRICT", "district_weight"]]

    combined = pairs[["GEOID20", "synthetic_dem", "synthetic_rep", "synthetic_other"]].merge(district_pairs, on="GEOID20", how="inner")
    for party in ("dem", "rep", "other"):
        combined[party] = combined[f"synthetic_{party}"] * combined["district_weight"]
    district_result = combined.groupby("DISTRICT")[["dem", "rep", "other"]].sum().sort_index()
    district_result["total"] = district_result.sum(axis=1)
    district_result["margin"] = district_result["rep"] - district_result["dem"]
    district_result.reset_index().to_csv(DISTRICT_OUT, index=False, float_format="%.4f")

    canonical_allocated = pairs.groupby("canonical_key")["canonical_weight"].sum()
    summary = {
        "canonical_precincts": int(len(canonical)),
        "canonical_precincts_with_votes": int(sum(canonical[["dem", "rep", "other"]].sum(axis=1) > 0)),
        "canonical_precincts_with_block_overlap": int(len(canonical_allocated)),
        "canonical_precincts_without_block_overlap": int(len(canonical) - len(canonical_allocated)),
        "canonical_vote_totals": {party: float(canonical[party].sum()) for party in ("dem", "rep", "other")},
        "bridged_vote_totals": {party: float(district_result[party].sum()) for party in ("dem", "rep", "other")},
        "vote_preservation_error": {party: float(district_result[party].sum() - canonical[party].sum()) for party in ("dem", "rep", "other")},
        "outputs": {"bridge": str(BRIDGE_OUT), "districts": str(DISTRICT_OUT)},
    }
    SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(SUMMARY)
    print(DISTRICT_OUT)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
