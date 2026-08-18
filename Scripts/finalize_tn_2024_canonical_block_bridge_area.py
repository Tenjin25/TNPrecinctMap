#!/usr/bin/env python3
"""Finalize the canonical 2024 -> RDH blocks -> 2026 district bridge."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pandas as pd
import shapely


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
OUT = DATA / "reports" / "district_crosswalk_comparison"
BRIDGE = OUT / "tn_2024_canonical_to_rdh_block_bridge.csv"
DISTRICT_OUT = OUT / "tn_2024_canonical_block_bridge_2026_districts_area_weighted.csv"
SUMMARY = OUT / "tn_2024_canonical_block_bridge_area_summary.json"


def num(value: object) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def main() -> None:
    bridge = pd.read_csv(BRIDGE)
    bridge["GEOID20"] = bridge["GEOID20"].astype(str)
    votes = json.loads((DATA / "contests" / "president_2024.json").read_text(encoding="utf-8"))["rows"]
    vote_map = {}
    for row in votes:
        county, prec_id = row["county"].split(" - ", 1)
        vote_map[(county.strip().upper(), prec_id.strip().zfill(6))] = {
            "dem": num(row.get("dem_votes")), "rep": num(row.get("rep_votes")), "other": num(row.get("other_votes")),
        }
    for party in ("dem", "rep", "other"):
        bridge[f"synthetic_{party}"] = [
            vote_map.get((str(c).upper(), str(p).zfill(6)), {}).get(party, 0.0) * w
            for c, p, w in zip(bridge.county_norm, bridge.prec_id, bridge.canonical_weight)
        ]

    block_url = "zip://Data/tn_2024_gen_2020_blocks.zip!tn_2024_gen_2020_blocks/tn_2024_gen_2020_blocks.shp"
    blocks = gpd.read_file(block_url, columns=["GEOID20", "geometry"]).to_crs(5070)
    blocks["GEOID20"] = blocks["GEOID20"].astype(str)
    blocks["block_area"] = blocks.geometry.area
    districts = gpd.read_file(DATA / "tl_2026_47_cd2026.geojson", columns=["DISTRICT", "geometry"]).to_crs(5070)

    # Interior blocks use centroid assignment. Only blocks near a district
    # boundary require polygon intersections.
    centroids = blocks[["GEOID20", "geometry"]].copy()
    centroids.geometry = centroids.geometry.centroid
    centroid_join = gpd.sjoin(centroids, districts, how="left", predicate="within")[["GEOID20", "DISTRICT"]]
    blocks = blocks.merge(centroid_join, on="GEOID20", how="left")
    boundary = shapely.union_all(districts.geometry.array).boundary
    boundary_mask = shapely.intersects(blocks.geometry.array, shapely.buffer(boundary, 1000))

    interior = blocks.loc[~boundary_mask, ["GEOID20", "DISTRICT"]].copy()
    interior["district_weight"] = 1.0
    pieces = [interior]
    boundary_blocks = blocks.loc[boundary_mask].copy()
    for district_row in districts.itertuples():
        idx = boundary_blocks.sindex.query(district_row.geometry, predicate="intersects")
        if len(idx) == 0:
            continue
        subset = boundary_blocks.iloc[idx][["GEOID20", "block_area", "geometry"]].copy()
        subset["district_area"] = shapely.area(shapely.intersection(subset.geometry.array, district_row.geometry))
        subset = subset[subset.district_area > 0].copy()
        subset["DISTRICT"] = district_row.DISTRICT
        subset["district_weight"] = subset.district_area / subset.block_area
        pieces.append(subset[["GEOID20", "DISTRICT", "district_weight"]])
    district_weights = pd.concat(pieces, ignore_index=True)
    district_weights["weight_sum"] = district_weights.groupby("GEOID20")["district_weight"].transform("sum")
    district_weights["district_weight"] /= district_weights["weight_sum"]
    district_weights = district_weights[["GEOID20", "DISTRICT", "district_weight"]]

    combined = bridge[["GEOID20", "synthetic_dem", "synthetic_rep", "synthetic_other"]].merge(district_weights, on="GEOID20", how="inner")
    for party in ("dem", "rep", "other"):
        combined[party] = combined[f"synthetic_{party}"] * combined.district_weight
    result = combined.groupby("DISTRICT")[["dem", "rep", "other"]].sum().sort_index()
    result["total"] = result.sum(axis=1)
    result["margin"] = result.rep - result.dem
    result.reset_index().to_csv(DISTRICT_OUT, index=False, float_format="%.4f")

    canonical_totals = {party: float(bridge[f"synthetic_{party}"].sum()) for party in ("dem", "rep", "other")}
    district_totals = {party: float(result[party].sum()) for party in ("dem", "rep", "other")}
    summary = {
        "bridge_rows": int(len(bridge)),
        "district_weight_rows": int(len(district_weights)),
        "blocks_with_multiple_districts": int((district_weights.groupby("GEOID20").size() > 1).sum()),
        "canonical_totals": canonical_totals,
        "district_totals": district_totals,
        "vote_preservation_error": {p: district_totals[p] - canonical_totals[p] for p in canonical_totals},
        "outputs": {"districts": str(DISTRICT_OUT)},
    }
    SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(DISTRICT_OUT)


if __name__ == "__main__":
    main()
