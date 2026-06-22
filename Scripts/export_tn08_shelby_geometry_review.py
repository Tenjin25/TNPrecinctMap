from __future__ import annotations

import csv
import sys
from pathlib import Path

import geopandas as gpd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
INPUT_CSV = DATA_DIR / "reports" / "tn08_shelby_review_2024_president.csv"
PRECINCT_GEOJSON = DATA_DIR / "tn_voting_precincts.geojson"
CATALOG_CSV = DATA_DIR / "crosswalks" / "tn_blockassign_vtd_with_names.csv"
OUTPUT_CSV = DATA_DIR / "reports" / "tn08_shelby_geometry_review_2024_president.csv"


def load_catalog_names() -> dict[str, str]:
    out: dict[str, str] = {}
    with CATALOG_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if str(row.get("county_fips", "")).strip() != "157":
                continue
            code = str(row.get("vtd_code", "")).strip()
            name = str(row.get("vtd_name", "")).strip()
            if code:
                out[code] = name
    return out


def classify_geometry_status(share: float) -> str:
    if share >= 0.98:
        return "core_tn08"
    if share >= 0.25:
        return "boundary_split"
    return "sliver_only"


def main() -> None:
    catalog_names = load_catalog_names()
    precinct_gdf = gpd.read_file(PRECINCT_GEOJSON)
    if precinct_gdf.crs is None:
        precinct_gdf = precinct_gdf.set_crs(4326)
    precinct_gdf = precinct_gdf.to_crs(4326)
    precinct_lookup = {
        str(row["prec_id"]).strip(): row
        for _, row in precinct_gdf.iterrows()
        if str(row.get("county_norm", "")).strip().upper() == "SHELBY"
    }

    rows = []
    with INPUT_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = str(row.get("code", "")).strip()
            share = float(row.get("district8_share", 0) or 0)
            geom_row = precinct_lookup.get(code)
            centroid_lon = ""
            centroid_lat = ""
            precinct_name = ""
            if geom_row is not None and geom_row.geometry is not None and not geom_row.geometry.is_empty:
                centroid = geom_row.geometry.centroid
                centroid_lon = f"{float(centroid.x):.6f}"
                centroid_lat = f"{float(centroid.y):.6f}"
                precinct_name = str(geom_row.get("precinct_name", "")).strip()
            row["vtd_name"] = catalog_names.get(code, "")
            row["precinct_geometry_name"] = precinct_name
            row["centroid_lon"] = centroid_lon
            row["centroid_lat"] = centroid_lat
            row["geometry_status"] = classify_geometry_status(share)
            rows.append(row)

    rows.sort(
        key=lambda item: (
            {"core_tn08": 0, "boundary_split": 1, "sliver_only": 2}.get(item["geometry_status"], 9),
            float(item.get("margin_pct_to_8", 0) or 0),
            -float(item.get("total_votes_to_8", 0) or 0),
        )
    )

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    core_count = sum(1 for row in rows if row["geometry_status"] == "core_tn08")
    split_count = sum(1 for row in rows if row["geometry_status"] == "boundary_split")
    sliver_count = sum(1 for row in rows if row["geometry_status"] == "sliver_only")
    print(
        f"Wrote {OUTPUT_CSV.relative_to(ROOT)} with {len(rows)} rows "
        f"(core_tn08={core_count}, boundary_split={split_count}, sliver_only={sliver_count})"
    )


if __name__ == "__main__":
    main()
