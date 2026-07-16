#!/usr/bin/env python3
"""Materialize a durable Tennessee VTD10 GeoJSON with Census NAME10 labels."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"

VTD10_GEOJSON = DATA_DIR / "tn_vtd_2010_census_county_merged.geojson"
VTD10_ROOT_ZIP = DATA_DIR / "tl_2012_47_vtd10.zip"
VTD10_CENSUS_ZIP = DATA_DIR / "census" / "tl_2012_47_vtd10.zip"
OUT_GEOJSON = DATA_DIR / "tn_vtd_2010.geojson"
OUT_SUMMARY = DATA_DIR / "tn_vtd_2010_summary.json"


def source_path() -> Path:
    for path in [VTD10_GEOJSON, VTD10_ROOT_ZIP, VTD10_CENSUS_ZIP]:
        if path.exists():
            return path
    raise FileNotFoundError(
        f"Missing {VTD10_GEOJSON}, {VTD10_ROOT_ZIP}, and {VTD10_CENSUS_ZIP}"
    )


def read_source(path: Path) -> gpd.GeoDataFrame:
    return gpd.read_file(f"zip://{path.resolve()}" if path.suffix.lower() == ".zip" else path)


def normalize_vtd10(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    rename = {
        "STATEFP": "STATEFP10",
        "COUNTYFP": "COUNTYFP10",
        "VTDST": "VTDST10",
        "GEOID": "GEOID10",
        "NAME": "NAME10",
        "NAMELSAD": "NAMELSAD10",
        "VTDI": "VTDI10",
    }
    gdf = gdf.rename(columns={k: v for k, v in rename.items() if k in gdf.columns})
    for col in ["STATEFP10", "COUNTYFP10", "VTDST10", "GEOID10", "VTDI10", "NAME10", "NAMELSAD10"]:
        if col not in gdf.columns:
            gdf[col] = ""

    gdf["STATEFP10"] = gdf["STATEFP10"].astype(str).str.zfill(2)
    gdf["COUNTYFP10"] = gdf["COUNTYFP10"].astype(str).str.zfill(3)
    gdf["VTDST10"] = gdf["VTDST10"].astype(str).str.strip().str.zfill(4)
    gdf["GEOID10"] = gdf["GEOID10"].astype(str).str.strip()
    gdf.loc[gdf["GEOID10"] == "", "GEOID10"] = (
        gdf["STATEFP10"] + gdf["COUNTYFP10"] + gdf["VTDST10"]
    )
    gdf["VTDI10"] = gdf["VTDI10"].astype(str).str.strip()
    gdf["NAME10"] = gdf["NAME10"].astype(str).str.strip()
    gdf["NAMELSAD10"] = gdf["NAMELSAD10"].astype(str).str.strip()
    gdf.loc[gdf["NAMELSAD10"] == "", "NAMELSAD10"] = gdf["NAME10"]
    return gdf[
        [
            "STATEFP10",
            "COUNTYFP10",
            "VTDST10",
            "GEOID10",
            "VTDI10",
            "NAME10",
            "NAMELSAD10",
            "geometry",
        ]
    ].copy()


def main() -> None:
    path = source_path()
    gdf = normalize_vtd10(read_source(path)).to_crs(4326)
    OUT_GEOJSON.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(OUT_GEOJSON, driver="GeoJSON")

    summary = {
        "output": str(OUT_GEOJSON.relative_to(DATA_DIR)),
        "source": str(path.relative_to(DATA_DIR)),
        "rows": int(len(gdf)),
        "distinct_counties": int(gdf["COUNTYFP10"].nunique()),
        "distinct_geoid10": int(gdf["GEOID10"].nunique()),
        "name10_nonempty": int((gdf["NAME10"].astype(str).str.strip() != "").sum()),
    }
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
