#!/usr/bin/env python3
"""Materialize a durable Tennessee VTD20 GeoJSON with vintage-specific names.

Preferred source order:
  1. Data/tl_2020_47_vtd20.zip
  2. Data/tn_vtd_2020_census_statewide.geojson
  3. Data/crosswalks/tn_dra_vtd20_boundaries_v07.geojson

The DRA fallback is already used elsewhere in the project for VTD20 geometry.
When using it, this script enriches names from
Data/crosswalks/tn_precinct_friendly_names_2020.json where possible.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Tuple

import geopandas as gpd

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"

VTD20_ZIP = DATA_DIR / "tl_2020_47_vtd20.zip"
LEGACY_CENSUS_GEOJSON = DATA_DIR / "tn_vtd_2020_census_statewide.geojson"
DRA_VTD20_GEOJSON = XWALK_DIR / "tn_dra_vtd20_boundaries_v07.geojson"
COUNTY_GEOJSON = DATA_DIR / "tl_2020_47_county20.geojson"
FRIENDLY_NAMES_JSON = XWALK_DIR / "tn_precinct_friendly_names_2020.json"
OUT_GEOJSON = DATA_DIR / "tn_vtd_2020.geojson"
OUT_SUMMARY = DATA_DIR / "tn_vtd_2020_summary.json"


def read_county_names() -> Dict[str, str]:
    if not COUNTY_GEOJSON.exists():
        return {}
    counties = gpd.read_file(COUNTY_GEOJSON, columns=["COUNTYFP20", "NAME20"])
    return {
        str(row.get("COUNTYFP20", "")).zfill(3): str(row.get("NAME20", "")).strip()
        for _, row in counties.iterrows()
        if str(row.get("COUNTYFP20", "")).strip()
    }


def read_friendly_names() -> Dict[Tuple[str, str], str]:
    if not FRIENDLY_NAMES_JSON.exists():
        return {}
    payload = json.loads(FRIENDLY_NAMES_JSON.read_text(encoding="utf-8"))
    out = {}
    for county_name, labels in (payload.get("counties", {}) or {}).items():
        county_norm = str(county_name or "").strip().upper()
        for code, label in (labels or {}).items():
            vtdst = str(code or "").strip().zfill(6)
            name = str(label or "").strip()
            if county_norm and vtdst and name:
                out[(county_norm, vtdst)] = name
    return out


def normalize_census_vtd(gdf: gpd.GeoDataFrame, source: str) -> gpd.GeoDataFrame:
    rename = {
        "STATEFP": "STATEFP20",
        "COUNTYFP": "COUNTYFP20",
        "VTDST": "VTDST20",
        "GEOID": "GEOID20",
        "NAME": "NAME20",
        "NAMELSAD": "NAMELSAD20",
        "VTDI": "VTDI20",
    }
    gdf = gdf.rename(columns={k: v for k, v in rename.items() if k in gdf.columns})
    for col in ["STATEFP20", "COUNTYFP20", "VTDST20", "GEOID20", "NAME20", "NAMELSAD20", "VTDI20"]:
        if col not in gdf.columns:
            gdf[col] = ""
    gdf["STATEFP20"] = gdf["STATEFP20"].astype(str).str.zfill(2)
    gdf["COUNTYFP20"] = gdf["COUNTYFP20"].astype(str).str.zfill(3)
    gdf["VTDST20"] = gdf["VTDST20"].astype(str).str.strip().str.zfill(6)
    gdf["GEOID20"] = gdf["GEOID20"].astype(str).str.strip()
    gdf.loc[gdf["GEOID20"] == "", "GEOID20"] = (
        gdf["STATEFP20"] + gdf["COUNTYFP20"] + gdf["VTDST20"]
    )
    gdf["NAME20"] = gdf["NAME20"].astype(str).str.strip()
    gdf["NAMELSAD20"] = gdf["NAMELSAD20"].astype(str).str.strip()
    gdf.loc[gdf["NAMELSAD20"] == "", "NAMELSAD20"] = gdf["NAME20"]
    gdf["VTDI20"] = gdf["VTDI20"].astype(str).str.strip()
    gdf["vtd20_name_source"] = source
    return gdf[
        [
            "STATEFP20",
            "COUNTYFP20",
            "VTDST20",
            "GEOID20",
            "NAME20",
            "NAMELSAD20",
            "VTDI20",
            "vtd20_name_source",
            "geometry",
        ]
    ].copy()


def build_from_dra() -> gpd.GeoDataFrame:
    if not DRA_VTD20_GEOJSON.exists():
        raise FileNotFoundError(f"Missing {DRA_VTD20_GEOJSON}")
    county_names = read_county_names()
    friendly_names = read_friendly_names()
    gdf = gpd.read_file(DRA_VTD20_GEOJSON)
    if "id" not in gdf.columns:
        raise RuntimeError("DRA VTD20 GeoJSON missing id field")
    gdf["GEOID20"] = gdf["id"].astype(str).str.strip()
    gdf = gdf[gdf["GEOID20"].str.match(r"^47\d{9}$", na=False)].copy()
    if gdf.empty:
        raise RuntimeError("DRA VTD20 GeoJSON had no GEOID20-like ids")
    gdf["STATEFP20"] = gdf["GEOID20"].str.slice(0, 2)
    gdf["COUNTYFP20"] = gdf["GEOID20"].str.slice(2, 5)
    gdf["VTDST20"] = gdf["GEOID20"].str.slice(5, 11)
    gdf["county_name"] = gdf["COUNTYFP20"].map(county_names).fillna("")
    gdf["dra_name"] = gdf.get("name", "").astype(str).str.strip()
    gdf["friendly_name"] = [
        friendly_names.get((county.upper(), vtdst), "")
        for county, vtdst in zip(gdf["county_name"], gdf["VTDST20"])
    ]
    gdf["NAME20"] = gdf["friendly_name"]
    gdf.loc[gdf["NAME20"] == "", "NAME20"] = gdf["dra_name"]
    gdf["NAMELSAD20"] = gdf["NAME20"]
    gdf["VTDI20"] = ""
    gdf["vtd20_name_source"] = [
        "tn_precinct_friendly_names_2020"
        if friendly
        else "tn_dra_vtd20_boundaries_v07"
        for friendly in gdf["friendly_name"]
    ]
    return gdf[
        [
            "STATEFP20",
            "COUNTYFP20",
            "VTDST20",
            "GEOID20",
            "NAME20",
            "NAMELSAD20",
            "VTDI20",
            "vtd20_name_source",
            "geometry",
        ]
    ].copy()


def main() -> None:
    if VTD20_ZIP.exists():
        gdf = normalize_census_vtd(gpd.read_file(f"zip://{VTD20_ZIP.resolve()}"), VTD20_ZIP.name)
        source = str(VTD20_ZIP.relative_to(DATA_DIR))
    elif LEGACY_CENSUS_GEOJSON.exists():
        gdf = normalize_census_vtd(gpd.read_file(LEGACY_CENSUS_GEOJSON), LEGACY_CENSUS_GEOJSON.name)
        source = str(LEGACY_CENSUS_GEOJSON.relative_to(DATA_DIR))
    else:
        gdf = build_from_dra()
        source = str(DRA_VTD20_GEOJSON.relative_to(DATA_DIR))

    gdf = gdf.to_crs(4326)
    OUT_GEOJSON.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(OUT_GEOJSON, driver="GeoJSON")

    summary = {
        "output": str(OUT_GEOJSON.relative_to(DATA_DIR)),
        "source": source,
        "rows": int(len(gdf)),
        "name20_nonempty": int((gdf["NAME20"].astype(str).str.strip() != "").sum()),
        "name_sources": {
            str(k): int(v)
            for k, v in gdf["vtd20_name_source"].value_counts().sort_index().items()
        },
    }
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
