#!/usr/bin/env python3
"""Download and merge official Census 2010 Tennessee county VTD shapefiles.

This script pulls the county-level 2010 VTD ZIPs from the official Census
TIGER2010 directory, stores the raw ZIPs under ``Data/raw/census_vtd_2010``,
and writes a merged statewide GeoJSON that can be used as a higher-confidence
source for historical Tennessee precinct/VTD overlap work.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import urllib.request
from pathlib import Path
from typing import List


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
RAW_DIR = DATA_DIR / "raw" / "census_vtd_2010"
OUT_PATH = DATA_DIR / "tn_vtd_2010_census_county_merged.geojson"
SUMMARY_PATH = DATA_DIR / "census_vtd_2010_fetch_summary.json"
COUNTY_PATH = DATA_DIR / "tl_2020_47_county20.geojson"
BASE_URL = "https://www2.census.gov/geo/tiger/TIGER2010/VTD/2010"


def load_county_fips() -> List[str]:
    payload = json.loads(COUNTY_PATH.read_text(encoding="utf-8"))
    county_fips = []
    for feat in payload.get("features", []):
        props = feat.get("properties", {}) or {}
        county_fp = str(props.get("COUNTYFP20", "")).zfill(3)
        if county_fp and county_fp not in county_fips:
            county_fips.append(county_fp)
    if not county_fips:
        raise RuntimeError(f"No county FIPS found in {COUNTY_PATH}")
    return sorted(county_fips)


def ensure_vendor_path() -> None:
    env_vendor = os.environ.get("TNPRECINCTMAP_GEO_VENDOR", "").strip()
    candidates = []
    if env_vendor:
        candidates.append(Path(env_vendor))
    candidates.extend([ROOT / "vendor_geo", ROOT / ".vendor" / "geo"])
    for candidate in candidates:
        if candidate.exists():
            sys.path.insert(0, str(candidate))
            return


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url) as response, dest.open("wb") as handle:
        shutil.copyfileobj(response, handle)


def merge_zipfiles(zip_paths: List[Path]) -> dict:
    ensure_vendor_path()
    import geopandas as gpd
    import pandas as pd

    parts = []
    for zip_path in zip_paths:
        gdf = gpd.read_file(f"zip://{zip_path.resolve()}")
        parts.append(gdf)

    merged = gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs=parts[0].crs)
    merged.to_file(OUT_PATH, driver="GeoJSON")

    county_counts = {}
    county_col = "COUNTYFP10" if "COUNTYFP10" in merged.columns else None
    if county_col:
        for county_fp, count in merged[county_col].astype(str).str.zfill(3).value_counts().items():
            county_counts[county_fp] = int(count)

    return {
        "rows": int(len(merged)),
        "columns": list(merged.columns),
        "crs": str(merged.crs),
        "county_feature_counts": county_counts,
    }


def main() -> None:
    county_fips = load_county_fips()
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    zip_paths: List[Path] = []
    downloaded = 0
    for county_fp in county_fips:
        name = f"tl_2010_47{county_fp}_vtd10.zip"
        dest = RAW_DIR / name
        zip_paths.append(dest)
        if dest.exists():
            continue
        download(f"{BASE_URL}/{name}", dest)
        downloaded += 1

    merge_summary = merge_zipfiles(zip_paths)
    summary = {
        "source": BASE_URL,
        "county_count": len(county_fips),
        "downloaded_this_run": downloaded,
        "zip_dir": str(RAW_DIR),
        "merged_output": OUT_PATH.name,
        "merge": merge_summary,
    }
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
