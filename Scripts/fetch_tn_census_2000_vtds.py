#!/usr/bin/env python3
"""Download and merge official Census 2000 Tennessee county VTD shapefiles.

This script pulls the county-level 2000 VTD ZIPs from the official Census
TIGER2010 directory, stores the raw ZIPs under ``Data/raw/census_vtd_2000``,
and writes a merged statewide GeoJSON in the legacy filename expected by the
historical overlap builder.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
RAW_DIR = DATA_DIR / "raw" / "census_vtd_2000"
OUT_PATH = DATA_DIR / "tn_vtd_2000.geojson"
SUMMARY_PATH = DATA_DIR / "census_vtd_2000_fetch_summary.json"
COUNTY_PATH = DATA_DIR / "tl_2020_47_county20.geojson"
BASE_URL = "https://www2.census.gov/geo/tiger/TIGER2010/VTD/2000"
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) TNPrecinctMap/1.0",
    "Accept": "*/*",
}


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


def download(url: str, dest: Path, timeout: int) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    temp_dest = dest.with_suffix(dest.suffix + ".part")
    req = urllib.request.Request(url, headers=REQUEST_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as response, temp_dest.open("wb") as handle:
        shutil.copyfileobj(response, handle)
    temp_dest.replace(dest)


def download_with_retries(county_fp: str, dest: Path, timeout: int, retries: int) -> dict:
    url = f"{BASE_URL}/{dest.name}"
    if dest.exists():
        return {"county_fp": county_fp, "status": "cached", "path": str(dest)}

    last_error = ""
    for attempt in range(1, retries + 1):
        try:
            download(url, dest, timeout=timeout)
            return {"county_fp": county_fp, "status": "downloaded", "path": str(dest)}
        except Exception as err:  # pragma: no cover - network path
            last_error = str(err)
            part = dest.with_suffix(dest.suffix + ".part")
            if part.exists():
                part.unlink(missing_ok=True)
            if attempt == retries:
                break
    return {"county_fp": county_fp, "status": "failed", "path": str(dest), "error": last_error}


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
    county_col = "COUNTYFP00" if "COUNTYFP00" in merged.columns else None
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
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=8, help="Parallel download workers")
    parser.add_argument("--timeout", type=int, default=90, help="Per-request timeout in seconds")
    parser.add_argument("--retries", type=int, default=3, help="Download attempts per county ZIP")
    args = parser.parse_args()

    county_fips = load_county_fips()
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    zip_by_county = {
        county_fp: RAW_DIR / f"tl_2010_47{county_fp}_vtd00.zip"
        for county_fp in county_fips
    }
    results = []
    downloaded = 0
    cached = 0
    failed = []
    max_workers = max(1, min(args.workers, len(county_fips)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                download_with_retries,
                county_fp,
                dest,
                args.timeout,
                args.retries,
            ): county_fp
            for county_fp, dest in zip_by_county.items()
        }
        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            status = result["status"]
            if status == "downloaded":
                downloaded += 1
            elif status == "cached":
                cached += 1
            else:
                failed.append(result)
            print(
                f"[{completed}/{len(county_fips)}] {result['county_fp']} {status}",
                flush=True,
            )

    if failed:
        failed_labels = ", ".join(f"{r['county_fp']}: {r.get('error', '')}" for r in failed)
        raise RuntimeError(f"Failed to download {len(failed)} VTD00 ZIPs: {failed_labels}")

    zip_paths: List[Path] = []
    for county_fp in county_fips:
        dest = zip_by_county[county_fp]
        zip_paths.append(dest)

    merge_summary = merge_zipfiles(zip_paths)
    summary = {
        "source": BASE_URL,
        "county_count": len(county_fips),
        "downloaded_this_run": downloaded,
        "cached_this_run": cached,
        "download_workers": max_workers,
        "zip_dir": str(RAW_DIR),
        "merged_output": OUT_PATH.name,
        "download_results": sorted(results, key=lambda row: row["county_fp"]),
        "merge": merge_summary,
    }
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
