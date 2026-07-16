#!/usr/bin/env python3
"""Build statewide Tennessee 2000 VTD geometry from TIGER2008 county zips.

Outputs:
  - Data/tiger2008_vtd00_counties/tl_2008_<countyfips>_vtd00.zip (download cache)
  - Data/tn_vtd_2000.geojson
  - Data/tn_vtd_2000_build_summary.json
"""

from __future__ import annotations

import argparse
import json
import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Tuple

import geopandas as gpd
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
DOWNLOAD_DIR = DATA_DIR / "tiger2008_vtd00_counties"
OUT_GEOJSON = DATA_DIR / "tn_vtd_2000.geojson"
OUT_SUMMARY = DATA_DIR / "tn_vtd_2000_build_summary.json"

INDEX_URL = "https://www2.census.gov/geo/tiger/TIGER2008/47_TENNESSEE/"
DIR_HREF_RE = re.compile(r'href="(47\d{3}_[^"/]+/)"')
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "*/*",
}


def norm_text(value: str) -> str:
    s = (value or "").strip().upper()
    s = re.sub(r"[^A-Z0-9 .-]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def fetch_index_html() -> str:
    req = urllib.request.Request(INDEX_URL, headers=REQUEST_HEADERS)
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read().decode("utf-8", errors="replace")


def list_county_dirs(index_html: str) -> List[str]:
    dirs = sorted(set(m.group(1) for m in DIR_HREF_RE.finditer(index_html)))
    if len(dirs) < 90:
        raise RuntimeError(f"Unexpected county directory count from index: {len(dirs)}")
    return dirs


def zip_name_for_county_dir(county_dir: str) -> str:
    fips = county_dir[:5]
    return f"tl_2008_{fips}_vtd00.zip"


def url_for_county_dir(county_dir: str) -> str:
    return f"{INDEX_URL}{county_dir}{zip_name_for_county_dir(county_dir)}"


def download_zip(url: str, dest: Path, timeout: int) -> None:
    temp_dest = dest.with_suffix(dest.suffix + ".part")
    req = urllib.request.Request(url, headers=REQUEST_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        temp_dest.write_bytes(resp.read())
    temp_dest.replace(dest)


def download_county_zip(
    county_dir: str,
    skip_download: bool,
    timeout: int,
    retries: int,
) -> dict:
    local_zip = DOWNLOAD_DIR / zip_name_for_county_dir(county_dir)
    if local_zip.exists():
        return {"county_dir": county_dir, "zip_path": local_zip, "status": "cached"}
    if skip_download:
        return {
            "county_dir": county_dir,
            "zip_path": local_zip,
            "status": "missing",
            "error": f"Missing cached zip with --skip-download: {local_zip}",
        }

    url = url_for_county_dir(county_dir)
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            download_zip(url, local_zip, timeout)
            return {"county_dir": county_dir, "zip_path": local_zip, "status": "downloaded"}
        except Exception as err:  # pragma: no cover - network path
            last_err = err
            part = local_zip.with_suffix(local_zip.suffix + ".part")
            if part.exists():
                part.unlink(missing_ok=True)
            if attempt < retries:
                time.sleep(1.5 * attempt)

    return {
        "county_dir": county_dir,
        "zip_path": local_zip,
        "status": "failed",
        "error": str(last_err),
    }


def ensure_downloads(
    county_dirs: List[str],
    skip_download: bool,
    workers: int,
    timeout: int,
    retries: int,
) -> Tuple[List[Tuple[str, Path]], int, int]:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    downloaded = 0
    cached = 0
    results = []
    max_workers = max(1, min(workers, len(county_dirs)))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                download_county_zip,
                county_dir,
                skip_download,
                timeout,
                retries,
            ): county_dir
            for county_dir in county_dirs
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
            print(f"[{completed}/{len(county_dirs)}] {result['county_dir']} {status}", flush=True)

    failed = [r for r in results if r["status"] in {"failed", "missing"}]
    if failed:
        details = "; ".join(f"{r['county_dir']}: {r.get('error', '')}" for r in failed)
        raise RuntimeError(f"Failed to resolve {len(failed)} VTD00 county ZIPs: {details}")

    result_by_county = {r["county_dir"]: r for r in results}
    out = [(county_dir, result_by_county[county_dir]["zip_path"]) for county_dir in county_dirs]
    return out, downloaded, cached


def county_name_from_dir(county_dir: str) -> str:
    # 47001_Anderson_County/ -> Anderson
    name = county_dir.rstrip("/").split("_", 1)[1]
    if name.endswith("_County"):
        name = name[: -len("_County")]
    return name.replace("_", " ").strip()


def load_county_name_map_from_2020() -> Dict[str, str]:
    county_path = DATA_DIR / "tl_2020_47_county20.geojson"
    if not county_path.exists():
        return {}
    gdf = gpd.read_file(county_path, columns=["COUNTYFP20", "NAME20"])
    out: Dict[str, str] = {}
    for _, row in gdf.iterrows():
        county_fp = str(row.get("COUNTYFP20", "")).zfill(3)
        county_name = str(row.get("NAME20", "")).strip()
        if county_fp and county_name:
            out[county_fp] = county_name
    return out


def build_statewide_vtd(zip_rows: List[Tuple[str, Path]]) -> gpd.GeoDataFrame:
    county_name_lookup = load_county_name_map_from_2020()
    frames: List[gpd.GeoDataFrame] = []

    keep = [
        "STATEFP00",
        "COUNTYFP00",
        "VTDST00",
        "VTDIDFP00",
        "VTDI00",
        "NAME00",
        "NAMELSAD00",
        "LSAD00",
        "MTFCC00",
        "FUNCSTAT00",
    ]

    for county_dir, zip_path in zip_rows:
        gdf = gpd.read_file(f"zip://{zip_path.resolve()}").to_crs(4326)
        cols = [c for c in keep if c in gdf.columns]
        gdf = gdf[cols + ["geometry"]].copy()
        gdf["COUNTYFP00"] = gdf["COUNTYFP00"].astype(str).str.zfill(3)
        fallback_name = county_name_from_dir(county_dir)
        gdf["county_nam"] = gdf["COUNTYFP00"].map(
            lambda c: county_name_lookup.get(str(c).zfill(3), fallback_name)
        )
        frames.append(gdf)

    if not frames:
        raise RuntimeError("No VTD county files were loaded")

    statewide = gpd.GeoDataFrame(
        pd.concat(frames, ignore_index=True),
        geometry="geometry",
        crs=frames[0].crs,
    )
    statewide["vtd_code"] = statewide["VTDST00"].astype(str).str.strip().str.zfill(4)
    statewide["vtd_status"] = statewide["VTDI00"].astype(str).str.strip()
    statewide["county_norm"] = statewide["county_nam"].map(norm_text)
    statewide["precinct_name"] = statewide["county_nam"] + " - " + statewide["vtd_code"]
    statewide["precinct_norm"] = statewide["precinct_name"].map(norm_text)
    statewide["id"] = range(1, len(statewide) + 1)
    statewide = statewide.sort_values(["COUNTYFP00", "vtd_code", "id"]).reset_index(drop=True)
    return statewide


def write_outputs(statewide: gpd.GeoDataFrame, county_dirs: List[str], downloaded: int, cached: int) -> None:
    OUT_GEOJSON.parent.mkdir(parents=True, exist_ok=True)
    out_cols = [
        "STATEFP00",
        "COUNTYFP00",
        "county_nam",
        "county_norm",
        "VTDST00",
        "VTDIDFP00",
        "vtd_code",
        "vtd_status",
        "NAME00",
        "NAMELSAD00",
        "LSAD00",
        "MTFCC00",
        "FUNCSTAT00",
        "precinct_name",
        "precinct_norm",
        "id",
        "geometry",
    ]
    statewide[out_cols].to_file(OUT_GEOJSON, driver="GeoJSON")

    summary = {
        "source_index_url": INDEX_URL,
        "county_dirs_found": len(county_dirs),
        "county_dirs_sample": county_dirs[:5],
        "county_zips_downloaded_this_run": downloaded,
        "county_zips_cached_this_run": cached,
        "statewide_feature_count": int(len(statewide)),
        "distinct_counties": int(statewide["COUNTYFP00"].nunique()),
        "distinct_vtdidfp00": int(statewide["VTDIDFP00"].nunique()),
        "output_geojson": OUT_GEOJSON.name,
    }
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Do not fetch from Census; use only local cached county zips.",
    )
    parser.add_argument("--workers", type=int, default=8, help="Parallel download workers")
    parser.add_argument("--timeout", type=int, default=120, help="Per-request timeout in seconds")
    parser.add_argument("--retries", type=int, default=3, help="Download attempts per county ZIP")
    args = parser.parse_args()

    county_dirs = list_county_dirs(fetch_index_html())
    zip_rows, downloaded, cached = ensure_downloads(
        county_dirs,
        skip_download=args.skip_download,
        workers=args.workers,
        timeout=args.timeout,
        retries=args.retries,
    )
    statewide = build_statewide_vtd(zip_rows)
    write_outputs(statewide, county_dirs, downloaded, cached)


if __name__ == "__main__":
    main()
