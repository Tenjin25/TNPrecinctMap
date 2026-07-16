#!/usr/bin/env python3
"""GIS seed: geocode Shelby polling places and point-in-polygon to official VTD20.

Election labels like 03-09 / PRCTSEQ 0035 have no Census NAME20 equivalent.
We do have polling-location strings from Shelby precinct PDFs. This script:

  1. Geocodes those locations (Nominatim, Shelby County TN bias)
  2. Spatially joins points into Data/tn_vtd_2020.geojson (COUNTYFP20=157)
  3. Writes candidate PRCTSEQ -> VTD20 rows and merges high-confidence hits
     into Data/crosswalks/tn_prctseq_to_vtd20_overrides.csv

Name matching cannot solve these; geometry (or a true election-precinct
shapefile) is required.
"""

from __future__ import annotations

import csv
import json
import re
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import geopandas as gpd
from shapely.geometry import Point


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
XWALK = DATA / "crosswalks"
REVIEW_CSV = XWALK / "shelby_prctseq_review.csv"
VTD20 = DATA / "tn_vtd_2020.geojson"
OVERRIDES = XWALK / "tn_prctseq_to_vtd20_overrides.csv"
OUT_CANDIDATES = DATA / "reports" / "shelby_polling_geocode_vtd20_candidates.csv"
OUT_SUMMARY = DATA / "reports" / "shelby_polling_geocode_vtd20_summary.json"
CACHE_JSON = XWALK / "shelby_polling_geocode_cache.json"

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "TNPrecinctMap/1.0 (local research; shelby precinct crosswalk)"


def norm_space(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def clean_polling_query(value: str) -> str:
    s = norm_space(value)
    # PDF scrapes sometimes glue two names: "Church1301 - Other Church"
    s = re.sub(r"(?<=[a-z])(?=\d)", " ", s)
    s = re.sub(r"\s*-\s*.*$", "", s)  # keep primary name before dashed alt
    s = norm_space(s)
    return s


def load_cache() -> Dict[str, dict]:
    if not CACHE_JSON.exists():
        return {}
    try:
        return json.loads(CACHE_JSON.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_cache(cache: Dict[str, dict]) -> None:
    CACHE_JSON.write_text(json.dumps(cache, indent=2), encoding="utf-8")


def geocode_nominatim(query: str, cache: Dict[str, dict]) -> Optional[Tuple[float, float, str]]:
    key = query.lower()
    if key in cache:
        hit = cache[key]
        if hit.get("lat") is None:
            return None
        return float(hit["lon"]), float(hit["lat"]), str(hit.get("display_name") or "")

    params = urllib.parse.urlencode(
        {
            "q": f"{query}, Shelby County, Tennessee, USA",
            "format": "json",
            "limit": 1,
            "countrycodes": "us",
        }
    )
    req = urllib.request.Request(
        f"{NOMINATIM_URL}?{params}",
        headers={"User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 - keep batch running
        cache[key] = {"lat": None, "lon": None, "display_name": "", "error": str(exc)}
        save_cache(cache)
        return None

    if not payload:
        cache[key] = {"lat": None, "lon": None, "display_name": ""}
        save_cache(cache)
        return None

    row = payload[0]
    lat = float(row["lat"])
    lon = float(row["lon"])
    display = str(row.get("display_name") or "")
    cache[key] = {"lat": lat, "lon": lon, "display_name": display}
    save_cache(cache)
    return lon, lat, display


def load_shelby_vtd20() -> gpd.GeoDataFrame:
    gdf = gpd.read_file(VTD20)
    gdf = gdf[gdf["COUNTYFP20"].astype(str).str.zfill(3) == "157"].copy()
    gdf["vtd20"] = gdf["VTDST20"].astype(str).str.zfill(6)
    gdf["vtd_name"] = gdf["NAME20"].astype(str)
    if gdf.crs is None:
        gdf = gdf.set_crs(4326)
    return gdf.to_crs(4326)[["vtd20", "vtd_name", "geometry"]]


def load_existing_override_prctseqs() -> set[int]:
    out: set[int] = set()
    if not OVERRIDES.exists():
        return out
    with OVERRIDES.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if str(row.get("county_norm") or "").upper() != "SHELBY":
                continue
            seq = str(row.get("prctseq") or "").strip()
            if seq.isdigit():
                out.add(int(seq))
    return out


def main() -> None:
    if not REVIEW_CSV.exists():
        raise SystemExit(f"Missing {REVIEW_CSV}")
    if not VTD20.exists():
        raise SystemExit(f"Missing {VTD20}")

    vtd = load_shelby_vtd20()
    cache = load_cache()
    existing = load_existing_override_prctseqs()

    review_rows = list(csv.DictReader(REVIEW_CSV.open("r", encoding="utf-8-sig", newline="")))
    targets = [
        r
        for r in review_rows
        if str(r.get("status") or "") == "needs_review" and norm_space(r.get("polling_location") or "")
    ]

    candidate_rows: List[dict] = []
    for idx, row in enumerate(targets, start=1):
        prctseq = int(str(row.get("prctseq") or "0"))
        display = norm_space(row.get("display_code") or "")
        polling_raw = norm_space(row.get("polling_location") or "")
        query = clean_polling_query(polling_raw)
        print(f"[{idx}/{len(targets)}] {display} ({prctseq}) <- {query}")
        geo = geocode_nominatim(query, cache)
        time.sleep(1.05)  # Nominatim usage policy
        if not geo:
            candidate_rows.append(
                {
                    "prctseq": prctseq,
                    "display_code": display,
                    "polling_location": polling_raw,
                    "geocode_query": query,
                    "lon": "",
                    "lat": "",
                    "geocode_display": "",
                    "vtd20": "",
                    "vtd_name": "",
                    "status": "geocode_miss",
                }
            )
            continue
        lon, lat, display_name = geo
        pt = gpd.GeoDataFrame(
            [{"prctseq": prctseq}],
            geometry=[Point(lon, lat)],
            crs="EPSG:4326",
        )
        hit = gpd.sjoin(pt, vtd, how="left", predicate="within")
        if hit.empty or str(hit.iloc[0].get("vtd20") or "") in {"", "nan"}:
            # Fallback: nearest VTD centroid within ~2km projected.
            vtd_proj = vtd.to_crs(3857)
            pt_proj = pt.to_crs(3857)
            vtd_proj["dist"] = vtd_proj.geometry.centroid.distance(pt_proj.geometry.iloc[0])
            nearest = vtd_proj.sort_values("dist").iloc[0]
            if float(nearest["dist"]) <= 2000:
                vtd20 = str(nearest["vtd20"])
                vtd_name = str(nearest["vtd_name"])
                status = "nearest_within_2km"
            else:
                candidate_rows.append(
                    {
                        "prctseq": prctseq,
                        "display_code": display,
                        "polling_location": polling_raw,
                        "geocode_query": query,
                        "lon": f"{lon:.6f}",
                        "lat": f"{lat:.6f}",
                        "geocode_display": display_name,
                        "vtd20": "",
                        "vtd_name": "",
                        "status": "no_vtd_within_2km",
                    }
                )
                continue
        else:
            vtd20 = str(hit.iloc[0]["vtd20"]).zfill(6)
            vtd_name = str(hit.iloc[0]["vtd_name"])
            status = "point_in_polygon"

        candidate_rows.append(
            {
                "prctseq": prctseq,
                "display_code": display,
                "polling_location": polling_raw,
                "geocode_query": query,
                "lon": f"{lon:.6f}",
                "lat": f"{lat:.6f}",
                "geocode_display": display_name,
                "vtd20": vtd20,
                "vtd_name": vtd_name,
                "status": status,
            }
        )

    OUT_CANDIDATES.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CANDIDATES.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "prctseq",
                "display_code",
                "polling_location",
                "geocode_query",
                "lon",
                "lat",
                "geocode_display",
                "vtd20",
                "vtd_name",
                "status",
            ],
        )
        writer.writeheader()
        writer.writerows(candidate_rows)

    # Merge unique, previously missing PRCTSEQ hits into overrides.
    existing_rows: List[dict] = []
    if OVERRIDES.exists():
        with OVERRIDES.open("r", encoding="utf-8-sig", newline="") as handle:
            existing_rows = list(csv.DictReader(handle))

    by_key = {
        (str(r.get("county_norm") or "").upper(), int(str(r.get("prctseq") or "0") or 0)): r
        for r in existing_rows
        if str(r.get("prctseq") or "").isdigit()
    }
    added = 0
    for row in candidate_rows:
        if row["status"] not in {"point_in_polygon", "nearest_within_2km"}:
            continue
        seq = int(row["prctseq"])
        if seq in existing:
            continue
        key = ("SHELBY", seq)
        if key in by_key:
            continue
        by_key[key] = {
            "county_fp": "157",
            "county_norm": "SHELBY",
            "prctseq": str(seq),
            "vtd20": row["vtd20"],
            "vtd_name": row["vtd_name"],
            "weight": "1.0",
            "source": "shelby_polling_geocode_pip",
            "confidence": row["status"],
        }
        added += 1

    merged = sorted(by_key.values(), key=lambda r: (r["county_fp"], int(r["prctseq"])))
    with OVERRIDES.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["county_fp", "county_norm", "prctseq", "vtd20", "vtd_name", "weight", "source", "confidence"],
        )
        writer.writeheader()
        writer.writerows(merged)

    status_counts: Dict[str, int] = defaultdict(int)
    for row in candidate_rows:
        status_counts[row["status"]] += 1
    summary = {
        "targets": len(targets),
        "candidates_csv": str(OUT_CANDIDATES),
        "overrides_added": added,
        "override_rows_total": len(merged),
        "status_counts": dict(status_counts),
        "unmatched_focus": [
            r
            for r in candidate_rows
            if str(r["prctseq"]) in {"35", "62", "65", "66", "74", "76", "98"}
            or int(r["prctseq"]) in {35, 62, 65, 66, 74, 76, 98}
        ],
    }
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
