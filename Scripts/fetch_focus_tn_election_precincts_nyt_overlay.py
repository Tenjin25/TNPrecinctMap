#!/usr/bin/env python3
"""Overlay NYT 2024 election precinct polygons for residual matched_no_transfer gaps.

Targets (PRCTSEQ matched, no official VTD20 transfer):
  Campbell 0027  5-6 LaFollette Rec     -> 47013-5/LREC
  Maury    0016  Wright 8-2             -> 47119-Wright Elementary
  Sumner   0018  14-2 Birdwell          -> 47165-1402 Birdwell Chapel...
  Sumner   0021  17-1 Goodlettsville    -> 47165-1701 Church of the Nazarene

Uses cached Data/raw/nyt_tn_precincts_2024.geojson when present.
Writes:
  Data/reports/focus_election_precinct_to_vtd20_area_overlay.csv
  Appends/replaces rows in Data/crosswalks/tn_prctseq_to_vtd20_overrides.csv
"""

from __future__ import annotations

import csv
import gzip
import json
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import geopandas as gpd
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
RAW = DATA / "raw"
REPORTS = DATA / "reports"
XWALK = DATA / "crosswalks"

NYT_URL = "https://int.nyt.com/newsgraphics/elections/map-data/2024/national/TN-precincts-with-results.geojson.gz"
NYT_CACHE = RAW / "nyt_tn_precincts_2024.geojson"
OVERLAY_CSV = REPORTS / "focus_election_precinct_to_vtd20_area_overlay.csv"
OVERRIDES = XWALK / "tn_prctseq_to_vtd20_overrides.csv"
VTD20 = DATA / "tn_vtd_2020.geojson"
SOURCE = "focus_nyt_precinct_area_overlay"

# Explicit NYT GEOID -> election PRCTSEQ bridge for residual gaps.
TARGETS: List[dict] = [
    {
        "geoid": "47013-5/LREC",
        "county_fp": "013",
        "county_norm": "CAMPBELL",
        "prctseq": "0027",
        "label": "5-6 LaFollette Rec",
    },
    {
        "geoid": "47119-Wright Elementary",
        "county_fp": "119",
        "county_norm": "MAURY",
        "prctseq": "0016",
        "label": "Wright 8-2",
    },
    {
        "geoid": "47165-1402 Birdwell Chapel Church of Christ",
        "county_fp": "165",
        "county_norm": "SUMNER",
        "prctseq": "0018",
        "label": "14-2 Birdwell",
    },
    {
        "geoid": "47165-1701 Church of the Nazarene",
        "county_fp": "165",
        "county_norm": "SUMNER",
        "prctseq": "0021",
        "label": "17-1 Goodlettsville",
    },
]


def ensure_nyt_cache() -> Path:
    RAW.mkdir(parents=True, exist_ok=True)
    if NYT_CACHE.exists() and NYT_CACHE.stat().st_size > 1_000_000:
        return NYT_CACHE
    print(f"Downloading {NYT_URL}")
    with urllib.request.urlopen(NYT_URL, timeout=300) as resp:
        raw = gzip.decompress(resp.read())
    NYT_CACHE.write_bytes(raw)
    return NYT_CACHE


def overlay_precinct(
    left: gpd.GeoDataFrame,
    right: gpd.GeoDataFrame,
) -> List[Tuple[str, str, float]]:
    """Return [(vtd20, vtd_name, weight), ...] for one election precinct."""
    left = left.to_crs(3857).copy()
    right = right.to_crs(3857).copy()
    left["prec_area"] = left.geometry.area
    inter = gpd.overlay(
        left[["prec_area", "geometry"]],
        right[["vtd20", "vtd_name", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )
    if inter.empty:
        return []
    inter["share"] = inter.geometry.area / inter["prec_area"]
    inter = inter[inter["share"] >= 0.01].copy()
    if inter.empty:
        return []
    total = float(inter["share"].sum())
    agg = (
        inter.assign(weight=inter["share"] / total if total > 0 else 0.0)
        .groupby(["vtd20", "vtd_name"], as_index=False)["weight"]
        .sum()
        .sort_values("weight", ascending=False)
    )
    return [(str(r.vtd20), str(r.vtd_name), float(r.weight)) for r in agg.itertuples(index=False)]


def main() -> None:
    ensure_nyt_cache()
    nyt = gpd.read_file(NYT_CACHE)
    nyt["GEOID"] = nyt["GEOID"].astype(str)
    vtd = gpd.read_file(VTD20)
    vtd["county_fp"] = vtd["COUNTYFP20"].astype(str).str.zfill(3)
    vtd["vtd20"] = vtd["VTDST20"].astype(str).str.zfill(6)
    vtd["vtd_name"] = vtd["NAME20"].astype(str)

    overlay_rows: List[dict] = []
    override_rows: List[dict] = []
    missing: List[str] = []

    for target in TARGETS:
        feat = nyt[nyt["GEOID"] == target["geoid"]].copy()
        if feat.empty:
            missing.append(target["geoid"])
            continue
        county_vtd = vtd[vtd["county_fp"] == target["county_fp"]].copy()
        pieces = overlay_precinct(feat, county_vtd)
        if not pieces:
            missing.append(target["geoid"] + " (empty overlay)")
            continue
        for vtd20, vtd_name, weight in pieces:
            overlay_rows.append(
                {
                    "geoid": target["geoid"],
                    "county_norm": target["county_norm"],
                    "prctseq": target["prctseq"],
                    "label": target["label"],
                    "vtd20": vtd20,
                    "vtd_name": vtd_name,
                    "weight": weight,
                }
            )
        if pieces[0][2] >= 0.60:
            keep = pieces[:1]
            conf = "area_core"
        else:
            keep = pieces[:3]
            conf = "area_split"
        wsum = sum(p[2] for p in keep) or 1.0
        for vtd20, vtd_name, weight in keep:
            override_rows.append(
                {
                    "county_fp": target["county_fp"],
                    "county_norm": target["county_norm"],
                    "prctseq": str(int(target["prctseq"])),
                    "vtd20": vtd20,
                    "vtd_name": vtd_name,
                    "weight": round(weight / wsum, 6),
                    "source": SOURCE,
                    "confidence": conf,
                }
            )

    REPORTS.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(overlay_rows).to_csv(OVERLAY_CSV, index=False)

    existing: List[dict] = []
    if OVERRIDES.exists():
        with OVERRIDES.open("r", encoding="utf-8-sig", newline="") as handle:
            existing = list(csv.DictReader(handle))

    drop_keys = {(r["county_norm"], str(int(r["prctseq"]))) for r in override_rows}
    keep = [
        r
        for r in existing
        if not (
            str(r.get("source") or "") == SOURCE
            or (
                str(r.get("county_norm") or "").upper() in {t["county_norm"] for t in TARGETS}
                and str(r.get("prctseq") or "").isdigit()
                and (str(r.get("county_norm") or "").upper(), str(int(r["prctseq"]))) in drop_keys
            )
        )
    ]
    final = keep + override_rows
    final.sort(key=lambda r: (r["county_fp"], int(r["prctseq"]), -float(r.get("weight") or 0)))
    with OVERRIDES.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["county_fp", "county_norm", "prctseq", "vtd20", "vtd_name", "weight", "source", "confidence"],
        )
        writer.writeheader()
        writer.writerows(final)

    summary = {
        "targets": len(TARGETS),
        "overlay_rows": len(overlay_rows),
        "override_rows_added": len(override_rows),
        "override_rows_total": len(final),
        "missing": missing,
        "by_target": {
            r["label"]: [
                {"vtd20": x["vtd20"], "vtd_name": x["vtd_name"], "weight": x["weight"], "confidence": x["confidence"]}
                for x in override_rows
                if x["county_norm"] == r["county_norm"] and str(int(x["prctseq"])) == str(int(r["prctseq"]))
            ]
            for r in TARGETS
        },
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
