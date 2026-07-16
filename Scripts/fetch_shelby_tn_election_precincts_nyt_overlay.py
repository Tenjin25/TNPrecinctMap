#!/usr/bin/env python3
"""Fetch NYT Shelby TN election precinct polygons and overlay onto Census VTD20.

Source:
  https://int.nyt.com/newsgraphics/elections/map-data/2024/national/TN-precincts-with-results.geojson.gz

Shelby features use GEOID like 47157-0309 (election display code 03-09) and are
marked official_boundary=true. This is the election-precinct geometry we were
missing for PRCTSEQ -> official VTDST20 transfers.

Outputs:
  Data/raw/shelby_tn_election_precincts_2024_nyt.geojson
  Data/reports/shelby_election_precinct_to_vtd20_area_overlay.csv
  Updates Data/crosswalks/tn_prctseq_to_vtd20_overrides.csv
    (replaces Shelby rows covered by NYT codes; keeps Davidson + uncovered Shelby)
"""

from __future__ import annotations

import csv
import gzip
import json
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Dict, List

import geopandas as gpd
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
RAW = DATA / "raw"
REPORTS = DATA / "reports"
XWALK = DATA / "crosswalks"

NYT_URL = "https://int.nyt.com/newsgraphics/elections/map-data/2024/national/TN-precincts-with-results.geojson.gz"
NYT_CACHE = RAW / "nyt_tn_precincts_2024.geojson"
SHELBY_GEOJSON = RAW / "shelby_tn_election_precincts_2024_nyt.geojson"
OVERLAY_CSV = REPORTS / "shelby_election_precinct_to_vtd20_area_overlay.csv"
REVIEW_CSV = XWALK / "shelby_prctseq_review.csv"
OVERRIDES = XWALK / "tn_prctseq_to_vtd20_overrides.csv"
VTD20 = DATA / "tn_vtd_2020.geojson"


def ensure_nyt_cache() -> Path:
    RAW.mkdir(parents=True, exist_ok=True)
    if NYT_CACHE.exists() and NYT_CACHE.stat().st_size > 1_000_000:
        return NYT_CACHE
    print(f"Downloading {NYT_URL}")
    with urllib.request.urlopen(NYT_URL, timeout=300) as resp:
        raw = gzip.decompress(resp.read())
    NYT_CACHE.write_bytes(raw)
    return NYT_CACHE


def load_code_to_prctseq() -> Dict[str, int]:
    out: Dict[str, int] = {}
    with REVIEW_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            code4 = str(row.get("code4") or "").strip().zfill(4)
            seq = str(row.get("prctseq") or "").strip()
            if code4.isdigit() and seq.isdigit():
                out[code4] = int(seq)
    return out


def main() -> None:
    ensure_nyt_cache()
    nyt = gpd.read_file(NYT_CACHE)
    nyt["GEOID"] = nyt["GEOID"].astype(str)
    shelby = nyt[nyt["GEOID"].str.startswith("47157-")].copy()
    shelby["code4"] = shelby["GEOID"].str.split("-").str[-1]
    shelby["display"] = shelby["code4"].str[:2] + "-" + shelby["code4"].str[2:]

    SHELBY_GEOJSON.parent.mkdir(parents=True, exist_ok=True)
    shelby.to_crs(4326)[
        ["GEOID", "code4", "display", "votes_dem", "votes_rep", "votes_total", "official_boundary", "geometry"]
    ].to_file(SHELBY_GEOJSON, driver="GeoJSON")

    vtd = gpd.read_file(VTD20)
    vtd = vtd[vtd["COUNTYFP20"].astype(str).str.zfill(3) == "157"].copy()
    vtd["vtd20"] = vtd["VTDST20"].astype(str).str.zfill(6)
    vtd["vtd_name"] = vtd["NAME20"].astype(str)

    left = shelby.to_crs(3857)
    right = vtd.to_crs(3857)
    left["prec_area"] = left.geometry.area
    inter = gpd.overlay(
        left[["code4", "display", "prec_area", "geometry"]],
        right[["vtd20", "vtd_name", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )
    inter["share"] = inter.geometry.area / inter["prec_area"]
    inter = inter[inter["share"] >= 0.01].copy()

    overlay_rows: List[dict] = []
    for code4, grp in inter.groupby("code4"):
        total = float(grp["share"].sum())
        agg = (
            grp.assign(weight=grp["share"] / total if total > 0 else 0.0)
            .groupby(["vtd20", "vtd_name"], as_index=False)["weight"]
            .sum()
            .sort_values("weight", ascending=False)
        )
        display = f"{int(str(code4)[:2]):02d}-{int(str(code4)[2:]):02d}"
        for _, row in agg.iterrows():
            overlay_rows.append(
                {
                    "code4": str(code4),
                    "display_code": display,
                    "vtd20": str(row["vtd20"]),
                    "vtd_name": str(row["vtd_name"]),
                    "weight": float(row["weight"]),
                }
            )
    REPORTS.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(overlay_rows).to_csv(OVERLAY_CSV, index=False)

    code_to_seq = load_code_to_prctseq()
    by_code: Dict[str, List[dict]] = defaultdict(list)
    for row in overlay_rows:
        by_code[row["code4"]].append(row)

    override_rows: List[dict] = []
    for code4, pieces in by_code.items():
        seq = code_to_seq.get(code4)
        if seq is None:
            continue
        pieces = sorted(pieces, key=lambda r: -r["weight"])
        if pieces[0]["weight"] >= 0.60:
            keep = pieces[:1]
            conf = "area_core"
        else:
            keep = pieces[:3]
            conf = "area_split"
        wsum = sum(p["weight"] for p in keep) or 1.0
        for p in keep:
            override_rows.append(
                {
                    "county_fp": "157",
                    "county_norm": "SHELBY",
                    "prctseq": str(seq),
                    "vtd20": p["vtd20"],
                    "vtd_name": p["vtd_name"],
                    "weight": round(p["weight"] / wsum, 6),
                    "source": "shelby_nyt_precinct_area_overlay",
                    "confidence": conf,
                }
            )

    existing: List[dict] = []
    if OVERRIDES.exists():
        with OVERRIDES.open("r", encoding="utf-8-sig", newline="") as handle:
            existing = list(csv.DictReader(handle))

    nyt_seqs = {int(r["prctseq"]) for r in override_rows}
    keep = [
        r
        for r in existing
        if not (
            str(r.get("county_norm") or "").upper() == "SHELBY"
            and str(r.get("prctseq") or "").isdigit()
            and int(r["prctseq"]) in nyt_seqs
        )
        and not str(r.get("source") or "").startswith("shelby_polling_geocode")
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
        "nyt_cache": str(NYT_CACHE),
        "shelby_geojson": str(SHELBY_GEOJSON),
        "overlay_csv": str(OVERLAY_CSV),
        "shelby_precincts": int(len(shelby)),
        "overlay_rows": len(overlay_rows),
        "override_rows_added": len(override_rows),
        "override_rows_total": len(final),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
