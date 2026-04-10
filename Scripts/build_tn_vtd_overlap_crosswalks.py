#!/usr/bin/env python3
"""Build Tennessee VTD overlap crosswalks across 2000/2010/2020 geometries.

Inputs (under Data/):
  - tn_vtd_2000.geojson
  - tl_2012_47_vtd10.zip
  - tl_2020_47_vtd20.zip

Outputs (under Data/crosswalks/):
  - tn_vtd00_to_vtd10_overlap.csv
  - tn_vtd10_to_vtd20_overlap.csv
  - tn_vtd00_to_vtd20_overlap.csv
  - tn_vtd_overlap_summary.json
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import geopandas as gpd
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
OUT_DIR = DATA_DIR / "crosswalks"

VTD00_PATH = DATA_DIR / "tn_vtd_2000.geojson"
VTD10_PATH = DATA_DIR / "tl_2012_47_vtd10.zip"
VTD20_PATH = DATA_DIR / "tl_2020_47_vtd20.zip"

OUT_00_10 = OUT_DIR / "tn_vtd00_to_vtd10_overlap.csv"
OUT_10_20 = OUT_DIR / "tn_vtd10_to_vtd20_overlap.csv"
OUT_00_20 = OUT_DIR / "tn_vtd00_to_vtd20_overlap.csv"
OUT_SUMMARY = OUT_DIR / "tn_vtd_overlap_summary.json"

# NAD83 / Conus Albers (meters), suitable for area weighting.
AREA_CRS = "EPSG:5070"


def read_vtd_2000() -> gpd.GeoDataFrame:
    if not VTD00_PATH.exists():
        raise FileNotFoundError(f"Missing {VTD00_PATH}")
    gdf = gpd.read_file(VTD00_PATH)
    gdf = gdf.rename(
        columns={
            "STATEFP00": "statefp",
            "COUNTYFP00": "countyfp",
            "VTDST00": "vtdst",
            "VTDIDFP00": "geoid",
            "NAME00": "name",
            "VTDI00": "vtdi",
            "vtd_status": "vtdi",
        }
    )
    gdf["statefp"] = gdf["statefp"].astype(str).str.zfill(2)
    gdf["countyfp"] = gdf["countyfp"].astype(str).str.zfill(3)
    gdf["vtdst"] = gdf["vtdst"].astype(str).str.strip().str.zfill(4)
    gdf["geoid"] = gdf["geoid"].astype(str).str.strip()
    gdf.loc[gdf["geoid"] == "", "geoid"] = (
        gdf["statefp"] + gdf["countyfp"] + gdf["vtdst"]
    )
    gdf["name"] = gdf["name"].astype(str).str.strip()
    if "vtdi" not in gdf.columns:
        gdf["vtdi"] = ""
    gdf["vtdi"] = gdf["vtdi"].astype(str).str.strip()
    out = gdf[["statefp", "countyfp", "vtdst", "vtdi", "geoid", "name", "geometry"]].copy()
    out["year"] = 2000
    return out


def read_vtd_2010() -> gpd.GeoDataFrame:
    if not VTD10_PATH.exists():
        raise FileNotFoundError(f"Missing {VTD10_PATH}")
    gdf = gpd.read_file(f"zip://{VTD10_PATH.resolve()}")
    gdf = gdf.rename(
        columns={
            "STATEFP10": "statefp",
            "COUNTYFP10": "countyfp",
            "VTDST10": "vtdst",
            "GEOID10": "geoid",
            "NAME10": "name",
            "VTDI10": "vtdi",
        }
    )
    gdf["statefp"] = gdf["statefp"].astype(str).str.zfill(2)
    gdf["countyfp"] = gdf["countyfp"].astype(str).str.zfill(3)
    gdf["vtdst"] = gdf["vtdst"].astype(str).str.strip().str.zfill(4)
    gdf["geoid"] = gdf["geoid"].astype(str).str.strip()
    gdf["name"] = gdf["name"].astype(str).str.strip()
    gdf["vtdi"] = gdf["vtdi"].astype(str).str.strip()
    out = gdf[["statefp", "countyfp", "vtdst", "vtdi", "geoid", "name", "geometry"]].copy()
    out["year"] = 2010
    return out


def read_vtd_2020() -> gpd.GeoDataFrame:
    if not VTD20_PATH.exists():
        raise FileNotFoundError(f"Missing {VTD20_PATH}")
    gdf = gpd.read_file(f"zip://{VTD20_PATH.resolve()}")
    gdf = gdf.rename(
        columns={
            "STATEFP20": "statefp",
            "COUNTYFP20": "countyfp",
            "VTDST20": "vtdst",
            "GEOID20": "geoid",
            "NAME20": "name",
            "VTDI20": "vtdi",
        }
    )
    gdf["statefp"] = gdf["statefp"].astype(str).str.zfill(2)
    gdf["countyfp"] = gdf["countyfp"].astype(str).str.zfill(3)
    gdf["vtdst"] = gdf["vtdst"].astype(str).str.strip()
    gdf["geoid"] = gdf["geoid"].astype(str).str.strip()
    gdf["name"] = gdf["name"].astype(str).str.strip()
    gdf["vtdi"] = gdf["vtdi"].astype(str).str.strip()
    out = gdf[["statefp", "countyfp", "vtdst", "vtdi", "geoid", "name", "geometry"]].copy()
    out["year"] = 2020
    return out


def prep(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    # Repair invalids before overlay.
    invalid = ~gdf.geometry.is_valid
    if invalid.any():
        gdf.loc[invalid, "geometry"] = gdf.loc[invalid, "geometry"].buffer(0)
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    return gdf.to_crs(AREA_CRS)


def build_overlap(src: gpd.GeoDataFrame, dst: gpd.GeoDataFrame, src_year: int, dst_year: int) -> pd.DataFrame:
    src = prep(src)
    dst = prep(dst)

    src = src.rename(
        columns={
            "statefp": "src_statefp",
            "countyfp": "src_countyfp",
            "vtdst": "src_vtdst",
            "vtdi": "src_vtdi",
            "geoid": "src_geoid",
            "name": "src_name",
        }
    )
    dst = dst.rename(
        columns={
            "statefp": "dst_statefp",
            "countyfp": "dst_countyfp",
            "vtdst": "dst_vtdst",
            "vtdi": "dst_vtdi",
            "geoid": "dst_geoid",
            "name": "dst_name",
        }
    )

    src["src_area_m2"] = src.geometry.area
    dst["dst_area_m2"] = dst.geometry.area

    src_area = src[["src_geoid", "src_area_m2"]].drop_duplicates()
    dst_area = dst[["dst_geoid", "dst_area_m2"]].drop_duplicates()

    src_cols = [
        "src_statefp",
        "src_countyfp",
        "src_vtdst",
        "src_vtdi",
        "src_geoid",
        "src_name",
        "geometry",
    ]
    dst_cols = [
        "dst_statefp",
        "dst_countyfp",
        "dst_vtdst",
        "dst_vtdi",
        "dst_geoid",
        "dst_name",
        "geometry",
    ]

    all_counties = sorted(set(src["src_countyfp"]).union(set(dst["dst_countyfp"])))
    chunks: List[pd.DataFrame] = []
    for county in all_counties:
        src_c = src[src["src_countyfp"] == county][src_cols]
        dst_c = dst[dst["dst_countyfp"] == county][dst_cols]
        if src_c.empty or dst_c.empty:
            continue
        inter = gpd.overlay(src_c, dst_c, how="intersection", keep_geom_type=False)
        if inter.empty:
            continue
        inter["intersection_area_m2"] = inter.geometry.area
        inter = inter[inter["intersection_area_m2"] > 0].copy()
        if inter.empty:
            continue

        grouped = (
            inter.drop(columns=["geometry"])
            .groupby(
                [
                    "src_statefp",
                    "src_countyfp",
                    "src_vtdst",
                    "src_vtdi",
                    "src_geoid",
                    "src_name",
                    "dst_statefp",
                    "dst_countyfp",
                    "dst_vtdst",
                    "dst_vtdi",
                    "dst_geoid",
                    "dst_name",
                ],
                as_index=False,
            )["intersection_area_m2"]
            .sum()
        )
        chunks.append(grouped)

    if not chunks:
        return pd.DataFrame(
            columns=[
                "src_year",
                "dst_year",
                "src_statefp",
                "src_countyfp",
                "src_vtdst",
                "src_vtdi",
                "src_geoid",
                "src_name",
                "dst_statefp",
                "dst_countyfp",
                "dst_vtdst",
                "dst_vtdi",
                "dst_geoid",
                "dst_name",
                "intersection_area_m2",
                "src_area_m2",
                "dst_area_m2",
                "src_weight",
                "dst_weight",
            ]
        )

    out = pd.concat(chunks, ignore_index=True)
    out = out.merge(src_area, on="src_geoid", how="left")
    out = out.merge(dst_area, on="dst_geoid", how="left")
    out["src_weight"] = np.where(
        out["src_area_m2"] > 0, out["intersection_area_m2"] / out["src_area_m2"], 0.0
    )
    out["dst_weight"] = np.where(
        out["dst_area_m2"] > 0, out["intersection_area_m2"] / out["dst_area_m2"], 0.0
    )
    out["src_year"] = src_year
    out["dst_year"] = dst_year

    out = out[
        [
            "src_year",
            "dst_year",
            "src_statefp",
            "src_countyfp",
            "src_vtdst",
            "src_vtdi",
            "src_geoid",
            "src_name",
            "dst_statefp",
            "dst_countyfp",
            "dst_vtdst",
            "dst_vtdi",
            "dst_geoid",
            "dst_name",
            "intersection_area_m2",
            "src_area_m2",
            "dst_area_m2",
            "src_weight",
            "dst_weight",
        ]
    ].sort_values(["src_countyfp", "src_geoid", "dst_geoid"]).reset_index(drop=True)
    return out


def summarize(df: pd.DataFrame, src_label: str, dst_label: str) -> Dict:
    if df.empty:
        return {
            "pair": f"{src_label}->{dst_label}",
            "rows": 0,
            "src_units": 0,
            "dst_units": 0,
            "src_weight_sum_min": None,
            "src_weight_sum_mean": None,
            "src_weight_sum_max": None,
        }

    src_weights = df.groupby("src_geoid", as_index=False)["src_weight"].sum()
    return {
        "pair": f"{src_label}->{dst_label}",
        "rows": int(len(df)),
        "src_units": int(df["src_geoid"].nunique()),
        "dst_units": int(df["dst_geoid"].nunique()),
        "src_weight_sum_min": float(src_weights["src_weight"].min()),
        "src_weight_sum_mean": float(src_weights["src_weight"].mean()),
        "src_weight_sum_max": float(src_weights["src_weight"].max()),
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    vtd00 = read_vtd_2000()
    vtd10 = read_vtd_2010()
    vtd20 = read_vtd_2020()

    ov_00_10 = build_overlap(vtd00, vtd10, 2000, 2010)
    ov_10_20 = build_overlap(vtd10, vtd20, 2010, 2020)
    ov_00_20 = build_overlap(vtd00, vtd20, 2000, 2020)

    ov_00_10.to_csv(OUT_00_10, index=False)
    ov_10_20.to_csv(OUT_10_20, index=False)
    ov_00_20.to_csv(OUT_00_20, index=False)

    summary = {
        "inputs": {
            "vtd00": VTD00_PATH.name,
            "vtd10": VTD10_PATH.name,
            "vtd20": VTD20_PATH.name,
        },
        "outputs": {
            "vtd00_to_vtd10": OUT_00_10.name,
            "vtd10_to_vtd20": OUT_10_20.name,
            "vtd00_to_vtd20": OUT_00_20.name,
        },
        "metrics": [
            summarize(ov_00_10, "2000", "2010"),
            summarize(ov_10_20, "2010", "2020"),
            summarize(ov_00_20, "2000", "2020"),
        ],
    }
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
