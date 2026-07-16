#!/usr/bin/env python3
"""Build Tennessee VTD overlap crosswalks across 2000/2010/2020 geometries.

Inputs (under Data/):
  - tn_vtd_2000.geojson
  - tn_vtd_2010_census_county_merged.geojson (preferred when available)
    or tl_2012_47_vtd10.zip / census/tl_2012_47_vtd10.zip
  - tl_2020_47_vtd20.zip (if available) or current DRA-backed precinct geometry

Outputs (under Data/crosswalks/):
  - tn_vtd00_to_vtd10_overlap.csv
  - tn_vtd10_to_vtd20_overlap.csv
  - tn_vtd00_to_vtd20_overlap.csv
  - tn_vtd_overlap_summary.json
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
OUT_DIR = DATA_DIR / "crosswalks"

VTD00_PATH = DATA_DIR / "tn_vtd_2000.geojson"
VTD10_DURABLE_PATH = DATA_DIR / "tn_vtd_2010.geojson"
VTD10_MERGED_PATH = DATA_DIR / "tn_vtd_2010_census_county_merged.geojson"
VTD10_FALLBACK_PATH = DATA_DIR / "tl_2012_47_vtd10.zip"
VTD10_CENSUS_PATH = DATA_DIR / "census" / "tl_2012_47_vtd10.zip"
VTD20_GEOJSON_PATH = DATA_DIR / "tn_vtd_2020.geojson"
VTD20_PATH = DATA_DIR / "tl_2020_47_vtd20.zip"
VTD20_DRA_PATH = OUT_DIR / "tn_dra_vtd20_boundaries_v07.geojson"

OUT_00_10 = OUT_DIR / "tn_vtd00_to_vtd10_overlap.csv"
OUT_10_20 = OUT_DIR / "tn_vtd10_to_vtd20_overlap.csv"
OUT_00_20 = OUT_DIR / "tn_vtd00_to_vtd20_overlap.csv"
OUT_SUMMARY = OUT_DIR / "tn_vtd_overlap_summary.json"

# NAD83 / Conus Albers (meters), suitable for area weighting.
AREA_CRS = "EPSG:5070"


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


ensure_vendor_path()

import geopandas as gpd
import numpy as np
import pandas as pd


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
    if VTD10_DURABLE_PATH.exists():
        gdf = gpd.read_file(VTD10_DURABLE_PATH)
    elif VTD10_MERGED_PATH.exists():
        gdf = gpd.read_file(VTD10_MERGED_PATH)
    elif VTD10_FALLBACK_PATH.exists():
        gdf = gpd.read_file(f"zip://{VTD10_FALLBACK_PATH.resolve()}")
    elif VTD10_CENSUS_PATH.exists():
        gdf = gpd.read_file(f"zip://{VTD10_CENSUS_PATH.resolve()}")
    else:
        raise FileNotFoundError(
            f"Missing {VTD10_DURABLE_PATH}, {VTD10_MERGED_PATH}, fallback {VTD10_FALLBACK_PATH}, and census fallback {VTD10_CENSUS_PATH}"
        )
    gdf = gdf.rename(
        columns={
            "STATEFP10": "statefp",
            "COUNTYFP10": "countyfp",
            "VTDST10": "vtdst",
            "GEOID10": "geoid",
            "NAME10": "name",
            "VTDI10": "vtdi",
            "vtd_status": "vtdi",
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
    if VTD20_GEOJSON_PATH.exists():
        gdf = gpd.read_file(VTD20_GEOJSON_PATH)
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
        gdf["vtdst"] = gdf["vtdst"].astype(str).str.strip().str.zfill(6)
        gdf["geoid"] = gdf["geoid"].astype(str).str.strip()
        gdf["name"] = gdf["name"].astype(str).str.strip()
        gdf["vtdi"] = gdf["vtdi"].astype(str).str.strip()
    elif VTD20_PATH.exists():
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
    elif VTD20_DRA_PATH.exists():
        gdf = gpd.read_file(VTD20_DRA_PATH)
        gdf["geoid"] = gdf["id"].astype(str).str.strip()
        gdf["statefp"] = gdf["geoid"].str.slice(0, 2)
        gdf["countyfp"] = gdf["geoid"].str.slice(2, 5)
        gdf["vtdst"] = gdf["geoid"].str.slice(5)
        gdf["name"] = gdf["name"].astype(str).str.strip()
        gdf["vtdi"] = ""
    else:
        raise FileNotFoundError(f"Missing {VTD20_PATH} and fallback {VTD20_DRA_PATH}")
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


def add_vintage_name_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add explicit name00/name10/name20 aliases for audit/frontend labels."""
    df = df.copy()
    for col in ["name00", "name10", "name20"]:
        df[col] = ""

    src_year = df["src_year"].astype(str)
    dst_year = df["dst_year"].astype(str)
    for year, col in [("2000", "name00"), ("2010", "name10"), ("2020", "name20")]:
        src_mask = src_year == year
        dst_mask = dst_year == year
        df.loc[src_mask, col] = df.loc[src_mask, "src_name"].fillna("").astype(str)
        df.loc[dst_mask, col] = df.loc[dst_mask, "dst_name"].fillna("").astype(str)
    return df


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
                "name00",
                "name10",
                "name20",
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
    out = add_vintage_name_columns(out)

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
            "name00",
            "name10",
            "name20",
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

    vtd20 = read_vtd_2020()

    summary = {
        "inputs": {
            "vtd00": VTD00_PATH.name if VTD00_PATH.exists() else None,
            "vtd10": (
                VTD10_DURABLE_PATH.name
                if VTD10_DURABLE_PATH.exists()
                else (
                    VTD10_MERGED_PATH.name
                    if VTD10_MERGED_PATH.exists()
                    else (
                        str(VTD10_FALLBACK_PATH.relative_to(DATA_DIR))
                        if VTD10_FALLBACK_PATH.exists()
                        else (
                            str(VTD10_CENSUS_PATH.relative_to(DATA_DIR))
                            if VTD10_CENSUS_PATH.exists()
                            else None
                        )
                    )
                )
            ),
            "vtd20": (
                VTD20_GEOJSON_PATH.name
                if VTD20_GEOJSON_PATH.exists()
                else (VTD20_PATH.name if VTD20_PATH.exists() else VTD20_DRA_PATH.name)
            ),
        },
        "outputs": {},
        "metrics": [],
        "skipped": [],
    }

    vtd10 = None
    if VTD10_DURABLE_PATH.exists() or VTD10_MERGED_PATH.exists() or VTD10_FALLBACK_PATH.exists() or VTD10_CENSUS_PATH.exists():
        vtd10 = read_vtd_2010()
        ov_10_20 = build_overlap(vtd10, vtd20, 2010, 2020)
        ov_10_20.to_csv(OUT_10_20, index=False)
        summary["outputs"]["vtd10_to_vtd20"] = OUT_10_20.name
        summary["metrics"].append(summarize(ov_10_20, "2010", "2020"))
    else:
        summary["skipped"].append("vtd10 inputs missing; skipped 2010->2020 and 2000->2010 overlaps")

    if VTD00_PATH.exists():
        vtd00 = read_vtd_2000()
        ov_00_20 = build_overlap(vtd00, vtd20, 2000, 2020)
        ov_00_20.to_csv(OUT_00_20, index=False)
        summary["outputs"]["vtd00_to_vtd20"] = OUT_00_20.name
        summary["metrics"].append(summarize(ov_00_20, "2000", "2020"))
        if vtd10 is not None:
            ov_00_10 = build_overlap(vtd00, vtd10, 2000, 2010)
            ov_00_10.to_csv(OUT_00_10, index=False)
            summary["outputs"]["vtd00_to_vtd10"] = OUT_00_10.name
            summary["metrics"].insert(0, summarize(ov_00_10, "2000", "2010"))
    else:
        summary["skipped"].append("vtd00 input missing; skipped 2000 overlaps")

    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
