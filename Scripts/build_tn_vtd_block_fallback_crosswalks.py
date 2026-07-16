#!/usr/bin/env python3
"""Build block-chain fallback crosswalks from historical VTDs to VTD20.

These outputs are intended as repair/fallback artifacts when a direct
VTD-to-VTD geometry overlap has weak or missing coverage for a source unit.

Chain (collapsed fallback — skips explicit VTD10 hop):
  - 2000 VTD -> 2000 tabblocks -> NHGIS 2000->2010 blocks
    -> NHGIS 2010->2020 blocks -> 2020 BlockAssign VTD
  - 2010 VTD -> 2010 tabblocks -> NHGIS 2010->2020 blocks
    -> 2020 BlockAssign VTD

For the full explicit chain (VTD00->block00->VTD10->block10->VTD20),
run Scripts/build_tn_vtd_full_block_chain_crosswalks.py instead.

Outputs:
  - Data/crosswalks/tn_vtd00_to_vtd20_block_fallback.csv
  - Data/crosswalks/tn_vtd10_to_vtd20_block_fallback.csv
  - Data/crosswalks/tn_vtd_block_fallback_summary.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, Iterable, List

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"

VTD00_PATH = DATA_DIR / "tn_vtd_2000.geojson"
VTD10_DURABLE_PATH = DATA_DIR / "tn_vtd_2010.geojson"
VTD10_MERGED_PATH = DATA_DIR / "tn_vtd_2010_census_county_merged.geojson"
VTD10_ROOT_ZIP = DATA_DIR / "tl_2012_47_vtd10.zip"
VTD10_CENSUS_ZIP = DATA_DIR / "census" / "tl_2012_47_vtd10.zip"
VTD20_GEOJSON = DATA_DIR / "tn_vtd_2020.geojson"
VTD20_ZIP = DATA_DIR / "tl_2020_47_vtd20.zip"
VTD20_DRA_PATH = XWALK_DIR / "tn_dra_vtd20_boundaries_v07.geojson"
TABBLOCK00_ZIP = DATA_DIR / "tl_2008_47_tabblock00.zip"
TABBLOCK10_ZIP = DATA_DIR / "tl_2012_47_tabblock10.zip"
NHGIS_00_10 = XWALK_DIR / "nhgis_blk2000_blk2010_47_tn_to_tn.csv"
NHGIS_10_20 = XWALK_DIR / "nhgis_blk2010_blk2020_47_tn_to_tn.csv"
BLOCKASSIGN_VTD = XWALK_DIR / "blockassign_tn_vtd.csv"

OUT_00_20 = XWALK_DIR / "tn_vtd00_to_vtd20_block_fallback.csv"
OUT_10_20 = XWALK_DIR / "tn_vtd10_to_vtd20_block_fallback.csv"
OUT_SUMMARY = XWALK_DIR / "tn_vtd_block_fallback_summary.json"
OUT_DIAGNOSTICS_2000 = XWALK_DIR / "tn_vtd00_block_fallback_diagnostics.csv"

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


SOURCE_FIELDS = [
    "src_year",
    "src_statefp",
    "src_countyfp",
    "src_vtdst",
    "src_vtdi",
    "src_geoid",
    "src_name",
]


def zip_uri(path: Path) -> str:
    return f"zip://{path.resolve()}"


def read_vtd_2000() -> gpd.GeoDataFrame:
    if not VTD00_PATH.exists():
        raise FileNotFoundError(f"Missing {VTD00_PATH}")
    gdf = gpd.read_file(VTD00_PATH)
    gdf = gdf.rename(
        columns={
            "STATEFP00": "src_statefp",
            "COUNTYFP00": "src_countyfp",
            "VTDST00": "src_vtdst",
            "VTDIDFP00": "src_geoid",
            "NAME00": "src_name",
            "VTDI00": "src_vtdi",
            "vtd_status": "src_vtdi",
        }
    )
    gdf["src_year"] = 2000
    gdf["src_statefp"] = gdf["src_statefp"].astype(str).str.zfill(2)
    gdf["src_countyfp"] = gdf["src_countyfp"].astype(str).str.zfill(3)
    gdf["src_vtdst"] = gdf["src_vtdst"].astype(str).str.strip().str.zfill(4)
    gdf["src_geoid"] = gdf["src_geoid"].astype(str).str.strip()
    gdf.loc[gdf["src_geoid"] == "", "src_geoid"] = (
        gdf["src_statefp"] + gdf["src_countyfp"] + gdf["src_vtdst"]
    )
    gdf["src_vtdi"] = gdf["src_vtdi"].fillna("").astype(str).str.strip()
    gdf["src_name"] = gdf["src_name"].fillna("").astype(str).str.strip()
    return gdf[SOURCE_FIELDS + ["geometry"]].copy()


def vtd10_path() -> Path:
    for path in [VTD10_DURABLE_PATH, VTD10_MERGED_PATH, VTD10_ROOT_ZIP, VTD10_CENSUS_ZIP]:
        if path.exists():
            return path
    raise FileNotFoundError(
        f"Missing {VTD10_DURABLE_PATH}, {VTD10_MERGED_PATH}, {VTD10_ROOT_ZIP}, and {VTD10_CENSUS_ZIP}"
    )


def read_vtd_2010() -> gpd.GeoDataFrame:
    path = vtd10_path()
    gdf = gpd.read_file(zip_uri(path) if path.suffix.lower() == ".zip" else path)
    gdf = gdf.rename(
        columns={
            "STATEFP10": "src_statefp",
            "COUNTYFP10": "src_countyfp",
            "VTDST10": "src_vtdst",
            "GEOID10": "src_geoid",
            "NAME10": "src_name",
            "VTDI10": "src_vtdi",
            "vtd_status": "src_vtdi",
        }
    )
    gdf["src_year"] = 2010
    gdf["src_statefp"] = gdf["src_statefp"].astype(str).str.zfill(2)
    gdf["src_countyfp"] = gdf["src_countyfp"].astype(str).str.zfill(3)
    gdf["src_vtdst"] = gdf["src_vtdst"].astype(str).str.strip().str.zfill(4)
    gdf["src_geoid"] = gdf["src_geoid"].astype(str).str.strip()
    gdf["src_vtdi"] = gdf["src_vtdi"].fillna("").astype(str).str.strip()
    gdf["src_name"] = gdf["src_name"].fillna("").astype(str).str.strip()
    return gdf[SOURCE_FIELDS + ["geometry"]].copy()


def read_vtd20_name_lookup() -> Dict[str, str]:
    if VTD20_GEOJSON.exists():
        gdf = gpd.read_file(VTD20_GEOJSON, columns=["GEOID20", "NAME20"])
        geoid_col = "GEOID20"
        name_col = "NAME20"
    elif VTD20_ZIP.exists():
        gdf = gpd.read_file(zip_uri(VTD20_ZIP), columns=["GEOID20", "NAME20"])
        geoid_col = "GEOID20"
        name_col = "NAME20"
    elif VTD20_DRA_PATH.exists():
        gdf = gpd.read_file(VTD20_DRA_PATH, columns=["id", "name"])
        geoid_col = "id"
        name_col = "name"
    else:
        return {}

    out = {}
    for _, row in gdf.iterrows():
        geoid = str(row.get(geoid_col, "")).strip()
        name = str(row.get(name_col, "")).strip()
        if geoid and name:
            out[geoid] = name
    return out


def read_tabblocks(year: int) -> gpd.GeoDataFrame:
    if year == 2000:
        path = TABBLOCK00_ZIP
        if not path.exists():
            raise FileNotFoundError(f"Missing {path}")
        gdf = gpd.read_file(
            zip_uri(path),
            columns=["STATEFP00", "COUNTYFP00", "BLKIDFP00", "geometry"],
        )
        gdf = gdf.rename(
            columns={
                "STATEFP00": "statefp",
                "COUNTYFP00": "countyfp",
                "BLKIDFP00": "block_geoid",
            }
        )
    elif year == 2010:
        path = TABBLOCK10_ZIP
        if not path.exists():
            raise FileNotFoundError(f"Missing {path}")
        gdf = gpd.read_file(
            zip_uri(path),
            columns=["STATEFP10", "COUNTYFP10", "GEOID", "geometry"],
        )
        gdf = gdf.rename(
            columns={
                "STATEFP10": "statefp",
                "COUNTYFP10": "countyfp",
                "GEOID": "block_geoid",
            }
        )
    else:
        raise ValueError(f"Unsupported tabblock year: {year}")

    gdf["statefp"] = gdf["statefp"].astype(str).str.zfill(2)
    gdf["countyfp"] = gdf["countyfp"].astype(str).str.zfill(3)
    gdf["block_geoid"] = gdf["block_geoid"].astype(str).str.strip()
    return gdf[["statefp", "countyfp", "block_geoid", "geometry"]].copy()


def prep(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    invalid = ~gdf.geometry.is_valid
    if invalid.any():
        gdf.loc[invalid, "geometry"] = gdf.loc[invalid, "geometry"].buffer(0)
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()
    return gdf.to_crs(AREA_CRS)


def add_vintage_name_columns(df: pd.DataFrame) -> pd.DataFrame:
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


def build_vtd_to_block(vtd: gpd.GeoDataFrame, blocks: gpd.GeoDataFrame) -> pd.DataFrame:
    """Return source VTD -> same-vintage tabblock weights by source area."""
    vtd = prep(vtd)
    blocks = prep(blocks)
    vtd["src_area_m2"] = vtd.geometry.area
    source_area = vtd[SOURCE_FIELDS + ["src_area_m2"]].drop_duplicates()

    chunks: List[pd.DataFrame] = []
    counties = sorted(set(vtd["src_countyfp"]).union(set(blocks["countyfp"])))
    for county in counties:
        vtd_c = vtd[vtd["src_countyfp"] == county][SOURCE_FIELDS + ["geometry"]]
        block_c = blocks[blocks["countyfp"] == county][["block_geoid", "geometry"]]
        if vtd_c.empty or block_c.empty:
            continue
        inter = gpd.overlay(vtd_c, block_c, how="intersection", keep_geom_type=False)
        if inter.empty:
            continue
        inter["intersection_area_m2"] = inter.geometry.area
        inter = inter[inter["intersection_area_m2"] > 0].copy()
        if inter.empty:
            continue
        grouped = (
            inter.drop(columns=["geometry"])
            .groupby(SOURCE_FIELDS + ["block_geoid"], as_index=False)["intersection_area_m2"]
            .sum()
        )
        chunks.append(grouped)
        print(f"mapped {county}: {len(grouped)} VTD/block intersections", flush=True)

    if not chunks:
        raise RuntimeError("No source VTD/tabblock intersections were produced")

    out = pd.concat(chunks, ignore_index=True)
    out = out.merge(source_area, on=SOURCE_FIELDS, how="left")
    out["weight"] = np.where(
        out["src_area_m2"] > 0,
        out["intersection_area_m2"] / out["src_area_m2"],
        0.0,
    )
    return out[SOURCE_FIELDS + ["block_geoid", "weight"]]


def read_nhgis(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")
    df = pd.read_csv(path, dtype=str)
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0.0)
    df = df[df["weight"] > 0].copy()
    return df[["source_block_geoid", "target_block_geoid", "weight"]]


def read_blockassign_vtd() -> pd.DataFrame:
    if not BLOCKASSIGN_VTD.exists():
        raise FileNotFoundError(f"Missing {BLOCKASSIGN_VTD}")
    df = pd.read_csv(BLOCKASSIGN_VTD, dtype=str)
    vtd20_names = read_vtd20_name_lookup()
    df["block_geoid_2020"] = df["block_geoid_2020"].astype(str).str.strip()
    df["dst_statefp"] = "47"
    df["dst_countyfp"] = df["county_fips"].astype(str).str.zfill(3)
    df["dst_vtdst"] = df["vtd_code"].astype(str).str.strip().str.zfill(6)
    df["dst_geoid"] = df["dst_statefp"] + df["dst_countyfp"] + df["dst_vtdst"]
    df["dst_vtdi"] = ""
    df["dst_name"] = df["dst_geoid"].map(vtd20_names).fillna("")
    return df[
        [
            "block_geoid_2020",
            "dst_statefp",
            "dst_countyfp",
            "dst_vtdst",
            "dst_vtdi",
            "dst_geoid",
            "dst_name",
        ]
    ].drop_duplicates()


def group_block_weights(df: pd.DataFrame, block_col: str, weight_col: str) -> pd.DataFrame:
    return (
        df.groupby(SOURCE_FIELDS + [block_col], as_index=False)[weight_col]
        .sum()
        .rename(columns={weight_col: "weight"})
    )


def chain_to_2020_blocks(vtd_block: pd.DataFrame, source_year: int) -> pd.DataFrame:
    if source_year == 2000:
        x00_10 = read_nhgis(NHGIS_00_10).rename(
            columns={
                "source_block_geoid": "block_geoid",
                "target_block_geoid": "block_geoid_2010",
                "weight": "xwalk_weight",
            }
        )
        step = vtd_block.merge(x00_10, on="block_geoid", how="inner")
        step["chain_weight"] = step["weight"] * step["xwalk_weight"]
        step = step[SOURCE_FIELDS + ["block_geoid_2010", "chain_weight"]]
        step = group_block_weights(
            step.rename(columns={"block_geoid_2010": "block_geoid"}),
            "block_geoid",
            "chain_weight",
        )
        source_year = 2010

    if source_year == 2010:
        x10_20 = read_nhgis(NHGIS_10_20).rename(
            columns={
                "source_block_geoid": "block_geoid",
                "target_block_geoid": "block_geoid_2020",
                "weight": "xwalk_weight",
            }
        )
        step = vtd_block.merge(x10_20, on="block_geoid", how="inner")
        step["chain_weight"] = step["weight"] * step["xwalk_weight"]
        return group_block_weights(step, "block_geoid_2020", "chain_weight")

    raise ValueError(f"Unsupported source year: {source_year}")


def block20_to_vtd20(block20: pd.DataFrame) -> pd.DataFrame:
    assign = read_blockassign_vtd()
    merged = block20.merge(assign, on="block_geoid_2020", how="inner")
    grouped = (
        merged.groupby(
            SOURCE_FIELDS
            + [
                "dst_statefp",
                "dst_countyfp",
                "dst_vtdst",
                "dst_vtdi",
                "dst_geoid",
                "dst_name",
            ],
            as_index=False,
        )["weight"]
        .sum()
        .rename(columns={"weight": "src_weight"})
    )
    grouped = grouped[grouped["src_weight"] > 0].copy()
    grouped["dst_year"] = 2020
    grouped["intersection_area_m2"] = ""
    grouped["src_area_m2"] = ""
    grouped["dst_area_m2"] = ""
    grouped["dst_weight"] = ""
    grouped["chain_method"] = "block_fallback"
    grouped = add_vintage_name_columns(grouped)
    return grouped[
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
            "chain_method",
        ]
    ].sort_values(["src_countyfp", "src_geoid", "dst_geoid"]).reset_index(drop=True)


def source_weight_sums(df: pd.DataFrame, weight_col: str) -> pd.DataFrame:
    return (
        df.groupby(SOURCE_FIELDS, as_index=False)[weight_col]
        .sum()
        .rename(columns={weight_col: "weight_sum"})
    )


def add_stage(
    base: pd.DataFrame,
    stage_df: pd.DataFrame,
    weight_col: str,
    stage_name: str,
) -> pd.DataFrame:
    sums = source_weight_sums(stage_df, weight_col)
    sums = sums.rename(columns={"weight_sum": f"{stage_name}_weight_sum"})
    base = base.merge(sums, on=SOURCE_FIELDS, how="left")
    base[f"{stage_name}_weight_sum"] = base[f"{stage_name}_weight_sum"].fillna(0.0)
    base[f"{stage_name}_has_rows"] = base[f"{stage_name}_weight_sum"] > 0
    return base


def write_2000_diagnostics(
    vtd: gpd.GeoDataFrame,
    vtd_block: pd.DataFrame,
    step_2010: pd.DataFrame,
    block20: pd.DataFrame,
    out: pd.DataFrame,
) -> Dict:
    base = vtd[SOURCE_FIELDS].drop_duplicates().copy()
    base = add_stage(base, vtd_block, "weight", "vtd00_tabblock00")
    base = add_stage(base, step_2010, "weight", "nhgis_2000_2010")
    base = add_stage(base, block20, "weight", "nhgis_2010_2020")
    out_weights = out.rename(columns={"src_weight": "weight"})
    base = add_stage(base, out_weights, "weight", "blockassign_vtd20")

    def failure_stage(row: pd.Series) -> str:
        if not row["vtd00_tabblock00_has_rows"]:
            return "vtd00_tabblock00"
        if not row["nhgis_2000_2010_has_rows"]:
            return "nhgis_2000_2010"
        if not row["nhgis_2010_2020_has_rows"]:
            return "nhgis_2010_2020"
        if not row["blockassign_vtd20_has_rows"]:
            return "blockassign_vtd20"
        return ""

    base["failure_stage"] = base.apply(failure_stage, axis=1)
    base["is_missing"] = base["failure_stage"] != ""
    base = base.sort_values(["src_countyfp", "src_geoid"]).reset_index(drop=True)
    base.to_csv(OUT_DIAGNOSTICS_2000, index=False)

    missing = base[base["is_missing"]].copy()
    by_stage_county = (
        missing.groupby(["failure_stage", "src_countyfp"], as_index=False)
        .size()
        .rename(columns={"size": "missing_src_units"})
        .sort_values(["failure_stage", "missing_src_units", "src_countyfp"], ascending=[True, False, True])
    )
    return {
        "diagnostics_csv": OUT_DIAGNOSTICS_2000.name,
        "missing_src_units": int(len(missing)),
        "missing_by_stage": {
            str(stage): int(count)
            for stage, count in missing["failure_stage"].value_counts().sort_index().items()
        },
        "missing_by_stage_county_top": by_stage_county.head(25).to_dict(orient="records"),
    }


def summarize(df: pd.DataFrame, label: str, source_units: int) -> Dict:
    if df.empty:
        return {
            "pair": label,
            "rows": 0,
            "src_units": 0,
            "dst_units": 0,
            "missing_src_units": source_units,
            "src_weight_sum_min": None,
            "src_weight_sum_mean": None,
            "src_weight_sum_max": None,
        }

    weights = df.groupby("src_geoid", as_index=False)["src_weight"].sum()
    return {
        "pair": label,
        "rows": int(len(df)),
        "src_units": int(df["src_geoid"].nunique()),
        "dst_units": int(df["dst_geoid"].nunique()),
        "missing_src_units": int(source_units - df["src_geoid"].nunique()),
        "src_weight_sum_min": float(weights["src_weight"].min()),
        "src_weight_sum_mean": float(weights["src_weight"].mean()),
        "src_weight_sum_max": float(weights["src_weight"].max()),
    }


def build_for_year(source_year: int) -> tuple[pd.DataFrame, Dict]:
    if source_year == 2000:
        vtd = read_vtd_2000()
        blocks = read_tabblocks(2000)
        output_path = OUT_00_20
    elif source_year == 2010:
        vtd = read_vtd_2010()
        blocks = read_tabblocks(2010)
        output_path = OUT_10_20
    else:
        raise ValueError(f"Unsupported source year: {source_year}")

    vtd_block = build_vtd_to_block(vtd, blocks)
    diagnostics = None
    if source_year == 2000:
        x00_10 = read_nhgis(NHGIS_00_10).rename(
            columns={
                "source_block_geoid": "block_geoid",
                "target_block_geoid": "block_geoid_2010",
                "weight": "xwalk_weight",
            }
        )
        step_2010 = vtd_block.merge(x00_10, on="block_geoid", how="inner")
        step_2010["chain_weight"] = step_2010["weight"] * step_2010["xwalk_weight"]
        step_2010 = step_2010[SOURCE_FIELDS + ["block_geoid_2010", "chain_weight"]]
        step_2010 = group_block_weights(
            step_2010.rename(columns={"block_geoid_2010": "block_geoid"}),
            "block_geoid",
            "chain_weight",
        )
        block20 = chain_to_2020_blocks(step_2010, 2010)
    else:
        block20 = chain_to_2020_blocks(vtd_block, source_year)
    out = block20_to_vtd20(block20)
    out.to_csv(output_path, index=False)
    summary = summarize(out, f"{source_year}->2020 block_fallback", int(vtd["src_geoid"].nunique()))
    summary["output_csv"] = output_path.name
    if source_year == 2000:
        diagnostics = write_2000_diagnostics(vtd, vtd_block, step_2010, block20, out)
        summary["diagnostics"] = diagnostics
    return out, summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[2000, 2010],
        choices=[2000, 2010],
        help="Source VTD vintages to build",
    )
    args = parser.parse_args()

    XWALK_DIR.mkdir(parents=True, exist_ok=True)
    summaries = []
    for year in args.years:
        print(f"Building {year}->2020 block fallback", flush=True)
        _, summary = build_for_year(year)
        summaries.append(summary)

    payload = {
        "inputs": {
            "vtd00": str(VTD00_PATH.relative_to(DATA_DIR)) if VTD00_PATH.exists() else None,
            "vtd10": str(vtd10_path().relative_to(DATA_DIR)),
            "tabblock00": str(TABBLOCK00_ZIP.relative_to(DATA_DIR)) if TABBLOCK00_ZIP.exists() else None,
            "tabblock10": str(TABBLOCK10_ZIP.relative_to(DATA_DIR)) if TABBLOCK10_ZIP.exists() else None,
            "nhgis_00_10": str(NHGIS_00_10.relative_to(DATA_DIR)) if NHGIS_00_10.exists() else None,
            "nhgis_10_20": str(NHGIS_10_20.relative_to(DATA_DIR)) if NHGIS_10_20.exists() else None,
            "blockassign_vtd": str(BLOCKASSIGN_VTD.relative_to(DATA_DIR)) if BLOCKASSIGN_VTD.exists() else None,
        },
        "outputs": {
            "vtd00_to_vtd20_block_fallback": OUT_00_20.name if 2000 in args.years else None,
            "vtd10_to_vtd20_block_fallback": OUT_10_20.name if 2010 in args.years else None,
        },
        "metrics": summaries,
    }
    OUT_SUMMARY.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
