#!/usr/bin/env python3
"""Build explicit VTD block-chain crosswalks through every census vintage.

Full chain (2000 sources):
  VTD00 -> tabblock00 -> (NHGIS 00->10) -> tabblock10 -> VTD10
  VTD10 -> tabblock10 -> (NHGIS 10->20) -> tabblock20 -> VTD20

Full chain (2010 sources):
  VTD10 -> tabblock10 -> (NHGIS 10->20) -> tabblock20 -> VTD20

NHGIS block crosswalks are the bridge between tabblock vintages. Each hop is
materialized under Data/crosswalks/block_chain/ for audit. Combined VTD-level
outputs use the same schema as tn_vtd*_to_vtd20_block_fallback.csv.

Outputs:
  Data/crosswalks/block_chain/hop_*.csv
  Data/crosswalks/tn_vtd00_to_vtd10_block_chain.csv
  Data/crosswalks/tn_vtd10_to_vtd20_block_chain.csv
  Data/crosswalks/tn_vtd00_to_vtd20_block_chain.csv
  Data/crosswalks/tn_vtd_full_block_chain_summary.json
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"
HOP_DIR = XWALK_DIR / "block_chain"

OUT_00_10 = XWALK_DIR / "tn_vtd00_to_vtd10_block_chain.csv"
OUT_10_20 = XWALK_DIR / "tn_vtd10_to_vtd20_block_chain.csv"
OUT_00_20 = XWALK_DIR / "tn_vtd00_to_vtd20_block_chain.csv"
OUT_SUMMARY = XWALK_DIR / "tn_vtd_full_block_chain_summary.json"

VTD_PAIR_FIELDS = [
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


def load_fallback_module():
    script = ROOT / "Scripts" / "build_tn_vtd_block_fallback_crosswalks.py"
    spec = importlib.util.spec_from_file_location("tn_vtd_block_fallback", script)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import {script}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


fb = load_fallback_module()

SOURCE_FIELDS = fb.SOURCE_FIELDS


def build_block_to_vtd(vtd, blocks) -> "fb.pd.DataFrame":
    """Return same-vintage tabblock -> VTD weights by block area."""
    import geopandas as gpd

    vtd = fb.prep(vtd)
    blocks = fb.prep(blocks)
    blocks["block_area_m2"] = blocks.geometry.area
    block_area = blocks[["block_geoid", "block_area_m2"]].drop_duplicates()

    dst_fields = [
        "dst_statefp",
        "dst_countyfp",
        "dst_vtdst",
        "dst_vtdi",
        "dst_geoid",
        "dst_name",
    ]
    vtd = vtd.copy()
    vtd = vtd.rename(
        columns={
            "src_statefp": "dst_statefp",
            "src_countyfp": "dst_countyfp",
            "src_vtdst": "dst_vtdst",
            "src_vtdi": "dst_vtdi",
            "src_geoid": "dst_geoid",
            "src_name": "dst_name",
        }
    )

    chunks: List["fb.pd.DataFrame"] = []
    counties = sorted(set(blocks["countyfp"]).union(set(vtd["dst_countyfp"])))
    for county in counties:
        block_c = blocks[blocks["countyfp"] == county][["block_geoid", "geometry"]]
        vtd_c = vtd[vtd["dst_countyfp"] == county][dst_fields + ["geometry"]]
        if block_c.empty or vtd_c.empty:
            continue
        inter = gpd.overlay(block_c, vtd_c, how="intersection", keep_geom_type=False)
        if inter.empty:
            continue
        inter["intersection_area_m2"] = inter.geometry.area
        inter = inter[inter["intersection_area_m2"] > 0].copy()
        if inter.empty:
            continue
        grouped = (
            inter.drop(columns=["geometry"])
            .groupby(["block_geoid"] + dst_fields, as_index=False)["intersection_area_m2"]
            .sum()
        )
        chunks.append(grouped)

    if not chunks:
        raise RuntimeError("No tabblock/VTD intersections were produced")

    out = fb.pd.concat(chunks, ignore_index=True)
    out = out.merge(block_area, on="block_geoid", how="left")
    out["weight"] = fb.np.where(
        out["block_area_m2"] > 0,
        out["intersection_area_m2"] / out["block_area_m2"],
        0.0,
    )
    return out[["block_geoid"] + dst_fields + ["weight"]]


def write_hop(path: Path, df: "fb.pd.DataFrame") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def chain_vtd00_to_vtd10(
    vtd00_block00: "fb.pd.DataFrame",
    nhgis_00_10: "fb.pd.DataFrame",
    block10_vtd10: "fb.pd.DataFrame",
) -> "fb.pd.DataFrame":
    step = vtd00_block00.merge(
        nhgis_00_10.rename(
            columns={
                "source_block_geoid": "block_geoid",
                "target_block_geoid": "block_geoid_2010",
                "weight": "xwalk_weight",
            }
        ),
        on="block_geoid",
        how="inner",
    )
    step["chain_weight"] = step["weight"] * step["xwalk_weight"]
    step = step.merge(
        block10_vtd10,
        left_on="block_geoid_2010",
        right_on="block_geoid",
        how="inner",
        suffixes=("", "_b10"),
    )
    step["src_weight"] = step["chain_weight"] * step["weight_b10"]
    grouped = (
        step.groupby(
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
        )["src_weight"]
        .sum()
        .rename(columns={"src_weight": "src_weight"})
    )
    grouped = grouped[grouped["src_weight"] > 0].copy()
    grouped["src_year"] = 2000
    grouped["dst_year"] = 2010
    grouped["intersection_area_m2"] = ""
    grouped["src_area_m2"] = ""
    grouped["dst_area_m2"] = ""
    grouped["dst_weight"] = ""
    grouped["chain_method"] = "block_chain"
    grouped = fb.add_vintage_name_columns(grouped)
    return grouped[VTD_PAIR_FIELDS].sort_values(
        ["src_countyfp", "src_geoid", "dst_geoid"]
    ).reset_index(drop=True)


def chain_vtd10_to_vtd20(vtd10: "fb.gpd.GeoDataFrame", blocks10) -> "fb.pd.DataFrame":
    vtd10_block10 = fb.build_vtd_to_block(vtd10, blocks10)
    block20 = fb.chain_to_2020_blocks(vtd10_block10, 2010)
    out = fb.block20_to_vtd20(block20)
    out["chain_method"] = "block_chain"
    return out[VTD_PAIR_FIELDS].sort_values(
        ["src_countyfp", "src_geoid", "dst_geoid"]
    ).reset_index(drop=True)


def compose_vtd00_to_vtd20(
    vtd00_vtd10: "fb.pd.DataFrame",
    vtd10_vtd20: "fb.pd.DataFrame",
) -> "fb.pd.DataFrame":
    left = vtd00_vtd10.copy()
    right = vtd10_vtd20.copy()
    join_keys = [
        "dst_statefp",
        "dst_countyfp",
        "dst_vtdst",
        "dst_vtdi",
        "dst_geoid",
    ]
    merged = left.merge(
        right,
        left_on=join_keys,
        right_on=["src_statefp", "src_countyfp", "src_vtdst", "src_vtdi", "src_geoid"],
        how="inner",
        suffixes=("_00_10", "_10_20"),
    )
    merged["src_weight"] = merged["src_weight_00_10"] * merged["src_weight_10_20"]
    grouped = (
        merged.groupby(
            [
                "src_year_00_10",
                "src_statefp_00_10",
                "src_countyfp_00_10",
                "src_vtdst_00_10",
                "src_vtdi_00_10",
                "src_geoid_00_10",
                "src_name_00_10",
                "name00_00_10",
                "name10_00_10",
                "dst_statefp_10_20",
                "dst_countyfp_10_20",
                "dst_vtdst_10_20",
                "dst_vtdi_10_20",
                "dst_geoid_10_20",
                "dst_name_10_20",
                "name20_10_20",
            ],
            as_index=False,
        )["src_weight"]
        .sum()
        .rename(columns={"src_weight": "src_weight"})
    )
    grouped = grouped.rename(
        columns={
            "src_year_00_10": "src_year",
            "src_statefp_00_10": "src_statefp",
            "src_countyfp_00_10": "src_countyfp",
            "src_vtdst_00_10": "src_vtdst",
            "src_vtdi_00_10": "src_vtdi",
            "src_geoid_00_10": "src_geoid",
            "src_name_00_10": "src_name",
            "name00_00_10": "name00",
            "name10_00_10": "name10",
            "dst_statefp_10_20": "dst_statefp",
            "dst_countyfp_10_20": "dst_countyfp",
            "dst_vtdst_10_20": "dst_vtdst",
            "dst_vtdi_10_20": "dst_vtdi",
            "dst_geoid_10_20": "dst_geoid",
            "dst_name_10_20": "dst_name",
            "name20_10_20": "name20",
        }
    )
    grouped["dst_year"] = 2020
    grouped["intersection_area_m2"] = ""
    grouped["src_area_m2"] = ""
    grouped["dst_area_m2"] = ""
    grouped["dst_weight"] = ""
    grouped["chain_method"] = "block_chain"
    return grouped[VTD_PAIR_FIELDS].sort_values(
        ["src_countyfp", "src_geoid", "dst_geoid"]
    ).reset_index(drop=True)


def export_block20_hops() -> Dict[str, str]:
    assign = fb.read_blockassign_vtd()
    hop_block20_vtd20 = assign.rename(columns={"block_geoid_2020": "block_geoid"}).copy()
    hop_block20_vtd20["weight"] = 1.0
    write_hop(HOP_DIR / "hop_tabblock20_to_vtd20_blockassign.csv", hop_block20_vtd20)

    nhgis_10_20 = fb.read_nhgis(fb.NHGIS_10_20)
    write_hop(
        HOP_DIR / "hop_tabblock10_to_tabblock20_nhgis.csv",
        nhgis_10_20.rename(
            columns={
                "source_block_geoid": "block_geoid_2010",
                "target_block_geoid": "block_geoid_2020",
                "weight": "xwalk_weight",
            }
        ),
    )
    return {
        "hop_tabblock20_to_vtd20_blockassign": "hop_tabblock20_to_vtd20_blockassign.csv",
        "hop_tabblock10_to_tabblock20_nhgis": "hop_tabblock10_to_tabblock20_nhgis.csv",
    }


def build_2000_chain() -> Tuple[Dict, "fb.pd.DataFrame", "fb.pd.DataFrame", "fb.pd.DataFrame"]:
    vtd00 = fb.read_vtd_2000()
    vtd10 = fb.read_vtd_2010()
    blocks00 = fb.read_tabblocks(2000)
    blocks10 = fb.read_tabblocks(2010)

    vtd00_block00 = fb.build_vtd_to_block(vtd00, blocks00)
    write_hop(HOP_DIR / "hop_vtd00_to_tabblock00.csv", vtd00_block00)

    nhgis_00_10 = fb.read_nhgis(fb.NHGIS_00_10)
    write_hop(
        HOP_DIR / "hop_tabblock00_to_tabblock10_nhgis.csv",
        nhgis_00_10.rename(
            columns={
                "source_block_geoid": "block_geoid_2000",
                "target_block_geoid": "block_geoid_2010",
                "weight": "xwalk_weight",
            }
        ),
    )

    block10_vtd10 = build_block_to_vtd(vtd10, blocks10)
    write_hop(HOP_DIR / "hop_tabblock10_to_vtd10.csv", block10_vtd10)

    vtd10_block10 = fb.build_vtd_to_block(vtd10, blocks10)
    write_hop(HOP_DIR / "hop_vtd10_to_tabblock10.csv", vtd10_block10)

    vtd00_vtd10 = chain_vtd00_to_vtd10(vtd00_block00, nhgis_00_10, block10_vtd10)
    vtd10_vtd20 = chain_vtd10_to_vtd20(vtd10, blocks10)
    vtd00_vtd20 = compose_vtd00_to_vtd20(vtd00_vtd10, vtd10_vtd20)

    vtd00_vtd10.to_csv(OUT_00_10, index=False)
    vtd00_vtd20.to_csv(OUT_00_20, index=False)

    metrics = {
        "vtd00_to_vtd10": fb.summarize(vtd00_vtd10, "2000->2010 block_chain", int(vtd00["src_geoid"].nunique())),
        "vtd10_to_vtd20": fb.summarize(vtd10_vtd20, "2010->2020 block_chain", int(vtd10["src_geoid"].nunique())),
        "vtd00_to_vtd20": fb.summarize(vtd00_vtd20, "2000->2020 block_chain", int(vtd00["src_geoid"].nunique())),
    }
    return metrics, vtd00_vtd10, vtd10_vtd20, vtd00_vtd20


def build_2010_chain() -> Tuple[Dict, "fb.pd.DataFrame"]:
    vtd10 = fb.read_vtd_2010()
    blocks10 = fb.read_tabblocks(2010)

    vtd10_block10 = fb.build_vtd_to_block(vtd10, blocks10)
    write_hop(HOP_DIR / "hop_vtd10_to_tabblock10.csv", vtd10_block10)

    vtd10_vtd20 = chain_vtd10_to_vtd20(vtd10, blocks10)
    vtd10_vtd20.to_csv(OUT_10_20, index=False)

    metrics = {
        "vtd10_to_vtd20": fb.summarize(vtd10_vtd20, "2010->2020 block_chain", int(vtd10["src_geoid"].nunique())),
    }
    return metrics, vtd10_vtd20


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[2000, 2010],
        choices=[2000, 2010],
        help="Source VTD vintages to build (2000 also emits 00->10 and 00->20)",
    )
    args = parser.parse_args()

    XWALK_DIR.mkdir(parents=True, exist_ok=True)
    HOP_DIR.mkdir(parents=True, exist_ok=True)

    hop_outputs = export_block20_hops()
    metrics: Dict[str, Dict] = {}
    outputs = {
        "hop_dir": str(HOP_DIR.relative_to(DATA_DIR)),
        **hop_outputs,
    }

    vtd10_vtd20 = None
    if 2000 in args.years:
        print("Building full 2000 block chain (VTD00->block00->VTD10->block10->VTD20)...", flush=True)
        metrics_2000, _, vtd10_vtd20, _ = build_2000_chain()
        metrics.update(metrics_2000)
        outputs["vtd00_to_vtd10_block_chain"] = OUT_00_10.name
        outputs["vtd00_to_vtd20_block_chain"] = OUT_00_20.name
        outputs["hop_vtd00_to_tabblock00"] = "hop_vtd00_to_tabblock00.csv"
        outputs["hop_tabblock00_to_tabblock10_nhgis"] = "hop_tabblock00_to_tabblock10_nhgis.csv"
        outputs["hop_tabblock10_to_vtd10"] = "hop_tabblock10_to_vtd10.csv"
        outputs["hop_vtd10_to_tabblock10"] = "hop_vtd10_to_tabblock10.csv"

    if 2010 in args.years:
        print("Building 2010 block chain (VTD10->block10->VTD20)...", flush=True)
        if vtd10_vtd20 is None:
            metrics_2010, _ = build_2010_chain()
        else:
            metrics_2010 = {"vtd10_to_vtd20": metrics["vtd10_to_vtd20"]}
            vtd10_vtd20.to_csv(OUT_10_20, index=False)
        metrics.update(metrics_2010)
        outputs["vtd10_to_vtd20_block_chain"] = OUT_10_20.name
        if "hop_vtd10_to_tabblock10" not in outputs:
            outputs["hop_vtd10_to_tabblock10"] = "hop_vtd10_to_tabblock10.csv"

    payload = {
        "chain": [
            "VTD00 -> tabblock00",
            "tabblock00 -> tabblock10 (NHGIS blk2000_blk2010)",
            "tabblock10 -> VTD10",
            "VTD10 -> tabblock10",
            "tabblock10 -> tabblock20 (NHGIS blk2010_blk2020)",
            "tabblock20 -> VTD20 (BlockAssign)",
        ],
        "inputs": {
            "vtd00": str(fb.VTD00_PATH.relative_to(DATA_DIR)),
            "vtd10": str(fb.vtd10_path().relative_to(DATA_DIR)),
            "tabblock00": str(fb.TABBLOCK00_ZIP.relative_to(DATA_DIR)),
            "tabblock10": str(fb.TABBLOCK10_ZIP.relative_to(DATA_DIR)),
            "nhgis_00_10": str(fb.NHGIS_00_10.relative_to(DATA_DIR)),
            "nhgis_10_20": str(fb.NHGIS_10_20.relative_to(DATA_DIR)),
            "blockassign_vtd": str(fb.BLOCKASSIGN_VTD.relative_to(DATA_DIR)),
        },
        "outputs": outputs,
        "metrics": metrics,
    }
    OUT_SUMMARY.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2), flush=True)


if __name__ == "__main__":
    main()
