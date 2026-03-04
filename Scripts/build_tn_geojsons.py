#!/usr/bin/env python3
"""Build Tennessee GeoJSON layers from local TIGER/BlockAssign inputs.

Outputs (under Data/):
  - tl_2022_47_cd118.geojson
  - tl_2022_47_sldl.geojson
  - tl_2022_47_sldu.geojson
  - tl_2020_47_county20.geojson
  - tn_voting_precincts.geojson
  - tn_precinct_centroids.geojson
"""

from __future__ import annotations

import io
import json
import re
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"


def norm_text(value: str) -> str:
    s = (value or "").strip().upper()
    s = re.sub(r"[^A-Z0-9 .-]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def read_zip_shapefile(zip_path: Path, columns=None) -> gpd.GeoDataFrame:
    return gpd.read_file(f"zip://{zip_path.resolve()}", columns=columns)


def write_geojson(gdf: gpd.GeoDataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(out_path, driver="GeoJSON")


def convert_district_layers(summary: dict) -> None:
    jobs = [
        ("tl_2022_47_cd118.zip", "tl_2022_47_cd118.geojson"),
        ("tl_2022_47_sldl.zip", "tl_2022_47_sldl.geojson"),
        ("tl_2022_47_sldu.zip", "tl_2022_47_sldu.geojson"),
    ]
    for zip_name, out_name in jobs:
        zpath = DATA_DIR / zip_name
        if not zpath.exists():
            summary["district_layers"][out_name] = {"ok": False, "reason": "missing_zip"}
            continue
        gdf = read_zip_shapefile(zpath).to_crs(4326)
        write_geojson(gdf, DATA_DIR / out_name)
        summary["district_layers"][out_name] = {
            "ok": True,
            "rows": int(len(gdf)),
            "columns": [c for c in gdf.columns if c != "geometry"],
        }


def build_county_layer(tabblocks: gpd.GeoDataFrame, summary: dict) -> gpd.GeoDataFrame:
    county_zip = DATA_DIR / "tl_2020_47_county20.zip"
    if county_zip.exists():
        county = read_zip_shapefile(county_zip).to_crs(4326)
        keep_cols = [
            "STATEFP20",
            "COUNTYFP20",
            "GEOID20",
            "NAME20",
            "NAMELSAD20",
            "geometry",
        ]
        missing = [c for c in keep_cols if c not in county.columns]
        if missing:
            raise RuntimeError(
                f"County zip is missing expected fields: {', '.join(missing)}"
            )
        county = county[keep_cols].copy()
        county_source = county_zip.name
    else:
        county = tabblocks[["COUNTYFP20", "geometry"]].copy()
        county["COUNTYFP20"] = county["COUNTYFP20"].astype(str).str.zfill(3)
        county = county.dissolve(by="COUNTYFP20", as_index=False)
        county["NAME20"] = county["COUNTYFP20"]
        county["GEOID20"] = "47" + county["COUNTYFP20"]
        county["NAMELSAD20"] = "County " + county["COUNTYFP20"]
        if "STATEFP20" in county.columns:
            county = county[
                [
                    "STATEFP20",
                    "COUNTYFP20",
                    "GEOID20",
                    "NAME20",
                    "NAMELSAD20",
                    "geometry",
                ]
            ]
        else:
            county = county.assign(STATEFP20="47")[
                [
                    "STATEFP20",
                    "COUNTYFP20",
                    "GEOID20",
                    "NAME20",
                    "NAMELSAD20",
                    "geometry",
                ]
            ]
        county = county.to_crs(4326)
        county_source = "dissolved_from_tabblock20"

    out_path = DATA_DIR / "tl_2020_47_county20.geojson"
    write_geojson(county, out_path)
    summary["county_layer"] = {
        "ok": True,
        "rows": int(len(county)),
        "output": out_path.name,
        "source": county_source,
    }
    return county


def read_blockassign_vtd() -> pd.DataFrame:
    zpath = DATA_DIR / "BlockAssign_ST47_TN.zip"
    if not zpath.exists():
        raise FileNotFoundError(f"Missing {zpath}")
    with zipfile.ZipFile(zpath) as zf:
        name = next((n for n in zf.namelist() if n.endswith("_VTD.txt")), None)
        if not name:
            raise RuntimeError("BlockAssign VTD text file not found in zip")
        raw = zf.read(name).decode("utf-8-sig", errors="replace")
    df = pd.read_csv(io.StringIO(raw), sep="|", dtype=str)
    df = df.rename(columns={"BLOCKID": "GEOID20", "COUNTYFP": "COUNTYFP20", "DISTRICT": "VTD_CODE"})
    df["GEOID20"] = df["GEOID20"].astype(str).str.strip()
    df["COUNTYFP20"] = df["COUNTYFP20"].astype(str).str.zfill(3)
    df["VTD_CODE"] = df["VTD_CODE"].astype(str).str.strip().str.zfill(6)
    df = df[(df["GEOID20"] != "") & (df["VTD_CODE"] != "")]
    return df[["GEOID20", "COUNTYFP20", "VTD_CODE"]].drop_duplicates()


def load_county_name_map() -> dict:
    """Return COUNTYFP20 -> NAME20 map."""
    county_zip = DATA_DIR / "tl_2020_47_county20.zip"
    if county_zip.exists():
        gdf = read_zip_shapefile(county_zip, columns=["COUNTYFP20", "NAME20"])
        return {
            str(r["COUNTYFP20"]).zfill(3): str(r["NAME20"]).strip()
            for _, r in gdf.iterrows()
        }

    county_geojson = DATA_DIR / "tl_2020_47_county20.geojson"
    if county_geojson.exists():
        gdf = gpd.read_file(county_geojson, columns=["COUNTYFP20", "NAME20"])
        return {
            str(r["COUNTYFP20"]).zfill(3): str(r["NAME20"]).strip()
            for _, r in gdf.iterrows()
        }
    return {}


def build_precinct_layers(tabblocks: gpd.GeoDataFrame, summary: dict) -> None:
    vtd = read_blockassign_vtd()
    county_name_map = load_county_name_map()
    blocks = tabblocks[["GEOID20", "COUNTYFP20", "geometry"]].copy()
    blocks["GEOID20"] = blocks["GEOID20"].astype(str)
    blocks["COUNTYFP20"] = blocks["COUNTYFP20"].astype(str).str.zfill(3)

    merged = blocks.merge(vtd, on=["GEOID20", "COUNTYFP20"], how="inner")
    if merged.empty:
        raise RuntimeError("No tabblock rows matched BlockAssign VTD rows")

    merged["county_nam"] = merged["COUNTYFP20"].map(
        lambda f: county_name_map.get(str(f).zfill(3), str(f).zfill(3))
    )
    merged["prec_id"] = merged["VTD_CODE"]
    merged = gpd.GeoDataFrame(merged, geometry="geometry", crs=tabblocks.crs)

    precinct = merged[["county_nam", "prec_id", "geometry"]].dissolve(
        by=["county_nam", "prec_id"],
        as_index=False,
    )
    precinct["county_norm"] = precinct["county_nam"].map(norm_text)
    precinct["precinct_name"] = precinct["county_nam"] + " - " + precinct["prec_id"]
    precinct["precinct_norm"] = precinct["precinct_name"].map(norm_text)
    precinct["id"] = range(1, len(precinct) + 1)
    precinct = precinct.to_crs(4326)

    precinct_out = DATA_DIR / "tn_voting_precincts.geojson"
    write_geojson(
        precinct[
            [
                "county_nam",
                "county_norm",
                "prec_id",
                "precinct_name",
                "precinct_norm",
                "id",
                "geometry",
            ]
        ],
        precinct_out,
    )

    centroid = precinct.copy()
    centroid["geometry"] = centroid.representative_point()
    centroid_out = DATA_DIR / "tn_precinct_centroids.geojson"
    write_geojson(
        centroid[
            [
                "county_nam",
                "county_norm",
                "prec_id",
                "precinct_name",
                "precinct_norm",
                "id",
                "geometry",
            ]
        ],
        centroid_out,
    )

    summary["precinct_layer"] = {
        "ok": True,
        "rows": int(len(precinct)),
        "outputs": [precinct_out.name, centroid_out.name],
    }


def main() -> None:
    summary = {"district_layers": {}, "county_layer": {}, "precinct_layer": {}}

    tabblock_zip = DATA_DIR / "tl_2020_47_tabblock20.zip"
    if not tabblock_zip.exists():
        raise FileNotFoundError(f"Missing {tabblock_zip}")

    convert_district_layers(summary)

    tabblocks = read_zip_shapefile(
        tabblock_zip,
        columns=["STATEFP20", "COUNTYFP20", "GEOID20", "geometry"],
    )
    tabblocks = tabblocks.to_crs(4326)

    build_county_layer(tabblocks, summary)
    build_precinct_layers(tabblocks, summary)

    summary_path = DATA_DIR / "tn_geojson_build_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
