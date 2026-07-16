#!/usr/bin/env python3
"""Build precinct-to-district carryover crosswalks for district line toggles.

The frontend uses these CSVs to map a selected district across scopes by
shared 2020 precinct geography. Tennessee legislative lines did not change
for the app's available post-2022 line sets, so legislative scopes only emit
the 2022 geometry crosswalk and the frontend falls back to it.

Optional reviewed split overrides live in
Data/crosswalks/tn_district_split_overrides.csv. When present, rows for a
(scope, lines_year, precinct_key) fully replace the geometric area weights for
that precinct.

Outputs under Data/crosswalks/:
  - tn_congressional_2022_precinct_crosswalk.csv
  - tn_congressional_2026_precinct_crosswalk.csv
  - tn_state_house_2022_precinct_crosswalk.csv
  - tn_state_senate_2022_precinct_crosswalk.csv
  - tn_district_carryover_crosswalk_summary.json
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"

PRECINCT_GEOJSON = DATA_DIR / "tn_voting_precincts.geojson"
PRECINCT_FRIENDLY_NAMES = XWALK_DIR / "tn_precinct_friendly_names_2020.json"
SPLIT_OVERRIDES_CSV = XWALK_DIR / "tn_district_split_overrides.csv"

JOBS = [
    {
        "scope": "congressional",
        "lines_year": 2022,
        "geometry": DATA_DIR / "tl_2022_47_cd118.geojson",
        "district_field": "CD118FP",
        "output": XWALK_DIR / "tn_congressional_2022_precinct_crosswalk.csv",
    },
    {
        "scope": "congressional",
        "lines_year": 2026,
        "geometry": DATA_DIR / "tl_2026_47_cd2026.geojson",
        "district_field": "DISTRICT",
        "output": XWALK_DIR / "tn_congressional_2026_precinct_crosswalk.csv",
    },
    {
        "scope": "state_house",
        "lines_year": 2022,
        "geometry": DATA_DIR / "tl_2022_47_sldl.geojson",
        "district_field": "SLDLST",
        "output": XWALK_DIR / "tn_state_house_2022_precinct_crosswalk.csv",
    },
    {
        "scope": "state_senate",
        "lines_year": 2022,
        "geometry": DATA_DIR / "tl_2022_47_sldu.geojson",
        "district_field": "SLDUST",
        "output": XWALK_DIR / "tn_state_senate_2022_precinct_crosswalk.csv",
    },
]

AREA_CRS = "EPSG:5070"


def normalize_district(value) -> str:
    s = str(value or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        return str(int(digits))
    return s.lstrip("0") or s


def normalize_key(value: str) -> str:
    return " ".join(str(value or "").strip().upper().split())


def numeric(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def load_split_overrides() -> Dict[Tuple[str, int, str], List[dict]]:
    """Load full-replacement district weights keyed by scope/year/precinct."""
    if not SPLIT_OVERRIDES_CSV.exists():
        return {}
    grouped: Dict[Tuple[str, int, str], List[dict]] = defaultdict(list)
    with SPLIT_OVERRIDES_CSV.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            scope = str(row.get("scope") or "").strip()
            lines_year_raw = str(row.get("lines_year") or "").strip()
            precinct_key = normalize_key(row.get("precinct_key") or "")
            district_num = normalize_district(row.get("district_num"))
            weight = numeric(row.get("area_weight"))
            if not scope or not lines_year_raw or not precinct_key or not district_num or weight <= 0:
                continue
            try:
                lines_year = int(float(lines_year_raw))
            except ValueError:
                continue
            grouped[(scope, lines_year, precinct_key)].append(
                {
                    "district_num": district_num,
                    "area_weight": weight,
                    "note": str(row.get("note") or "").strip(),
                    "source": str(row.get("source") or "").strip(),
                }
            )

    out: Dict[Tuple[str, int, str], List[dict]] = {}
    for key, rows in grouped.items():
        total = sum(r["area_weight"] for r in rows)
        if total <= 0:
            continue
        normalized = []
        for row in rows:
            item = dict(row)
            item["area_weight"] = row["area_weight"] / total
            normalized.append(item)
        out[key] = normalized
    return out


def apply_split_overrides(grouped, job: dict, overrides: Dict[Tuple[str, int, str], List[dict]]) -> tuple:
    """Replace geometric weights for overridden precincts and return (df, stats)."""
    import pandas as pd

    scope = str(job["scope"])
    lines_year = int(job["lines_year"])
    relevant = {
        precinct_key: rows
        for (ov_scope, ov_year, precinct_key), rows in overrides.items()
        if ov_scope == scope and ov_year == lines_year
    }
    if not relevant:
        return grouped, {
            "override_precincts": 0,
            "override_rows": 0,
            "override_csv": str(SPLIT_OVERRIDES_CSV.relative_to(DATA_DIR)) if SPLIT_OVERRIDES_CSV.exists() else None,
        }

    base = grouped[~grouped["precinct_key"].isin(relevant.keys())].copy()
    replacement_rows = []
    for precinct_key, ov_rows in relevant.items():
        sample = grouped[grouped["precinct_key"] == precinct_key]
        if sample.empty:
            continue
        sample_row = sample.iloc[0]
        precinct_area = float(sample_row["precinct_area_m2"])
        for ov in ov_rows:
            weight = float(ov["area_weight"])
            replacement_rows.append(
                {
                    "county_norm": sample_row["county_norm"],
                    "prec_id": sample_row["prec_id"],
                    "precinct_key": precinct_key,
                    "district_num": ov["district_num"],
                    "vtd_name20": sample_row["vtd_name20"],
                    "intersection_area_m2": precinct_area * weight,
                    "precinct_area_m2": precinct_area,
                    "area_weight": weight,
                    "name20": sample_row.get("name20", sample_row["vtd_name20"]),
                }
            )

    if replacement_rows:
        replaced = pd.DataFrame(replacement_rows)
        out = pd.concat([base, replaced], ignore_index=True)
    else:
        out = base
    out = out[out["area_weight"] > 0].copy()
    out = out.sort_values(["district_num", "county_norm", "prec_id"]).reset_index(drop=True)
    return out, {
        "override_precincts": int(len(relevant)),
        "override_rows": int(sum(len(v) for v in relevant.values())),
        "override_csv": str(SPLIT_OVERRIDES_CSV.relative_to(DATA_DIR)),
    }


def load_precinct_friendly_names() -> Dict[tuple[str, str], str]:
    if not PRECINCT_FRIENDLY_NAMES.exists():
        return {}
    payload = json.loads(PRECINCT_FRIENDLY_NAMES.read_text(encoding="utf-8"))
    out = {}
    for county, labels in (payload.get("counties", {}) or {}).items():
        county_norm = str(county or "").strip().upper()
        for code, label in (labels or {}).items():
            prec_id = str(code or "").strip().upper().zfill(6)
            name = str(label or "").strip()
            if county_norm and prec_id and name:
                out[(county_norm, prec_id)] = name
    return out


def load_precincts():
    import geopandas as gpd

    if not PRECINCT_GEOJSON.exists():
        raise FileNotFoundError(f"Missing {PRECINCT_GEOJSON}")
    friendly_names = load_precinct_friendly_names()
    gdf = gpd.read_file(PRECINCT_GEOJSON)[
        ["county_norm", "prec_id", "precinct_name", "precinct_norm", "geometry"]
    ].copy()
    gdf["county_norm"] = gdf["county_norm"].astype(str).str.strip().str.upper()
    gdf["prec_id"] = gdf["prec_id"].astype(str).str.strip().str.zfill(6)
    gdf["vtd_name20"] = gdf["precinct_name"].fillna("").astype(str).str.strip()
    gdf["friendly_name20"] = [
        friendly_names.get((county, prec_id), "")
        for county, prec_id in zip(gdf["county_norm"], gdf["prec_id"])
    ]
    gdf.loc[gdf["friendly_name20"] != "", "vtd_name20"] = gdf["friendly_name20"]
    gdf["precinct_key"] = gdf["precinct_norm"].astype(str).str.strip().str.upper()
    gdf.loc[gdf["precinct_key"] == "", "precinct_key"] = (
        gdf["county_norm"] + " - " + gdf["prec_id"]
    )
    gdf.loc[gdf["vtd_name20"] == "", "vtd_name20"] = gdf["precinct_key"]
    gdf = gdf[gdf["geometry"].notna() & (gdf["precinct_key"] != "")].copy()
    gdf = gdf.to_crs(AREA_CRS)
    gdf["precinct_area_m2"] = gdf.geometry.area
    gdf = gdf[gdf["precinct_area_m2"] > 0].copy()
    return gdf


def build_one(job: dict, precincts, overrides: Dict[Tuple[str, int, str], List[dict]]) -> Dict:
    import geopandas as gpd

    geometry_path = Path(job["geometry"])
    if not geometry_path.exists():
        raise FileNotFoundError(f"Missing {geometry_path}")

    district_field = str(job["district_field"])
    districts = gpd.read_file(geometry_path)[[district_field, "geometry"]].copy()
    districts["district_num"] = districts[district_field].apply(normalize_district)
    districts = districts[
        districts["geometry"].notna() & (districts["district_num"] != "")
    ].copy()
    districts = districts.to_crs(AREA_CRS)

    left = precincts[
        ["county_norm", "prec_id", "precinct_key", "vtd_name20", "precinct_area_m2", "geometry"]
    ].copy()
    right = districts[["district_num", "geometry"]].copy()
    joined = gpd.overlay(left, right, how="intersection", keep_geom_type=False)
    if joined.empty:
        raise RuntimeError(f"No intersections for {job['scope']} {job['lines_year']}")

    joined["intersection_area_m2"] = joined.geometry.area
    joined = joined[joined["intersection_area_m2"] > 0].copy()
    joined["area_weight"] = joined["intersection_area_m2"] / joined["precinct_area_m2"]

    grouped = (
        joined.drop(columns=["geometry"])
        .groupby(
            ["county_norm", "prec_id", "precinct_key", "district_num"],
            as_index=False,
        )
        .agg(
            vtd_name20=("vtd_name20", "first"),
            intersection_area_m2=("intersection_area_m2", "sum"),
            precinct_area_m2=("precinct_area_m2", "first"),
            area_weight=("area_weight", "sum"),
        )
    )
    grouped = grouped[grouped["area_weight"] > 0].copy()
    grouped["name20"] = grouped["vtd_name20"]
    grouped, override_stats = apply_split_overrides(grouped, job, overrides)

    out_rows = grouped[
        [
            "precinct_key",
            "vtd_name20",
            "name20",
            "county_norm",
            "prec_id",
            "district_num",
            "area_weight",
            "intersection_area_m2",
            "precinct_area_m2",
        ]
    ].copy()
    job["output"].parent.mkdir(parents=True, exist_ok=True)
    out_rows.to_csv(job["output"], index=False)

    weight_sums = grouped.groupby("precinct_key", as_index=False)["area_weight"].sum()
    return {
        "scope": job["scope"],
        "lines_year": job["lines_year"],
        "geometry": str(geometry_path.relative_to(DATA_DIR)),
        "output_csv": job["output"].name,
        "source_note": job.get("source_note", ""),
        "rows": int(len(grouped)),
        "districts": int(grouped["district_num"].nunique()),
        "precincts": int(grouped["precinct_key"].nunique()),
        "total_precincts": int(len(precincts)),
        "missing_precincts": int(len(precincts) - grouped["precinct_key"].nunique()),
        "precinct_weight_sum_min": float(weight_sums["area_weight"].min()),
        "precinct_weight_sum_mean": float(weight_sums["area_weight"].mean()),
        "precinct_weight_sum_max": float(weight_sums["area_weight"].max()),
        **override_stats,
    }


def main() -> None:
    precincts = load_precincts()
    overrides = load_split_overrides()
    summaries: List[Dict] = []
    for job in JOBS:
        print(f"Building {job['scope']} {job['lines_year']} carryover crosswalk")
        summaries.append(build_one(job, precincts, overrides))

    payload = {
        "source_precinct_geometry": str(PRECINCT_GEOJSON.relative_to(DATA_DIR)),
        "split_overrides_csv": str(SPLIT_OVERRIDES_CSV.relative_to(DATA_DIR)) if SPLIT_OVERRIDES_CSV.exists() else None,
        "split_override_precincts": int(len(overrides)),
        "area_crs": AREA_CRS,
        "outputs": summaries,
    }
    summary_path = XWALK_DIR / "tn_district_carryover_crosswalk_summary.json"
    summary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
