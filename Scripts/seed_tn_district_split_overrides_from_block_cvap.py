#!/usr/bin/env python3
"""Build block-CVAP-weighted district split overrides for high-priority precincts.

For each high-priority geometry-review precinct with a real secondary split,
assign BlockAssign 2020 blocks (weighted by CVAP_TOT24) to districts via block
internal points, then replace that precinct's override weights.

Falls back to the sibling TNPrecinctMap block-CVAP CSV when the local copy is
missing. Existing sliver-prune overrides are preserved for precincts that are
not reweighted here.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"
REPORT_DIR = DATA_DIR / "reports" / "district_crosswalk_comparison"

SUMMARY_CSV = REPORT_DIR / "split_geometry_review" / "split_geometry_review_summary.csv"
QUEUE_CSV = REPORT_DIR / "district_split_review_queue.csv"
EXISTING_OVERRIDES = XWALK_DIR / "tn_district_split_overrides.csv"
BLOCKASSIGN = XWALK_DIR / "blockassign_tn_vtd.csv"
TABBLOCK_ZIP = DATA_DIR / "tl_2020_47_tabblock20.zip"
COUNTY_GEOJSON = DATA_DIR / "tl_2020_47_county20.geojson"
PRECINCT_GEOJSON = DATA_DIR / "tn_voting_precincts.geojson"

CVAP_CANDIDATES = [
    DATA_DIR / "tn_cvap_2024_2020_b_csv" / "tn_cvap_2024_2020_b.csv",
    ROOT.parent / "TNPrecinctMap" / "Data" / "tn_cvap_2024_2020_b_csv" / "tn_cvap_2024_2020_b.csv",
]

OUT_OVERRIDES = XWALK_DIR / "tn_district_split_overrides.csv"
OUT_COMPARE = REPORT_DIR / "district_split_cvap_weight_comparison.csv"
OUT_SUMMARY = XWALK_DIR / "tn_district_split_cvap_override_summary.json"

# Drop residual CVAP pieces below this share, then renormalize.
MIN_CVAP_WEIGHT = 0.01

GEOMETRY_JOBS = {
    ("congressional", 2022): {
        "path": DATA_DIR / "tl_2022_47_cd118.geojson",
        "district_field": "CD118FP",
    },
    ("congressional", 2026): {
        "path": DATA_DIR / "tl_2026_47_cd2026.geojson",
        "district_field": "DISTRICT",
    },
    ("state_house", 2022): {
        "path": DATA_DIR / "tl_2022_47_sldl.geojson",
        "district_field": "SLDLST",
    },
    ("state_senate", 2022): {
        "path": DATA_DIR / "tl_2022_47_sldu.geojson",
        "district_field": "SLDUST",
    },
}

OVERRIDE_FIELDS = [
    "scope",
    "lines_year",
    "precinct_key",
    "county_norm",
    "prec_id",
    "name20",
    "district_num",
    "area_weight",
    "note",
    "source",
]


def numeric(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def normalize_key(value: str) -> str:
    return " ".join(str(value or "").strip().upper().split())


def normalize_district(value) -> str:
    s = str(value or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        return str(int(digits))
    return s.lstrip("0") or s


def find_cvap_csv() -> Path:
    for path in CVAP_CANDIDATES:
        if path.exists():
            return path
    raise FileNotFoundError(
        "Missing block CVAP CSV. Expected one of:\n"
        + "\n".join(str(p) for p in CVAP_CANDIDATES)
    )


def county_name_by_fips() -> Dict[str, str]:
    gdf = gpd.read_file(COUNTY_GEOJSON, columns=["COUNTYFP20", "NAME20"])
    return {
        str(row.COUNTYFP20).zfill(3): normalize_key(row.NAME20)
        for _, row in gdf.iterrows()
        if str(row.COUNTYFP20).strip()
    }


def load_targets() -> List[dict]:
    path = SUMMARY_CSV if SUMMARY_CSV.exists() else QUEUE_CSV
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    out = []
    for row in rows:
        if str(row.get("review_priority") or "").strip().lower() != "high":
            continue
        if path == SUMMARY_CSV and numeric(row.get("real_secondary_count")) <= 0:
            continue
        out.append(
            {
                "scope": str(row.get("scope") or "").strip(),
                "lines_year": int(float(row.get("lines_year") or 0)),
                "precinct_key": normalize_key(row.get("precinct_key") or ""),
                "county_norm": normalize_key(row.get("county_norm") or ""),
                "prec_id": str(row.get("prec_id") or "").strip().zfill(6),
                "name20": str(row.get("name20") or "").strip(),
                "area_weights": str(
                    row.get("recomputed_district_weights")
                    or row.get("queue_district_weights")
                    or row.get("district_weights")
                    or ""
                ),
            }
        )
    # Unique by scope/year/precinct.
    seen = set()
    unique = []
    for row in out:
        key = (row["scope"], row["lines_year"], row["precinct_key"])
        if key in seen or not all(key):
            continue
        seen.add(key)
        unique.append(row)
    return unique


def parse_weights(raw: str) -> Dict[str, float]:
    out = {}
    for part in str(raw or "").split(";"):
        part = part.strip()
        if ":" not in part:
            continue
        district, weight = part.split(":", 1)
        out[normalize_district(district)] = numeric(weight)
    return out


def load_existing_overrides() -> List[dict]:
    if not EXISTING_OVERRIDES.exists():
        return []
    with EXISTING_OVERRIDES.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def load_blockassign_for_precincts(targets: Iterable[dict], county_fips: Dict[str, str]) -> pd.DataFrame:
    wanted = {
        (county_fips[t["county_norm"]], t["prec_id"])
        for t in targets
        if t["county_norm"] in county_fips
    }
    rows = []
    with BLOCKASSIGN.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            county = str(row.get("county_fips") or "").zfill(3)
            vtd = str(row.get("vtd_code") or "").strip().zfill(6)
            if (county, vtd) not in wanted:
                continue
            rows.append(
                {
                    "block_geoid_2020": str(row.get("block_geoid_2020") or "").strip(),
                    "county_fips": county,
                    "vtd_code": vtd,
                }
            )
    if not rows:
        raise RuntimeError("No BlockAssign rows matched the target precincts")
    return pd.DataFrame(rows)


def load_cvap(block_ids: Iterable[str]) -> pd.DataFrame:
    wanted = set(block_ids)
    path = find_cvap_csv()
    rows = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            geoid = str(row.get("GEOID20") or "").strip()
            if geoid not in wanted:
                continue
            rows.append({"block_geoid_2020": geoid, "cvap_tot": numeric(row.get("CVAP_TOT24"))})
    df = pd.DataFrame(rows)
    if df.empty:
        raise RuntimeError(f"No CVAP rows matched target blocks from {path}")
    return df, path


def load_block_points(counties: Iterable[str]) -> gpd.GeoDataFrame:
    county_set = {str(c).zfill(3) for c in counties}
    gdf = gpd.read_file(
        f"zip://{TABBLOCK_ZIP.resolve()}",
        columns=["GEOID20", "COUNTYFP20", "INTPTLAT20", "INTPTLON20"],
    )
    gdf["COUNTYFP20"] = gdf["COUNTYFP20"].astype(str).str.zfill(3)
    gdf = gdf[gdf["COUNTYFP20"].isin(county_set)].copy()
    gdf["block_geoid_2020"] = gdf["GEOID20"].astype(str).str.strip()
    gdf["geometry"] = [
        Point(float(lon), float(lat))
        for lon, lat in zip(gdf["INTPTLON20"], gdf["INTPTLAT20"])
    ]
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:4326")
    return gdf[["block_geoid_2020", "COUNTYFP20", "geometry"]].copy()


def load_districts(scope: str, lines_year: int) -> gpd.GeoDataFrame:
    job = GEOMETRY_JOBS[(scope, lines_year)]
    path = job["path"]
    field = job["district_field"]
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(4326)
    gdf["district_num"] = gdf[field].apply(normalize_district)
    return gdf[gdf["district_num"].astype(str).str.len() > 0][["district_num", "geometry"]].copy()


def precinct_name_lookup() -> Dict[str, str]:
    if not PRECINCT_GEOJSON.exists():
        return {}
    gdf = gpd.read_file(PRECINCT_GEOJSON, columns=["precinct_norm", "precinct_name", "county_norm", "prec_id"])
    out = {}
    for _, row in gdf.iterrows():
        key = normalize_key(row.get("precinct_norm") or f"{row.get('county_norm')} - {row.get('prec_id')}")
        name = str(row.get("precinct_name") or "").strip()
        if key:
            out[key] = name or key
    return out


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    targets = load_targets()
    if not targets:
        raise RuntimeError("No high-priority real-secondary targets found")

    county_by_fips = county_name_by_fips()
    fips_by_county = {v: k for k, v in county_by_fips.items()}
    for row in targets:
        if not row["county_norm"] and " - " in row["precinct_key"]:
            row["county_norm"] = row["precinct_key"].split(" - ", 1)[0]
        if not row["prec_id"] and " - " in row["precinct_key"]:
            row["prec_id"] = row["precinct_key"].split(" - ", 1)[1].zfill(6)
        if not row["county_norm"] or row["county_norm"] not in fips_by_county:
            raise RuntimeError(f"Missing county FIPS for {row['precinct_key']}")

    print(f"Targets: {len(targets)}", flush=True)
    blockassign = load_blockassign_for_precincts(targets, fips_by_county)
    print(f"BlockAssign rows: {len(blockassign)}", flush=True)
    cvap, cvap_path = load_cvap(blockassign["block_geoid_2020"])
    print(f"CVAP source: {cvap_path}", flush=True)
    print(f"CVAP rows: {len(cvap)}", flush=True)

    counties = sorted({fips_by_county[t["county_norm"]] for t in targets})
    points = load_block_points(counties)
    print(f"Block points: {len(points)}", flush=True)

    blocks = (
        blockassign.merge(cvap, on="block_geoid_2020", how="inner")
        .merge(points, on="block_geoid_2020", how="inner")
    )
    blocks = gpd.GeoDataFrame(blocks, geometry="geometry", crs="EPSG:4326")
    blocks["county_norm"] = blocks["county_fips"].map(county_by_fips)
    blocks["precinct_key"] = (
        blocks["county_norm"].astype(str) + " - " + blocks["vtd_code"].astype(str)
    ).map(normalize_key)

    names = precinct_name_lookup()
    district_cache: Dict[Tuple[str, int], gpd.GeoDataFrame] = {}
    compare_rows: List[dict] = []
    cvap_overrides: List[dict] = []
    replaced_keys = set()

    for target in targets:
        key = (target["scope"], target["lines_year"], target["precinct_key"])
        cache_key = (target["scope"], target["lines_year"])
        if cache_key not in district_cache:
            district_cache[cache_key] = load_districts(*cache_key)
        districts = district_cache[cache_key]

        subset = blocks[blocks["precinct_key"] == target["precinct_key"]].copy()
        if subset.empty:
            print(f"SKIP no blocks: {target['precinct_key']}", flush=True)
            continue

        joined = gpd.sjoin(subset, districts, how="inner", predicate="within")
        if joined.empty:
            # Fall back to intersects for edge points.
            joined = gpd.sjoin(subset, districts, how="inner", predicate="intersects")
        if joined.empty:
            print(f"SKIP no district hits: {target['precinct_key']} {target['scope']}", flush=True)
            continue

        totals = (
            joined.groupby("district_num", as_index=False)["cvap_tot"]
            .sum()
            .sort_values("cvap_tot", ascending=False)
        )
        total_cvap = float(totals["cvap_tot"].sum())
        if total_cvap <= 0:
            print(f"SKIP zero CVAP: {target['precinct_key']}", flush=True)
            continue

        area_weights = parse_weights(target["area_weights"])
        name20 = target["name20"] or names.get(target["precinct_key"], target["precinct_key"])
        raw_parts = []
        for _, row in totals.iterrows():
            district = normalize_district(row["district_num"])
            cvap_weight = float(row["cvap_tot"]) / total_cvap
            if cvap_weight <= 0:
                continue
            raw_parts.append(
                {
                    "district_num": district,
                    "cvap_tot": float(row["cvap_tot"]),
                    "cvap_weight": cvap_weight,
                    "area_weight": area_weights.get(district, 0.0),
                }
            )

        keepers = [p for p in raw_parts if p["cvap_weight"] >= MIN_CVAP_WEIGHT]
        dropped = [p for p in raw_parts if p["cvap_weight"] < MIN_CVAP_WEIGHT]
        if not keepers:
            # Extremely fragmented CVAP: keep the dominant piece only.
            keepers = sorted(raw_parts, key=lambda p: p["cvap_weight"], reverse=True)[:1]
            dropped = [p for p in raw_parts if p not in keepers]
        keep_cvap = sum(p["cvap_tot"] for p in keepers)
        if keep_cvap <= 0:
            print(f"SKIP zero kept CVAP: {target['precinct_key']}", flush=True)
            continue

        dropped_note = ";".join(
            f"{p['district_num']}:{p['cvap_weight']:.6f}" for p in dropped
        )
        for part in keepers:
            final_weight = float(part["cvap_tot"]) / keep_cvap
            note = (
                f"block_cvap_weighted; area_weight={part['area_weight']:.6f}; "
                f"cvap={part['cvap_tot']:.0f}/{total_cvap:.0f}"
            )
            if dropped_note:
                note += f"; drop_cvap_slivers[{dropped_note}]; renormalize"
            cvap_overrides.append(
                {
                    "scope": target["scope"],
                    "lines_year": str(target["lines_year"]),
                    "precinct_key": target["precinct_key"],
                    "county_norm": target["county_norm"],
                    "prec_id": target["prec_id"],
                    "name20": name20,
                    "district_num": part["district_num"],
                    "area_weight": f"{final_weight:.8f}",
                    "note": note,
                    "source": "block_cvap_weighted",
                }
            )
            compare_rows.append(
                {
                    "scope": target["scope"],
                    "lines_year": target["lines_year"],
                    "precinct_key": target["precinct_key"],
                    "name20": name20,
                    "district_num": part["district_num"],
                    "area_weight": round(part["area_weight"], 8),
                    "cvap_weight_raw": round(part["cvap_weight"], 8),
                    "cvap_weight": round(final_weight, 8),
                    "weight_delta_cvap_minus_area": round(final_weight - part["area_weight"], 8),
                    "cvap_tot": round(part["cvap_tot"], 4),
                    "precinct_cvap_tot": round(total_cvap, 4),
                    "dropped_cvap_sliver": False,
                }
            )
        for part in dropped:
            compare_rows.append(
                {
                    "scope": target["scope"],
                    "lines_year": target["lines_year"],
                    "precinct_key": target["precinct_key"],
                    "name20": name20,
                    "district_num": part["district_num"],
                    "area_weight": round(part["area_weight"], 8),
                    "cvap_weight_raw": round(part["cvap_weight"], 8),
                    "cvap_weight": 0.0,
                    "weight_delta_cvap_minus_area": round(0.0 - part["area_weight"], 8),
                    "cvap_tot": round(part["cvap_tot"], 4),
                    "precinct_cvap_tot": round(total_cvap, 4),
                    "dropped_cvap_sliver": True,
                }
            )
        replaced_keys.add(key)
        print(
            f"OK {target['scope']} {target['lines_year']} {target['precinct_key']}: "
            f"{len(keepers)} districts kept, {len(dropped)} CVAP slivers dropped, "
            f"CVAP={total_cvap:.0f}",
            flush=True,
        )

    # Preserve prior overrides for precincts not replaced by CVAP weights.
    preserved = []
    for row in load_existing_overrides():
        key = (
            str(row.get("scope") or "").strip(),
            int(float(row.get("lines_year") or 0)),
            normalize_key(row.get("precinct_key") or ""),
        )
        if key in replaced_keys:
            continue
        preserved.append(
            {
                "scope": row.get("scope", ""),
                "lines_year": str(row.get("lines_year") or ""),
                "precinct_key": normalize_key(row.get("precinct_key") or ""),
                "county_norm": normalize_key(row.get("county_norm") or ""),
                "prec_id": str(row.get("prec_id") or "").strip().zfill(6),
                "name20": str(row.get("name20") or "").strip(),
                "district_num": normalize_district(row.get("district_num")),
                "area_weight": f"{numeric(row.get('area_weight')):.8f}",
                "note": str(row.get("note") or "").strip(),
                "source": str(row.get("source") or "").strip(),
            }
        )

    merged = preserved + cvap_overrides
    merged.sort(
        key=lambda r: (
            r["scope"],
            r["lines_year"],
            r["precinct_key"],
            -numeric(r["area_weight"]),
            r["district_num"],
        )
    )
    write_csv(OUT_OVERRIDES, merged, OVERRIDE_FIELDS)
    write_csv(
        OUT_COMPARE,
        sorted(
            compare_rows,
            key=lambda r: abs(r["weight_delta_cvap_minus_area"]),
            reverse=True,
        ),
        [
            "scope",
            "lines_year",
            "precinct_key",
            "name20",
            "district_num",
            "area_weight",
            "cvap_weight_raw",
            "cvap_weight",
            "weight_delta_cvap_minus_area",
            "cvap_tot",
            "precinct_cvap_tot",
            "dropped_cvap_sliver",
        ],
    )

    dropped_sliver_rows = sum(1 for r in compare_rows if r.get("dropped_cvap_sliver"))
    summary = {
        "targets": len(targets),
        "cvap_reweighted_precincts": len(replaced_keys),
        "cvap_override_rows": len(cvap_overrides),
        "dropped_cvap_sliver_pieces": dropped_sliver_rows,
        "min_cvap_weight": MIN_CVAP_WEIGHT,
        "preserved_prior_override_rows": len(preserved),
        "merged_override_rows": len(merged),
        "cvap_source": str(cvap_path),
        "comparison_csv": str(OUT_COMPARE.relative_to(DATA_DIR)),
        "overrides_csv": str(OUT_OVERRIDES.relative_to(DATA_DIR)),
        "max_abs_weight_delta": max(
            (abs(r["weight_delta_cvap_minus_area"]) for r in compare_rows),
            default=0.0,
        ),
    }
    OUT_SUMMARY.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
