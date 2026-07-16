#!/usr/bin/env python3
"""Review high-priority district split precincts with GeoPandas overlays.

Reads Data/reports/district_crosswalk_comparison/district_split_review_queue.csv,
recomputes precinct-to-district intersections for the top N rows, and writes:

  - a CSV report comparing queue weights to recomputed geometry weights
  - per-split GeoJSON map packs (precinct, districts, intersection pieces)
  - optional PNGs when matplotlib is installed
  - a summary JSON
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
REPORT_DIR = DATA_DIR / "reports" / "district_crosswalk_comparison"
QUEUE_CSV = REPORT_DIR / "district_split_review_queue.csv"
OUT_DIR = REPORT_DIR / "split_geometry_review"
PRECINCT_GEOJSON = DATA_DIR / "tn_voting_precincts.geojson"

AREA_CRS = "EPSG:5070"

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


def numeric(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


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


def slug(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip().lower())
    return text.strip("_") or "row"


def classify_piece(weight: float, is_dominant: bool) -> str:
    if is_dominant:
        return "dominant"
    if weight >= 0.10:
        return "real_secondary"
    if weight >= 0.01:
        return "minor_secondary"
    return "sliver"


def suggested_action(pieces: List[dict], non_dominant_share: float) -> str:
    secondary = [p for p in pieces if p["geometry_status"] != "dominant"]
    if not secondary:
        return "keep_area_weights"
    if all(p["geometry_status"] == "sliver" for p in secondary):
        return "consider_dominant_only"
    if non_dominant_share >= 0.25 and any(p["geometry_status"] == "real_secondary" for p in secondary):
        return "manual_split_review"
    if any(p["geometry_status"] in {"real_secondary", "minor_secondary"} for p in secondary):
        return "keep_area_weights_check_votes"
    return "keep_area_weights"


def parse_queue_weights(raw: str) -> List[Tuple[str, float]]:
    out = []
    for part in str(raw or "").split(";"):
        part = part.strip()
        if not part or ":" not in part:
            continue
        district, weight = part.split(":", 1)
        out.append((normalize_district(district), numeric(weight)))
    return out


def read_queue(path: Path, top_n: int, priority: Optional[str]) -> List[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if priority:
        rows = [r for r in rows if str(r.get("review_priority", "")).strip().lower() == priority.lower()]
    rows.sort(key=lambda r: numeric(r.get("review_rank")), reverse=False)
    return rows[:top_n]


def load_precincts():
    import geopandas as gpd

    gdf = gpd.read_file(PRECINCT_GEOJSON)
    if gdf.crs is None:
        gdf = gdf.set_crs(4326)
    gdf["county_norm"] = gdf["county_norm"].astype(str).str.strip().str.upper()
    gdf["prec_id"] = gdf["prec_id"].astype(str).str.strip().str.zfill(6)
    gdf["precinct_key"] = gdf.get("precinct_norm", "").astype(str).str.strip().str.upper()
    gdf.loc[gdf["precinct_key"] == "", "precinct_key"] = (
        gdf["county_norm"] + " - " + gdf["prec_id"]
    )
    gdf["name20"] = gdf.get("precinct_name", "").astype(str).str.strip()
    gdf.loc[gdf["name20"] == "", "name20"] = gdf["precinct_key"]
    return gdf


def load_districts(scope: str, lines_year: int):
    import geopandas as gpd

    key = (scope, int(lines_year))
    job = GEOMETRY_JOBS.get(key)
    if not job:
        raise KeyError(f"No geometry configured for {scope} {lines_year}")
    path = job["path"]
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")
    field = job["district_field"]
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(4326)
    gdf["district_num"] = gdf[field].apply(normalize_district)
    gdf = gdf[gdf["district_num"].astype(str).str.len() > 0].copy()
    return gdf[["district_num", "geometry"]].copy()


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def maybe_write_png(map_dir: Path, pieces_ll, precinct_ll, districts_ll, title: str) -> Optional[str]:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        return None

    fig, ax = plt.subplots(1, 1, figsize=(8, 8))
    districts_ll.boundary.plot(ax=ax, color="#94a3b8", linewidth=1.0, label="districts")
    precinct_ll.boundary.plot(ax=ax, color="#0f172a", linewidth=2.0, label="precinct")
    colors = {
        "dominant": "#2563eb",
        "real_secondary": "#dc2626",
        "minor_secondary": "#f59e0b",
        "sliver": "#a855f7",
    }
    for status, color in colors.items():
        subset = pieces_ll[pieces_ll["geometry_status"] == status]
        if not subset.empty:
            subset.plot(ax=ax, color=color, alpha=0.45, edgecolor=color, linewidth=0.8, label=status)
    ax.set_title(title)
    ax.set_axis_off()
    handles, labels = ax.get_legend_handles_labels()
    if handles:
        ax.legend(loc="lower left", fontsize=8)
    png_path = map_dir / "split_map.png"
    fig.tight_layout()
    fig.savefig(png_path, dpi=160)
    plt.close(fig)
    return png_path.name


def review_one(row: dict, precincts, district_cache: Dict[Tuple[str, int], object], out_root: Path) -> Tuple[dict, List[dict]]:
    import geopandas as gpd

    scope = str(row["scope"]).strip()
    lines_year = int(row["lines_year"])
    precinct_key = normalize_key(row["precinct_key"])
    cache_key = (scope, lines_year)
    if cache_key not in district_cache:
        district_cache[cache_key] = load_districts(scope, lines_year)
    districts = district_cache[cache_key]

    precinct = precincts[precincts["precinct_key"] == precinct_key].copy()
    if precinct.empty:
        raise RuntimeError(f"Missing precinct geometry for {precinct_key}")

    precinct_area = precinct.to_crs(AREA_CRS)
    precinct_area["precinct_area_m2"] = precinct_area.geometry.area
    districts_area = districts.to_crs(AREA_CRS)

    joined = gpd.overlay(
        precinct_area[["precinct_key", "name20", "county_norm", "prec_id", "precinct_area_m2", "geometry"]],
        districts_area,
        how="intersection",
        keep_geom_type=False,
    )
    if joined.empty:
        raise RuntimeError(f"No district intersections for {precinct_key}")

    joined["intersection_area_m2"] = joined.geometry.area
    joined = joined[joined["intersection_area_m2"] > 0].copy()
    joined["area_weight"] = joined["intersection_area_m2"] / joined["precinct_area_m2"]
    grouped = (
        joined.drop(columns=["geometry"])
        .groupby(["precinct_key", "name20", "county_norm", "prec_id", "district_num"], as_index=False)
        .agg(
            intersection_area_m2=("intersection_area_m2", "sum"),
            precinct_area_m2=("precinct_area_m2", "first"),
            area_weight=("area_weight", "sum"),
        )
    )
    total_weight = float(grouped["area_weight"].sum())
    grouped = grouped.sort_values("area_weight", ascending=False).reset_index(drop=True)
    dominant = str(grouped.iloc[0]["district_num"])
    pieces = []
    for idx, piece in grouped.iterrows():
        weight = float(piece["area_weight"])
        status = classify_piece(weight, idx == 0)
        pieces.append(
            {
                "district_num": str(piece["district_num"]),
                "area_weight": round(weight, 8),
                "normalized_weight": round(weight / total_weight, 8) if total_weight > 0 else 0.0,
                "intersection_area_m2": round(float(piece["intersection_area_m2"]), 4),
                "geometry_status": status,
                "is_dominant": idx == 0,
            }
        )

    queue_weights = dict(parse_queue_weights(row.get("district_weights", "")))
    recomputed_weights = {p["district_num"]: p["area_weight"] for p in pieces}
    all_districts = sorted(set(queue_weights).union(recomputed_weights), key=lambda d: (not d.isdigit(), int(d) if d.isdigit() else d))
    max_abs_delta = 0.0
    for district in all_districts:
        delta = abs(recomputed_weights.get(district, 0.0) - queue_weights.get(district, 0.0))
        max_abs_delta = max(max_abs_delta, delta)

    non_dominant_share = max(0.0, 1.0 - (pieces[0]["normalized_weight"] if pieces else 0.0))
    action = suggested_action(pieces, non_dominant_share)

    folder = out_root / f"{int(row['review_rank']):03d}_{slug(scope)}_{slug(row['precinct_key'])}"
    folder.mkdir(parents=True, exist_ok=True)

    # Map geometries in WGS84 for easy viewing.
    pieces_geom = joined.copy()
    pieces_geom = pieces_geom.merge(
        grouped[["district_num", "area_weight"]],
        on="district_num",
        how="left",
        suffixes=("", "_grouped"),
    )
    status_by_district = {p["district_num"]: p["geometry_status"] for p in pieces}
    pieces_geom["geometry_status"] = pieces_geom["district_num"].map(status_by_district)
    pieces_geom["suggested_action"] = action
    pieces_ll = pieces_geom.to_crs(4326)
    precinct_ll = precinct.to_crs(4326)
    involved = set(recomputed_weights)
    districts_ll = districts[districts["district_num"].isin(involved)].to_crs(4326)

    precinct_ll[["precinct_key", "name20", "county_norm", "prec_id", "geometry"]].to_file(
        folder / "precinct.geojson", driver="GeoJSON"
    )
    districts_ll.to_file(folder / "districts.geojson", driver="GeoJSON")
    pieces_ll[
        [
            "precinct_key",
            "name20",
            "county_norm",
            "prec_id",
            "district_num",
            "area_weight",
            "geometry_status",
            "suggested_action",
            "geometry",
        ]
    ].to_file(folder / "intersection_pieces.geojson", driver="GeoJSON")

    png_name = maybe_write_png(
        folder,
        pieces_ll,
        precinct_ll,
        districts_ll,
        f"{row['precinct_key']} · {scope} {lines_year}",
    )

    summary = {
        "review_rank": int(row["review_rank"]),
        "review_priority": row.get("review_priority", ""),
        "review_reasons": row.get("review_reasons", ""),
        "audit_label": row.get("audit_label", ""),
        "scope": scope,
        "lines_year": lines_year,
        "precinct_key": precinct_key,
        "name20": str(precinct.iloc[0]["name20"]),
        "county_norm": str(precinct.iloc[0]["county_norm"]),
        "prec_id": str(precinct.iloc[0]["prec_id"]),
        "queue_district_weights": row.get("district_weights", ""),
        "recomputed_district_weights": ";".join(f"{p['district_num']}:{p['area_weight']:.6f}" for p in pieces),
        "queue_dominant_district": normalize_district(row.get("dominant_district")),
        "recomputed_dominant_district": dominant,
        "queue_non_dominant_share": round(numeric(row.get("normalized_non_dominant_share")), 8),
        "recomputed_non_dominant_share": round(non_dominant_share, 8),
        "max_abs_weight_delta_vs_queue": round(max_abs_delta, 8),
        "split_count": len(pieces),
        "sliver_count": sum(1 for p in pieces if p["geometry_status"] == "sliver"),
        "real_secondary_count": sum(1 for p in pieces if p["geometry_status"] == "real_secondary"),
        "cvap_total": round(numeric(row.get("cvap_total")), 4),
        "non_dominant_cvap_exposure": round(numeric(row.get("non_dominant_cvap_exposure")), 4),
        "max_2024_non_dominant_vote_exposure": round(numeric(row.get("max_2024_non_dominant_vote_exposure")), 4),
        "suggested_action": action,
        "map_folder": str(folder.relative_to(DATA_DIR)),
        "png_map": png_name or "",
    }
    piece_rows = []
    for piece in pieces:
        piece_rows.append(
            {
                "review_rank": summary["review_rank"],
                "audit_label": summary["audit_label"],
                "scope": summary["scope"],
                "lines_year": summary["lines_year"],
                "precinct_key": summary["precinct_key"],
                "name20": summary["name20"],
                "district_num": piece["district_num"],
                "geometry_status": piece["geometry_status"],
                "area_weight": piece["area_weight"],
                "normalized_weight": piece["normalized_weight"],
                "queue_area_weight": round(queue_weights.get(piece["district_num"], 0.0), 8),
                "weight_delta_vs_queue": round(
                    piece["area_weight"] - queue_weights.get(piece["district_num"], 0.0),
                    8,
                ),
                "intersection_area_m2": piece["intersection_area_m2"],
                "suggested_action": summary["suggested_action"],
                "map_folder": summary["map_folder"],
            }
        )
    return summary, piece_rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--top", type=int, default=25, help="Number of queue rows to review")
    parser.add_argument(
        "--priority",
        default="high",
        help="Optional review_priority filter (default: high). Use '' for all.",
    )
    args = parser.parse_args()

    if not QUEUE_CSV.exists():
        raise FileNotFoundError(f"Missing {QUEUE_CSV}. Run audit_tn_district_crosswalk_vote_accuracy.py first.")
    if not PRECINCT_GEOJSON.exists():
        raise FileNotFoundError(f"Missing {PRECINCT_GEOJSON}")

    priority = args.priority.strip() or None
    queue_rows = read_queue(QUEUE_CSV, args.top, priority)
    if not queue_rows:
        raise RuntimeError("No review-queue rows matched the selected filters")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    precincts = load_precincts()
    district_cache: Dict[Tuple[str, int], object] = {}
    summaries: List[dict] = []
    piece_rows: List[dict] = []

    for row in queue_rows:
        print(
            f"Reviewing rank {row['review_rank']}: {row['precinct_key']} ({row['scope']} {row['lines_year']})",
            flush=True,
        )
        summary, pieces = review_one(row, precincts, district_cache, OUT_DIR)
        summaries.append(summary)
        piece_rows.extend(pieces)

    summary_csv = OUT_DIR / "split_geometry_review_summary.csv"
    pieces_csv = OUT_DIR / "split_geometry_review_pieces.csv"
    summary_json = OUT_DIR / "split_geometry_review_summary.json"

    write_csv(
        summary_csv,
        summaries,
        [
            "review_rank",
            "review_priority",
            "review_reasons",
            "audit_label",
            "scope",
            "lines_year",
            "precinct_key",
            "name20",
            "county_norm",
            "prec_id",
            "queue_district_weights",
            "recomputed_district_weights",
            "queue_dominant_district",
            "recomputed_dominant_district",
            "queue_non_dominant_share",
            "recomputed_non_dominant_share",
            "max_abs_weight_delta_vs_queue",
            "split_count",
            "sliver_count",
            "real_secondary_count",
            "cvap_total",
            "non_dominant_cvap_exposure",
            "max_2024_non_dominant_vote_exposure",
            "suggested_action",
            "map_folder",
            "png_map",
        ],
    )
    write_csv(
        pieces_csv,
        piece_rows,
        [
            "review_rank",
            "audit_label",
            "scope",
            "lines_year",
            "precinct_key",
            "name20",
            "district_num",
            "geometry_status",
            "area_weight",
            "normalized_weight",
            "queue_area_weight",
            "weight_delta_vs_queue",
            "intersection_area_m2",
            "suggested_action",
            "map_folder",
        ],
    )

    action_counts = {}
    for row in summaries:
        action_counts[row["suggested_action"]] = action_counts.get(row["suggested_action"], 0) + 1
    payload = {
        "queue_csv": str(QUEUE_CSV.relative_to(DATA_DIR)),
        "top_n": len(summaries),
        "priority_filter": priority or "",
        "output_dir": str(OUT_DIR.relative_to(DATA_DIR)),
        "summary_csv": str(summary_csv.relative_to(DATA_DIR)),
        "pieces_csv": str(pieces_csv.relative_to(DATA_DIR)),
        "suggested_action_counts": action_counts,
        "png_maps_written": sum(1 for row in summaries if row.get("png_map")),
    }
    summary_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
