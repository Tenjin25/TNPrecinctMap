#!/usr/bin/env python3
"""County contributions within 2026 congressional districts (esp. CD 5/8/9).

Allocates precinct contest votes through the area-weighted 2026 carryover
crosswalk (matches the uncalibrated contest builder's area overlay), then
compares district totals to calibrated live district_contests_2026.

Outputs under:
  Data/reports/district_crosswalk_comparison/cvap_override_outputs/
    congressional_2026_county_contributions_president_2024.csv
    congressional_2026_county_contributions_cd589_summary.csv
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
BUNDLE = DATA_DIR / "reports" / "district_crosswalk_comparison" / "cvap_override_outputs"

CROSSWALK = BUNDLE / "area_weighted_crosswalks" / "tn_congressional_2026_precinct_crosswalk.csv"
if not CROSSWALK.exists():
    CROSSWALK = DATA_DIR / "crosswalks" / "tn_congressional_2026_precinct_crosswalk.csv"

PRECINCT_CONTEST = DATA_DIR / "contests" / "president_2024.json"
LIVE_DISTRICT = BUNDLE / "live_district_contests_2026" / "congressional_president_2024.json"
if not LIVE_DISTRICT.exists():
    LIVE_DISTRICT = DATA_DIR / "district_contests_2026" / "congressional_president_2024.json"

UNCAL_DISTRICT = BUNDLE / "uncalibrated_district_contests_2026" / "congressional_president_2024.json"

OUT_DETAIL = BUNDLE / "congressional_2026_county_contributions_president_2024.csv"
OUT_SUMMARY = BUNDLE / "congressional_2026_county_contributions_cd589_summary.csv"

FOCUS = {"5", "8", "9"}


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


def load_precinct_votes() -> dict[str, dict]:
    payload = json.loads(PRECINCT_CONTEST.read_text(encoding="utf-8"))
    out = {}
    for row in payload.get("rows") or []:
        key = normalize_key(row.get("county") or row.get("precinct_key") or "")
        if not key:
            continue
        dem = numeric(row.get("dem_votes"))
        rep = numeric(row.get("rep_votes"))
        other = numeric(row.get("other_votes"))
        out[key] = {
            "county_norm": key.split(" - ", 1)[0] if " - " in key else key,
            "dem": dem,
            "rep": rep,
            "other": other,
            "total": dem + rep + other,
        }
    return out


def load_district_results(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    results = payload.get("general", {}).get("results") or {}
    out = {}
    for district, row in results.items():
        d = normalize_district(district)
        dem = numeric(row.get("dem_votes"))
        rep = numeric(row.get("rep_votes"))
        other = numeric(row.get("other_votes"))
        total = dem + rep + other
        margin = rep - dem
        out[d] = {
            "dem": dem,
            "rep": rep,
            "other": other,
            "total": total,
            "margin": margin,
            "margin_pct": (100.0 * margin / total) if total else 0.0,
        }
    return out


def allocate_by_county(precinct_votes: dict[str, dict]) -> dict[tuple[str, str], dict]:
    totals: dict[tuple[str, str], dict] = defaultdict(
        lambda: {"dem": 0.0, "rep": 0.0, "other": 0.0, "total": 0.0, "weight_sum": 0.0}
    )
    with CROSSWALK.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            key = normalize_key(row.get("precinct_key") or "")
            district = normalize_district(row.get("district_num"))
            county = normalize_key(row.get("county_norm") or "")
            weight = numeric(row.get("area_weight"))
            votes = precinct_votes.get(key)
            if not votes or weight <= 0 or not district or not county:
                continue
            bucket = totals[(district, county)]
            bucket["dem"] += votes["dem"] * weight
            bucket["rep"] += votes["rep"] * weight
            bucket["other"] += votes["other"] * weight
            bucket["total"] += votes["total"] * weight
            bucket["weight_sum"] += weight
    return dict(totals)


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    precinct_votes = load_precinct_votes()
    by_county = allocate_by_county(precinct_votes)
    calibrated = load_district_results(LIVE_DISTRICT)
    uncalibrated = load_district_results(UNCAL_DISTRICT)

    # District rollups from county allocation.
    district_alloc: dict[str, dict] = defaultdict(
        lambda: {"dem": 0.0, "rep": 0.0, "other": 0.0, "total": 0.0}
    )
    for (district, _county), vals in by_county.items():
        bucket = district_alloc[district]
        for k in ("dem", "rep", "other", "total"):
            bucket[k] += vals[k]

    detail_rows = []
    for (district, county), vals in sorted(
        by_county.items(), key=lambda x: (int(x[0][0]), -x[1]["total"], x[0][1])
    ):
        dist_total = district_alloc[district]["total"] or 1.0
        dem, rep, other, total = vals["dem"], vals["rep"], vals["other"], vals["total"]
        margin = rep - dem
        detail_rows.append(
            {
                "district_num": district,
                "county_norm": county,
                "dem_votes": round(dem, 2),
                "rep_votes": round(rep, 2),
                "other_votes": round(other, 2),
                "total_votes": round(total, 2),
                "margin_r_minus_d": round(margin, 2),
                "margin_pct": round((100.0 * margin / total) if total else 0.0, 4),
                "share_of_district_total_pct": round(100.0 * total / dist_total, 4),
                "share_of_district_dem_pct": round(
                    100.0 * dem / (district_alloc[district]["dem"] or 1.0), 4
                ),
                "share_of_district_rep_pct": round(
                    100.0 * rep / (district_alloc[district]["rep"] or 1.0), 4
                ),
                "focus_cd589": district in FOCUS,
            }
        )

    write_csv(
        OUT_DETAIL,
        detail_rows,
        [
            "district_num",
            "county_norm",
            "dem_votes",
            "rep_votes",
            "other_votes",
            "total_votes",
            "margin_r_minus_d",
            "margin_pct",
            "share_of_district_total_pct",
            "share_of_district_dem_pct",
            "share_of_district_rep_pct",
            "focus_cd589",
        ],
    )

    summary_rows = []
    for district in sorted(FOCUS, key=int):
        cal = calibrated.get(district, {})
        uncal = uncalibrated.get(district, {})
        alloc = district_alloc.get(district, {})
        counties = [
            r for r in detail_rows if r["district_num"] == district and r["total_votes"] >= 1
        ]
        counties_sorted = sorted(counties, key=lambda r: r["total_votes"], reverse=True)
        top = counties_sorted[:8]
        for rank, row in enumerate(top, start=1):
            summary_rows.append(
                {
                    "district_num": district,
                    "rank_in_district": rank,
                    "county_norm": row["county_norm"],
                    "county_dem": row["dem_votes"],
                    "county_rep": row["rep_votes"],
                    "county_total": row["total_votes"],
                    "county_margin_r_minus_d": row["margin_r_minus_d"],
                    "county_margin_pct": row["margin_pct"],
                    "share_of_district_total_pct": row["share_of_district_total_pct"],
                    "uncalibrated_district_dem": round(uncal.get("dem", alloc.get("dem", 0)), 2),
                    "uncalibrated_district_rep": round(uncal.get("rep", alloc.get("rep", 0)), 2),
                    "uncalibrated_district_margin_pct": round(uncal.get("margin_pct", 0), 4),
                    "calibrated_district_dem": round(cal.get("dem", 0), 2),
                    "calibrated_district_rep": round(cal.get("rep", 0), 2),
                    "calibrated_district_margin_pct": round(cal.get("margin_pct", 0), 4),
                    "district_dem_gap_uncal_minus_cal": round(
                        uncal.get("dem", 0) - cal.get("dem", 0), 2
                    ),
                    "district_rep_gap_uncal_minus_cal": round(
                        uncal.get("rep", 0) - cal.get("rep", 0), 2
                    ),
                    "district_margin_pp_gap_uncal_minus_cal": round(
                        uncal.get("margin_pct", 0) - cal.get("margin_pct", 0), 4
                    ),
                }
            )

    write_csv(
        OUT_SUMMARY,
        summary_rows,
        [
            "district_num",
            "rank_in_district",
            "county_norm",
            "county_dem",
            "county_rep",
            "county_total",
            "county_margin_r_minus_d",
            "county_margin_pct",
            "share_of_district_total_pct",
            "uncalibrated_district_dem",
            "uncalibrated_district_rep",
            "uncalibrated_district_margin_pct",
            "calibrated_district_dem",
            "calibrated_district_rep",
            "calibrated_district_margin_pct",
            "district_dem_gap_uncal_minus_cal",
            "district_rep_gap_uncal_minus_cal",
            "district_margin_pp_gap_uncal_minus_cal",
        ],
    )

    # Console: compact CD5/8/9 top counties.
    print("Wrote:")
    print(f"  {OUT_DETAIL.relative_to(DATA_DIR)}")
    print(f"  {OUT_SUMMARY.relative_to(DATA_DIR)}")
    for district in sorted(FOCUS, key=int):
        cal = calibrated.get(district, {})
        uncal = uncalibrated.get(district, {})
        print(
            f"\nCD{district}: uncal R+{uncal.get('margin_pct', 0):.2f}% "
            f"vs cal R+{cal.get('margin_pct', 0):.2f}% "
            f"(Dem gap {uncal.get('dem', 0) - cal.get('dem', 0):+.0f}, "
            f"Rep gap {uncal.get('rep', 0) - cal.get('rep', 0):+.0f})"
        )
        rows = [r for r in detail_rows if r["district_num"] == district]
        rows = sorted(rows, key=lambda r: r["total_votes"], reverse=True)[:8]
        for r in rows:
            print(
                f"  {r['county_norm']:<12} "
                f"share={r['share_of_district_total_pct']:5.1f}%  "
                f"D={r['dem_votes']:8.0f} R={r['rep_votes']:8.0f}  "
                f"margin={r['margin_pct']:+6.1f}%"
            )


if __name__ == "__main__":
    main()
