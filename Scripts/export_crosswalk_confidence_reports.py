#!/usr/bin/env python3
"""Export strict-vs-full crosswalk confidence summaries by year and county.

Reads Data/crosswalks/tn_precinct_to_vtd20_blockweighted_*.csv and writes:
  - Data/crosswalks/tn_crosswalk_confidence_by_year.csv
  - Data/crosswalks/tn_crosswalk_confidence_by_county_year.csv
  - Data/crosswalks/tn_crosswalk_manual_fix_queue.csv
  - Data/crosswalks/tn_crosswalk_manual_fix_queue_summary.csv
"""

from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"

OUT_YEAR = XWALK_DIR / "tn_crosswalk_confidence_by_year.csv"
OUT_COUNTY_YEAR = XWALK_DIR / "tn_crosswalk_confidence_by_county_year.csv"
OUT_FIX_QUEUE = XWALK_DIR / "tn_crosswalk_manual_fix_queue.csv"
OUT_FIX_QUEUE_SUMMARY = XWALK_DIR / "tn_crosswalk_manual_fix_queue_summary.csv"


HIGH_METHODS = {
    "exact_name",
    "manual_override",
    "prefix_name",
    "code_token_name",
    "token_vtd",
    "shelby_alias_name",
    "alpha_code_name",
}
MEDIUM_METHODS = {
    "simple_exact_name",
    "compact_exact_name",
    "tail_exact_name",
}


def parse_year(path: Path) -> int:
    m = re.search(r"blockweighted_(\d{4})(?:__[^.]+)?\.csv$", path.name)
    if not m:
        return 0
    return int(m.group(1))


def iter_rows(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            yield row


def pct(numer: int, denom: int) -> float:
    if denom <= 0:
        return 0.0
    return round((numer / denom) * 100.0, 4)


def parse_float(value: str) -> float:
    try:
        return float(str(value or "").strip())
    except ValueError:
        return 0.0


def method_rank(method: str) -> int:
    if method in HIGH_METHODS:
        return 2
    if method in MEDIUM_METHODS:
        return 1
    return 0


def export_manual_fix_queue() -> Tuple[int, int]:
    """Write prioritized low-confidence review queues from current crosswalk outputs."""
    selected_groups = {}
    selected_priority = {}

    files = sorted(XWALK_DIR.glob("tn_precinct_to_vtd20_blockweighted_*_low_confidence.csv"))
    for path in files:
        file_groups = defaultdict(
            lambda: {
                "rows": 0,
                "score_sum": 0.0,
                "weight_sum": 0.0,
                "dst_counts": Counter(),
            }
        )
        for row in iter_rows(path):
            year_raw = (row.get("from_year") or "").strip()
            if not year_raw:
                continue
            year = int(year_raw)
            county = (row.get("county_norm") or "").strip().upper()
            precinct = (row.get("from_precinct_norm") or "").strip().upper()
            method = (row.get("match_method") or "").strip()
            dst_vtd20 = (row.get("dst_vtd20") or "").strip()
            score = parse_float(row.get("match_score") or "")
            weight = parse_float(row.get("weight") or "")
            if not county or not precinct or not method:
                continue

            key = (year, county, precinct, method)
            group = file_groups[key]
            group["rows"] += 1
            group["score_sum"] += score
            group["weight_sum"] += weight
            if dst_vtd20:
                group["dst_counts"][dst_vtd20] += 1

        file_priority = 1 if "__" in path.name else 0
        for key, group in file_groups.items():
            rows = int(group["rows"])
            avg_score = group["score_sum"] / rows if rows else 0.0
            unique_dst_count = len(group["dst_counts"])
            impact_score = rows * (1.0 - avg_score) + unique_dst_count * 0.01
            priority = (
                method_rank(key[3]),
                file_priority,
                avg_score,
                -impact_score,
                -rows,
                path.name,
            )
            if priority > selected_priority.get(key, (-1, -1, -1.0, float("-inf"), float("-inf"), "")):
                selected_groups[key] = group
                selected_priority[key] = priority

    queue_rows: List[dict] = []
    summary_groups = defaultdict(lambda: {"precincts": set(), "rows": 0, "score_sum": 0.0})
    for (year, county, precinct, method), group in selected_groups.items():
        rows = int(group["rows"])
        avg_score = round(group["score_sum"] / rows, 6) if rows else 0.0
        dst_counts = group["dst_counts"]
        top_dst = ""
        top_dst_count = 0
        if dst_counts:
            top_dst, top_dst_count = sorted(
                dst_counts.items(),
                key=lambda item: (-item[1], item[0]),
            )[0]
        unique_dst_count = len(dst_counts)
        impact_score = round(rows * (1.0 - avg_score) + unique_dst_count * 0.01, 6)
        queue_rows.append(
            {
                "year": year,
                "county_norm": county,
                "from_precinct_norm": precinct,
                "match_method": method,
                "low_conf_rows": rows,
                "avg_match_score": avg_score,
                "top_suggested_dst_vtd20": top_dst,
                "top_suggested_dst_count": top_dst_count,
                "unique_suggested_dst_count": unique_dst_count,
                "total_weight": round(group["weight_sum"], 6),
                "impact_score": impact_score,
            }
        )
        summary_key = (year, county)
        summary = summary_groups[summary_key]
        summary["precincts"].add(precinct)
        summary["rows"] += rows
        summary["score_sum"] += group["score_sum"]

    queue_rows.sort(
        key=lambda row: (
            -float(row["impact_score"]),
            int(row["year"]),
            row["county_norm"],
            row["from_precinct_norm"],
            row["match_method"],
        )
    )

    with OUT_FIX_QUEUE.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "year",
            "county_norm",
            "from_precinct_norm",
            "match_method",
            "low_conf_rows",
            "avg_match_score",
            "top_suggested_dst_vtd20",
            "top_suggested_dst_count",
            "unique_suggested_dst_count",
            "total_weight",
            "impact_score",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        w.writeheader()
        w.writerows(queue_rows)

    summary_rows: List[dict] = []
    for (year, county), group in summary_groups.items():
        rows = int(group["rows"])
        avg_score = round(group["score_sum"] / rows, 6) if rows else 0.0
        summary_rows.append(
            {
                "year": year,
                "county_norm": county,
                "low_conf_precinct_keys": len(group["precincts"]),
                "low_conf_rows": rows,
                "weighted_avg_match_score": avg_score,
            }
        )

    summary_rows.sort(
        key=lambda row: (
            -int(row["low_conf_rows"]),
            int(row["year"]),
            row["county_norm"],
        )
    )

    with OUT_FIX_QUEUE_SUMMARY.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "year",
            "county_norm",
            "low_conf_precinct_keys",
            "low_conf_rows",
            "weighted_avg_match_score",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
        w.writeheader()
        w.writerows(summary_rows)

    return len(queue_rows), len(summary_rows)


def main() -> None:
    files = sorted(
        p for p in XWALK_DIR.glob("tn_precinct_to_vtd20_blockweighted_*.csv")
        if "_strict.csv" not in p.name and "_low_confidence.csv" not in p.name and "_unmatched.csv" not in p.name
    )
    if not files:
        raise SystemExit("No crosswalk CSV files found.")

    # (year, county, precinct) -> method
    key_method: Dict[Tuple[int, str, str], str] = {}
    key_priority: Dict[Tuple[int, str, str], Tuple[int, int]] = {}

    for p in files:
        year = parse_year(p)
        if year <= 0:
            continue
        file_priority = 1 if "__" in p.name else 0
        for row in iter_rows(p):
            county = (row.get("county_norm") or "").strip().upper()
            precinct = (row.get("from_precinct_norm") or "").strip().upper()
            method = (row.get("match_method") or "").strip()
            if not county or not precinct or not method:
                continue
            key = (year, county, precinct)
            priority = (method_rank(method), file_priority)
            if priority > key_priority.get(key, (-1, -1)):
                key_method[key] = method
                key_priority[key] = priority

    by_year = defaultdict(lambda: {"total": 0, "high": 0, "high_medium": 0})
    by_county_year = defaultdict(lambda: {"total": 0, "high": 0, "high_medium": 0})

    for (year, county, _precinct), method in key_method.items():
        by_year[year]["total"] += 1
        by_county_year[(year, county)]["total"] += 1

        is_high = method in HIGH_METHODS
        is_high_medium = is_high or method in MEDIUM_METHODS

        if is_high:
            by_year[year]["high"] += 1
            by_county_year[(year, county)]["high"] += 1
        if is_high_medium:
            by_year[year]["high_medium"] += 1
            by_county_year[(year, county)]["high_medium"] += 1

    with OUT_YEAR.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "year",
                "total_precinct_keys",
                "strict_high_keys",
                "strict_high_pct",
                "strict_high_medium_keys",
                "strict_high_medium_pct",
                "full_keys",
                "full_pct",
            ],
            lineterminator="\n",
        )
        w.writeheader()
        for year in sorted(by_year.keys()):
            y = by_year[year]
            total = y["total"]
            high = y["high"]
            high_medium = y["high_medium"]
            w.writerow(
                {
                    "year": year,
                    "total_precinct_keys": total,
                    "strict_high_keys": high,
                    "strict_high_pct": pct(high, total),
                    "strict_high_medium_keys": high_medium,
                    "strict_high_medium_pct": pct(high_medium, total),
                    "full_keys": total,
                    "full_pct": 100.0,
                }
            )

    with OUT_COUNTY_YEAR.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "year",
                "county_norm",
                "total_precinct_keys",
                "strict_high_keys",
                "strict_high_pct",
                "strict_high_medium_keys",
                "strict_high_medium_pct",
                "full_keys",
                "full_pct",
            ],
            lineterminator="\n",
        )
        w.writeheader()
        for year, county in sorted(by_county_year.keys()):
            y = by_county_year[(year, county)]
            total = y["total"]
            high = y["high"]
            high_medium = y["high_medium"]
            w.writerow(
                {
                    "year": year,
                    "county_norm": county,
                    "total_precinct_keys": total,
                    "strict_high_keys": high,
                    "strict_high_pct": pct(high, total),
                    "strict_high_medium_keys": high_medium,
                    "strict_high_medium_pct": pct(high_medium, total),
                    "full_keys": total,
                    "full_pct": 100.0,
                }
            )

    print(f"Wrote {OUT_YEAR}")
    print(f"Wrote {OUT_COUNTY_YEAR}")
    queue_count, summary_count = export_manual_fix_queue()
    print(f"Wrote {OUT_FIX_QUEUE} ({queue_count} rows)")
    print(f"Wrote {OUT_FIX_QUEUE_SUMMARY} ({summary_count} rows)")


if __name__ == "__main__":
    main()

