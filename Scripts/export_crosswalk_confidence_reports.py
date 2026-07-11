#!/usr/bin/env python3
"""Export strict-vs-full crosswalk confidence summaries by year and county.

Reads Data/crosswalks/tn_precinct_to_vtd20_blockweighted_*.csv and writes:
  - Data/crosswalks/tn_crosswalk_confidence_by_year.csv
  - Data/crosswalks/tn_crosswalk_confidence_by_county_year.csv
"""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Tuple


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"

OUT_YEAR = XWALK_DIR / "tn_crosswalk_confidence_by_year.csv"
OUT_COUNTY_YEAR = XWALK_DIR / "tn_crosswalk_confidence_by_county_year.csv"


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


def method_rank(method: str) -> int:
    if method in HIGH_METHODS:
        return 2
    if method in MEDIUM_METHODS:
        return 1
    return 0


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


if __name__ == "__main__":
    main()

