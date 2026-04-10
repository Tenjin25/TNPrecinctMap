#!/usr/bin/env python3
"""Sync district calibration overrides from CSV.

Usage:
  .venv\\Scripts\\python.exe Scripts/sync_calibration_overrides.py
  .venv\\Scripts\\python.exe Scripts/sync_calibration_overrides.py --dry-run

Input CSV default:
  Data/district_contests/calibration_targets.csv

Supported columns:
  - scope, contest_type, year, district (required)
  - dem_votes, rep_votes, other_votes
    OR
  - total_votes, dem_pct, rep_pct, [other_pct]
  - dem_candidate, rep_candidate (optional)
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / "Data" / "district_contests" / "calibration_targets.csv"
DEFAULT_OVERRIDES = ROOT / "Data" / "district_contests" / "calibration_overrides.json"


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def parse_int(value) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip().replace(",", "")
    if not s:
        return None
    try:
        return int(round(float(s)))
    except ValueError:
        return None


def parse_float(value) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip().replace("%", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_district(raw: str) -> Optional[str]:
    m = re.search(r"(\d+)", norm_space(raw))
    if not m:
        return None
    return str(int(m.group(1)))


def allocate_votes_from_pct(
    total_votes: int,
    dem_pct: float,
    rep_pct: float,
    other_pct: Optional[float],
) -> Tuple[int, int, int]:
    # If other% is omitted, keep totals balanced with the remainder.
    if other_pct is None:
        dem_votes = int(round(total_votes * dem_pct / 100.0))
        rep_votes = int(round(total_votes * rep_pct / 100.0))
        other_votes = total_votes - dem_votes - rep_votes
        return dem_votes, rep_votes, other_votes

    float_votes = {
        "dem": total_votes * dem_pct / 100.0,
        "rep": total_votes * rep_pct / 100.0,
        "other": total_votes * other_pct / 100.0,
    }
    base = {k: int(math.floor(v)) for k, v in float_votes.items()}
    remainder = total_votes - sum(base.values())
    order = sorted(
        float_votes.keys(),
        key=lambda k: float_votes[k] - base[k],
        reverse=True,
    )
    for i in range(max(0, remainder)):
        base[order[i % len(order)]] += 1
    return base["dem"], base["rep"], base["other"]


def normalize_key(row: dict) -> Tuple[str, str, int, str]:
    scope = norm_space(str(row.get("scope", ""))).lower()
    contest_type = norm_space(str(row.get("contest_type", ""))).lower()
    year = parse_int(row.get("year"))
    district = parse_district(str(row.get("district", "")))
    if not scope or not contest_type or year is None or district is None:
        raise ValueError("missing/invalid scope, contest_type, year, or district")
    return scope, contest_type, year, district


def build_override_from_csv_row(row: dict) -> dict:
    dem_votes = parse_int(row.get("dem_votes"))
    rep_votes = parse_int(row.get("rep_votes"))
    other_votes = parse_int(row.get("other_votes"))

    if dem_votes is None or rep_votes is None or other_votes is None:
        total_votes = parse_int(row.get("total_votes"))
        dem_pct = parse_float(row.get("dem_pct"))
        rep_pct = parse_float(row.get("rep_pct"))
        other_pct = parse_float(row.get("other_pct"))
        if total_votes is None or dem_pct is None or rep_pct is None:
            raise ValueError(
                "need either dem/rep/other votes OR total_votes + dem_pct + rep_pct"
            )
        dem_votes, rep_votes, other_votes = allocate_votes_from_pct(
            total_votes=total_votes,
            dem_pct=dem_pct,
            rep_pct=rep_pct,
            other_pct=other_pct,
        )

    if dem_votes < 0 or rep_votes < 0 or other_votes < 0:
        raise ValueError("vote values must be non-negative")

    out = {
        "dem_votes": int(dem_votes),
        "rep_votes": int(rep_votes),
        "other_votes": int(other_votes),
    }
    dem_candidate = norm_space(str(row.get("dem_candidate", "")))
    rep_candidate = norm_space(str(row.get("rep_candidate", "")))
    if dem_candidate:
        out["dem_candidate"] = dem_candidate
    if rep_candidate:
        out["rep_candidate"] = rep_candidate
    return out


def load_overrides(path: Path) -> List[dict]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("overrides", []) if isinstance(payload, dict) else []
    return rows if isinstance(rows, list) else []


def write_overrides(path: Path, rows: List[dict]) -> None:
    payload = {
        "generated_by": "manual/CSV calibration overrides for build_tn_contests.py",
        "overrides": rows,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync calibration overrides from CSV")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--overrides", type=Path, default=DEFAULT_OVERRIDES)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Missing input CSV: {args.input}")

    merged: Dict[Tuple[str, str, int, str], dict] = {}
    for existing in load_overrides(args.overrides):
        try:
            key = normalize_key(existing)
        except ValueError:
            continue
        kept = {
            "scope": key[0],
            "contest_type": key[1],
            "year": key[2],
            "district": key[3],
            "dem_votes": parse_int(existing.get("dem_votes")) or 0,
            "rep_votes": parse_int(existing.get("rep_votes")) or 0,
            "other_votes": parse_int(existing.get("other_votes")) or 0,
        }
        dem_candidate = norm_space(str(existing.get("dem_candidate", "")))
        rep_candidate = norm_space(str(existing.get("rep_candidate", "")))
        if dem_candidate:
            kept["dem_candidate"] = dem_candidate
        if rep_candidate:
            kept["rep_candidate"] = rep_candidate
        merged[key] = kept

    with args.input.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            if not row:
                continue
            scope_raw = norm_space(str(row.get("scope", "")))
            if not scope_raw or scope_raw.startswith("#"):
                continue
            try:
                key = normalize_key(row)
                votes = build_override_from_csv_row(row)
            except ValueError as exc:
                raise SystemExit(f"{args.input}:{i} invalid row: {exc}") from exc

            merged[key] = {
                "scope": key[0],
                "contest_type": key[1],
                "year": key[2],
                "district": key[3],
                **votes,
            }

    rows_out = sorted(
        merged.values(),
        key=lambda r: (
            str(r.get("scope", "")),
            str(r.get("contest_type", "")),
            int(parse_int(r.get("year")) or 0),
            int(parse_int(r.get("district")) or 0),
        ),
    )

    if args.dry_run:
        print(json.dumps({"overrides": len(rows_out), "path": str(args.overrides)}, indent=2))
        return

    write_overrides(args.overrides, rows_out)
    print(json.dumps({"overrides": len(rows_out), "written": str(args.overrides)}, indent=2))


if __name__ == "__main__":
    main()
