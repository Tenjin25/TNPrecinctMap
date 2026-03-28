#!/usr/bin/env python3
"""Clean Census county population estimate tables (CO-EST...) into tidy CSV.

This script is intentionally lightweight (stdlib only) and tailored to the
downloaded "table with row headers..." CSV format from Census.
"""

from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def norm_key(s: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", norm_space(s).upper())


def parse_int(s: str) -> int:
    raw = norm_space(s).replace(",", "")
    if raw in {"", "NA", "N/A"}:
        return 0
    try:
        return int(raw)
    except ValueError:
        try:
            return int(round(float(raw)))
        except ValueError:
            return 0


def load_tn_county_name_to_fp() -> Dict[str, Tuple[str, str]]:
    """Return normalized county name -> (canonical NAME20, COUNTYFP20)."""
    path = ROOT / "Data" / "tl_2020_47_county20.geojson"
    with path.open("r", encoding="utf-8") as f:
        gj = json.load(f)
    out: Dict[str, Tuple[str, str]] = {}
    for feat in gj.get("features", []):
        p = feat.get("properties", {}) or {}
        name20 = norm_space(str(p.get("NAME20", "")))
        fp = norm_space(str(p.get("COUNTYFP20", ""))).zfill(3)
        if not (name20 and fp.isdigit()):
            continue
        out[norm_key(name20)] = (name20, fp)
    return out


def find_header_rows(rows: List[List[str]]) -> Tuple[int, int]:
    """Return (header_row_idx, year_row_idx)."""
    for i, r in enumerate(rows):
        if not r:
            continue
        if norm_space(r[0]).lower() == "geographic area":
            return i, i + 1
    raise RuntimeError("Could not find 'Geographic Area' header row.")


def clean_geographic_area(raw: str) -> Tuple[str, str]:
    """Return (level, cleaned_name). level is 'state' or 'county'."""
    s = norm_space(raw).lstrip(".")
    if s.upper() == "TENNESSEE":
        return "state", "Tennessee"
    s = re.sub(r",\s*Tennessee\s*$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+County\s*$", "", s, flags=re.IGNORECASE)
    return "county", s


def build_columns(header_row: List[str], year_row: List[str]) -> List[str]:
    cols = []
    for h, y in zip(header_row, year_row):
        h2 = norm_space(h)
        y2 = norm_space(y)
        if h2.lower() == "geographic area":
            cols.append("geographic_area")
            continue
        if "estimates base" in h2.lower():
            cols.append("pop_base_2020")
            continue
        if y2.isdigit():
            cols.append(f"pop_{y2}")
            continue
        cols.append(norm_space(h2) or norm_space(y2) or "")
    return cols


def clean_file(in_path: Path, out_path: Path) -> None:
    rows: List[List[str]] = []
    with in_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for r in reader:
            rows.append(list(r))

    header_i, year_i = find_header_rows(rows)
    header_row = rows[header_i]
    year_row = rows[year_i] if year_i < len(rows) else []
    cols = build_columns(header_row, year_row)

    # Data begins after the year-row.
    data_rows = rows[year_i + 1 :]
    tn_map = load_tn_county_name_to_fp()

    wanted_years = [2020, 2021, 2022, 2023, 2024, 2025]
    wanted_cols = ["pop_base_2020"] + [f"pop_{y}" for y in wanted_years]
    pct_cols = [f"pct_state_{c}" for c in wanted_cols]

    out_fieldnames = [
        "level",
        "state",
        "statefp",
        "county_name",
        "countyfp",
        "geoid",
        *wanted_cols,
        *pct_cols,
    ]

    cleaned: List[dict] = []

    for r in data_rows:
        if not any(norm_space(x) for x in r):
            continue
        r = r + [""] * (len(cols) - len(r))
        row = {c: r[idx] for idx, c in enumerate(cols) if c}
        geo_raw = row.get("geographic_area", "")
        if not geo_raw:
            continue
        level, name = clean_geographic_area(geo_raw)
        values = {k: parse_int(row.get(k, "")) for k in wanted_cols}
        # Skip trailing footnotes/citations that appear as 1-column rows.
        if all(int(v) == 0 for v in values.values()):
            continue

        if level == "state":
            statefp = "47"
            cleaned.append(
                {
                    "level": "state",
                    "state": "Tennessee",
                    "statefp": statefp,
                    "county_name": "",
                    "countyfp": "",
                    "geoid": statefp,
                    **values,
                }
            )
            continue

        key = norm_key(name)
        canon, countyfp = tn_map.get(key, (name, ""))
        statefp = "47"
        geoid = f"{statefp}{countyfp}" if countyfp else ""
        if not countyfp:
            print(f"WARNING: Unmapped county name: {name!r}", file=sys.stderr)
        cleaned.append(
            {
                "level": "county",
                "state": "Tennessee",
                "statefp": statefp,
                "county_name": canon,
                "countyfp": countyfp,
                "geoid": geoid,
                **values,
            }
        )

    state_row = next((r for r in cleaned if r.get("level") == "state"), None)
    state_vals = {c: int(state_row.get(c, 0)) for c in wanted_cols} if state_row else {}
    for r in cleaned:
        if r.get("level") == "state":
            for c in wanted_cols:
                r[f"pct_state_{c}"] = 100.0 if state_vals.get(c, 0) else 0.0
            continue
        if r.get("level") != "county":
            continue
        for c in wanted_cols:
            denom = float(state_vals.get(c, 0) or 0)
            num = float(r.get(c, 0) or 0)
            r[f"pct_state_{c}"] = round((num / denom * 100.0), 4) if denom > 0 else 0.0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_fieldnames)
        w.writeheader()
        for r in cleaned:
            w.writerow(r)


def main(argv: List[str]) -> int:
    in_path = Path(argv[1]) if len(argv) > 1 else ROOT / "Data" / "CO-EST2025-POP-47.csv"
    out_path = (
        Path(argv[2])
        if len(argv) > 2
        else ROOT / "Data" / "CO-EST2025-POP-47.cleaned.csv"
    )
    clean_file(in_path=in_path, out_path=out_path)
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
