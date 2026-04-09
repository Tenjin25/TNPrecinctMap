#!/usr/bin/env python3
"""Convert Tennessee 2022 statewide Governor precinct XLSX to OpenElections CSV.

Input:
  Data/20221108AllbyPrecinct.xlsx (SOFFICEL export; wide candidate columns)

Output:
  Data/20221108__tn__general__governor__precinct.csv (OpenElections-style rows)
    columns: county,precinct,office,district,party,candidate,votes
"""

from __future__ import annotations

import csv
import math
from pathlib import Path
from typing import Dict, Iterable, Iterator, List

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
INPUT_XLSX = DATA / "20221108AllbyPrecinct.xlsx"
OUTPUT_CSV = DATA / "20221108__tn__general__governor__precinct.csv"
SHEET_NAME = "SOFFICELso"


def _as_str(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def _as_int(value) -> int:
    if value is None:
        return 0
    if isinstance(value, float) and math.isnan(value):
        return 0
    s = _as_str(value).replace(",", "")
    if not s:
        return 0
    try:
        return int(round(float(s)))
    except ValueError:
        return 0


def iter_governor_rows(df: pd.DataFrame) -> Iterator[Dict[str, object]]:
    if df is None or df.empty:
        return
    office_col = "OFFICENAME"
    if office_col not in df.columns:
        raise KeyError(f"Missing required column {office_col!r}")

    gov = df[df[office_col].astype(str).str.strip().str.upper().eq("GOVERNOR")]
    for _idx, row in gov.iterrows():
        county = _as_str(row.get("COUNTY"))
        precinct = _as_str(row.get("PRECINCT"))
        if not county or not precinct:
            continue

        for i in range(1, 11):
            candidate = _as_str(row.get(f"RNAME{i}"))
            party = _as_str(row.get(f"PARTY{i}"))
            votes = _as_int(row.get(f"PVTALLY{i}"))
            if votes <= 0 or not candidate:
                continue
            yield {
                "county": county,
                "precinct": precinct,
                "office": "Governor",
                "district": "NA",
                "party": party,
                "candidate": candidate,
                "votes": votes,
            }


def read_input_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing input XLSX: {path}")
    return pd.read_excel(path, sheet_name=SHEET_NAME)


def write_csv(path: Path, rows: Iterable[Dict[str, object]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: List[str] = ["county", "precinct", "office", "district", "party", "candidate", "votes"]
    written = 0
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
            written += 1
    return written


def main() -> None:
    df = read_input_df(INPUT_XLSX)
    rows = list(iter_governor_rows(df))
    rows.sort(key=lambda r: (str(r["county"]), str(r["precinct"]), str(r["candidate"])))
    n = write_csv(OUTPUT_CSV, rows)
    print(f"Wrote {n} rows -> {OUTPUT_CSV.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

