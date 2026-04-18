#!/usr/bin/env python3
"""Report how well contest precinct codes align to the 2020 precinct overlay (VTD20).

This helps identify which historical contest rows are:
  - non-geographic buckets (NG-*)
  - unresolved/mismatched codes (UNM-* or numeric codes missing from the overlay)

Usage (from repo root):
  .\\.venv\\Scripts\\python.exe Scripts\\report_precinct_vtd20_coverage.py
  .\\.venv\\Scripts\\python.exe Scripts\\report_precinct_vtd20_coverage.py --year 2008
  .\\.venv\\Scripts\\python.exe Scripts\\report_precinct_vtd20_coverage.py --top 30
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
CONTESTS_DIR = DATA_DIR / "contests"
PRECINCT_OVERLAY_GEOJSON = DATA_DIR / "tn_voting_precincts.geojson"


def parse_label(label: str) -> Tuple[str, str]:
    s = str(label or "").strip()
    if " - " not in s:
        return "", ""
    county, code = s.split(" - ", 1)
    return county.strip().upper(), code.strip()


def load_overlay_index(path: Path) -> Dict[str, set]:
    if not path.exists():
        raise FileNotFoundError(f"Missing precinct overlay: {path}")
    with path.open("r", encoding="utf-8") as f:
        gj = json.load(f)
    features = gj.get("features") or []
    out: Dict[str, set] = defaultdict(set)
    for feat in features:
        props = feat.get("properties") or {}
        county_norm = str(props.get("county_norm") or "").strip().upper()
        prec_id = str(props.get("prec_id") or "").strip()
        if not county_norm or not prec_id:
            continue
        if prec_id.isdigit():
            prec_id = str(int(prec_id)).zfill(6)
        out[county_norm].add(prec_id)
    return out


def iter_contest_files(year: Optional[int]) -> Iterable[Path]:
    for path in sorted(CONTESTS_DIR.glob("*.json")):
        if path.name == "manifest.json":
            continue
        if year is not None and not path.name.endswith(f"_{year}.json"):
            continue
        yield path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=None, help="Limit to a single year (e.g. 2008).")
    ap.add_argument("--top", type=int, default=25, help="Show top-N missing numeric codes.")
    args = ap.parse_args()

    overlay = load_overlay_index(PRECINCT_OVERLAY_GEOJSON)

    summary_by_file: List[dict] = []
    missing_numeric_by_file: Dict[str, Counter] = {}

    for path in iter_contest_files(args.year):
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        rows = payload.get("rows") or []
        ng = 0
        unm = 0
        numeric = 0
        numeric_missing = 0
        missing_votes = 0.0
        missing_counts = Counter()

        for r in rows:
            county_norm, code = parse_label(r.get("county", ""))
            votes = float(r.get("total_votes") or 0.0)
            if code.startswith("NG-"):
                ng += 1
                continue
            if code.startswith("UNM-"):
                unm += 1
                continue
            if code.isdigit():
                numeric += 1
                code6 = str(int(code)).zfill(6)
                if code6 not in overlay.get(county_norm, set()):
                    numeric_missing += 1
                    missing_votes += votes
                    missing_counts[(county_norm, code6)] += votes
                continue

            # Unknown non-numeric bucket; treat as unresolved.
            unm += 1

        summary_by_file.append(
            {
                "file": path.name,
                "year": int(payload.get("year") or 0),
                "contest_type": str(payload.get("contest_type") or ""),
                "rows": int(len(rows)),
                "numeric_rows": int(numeric),
                "numeric_missing": int(numeric_missing),
                "ng_rows": int(ng),
                "unm_rows": int(unm),
                "missing_votes": round(float(missing_votes), 3),
            }
        )
        missing_numeric_by_file[path.name] = missing_counts

    if not summary_by_file:
        print("No contest files found.")
        return 2

    summary_by_file.sort(key=lambda x: (x["year"], x["contest_type"]))
    print("Precinct (VTD20) coverage summary:")
    for s in summary_by_file:
        print(
            f"- {s['file']}: rows={s['rows']} "
            f"numeric={s['numeric_rows']} missing_numeric={s['numeric_missing']} "
            f"NG={s['ng_rows']} UNM={s['unm_rows']} missing_votes={s['missing_votes']}"
        )

    # Show the biggest missing numeric codes across all selected files.
    combined = Counter()
    for counts in missing_numeric_by_file.values():
        combined.update(counts)
    if combined:
        print(f"\nTop missing numeric precinct codes (not present in tn_voting_precincts.geojson), top {args.top}:")
        for (county_norm, code6), votes in combined.most_common(max(1, int(args.top))):
            print(f"- {county_norm} - {code6}: ~{round(float(votes), 1)} votes")
    else:
        print("\nNo missing numeric precinct codes detected (all numeric codes join to the overlay).")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
