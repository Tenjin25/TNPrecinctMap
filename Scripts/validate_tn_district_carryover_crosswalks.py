#!/usr/bin/env python3
"""Validate TN district carryover crosswalk CSVs used by the frontend."""

from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"
PRECINCT_GEOJSON = DATA_DIR / "tn_voting_precincts.geojson"
SUMMARY_PATH = XWALK_DIR / "tn_district_carryover_crosswalk_summary.json"

EXPECTED = [
    XWALK_DIR / "tn_congressional_2022_precinct_crosswalk.csv",
    XWALK_DIR / "tn_congressional_2026_precinct_crosswalk.csv",
    XWALK_DIR / "tn_state_house_2022_precinct_crosswalk.csv",
    XWALK_DIR / "tn_state_senate_2022_precinct_crosswalk.csv",
]

REQUIRED_COLUMNS = {
    "precinct_key",
    "county_norm",
    "prec_id",
    "district_num",
    "area_weight",
}


def load_precinct_keys() -> set[str]:
    payload = json.loads(PRECINCT_GEOJSON.read_text(encoding="utf-8"))
    out = set()
    for feature in payload.get("features", []):
        props = feature.get("properties") or {}
        key = str(props.get("precinct_norm") or "").strip().upper()
        if not key:
            county = str(props.get("county_norm") or "").strip().upper()
            prec_id = str(props.get("prec_id") or "").strip().zfill(6)
            if county and prec_id:
                key = f"{county} - {prec_id}"
        if key:
            out.add(key)
    return out


def read_rows(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        missing_cols = sorted(REQUIRED_COLUMNS.difference(reader.fieldnames or []))
        if missing_cols:
            raise AssertionError(f"{path.name} missing columns: {', '.join(missing_cols)}")
        return list(reader)


def validate_one(path: Path, precinct_keys: set[str]) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")

    rows = read_rows(path)
    if not rows:
        raise AssertionError(f"{path.name} has no rows")

    by_precinct: dict[str, float] = {}
    districts = set()
    bad_weights = 0
    for row in rows:
        precinct_key = str(row.get("precinct_key") or "").strip().upper()
        district_num = str(row.get("district_num") or "").strip()
        try:
            weight = float(row.get("area_weight") or 0.0)
        except ValueError:
            weight = 0.0
        if not precinct_key or not district_num or weight <= 0:
            bad_weights += 1
            continue
        by_precinct[precinct_key] = by_precinct.get(precinct_key, 0.0) + weight
        districts.add(district_num)

    missing_precincts = sorted(precinct_keys.difference(by_precinct.keys()))
    extra_precincts = sorted(set(by_precinct.keys()).difference(precinct_keys))
    low_weight = {k: v for k, v in by_precinct.items() if v < 0.995}
    high_weight = {k: v for k, v in by_precinct.items() if v > 1.005}

    if bad_weights:
        raise AssertionError(f"{path.name} has {bad_weights} rows with invalid weights/keys")
    if missing_precincts:
        raise AssertionError(f"{path.name} missing {len(missing_precincts)} precincts")
    if extra_precincts:
        raise AssertionError(f"{path.name} has {len(extra_precincts)} unknown precincts")
    if low_weight or high_weight:
        raise AssertionError(
            f"{path.name} has precinct weight sums outside [0.995, 1.005]: "
            f"low={len(low_weight)} high={len(high_weight)}"
        )

    return {
        "file": path.name,
        "rows": len(rows),
        "precincts": len(by_precinct),
        "districts": len(districts),
        "weight_sum_min": min(by_precinct.values()),
        "weight_sum_mean": sum(by_precinct.values()) / len(by_precinct),
        "weight_sum_max": max(by_precinct.values()),
    }


def validate_summary(expected_files: list[Path]) -> None:
    if not SUMMARY_PATH.exists():
        raise FileNotFoundError(f"Missing {SUMMARY_PATH}")
    payload = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
    outputs = payload.get("outputs") or []
    actual_files = {str(row.get("output_csv") or "") for row in outputs}
    expected_names = {path.name for path in expected_files}
    missing = sorted(expected_names.difference(actual_files))
    extra = sorted(actual_files.difference(expected_names))
    if missing or extra:
        raise AssertionError(f"Summary output mismatch: missing={missing} extra={extra}")


def validate_frontend_config(expected_files: list[Path]) -> None:
    html = (ROOT / "index.html").read_text(encoding="utf-8", errors="replace")
    for path in expected_files:
        rel = f"./Data/crosswalks/{path.name}"
        if rel not in html:
            raise AssertionError(f"index.html does not reference {rel}")
    for stale in [
        "tn_state_house_2026_precinct_crosswalk.csv",
        "tn_state_senate_2026_precinct_crosswalk.csv",
        "districtManifestByLines = { '2022': [], '2024': [], '2026': [] }",
        "for (const linesYear of [2022, 2024, 2026])",
    ]:
        if stale in html:
            raise AssertionError(f"index.html still contains stale line-mode reference: {stale}")


def main() -> None:
    precinct_keys = load_precinct_keys()
    results = [validate_one(path, precinct_keys) for path in EXPECTED]
    validate_summary(EXPECTED)
    validate_frontend_config(EXPECTED)
    print(json.dumps({"ok": True, "results": results}, indent=2))


if __name__ == "__main__":
    main()
