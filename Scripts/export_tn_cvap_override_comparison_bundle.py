#!/usr/bin/env python3
"""Export CVAP-override carryover outputs into a side-by-side comparison folder.

Creates:
  Data/reports/district_crosswalk_comparison/cvap_override_outputs/
    candidate_crosswalks/     # current CVAP-override carryovers
    area_weighted_crosswalks/ # pure geometry rebuild (no overrides)
    live_district_contests_2026/  # snapshot from sibling live TNPrecinctMap
    congressional_2026_weight_diff.csv
    congressional_2026_president_2024_totals_diff.csv
    manifest.json
"""

from __future__ import annotations

import csv
import json
import shutil
from collections import defaultdict
from pathlib import Path

import build_tn_district_carryover_crosswalks as carry


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"
BUNDLE = DATA_DIR / "reports" / "district_crosswalk_comparison" / "cvap_override_outputs"

LIVE_ROOT = ROOT.parent / "TNPrecinctMap"
LIVE_CONTESTS_2026 = LIVE_ROOT / "Data" / "district_contests_2026"
LOCAL_CONTESTS_2026 = DATA_DIR / "district_contests_2026"
PRECINCT_CONTESTS = DATA_DIR / "contests"


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


def copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def load_crosswalk(path: Path) -> dict[tuple[str, str], float]:
    out: dict[tuple[str, str], float] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            key = normalize_key(row.get("precinct_key") or "")
            district = normalize_district(row.get("district_num"))
            out[(key, district)] = numeric(row.get("area_weight"))
    return out


def load_precinct_contest(contest_type: str, year: int) -> dict[str, dict]:
    path = PRECINCT_CONTESTS / f"{contest_type}_{year}.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("rows") or payload.get("precincts") or []
    out = {}
    for row in rows:
        key = normalize_key(
            row.get("precinct_key")
            or row.get("precinct_norm")
            or row.get("county")  # TN contest rows often store "COUNTY - PREC_ID" here
            or f"{row.get('county_norm')} - {row.get('prec_id') or row.get('vtd')}"
        )
        if not key or key == "-":
            continue
        out[key] = {
            "dem": numeric(row.get("dem_votes") or row.get("dem")),
            "rep": numeric(row.get("rep_votes") or row.get("rep")),
            "other": numeric(row.get("other_votes") or row.get("other")),
            "total": numeric(
                row.get("total_votes")
                or row.get("total")
                or (
                    numeric(row.get("dem_votes") or row.get("dem"))
                    + numeric(row.get("rep_votes") or row.get("rep"))
                    + numeric(row.get("other_votes") or row.get("other"))
                )
            ),
        }
    return out


def allocate_district_totals(crosswalk_path: Path, precinct_votes: dict[str, dict]) -> dict[str, dict]:
    totals: dict[str, dict] = defaultdict(lambda: {"dem": 0.0, "rep": 0.0, "other": 0.0, "total": 0.0})
    with crosswalk_path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            key = normalize_key(row.get("precinct_key") or "")
            district = normalize_district(row.get("district_num"))
            weight = numeric(row.get("area_weight"))
            votes = precinct_votes.get(key)
            if not votes or weight <= 0:
                continue
            bucket = totals[district]
            bucket["dem"] += votes["dem"] * weight
            bucket["rep"] += votes["rep"] * weight
            bucket["other"] += votes["other"] * weight
            bucket["total"] += votes["total"] * weight
    return dict(totals)


def load_live_district_totals(path: Path) -> dict[str, dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    results = {}
    if isinstance(payload.get("general"), dict) and isinstance(payload["general"].get("results"), dict):
        results = payload["general"]["results"]
    elif isinstance(payload.get("results"), dict):
        results = payload["results"]
    else:
        rows = payload.get("rows") or payload.get("districts") or []
        out = {}
        for row in rows:
            district = normalize_district(row.get("district") or row.get("district_num"))
            out[district] = {
                "dem": numeric(row.get("dem_votes") or row.get("dem")),
                "rep": numeric(row.get("rep_votes") or row.get("rep")),
                "other": numeric(row.get("other_votes") or row.get("other")),
                "total": numeric(
                    row.get("total_votes")
                    or row.get("total")
                    or (
                        numeric(row.get("dem_votes") or row.get("dem"))
                        + numeric(row.get("rep_votes") or row.get("rep"))
                        + numeric(row.get("other_votes") or row.get("other"))
                    )
                ),
            }
        return out

    out = {}
    for district, row in results.items():
        d = normalize_district(district)
        out[d] = {
            "dem": numeric(row.get("dem_votes") or row.get("dem")),
            "rep": numeric(row.get("rep_votes") or row.get("rep")),
            "other": numeric(row.get("other_votes") or row.get("other")),
            "total": numeric(
                row.get("total_votes")
                or row.get("total")
                or (
                    numeric(row.get("dem_votes") or row.get("dem"))
                    + numeric(row.get("rep_votes") or row.get("rep"))
                    + numeric(row.get("other_votes") or row.get("other"))
                )
            ),
        }
    return out

def snapshot_candidate(dest: Path) -> list[str]:
    files = [
        "tn_congressional_2022_precinct_crosswalk.csv",
        "tn_congressional_2026_precinct_crosswalk.csv",
        "tn_state_house_2022_precinct_crosswalk.csv",
        "tn_state_senate_2022_precinct_crosswalk.csv",
        "tn_district_carryover_crosswalk_summary.json",
        "tn_district_split_overrides.csv",
        "tn_district_split_cvap_override_summary.json",
    ]
    copied = []
    for name in files:
        src = XWALK_DIR / name
        if src.exists():
            copy_file(src, dest / name)
            copied.append(name)
    compare = (
        DATA_DIR
        / "reports"
        / "district_crosswalk_comparison"
        / "district_split_cvap_weight_comparison.csv"
    )
    if compare.exists():
        copy_file(compare, dest / compare.name)
        copied.append(compare.name)
    return copied


def rebuild_area_weighted(dest: Path) -> list[str]:
    dest.mkdir(parents=True, exist_ok=True)
    # Build without overrides into the comparison folder only.
    carry.SPLIT_OVERRIDES_CSV = dest / "__no_overrides__.csv"
    jobs = []
    for job in carry.JOBS:
        cloned = dict(job)
        cloned["output"] = dest / job["output"].name
        jobs.append(cloned)
    carry.JOBS = jobs
    precincts = carry.load_precincts()
    overrides = {}
    summaries = []
    for job in carry.JOBS:
        summaries.append(carry.build_one(job, precincts, overrides))
    (dest / "tn_district_carryover_crosswalk_summary.json").write_text(
        json.dumps(
            {
                "mode": "area_weighted_no_overrides",
                "outputs": summaries,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return [p.name for p in dest.glob("*.csv")]


def snapshot_live_contests(dest: Path) -> dict:
    dest.mkdir(parents=True, exist_ok=True)
    source = LIVE_CONTESTS_2026 if LIVE_CONTESTS_2026.exists() else LOCAL_CONTESTS_2026
    copied = []
    for path in sorted(source.glob("*")):
        if path.is_file():
            copy_file(path, dest / path.name)
            copied.append(path.name)
    return {"source": str(source), "files": copied}


def build_weight_diff(candidate: Path, area: Path, out_csv: Path) -> int:
    cand = load_crosswalk(candidate)
    area_w = load_crosswalk(area)
    keys = sorted(set(cand) | set(area_w))
    rows = []
    for precinct_key, district in keys:
        c = cand.get((precinct_key, district), 0.0)
        a = area_w.get((precinct_key, district), 0.0)
        delta = c - a
        if abs(delta) < 1e-9 and abs(c) < 1e-9 and abs(a) < 1e-9:
            continue
        if abs(delta) < 1e-8:
            continue
        rows.append(
            {
                "precinct_key": precinct_key,
                "district_num": district,
                "area_weight": round(a, 8),
                "cvap_override_weight": round(c, 8),
                "weight_delta_cvap_minus_area": round(delta, 8),
            }
        )
    rows.sort(key=lambda r: abs(r["weight_delta_cvap_minus_area"]), reverse=True)
    write_csv(
        out_csv,
        rows,
        [
            "precinct_key",
            "district_num",
            "area_weight",
            "cvap_override_weight",
            "weight_delta_cvap_minus_area",
        ],
    )
    return len(rows)


def build_totals_diff(candidate_xwalk: Path, live_json: Path, out_csv: Path) -> int:
    precinct_votes = load_precinct_contest("president", 2024)
    cand_totals = allocate_district_totals(candidate_xwalk, precinct_votes)
    live_totals = load_live_district_totals(live_json)
    districts = sorted(set(cand_totals) | set(live_totals), key=lambda d: int(d) if d.isdigit() else d)
    rows = []
    for district in districts:
        c = cand_totals.get(district, {"dem": 0.0, "rep": 0.0, "other": 0.0, "total": 0.0})
        live = live_totals.get(district, {"dem": 0.0, "rep": 0.0, "other": 0.0, "total": 0.0})
        rows.append(
            {
                "district_num": district,
                "candidate_dem": round(c["dem"], 4),
                "live_dem": round(live["dem"], 4),
                "dem_delta": round(c["dem"] - live["dem"], 4),
                "candidate_rep": round(c["rep"], 4),
                "live_rep": round(live["rep"], 4),
                "rep_delta": round(c["rep"] - live["rep"], 4),
                "candidate_total": round(c["total"], 4),
                "live_total": round(live["total"], 4),
                "total_delta": round(c["total"] - live["total"], 4),
            }
        )
    write_csv(
        out_csv,
        rows,
        [
            "district_num",
            "candidate_dem",
            "live_dem",
            "dem_delta",
            "candidate_rep",
            "live_rep",
            "rep_delta",
            "candidate_total",
            "live_total",
            "total_delta",
        ],
    )
    return len(rows)


def main() -> None:
    if BUNDLE.exists():
        shutil.rmtree(BUNDLE)
    candidate_dir = BUNDLE / "candidate_crosswalks"
    area_dir = BUNDLE / "area_weighted_crosswalks"
    live_dir = BUNDLE / "live_district_contests_2026"

    candidate_files = snapshot_candidate(candidate_dir)
    area_files = rebuild_area_weighted(area_dir)
    live_info = snapshot_live_contests(live_dir)

    weight_rows = build_weight_diff(
        candidate_dir / "tn_congressional_2026_precinct_crosswalk.csv",
        area_dir / "tn_congressional_2026_precinct_crosswalk.csv",
        BUNDLE / "congressional_2026_weight_diff.csv",
    )
    totals_rows = 0
    live_pres = live_dir / "congressional_president_2024.json"
    if live_pres.exists() and (PRECINCT_CONTESTS / "president_2024.json").exists():
        totals_rows = build_totals_diff(
            candidate_dir / "tn_congressional_2026_precinct_crosswalk.csv",
            live_pres,
            BUNDLE / "congressional_2026_president_2024_totals_diff.csv",
        )

    readme = """# CVAP override comparison bundle

This folder keeps candidate (CVAP-split-override) carryover outputs separate from live app files.

## Layout

- `candidate_crosswalks/` — current carryovers with CVAP split overrides applied
- `area_weighted_crosswalks/` — geometry-only rebuild with no split overrides
- `live_district_contests_2026/` — snapshot of live 2026 congressional district contest JSONs
- `congressional_2026_weight_diff.csv` — precinct/district rows where candidate weights differ from area
- `congressional_2026_president_2024_totals_diff.csv` — district totals from candidate crosswalk vs live contest JSON

## Notes

- Live TNPrecinctMap currently loads `Data/district_contests_2026/` directly; it does not ship the precinct carryover CSVs.
- Only three 2026 congressional precincts currently have CVAP overrides (Davidson 001898, Montgomery 006457, Rutherford 007691).
"""
    (BUNDLE / "README.md").write_text(readme, encoding="utf-8")

    manifest = {
        "bundle_dir": str(BUNDLE.relative_to(DATA_DIR)),
        "candidate_files": candidate_files,
        "area_weighted_files": area_files,
        "live_district_contests_2026": live_info,
        "congressional_2026_weight_diff_rows": weight_rows,
        "congressional_2026_president_2024_totals_diff_rows": totals_rows,
    }
    (BUNDLE / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
