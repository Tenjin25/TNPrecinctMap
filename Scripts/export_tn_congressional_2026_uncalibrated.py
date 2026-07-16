#!/usr/bin/env python3
"""Rebuild 2026 congressional contests without calibration_overrides.json.

Writes into:
  Data/reports/district_crosswalk_comparison/cvap_override_outputs/uncalibrated_district_contests_2026/

Also emits president_2024 and a multi-contest calibrated-vs-uncalibrated delta CSV.
"""

from __future__ import annotations

import csv
import json
import shutil
from pathlib import Path

import build_tn_congressional_2026_district_contests as cd26


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
BUNDLE = DATA_DIR / "reports" / "district_crosswalk_comparison" / "cvap_override_outputs"
OUT_DIR = BUNDLE / "uncalibrated_district_contests_2026"
LIVE_DIR = BUNDLE / "live_district_contests_2026"
FALLBACK_LIVE = DATA_DIR / "district_contests_2026"


def numeric(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def load_results(path: Path) -> dict[str, dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return dict(payload.get("general", {}).get("results") or {})


def write_delta_csv(live_dir: Path, uncal_dir: Path, out_csv: Path) -> int:
    rows = []
    for live_path in sorted(live_dir.glob("congressional_*.json")):
        uncal_path = uncal_dir / live_path.name
        if not uncal_path.exists():
            continue
        live = load_results(live_path)
        uncal = load_results(uncal_path)
        contest_type = live_path.name.replace("congressional_", "").rsplit("_", 1)[0]
        year = int(live_path.stem.rsplit("_", 1)[-1])
        districts = sorted(set(live) | set(uncal), key=lambda d: int(d))
        for district in districts:
            l = live.get(district, {})
            u = uncal.get(district, {})
            l_dem, l_rep, l_oth = numeric(l.get("dem_votes")), numeric(l.get("rep_votes")), numeric(l.get("other_votes"))
            u_dem, u_rep, u_oth = numeric(u.get("dem_votes")), numeric(u.get("rep_votes")), numeric(u.get("other_votes"))
            l_tot = l_dem + l_rep + l_oth
            u_tot = u_dem + u_rep + u_oth
            if abs(u_dem - l_dem) < 0.5 and abs(u_rep - l_rep) < 0.5 and abs(u_tot - l_tot) < 0.5:
                continue
            rows.append(
                {
                    "contest_type": contest_type,
                    "year": year,
                    "district_num": district,
                    "uncalibrated_dem": round(u_dem, 1),
                    "calibrated_dem": round(l_dem, 1),
                    "dem_delta_uncal_minus_cal": round(u_dem - l_dem, 1),
                    "uncalibrated_rep": round(u_rep, 1),
                    "calibrated_rep": round(l_rep, 1),
                    "rep_delta_uncal_minus_cal": round(u_rep - l_rep, 1),
                    "uncalibrated_total": round(u_tot, 1),
                    "calibrated_total": round(l_tot, 1),
                    "total_delta_uncal_minus_cal": round(u_tot - l_tot, 1),
                }
            )
    rows.sort(key=lambda r: (r["contest_type"], r["year"], int(r["district_num"])))
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "contest_type",
                "year",
                "district_num",
                "uncalibrated_dem",
                "calibrated_dem",
                "dem_delta_uncal_minus_cal",
                "uncalibrated_rep",
                "calibrated_rep",
                "rep_delta_uncal_minus_cal",
                "uncalibrated_total",
                "calibrated_total",
                "total_delta_uncal_minus_cal",
            ],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def main() -> None:
    BUNDLE.mkdir(parents=True, exist_ok=True)
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Point builder at the comparison folder and disable calibration overrides.
    cd26.OUTPUT_DIR = OUT_DIR
    cd26.OVERRIDES_PATH = OUT_DIR / "calibration_overrides.disabled.json"
    # Keep legacy CD1/CD2 transfer behavior; only remove manual CD5/8/9 calibration.
    summary = cd26.build_congressional_2026()

    live_dir = LIVE_DIR if LIVE_DIR.exists() else FALLBACK_LIVE
    delta_rows = write_delta_csv(
        live_dir,
        OUT_DIR,
        BUNDLE / "congressional_2026_calibrated_vs_uncalibrated.csv",
    )

    # Focused president 2024 view.
    live_p = live_dir / "congressional_president_2024.json"
    uncal_p = OUT_DIR / "congressional_president_2024.json"
    focus = []
    if live_p.exists() and uncal_p.exists():
        live = load_results(live_p)
        uncal = load_results(uncal_p)
        for district in sorted(set(live) | set(uncal), key=lambda d: int(d)):
            l = live.get(district, {})
            u = uncal.get(district, {})
            focus.append(
                {
                    "district_num": district,
                    "uncalibrated_dem": int(numeric(u.get("dem_votes"))),
                    "calibrated_dem": int(numeric(l.get("dem_votes"))),
                    "dem_delta": int(numeric(u.get("dem_votes")) - numeric(l.get("dem_votes"))),
                    "uncalibrated_rep": int(numeric(u.get("rep_votes"))),
                    "calibrated_rep": int(numeric(l.get("rep_votes"))),
                    "rep_delta": int(numeric(u.get("rep_votes")) - numeric(l.get("rep_votes"))),
                    "uncalibrated_total": int(
                        numeric(u.get("dem_votes")) + numeric(u.get("rep_votes")) + numeric(u.get("other_votes"))
                    ),
                    "calibrated_total": int(
                        numeric(l.get("dem_votes")) + numeric(l.get("rep_votes")) + numeric(l.get("other_votes"))
                    ),
                }
            )

    payload = {
        "output_dir": str(OUT_DIR.relative_to(DATA_DIR)),
        "builder_summary": summary,
        "delta_csv": "reports/district_crosswalk_comparison/cvap_override_outputs/congressional_2026_calibrated_vs_uncalibrated.csv",
        "delta_rows_changed": delta_rows,
        "president_2024": focus,
        "note": (
            "Uncalibrated rebuild disables calibration_overrides.json for CD 5/8/9. "
            "Districts 1 and 2 still use the 2022-line legacy transfer."
        ),
    }
    (BUNDLE / "uncalibrated_summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
