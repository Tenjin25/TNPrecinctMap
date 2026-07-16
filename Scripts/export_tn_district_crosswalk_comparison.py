#!/usr/bin/env python3
"""Export comparison tables for district carryover crosswalk outputs."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"
OUT_DIR = DATA_DIR / "reports" / "district_crosswalk_comparison"

CROSSWALKS = {
    "congressional_2022": XWALK_DIR / "tn_congressional_2022_precinct_crosswalk.csv",
    "congressional_2026": XWALK_DIR / "tn_congressional_2026_precinct_crosswalk.csv",
    "state_house_2022": XWALK_DIR / "tn_state_house_2022_precinct_crosswalk.csv",
    "state_senate_2022": XWALK_DIR / "tn_state_senate_2022_precinct_crosswalk.csv",
}


def read_rows(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def dominant_assignments(path: Path) -> List[dict]:
    rows = read_rows(path)
    grouped: Dict[str, dict] = {}
    for row in rows:
        precinct_key = str(row.get("precinct_key") or "").strip().upper()
        if not precinct_key:
            continue
        try:
            weight = float(row.get("area_weight") or 0.0)
        except ValueError:
            weight = 0.0
        node = grouped.setdefault(
            precinct_key,
            {
                "precinct_key": precinct_key,
                "county_norm": str(row.get("county_norm") or "").strip().upper(),
                "prec_id": str(row.get("prec_id") or "").strip(),
                "district_num": "",
                "dominant_weight": 0.0,
                "total_weight": 0.0,
                "split_count": 0,
            },
        )
        node["total_weight"] += weight
        node["split_count"] += 1
        candidate_district = str(row.get("district_num") or "").strip()
        if weight > float(node["dominant_weight"]):
            node["district_num"] = candidate_district
            node["dominant_weight"] = weight

    out = []
    for node in grouped.values():
        out.append(
            {
                "precinct_key": node["precinct_key"],
                "county_norm": node["county_norm"],
                "prec_id": node["prec_id"],
                "district_num": node["district_num"],
                "dominant_weight": round(float(node["dominant_weight"]), 10),
                "total_weight": round(float(node["total_weight"]), 10),
                "split_count": int(node["split_count"]),
                "is_split": int(int(node["split_count"]) > 1),
            }
        )
    return sorted(out, key=lambda r: (r["county_norm"], r["prec_id"], r["district_num"]))


def split_rows(label: str, dominant_rows: List[dict]) -> List[dict]:
    out = []
    for row in dominant_rows:
        if int(row["split_count"]) <= 1:
            continue
        out.append(
            {
                "crosswalk": label,
                "precinct_key": row["precinct_key"],
                "county_norm": row["county_norm"],
                "prec_id": row["prec_id"],
                "dominant_district": row["district_num"],
                "dominant_weight": row["dominant_weight"],
                "total_weight": row["total_weight"],
                "split_count": row["split_count"],
            }
        )
    return out


def compare_congressional(current_rows: List[dict], new_rows: List[dict]) -> List[dict]:
    current = {row["precinct_key"]: row for row in current_rows}
    new = {row["precinct_key"]: row for row in new_rows}
    keys = sorted(set(current.keys()).union(new.keys()))
    out = []
    for key in keys:
        old = current.get(key, {})
        nxt = new.get(key, {})
        old_d = str(old.get("district_num") or "")
        new_d = str(nxt.get("district_num") or "")
        out.append(
            {
                "precinct_key": key,
                "county_norm": str(nxt.get("county_norm") or old.get("county_norm") or ""),
                "prec_id": str(nxt.get("prec_id") or old.get("prec_id") or ""),
                "current_2022_district": old_d,
                "new_2026_district": new_d,
                "dominant_district_changed": int(old_d != new_d),
                "current_2022_dominant_weight": old.get("dominant_weight", ""),
                "new_2026_dominant_weight": nxt.get("dominant_weight", ""),
                "current_2022_split_count": old.get("split_count", ""),
                "new_2026_split_count": nxt.get("split_count", ""),
            }
        )
    return out


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dominant_by_label = {}
    split_summary = []
    summary = {
        "output_dir": str(OUT_DIR.relative_to(DATA_DIR)),
        "crosswalks": {},
    }

    dominant_fields = [
        "precinct_key",
        "county_norm",
        "prec_id",
        "district_num",
        "dominant_weight",
        "total_weight",
        "split_count",
        "is_split",
    ]
    for label, path in CROSSWALKS.items():
        if not path.exists():
            raise FileNotFoundError(f"Missing {path}")
        rows = dominant_assignments(path)
        dominant_by_label[label] = rows
        out_path = OUT_DIR / f"{label}_dominant_precinct_assignments.csv"
        write_csv(out_path, rows, dominant_fields)
        split_summary.extend(split_rows(label, rows))
        summary["crosswalks"][label] = {
            "source_csv": str(path.relative_to(DATA_DIR)),
            "dominant_assignments_csv": str(out_path.relative_to(DATA_DIR)),
            "precincts": len(rows),
            "split_precincts": sum(1 for row in rows if int(row["is_split"]) == 1),
        }

    comparison = compare_congressional(
        dominant_by_label["congressional_2022"],
        dominant_by_label["congressional_2026"],
    )
    comparison_path = OUT_DIR / "congressional_2022_vs_2026_dominant_changes.csv"
    write_csv(
        comparison_path,
        comparison,
        [
            "precinct_key",
            "county_norm",
            "prec_id",
            "current_2022_district",
            "new_2026_district",
            "dominant_district_changed",
            "current_2022_dominant_weight",
            "new_2026_dominant_weight",
            "current_2022_split_count",
            "new_2026_split_count",
        ],
    )
    split_path = OUT_DIR / "split_precincts_by_crosswalk.csv"
    write_csv(
        split_path,
        sorted(split_summary, key=lambda r: (r["crosswalk"], r["county_norm"], r["prec_id"])),
        [
            "crosswalk",
            "precinct_key",
            "county_norm",
            "prec_id",
            "dominant_district",
            "dominant_weight",
            "total_weight",
            "split_count",
        ],
    )

    changed = [row for row in comparison if int(row["dominant_district_changed"]) == 1]
    summary["congressional_2022_vs_2026"] = {
        "comparison_csv": str(comparison_path.relative_to(DATA_DIR)),
        "precincts_compared": len(comparison),
        "dominant_district_changes": len(changed),
    }
    summary["split_precincts_csv"] = str(split_path.relative_to(DATA_DIR))

    summary_path = OUT_DIR / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
