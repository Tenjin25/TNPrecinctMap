#!/usr/bin/env python3
"""Batch runner for DRA-style precinct->VTD20 blockweighted crosswalks.

Runs `build_dra_style_block_crosswalks.py` across all TN precinct CSV sources
and writes a combined manifest + aggregate summary under Data/crosswalks.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"
BUILDER = ROOT / "Scripts" / "build_dra_style_block_crosswalks.py"


def parse_year(name: str) -> int:
    m = re.match(r"^(\d{4})", name)
    if not m:
        return 0
    return int(m.group(1))


def candidate_csvs() -> List[Path]:
    files = sorted(DATA_DIR.glob("*__tn__*__precinct.csv"))
    return [p for p in files if parse_year(p.name) > 0]


def run_one(py_exe: Path, source_csv: Path) -> Dict:
    cmd = [str(py_exe), str(BUILDER), "--source-csv", str(source_csv)]
    proc = subprocess.run(cmd, cwd=str(ROOT), capture_output=True, text=True)
    if proc.returncode != 0:
        return {
            "source_csv": source_csv.name,
            "ok": False,
            "returncode": proc.returncode,
            "stderr": (proc.stderr or "").strip(),
            "stdout": (proc.stdout or "").strip(),
        }
    stdout = (proc.stdout or "").strip()
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return {
            "source_csv": source_csv.name,
            "ok": False,
            "returncode": 0,
            "stderr": "Builder output was not valid JSON",
            "stdout": stdout,
        }
    payload["ok"] = True
    return payload


def write_manifest(rows: List[Dict], path: Path) -> int:
    fieldnames = [
        "source_csv",
        "source_year",
        "source_vintage",
        "overlap_csv",
        "input_precinct_keys",
        "crosswalk_rows",
        "strict_crosswalk_rows",
        "unmatched_rows",
        "method_counts_exact_name",
        "method_counts_token_vtd",
        "method_counts_fuzzy_name",
        "method_counts_unmatched",
        "ok",
    ]
    count = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            mc = r.get("method_counts", {}) if isinstance(r, dict) else {}
            writer.writerow(
                {
                    "source_csv": r.get("source_csv", ""),
                    "source_year": r.get("source_year", ""),
                    "source_vintage": r.get("source_vintage", ""),
                    "overlap_csv": r.get("overlap_csv", ""),
                    "input_precinct_keys": r.get("input_precinct_keys", 0),
                    "crosswalk_rows": r.get("crosswalk_rows", 0),
                    "strict_crosswalk_rows": r.get("strict_crosswalk_rows", 0),
                    "unmatched_rows": r.get("unmatched_rows", 0),
                    "method_counts_exact_name": mc.get("exact_name", 0),
                    "method_counts_token_vtd": mc.get("token_vtd", 0),
                    "method_counts_fuzzy_name": mc.get("fuzzy_name", 0),
                    "method_counts_unmatched": mc.get("unmatched", 0),
                    "ok": int(bool(r.get("ok", False))),
                }
            )
            count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--python",
        default=str(ROOT / ".venv" / "Scripts" / "python.exe"),
        help="Python executable to run builder script",
    )
    parser.add_argument(
        "--min-year",
        type=int,
        default=0,
        help="Only process source CSVs with year >= min-year",
    )
    parser.add_argument(
        "--max-year",
        type=int,
        default=9999,
        help="Only process source CSVs with year <= max-year",
    )
    args = parser.parse_args()

    py_exe = Path(args.python)
    if not py_exe.exists():
        raise FileNotFoundError(f"Python executable not found: {py_exe}")
    if not BUILDER.exists():
        raise FileNotFoundError(f"Builder script not found: {BUILDER}")

    XWALK_DIR.mkdir(parents=True, exist_ok=True)
    srcs = [
        p for p in candidate_csvs()
        if args.min_year <= parse_year(p.name) <= args.max_year
    ]
    if not srcs:
        raise SystemExit("No matching source precinct CSV files found.")

    results: List[Dict] = []
    for src in srcs:
        res = run_one(py_exe, src)
        results.append(res)
        status = "ok" if res.get("ok") else "fail"
        print(f"[{status}] {src.name}")

    manifest_csv = XWALK_DIR / "tn_precinct_to_vtd20_blockweighted_manifest.csv"
    manifest_count = write_manifest(results, manifest_csv)

    ok_rows = [r for r in results if r.get("ok")]
    failed_rows = [r for r in results if not r.get("ok")]
    total_inputs = sum(int(r.get("input_precinct_keys", 0) or 0) for r in ok_rows)
    total_crosswalk_rows = sum(int(r.get("crosswalk_rows", 0) or 0) for r in ok_rows)
    total_strict_crosswalk_rows = sum(int(r.get("strict_crosswalk_rows", 0) or 0) for r in ok_rows)
    total_unmatched = sum(int(r.get("unmatched_rows", 0) or 0) for r in ok_rows)

    agg_methods: Dict[str, int] = {}
    for r in ok_rows:
        mc = r.get("method_counts", {}) or {}
        for k, v in mc.items():
            agg_methods[k] = agg_methods.get(k, 0) + int(v or 0)

    summary = {
        "builder_script": BUILDER.name,
        "processed_files": len(results),
        "processed_ok": len(ok_rows),
        "processed_failed": len(failed_rows),
        "manifest_csv": manifest_csv.name,
        "manifest_rows": manifest_count,
        "totals": {
            "input_precinct_keys": total_inputs,
            "crosswalk_rows": total_crosswalk_rows,
            "strict_crosswalk_rows": total_strict_crosswalk_rows,
            "unmatched_rows": total_unmatched,
            "method_counts": dict(sorted(agg_methods.items())),
        },
        "failed": failed_rows,
    }

    summary_json = XWALK_DIR / "tn_precinct_to_vtd20_blockweighted_batch_summary.json"
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
