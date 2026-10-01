#!/usr/bin/env python3
"""Validate a staged Tennessee contest rebuild and compare it with production."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


FIELDS = ("dem_votes", "rep_votes", "other_votes")
EXPECTED = {"congressional": 9, "state_house": 99, "state_senate": 33}


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def result_rows(payload: dict):
    if isinstance(payload.get("rows"), list):
        return payload["rows"]
    return list(payload.get("general", {}).get("results", {}).values())


def validate_file(path: Path, errors: list[dict]) -> None:
    payload = load(path)
    rows = result_rows(payload)
    for index, row in enumerate(rows):
        values = [row.get(field) for field in FIELDS]
        if any(not isinstance(value, int) or value < 0 for value in values):
            errors.append({"file": path.name, "row": index, "error": "nonnegative_integer_votes"})
            continue
        if sum(values) != row.get("total_votes"):
            errors.append({"file": path.name, "row": index, "error": "party_sum_mismatch"})
    if "scope" in payload:
        results = payload.get("general", {}).get("results", {})
        expected = EXPECTED.get(payload["scope"])
        if payload.get("scope") == "state_senate" and payload.get("contest_type") == "state_senate":
            expected = 17 if int(payload.get("year", 0)) % 4 == 2 else 16
        if payload.get("scope") == "state_senate" and payload.get("contest_type") == "state_senate":
            if len(results) not in {16, 17, 18}:
                errors.append({"file": path.name, "error": "district_count", "expected": "16-18", "actual": len(results)})
            expected = None
        if expected and len(results) != expected:
            errors.append({"file": path.name, "error": "district_count", "expected": expected, "actual": len(results)})
        if len(results) != len(set(results)):
            errors.append({"file": path.name, "error": "duplicate_district"})


def compare_dirs(staged: Path, published: Path) -> dict:
    flips, margins, totals = [], [], []
    for new_path in sorted(staged.glob("*.json")):
        if new_path.name in {"manifest.json", "unresolved_unm_buckets.json"}:
            continue
        old_path = published / new_path.name
        if not old_path.exists():
            continue
        new = load(new_path)
        old = load(old_path)
        new_results = new.get("general", {}).get("results", {})
        old_results = old.get("general", {}).get("results", {})
        for district in sorted(set(new_results) & set(old_results), key=lambda value: int(value)):
            n, o = new_results[district], old_results[district]
            base = {"file": new_path.name, "district": district}
            if n.get("winner") != o.get("winner"):
                flips.append({**base, "before": o.get("winner"), "after": n.get("winner"), "before_votes": {f: o.get(f) for f in FIELDS}, "after_votes": {f: n.get(f) for f in FIELDS}})
            margin_change = float(n.get("margin_pct", 0)) - float(o.get("margin_pct", 0))
            if abs(margin_change) >= 0.25:
                margins.append({**base, "before_margin_pct": o.get("margin_pct"), "after_margin_pct": n.get("margin_pct"), "change_pp": round(margin_change, 4)})
            total_change = int(n.get("total_votes", 0)) - int(o.get("total_votes", 0))
            if abs(total_change) >= 100:
                totals.append({**base, "before_total": o.get("total_votes"), "after_total": n.get("total_votes"), "change": total_change})
    return {"party_flips": flips, "margin_changes_ge_0_25pp": margins, "vote_total_changes_ge_100": totals}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--staged-root", type=Path, required=True)
    parser.add_argument("--published-root", type=Path, default=Path("Data"))
    parser.add_argument("--staged-2026", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    errors: list[dict] = []
    contest_dir = args.staged_root / "contests"
    district_dir = args.staged_root / "district_contests"
    files = sorted(contest_dir.glob("*.json")) + sorted(district_dir.glob("*.json"))
    files_2026 = sorted(args.staged_2026.glob("*.json")) if args.staged_2026 else []
    files += files_2026
    for path in files:
        if path.name not in {"manifest.json", "unresolved_unm_buckets.json"}:
            validate_file(path, errors)
        else:
            load(path)
    audit_path = args.staged_root / "audit" / "tn_election_reconciliation_audit.json"
    if not audit_path.exists():
        audit_path = args.staged_root / "reports" / "tn_election_reconciliation_audit.json"
    audit = load(audit_path)
    county_mismatches = []
    for row in audit.get("county_rows", []):
        if row.get("final_totals") != row.get("certified_targets"):
            county_mismatches.append({k: row.get(k) for k in ("contest_type", "year", "county", "certified_targets", "final_totals")})
    district_mismatches = [row for row in audit.get("district_reconciliation", []) if row.get("after") != row.get("targets")]
    comparison = compare_dirs(district_dir, args.published_root / "district_contests")
    comparison_2026 = (
        compare_dirs(args.staged_2026, args.published_root / "district_contests_2026")
        if args.staged_2026 else {"party_flips": [], "margin_changes_ge_0_25pp": [], "vote_total_changes_ge_100": []}
    )
    statewide_2026_mismatches = []
    if args.staged_2026:
        for path in sorted(args.staged_2026.glob("congressional_*.json")):
            payload = load(path)
            contest_path = contest_dir / f"{payload.get('contest_type')}_{payload.get('year')}.json"
            if not contest_path.exists():
                continue
            expected = {field: sum(int(row.get(field, 0) or 0) for row in load(contest_path).get("rows", [])) for field in FIELDS}
            actual = {field: sum(int(row.get(field, 0) or 0) for row in payload.get("general", {}).get("results", {}).values()) for field in FIELDS}
            if actual != expected:
                statewide_2026_mismatches.append({"file": path.name, "expected": expected, "actual": actual})
    narrow = []
    fallback = []
    for path in sorted(district_dir.glob("*.json")):
        if path.name in {"manifest.json", "unresolved_unm_buckets.json"}:
            continue
        payload = load(path)
        meta = payload.get("meta", {})
        if float(meta.get("county_fallback_vote_pct", 0) or 0) > 0:
            fallback.append({"file": path.name, "county_fallback_vote_pct": meta.get("county_fallback_vote_pct")})
        for district, row in payload.get("general", {}).get("results", {}).items():
            if abs(float(row.get("margin_pct", 0))) < 1:
                narrow.append({"file": path.name, "district": district, "margin_pct": row.get("margin_pct"), **{f: row.get(f) for f in FIELDS}})
    report = {
        "valid": not errors and not county_mismatches and not district_mismatches and not statewide_2026_mismatches,
        "json_files_checked": len(files),
        "errors": errors,
        "county_reconciliation_mismatches": county_mismatches,
        "district_statewide_mismatches": district_mismatches,
        "comparison": comparison,
        "comparison_2026": comparison_2026,
        "statewide_2026_mismatches": statewide_2026_mismatches,
        "narrow_districts": narrow,
        "district_files_with_fallback": fallback,
        "unresolved_buckets": load(district_dir / "unresolved_unm_buckets.json"),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({
        "valid": report["valid"],
        "json_files_checked": len(files),
        "errors": len(errors),
        "county_mismatches": len(county_mismatches),
        "district_statewide_mismatches": len(district_mismatches),
        "statewide_2026_mismatches": len(statewide_2026_mismatches),
        "party_flips": len(comparison["party_flips"]),
        "margin_changes": len(comparison["margin_changes_ge_0_25pp"]),
        "vote_total_changes": len(comparison["vote_total_changes_ge_100"]),
        "narrow_districts": len(narrow),
        "fallback_files": len(fallback),
        "unresolved_buckets": report["unresolved_buckets"].get("count"),
    }, indent=2))


if __name__ == "__main__":
    main()
