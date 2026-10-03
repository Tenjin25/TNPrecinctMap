#!/usr/bin/env python3
"""Validate published RDH legislative-plan result slices and source hierarchy."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


FIELDS = ("dem_votes", "rep_votes", "other_votes")


def load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("Data"))
    parser.add_argument("--baseline-dir", type=Path)
    args = parser.parse_args()
    errors = []
    checked = 0
    audits = [load(args.data_dir / "reports" / "tn_legislative_rdh_audit.json")]
    audit_2012_path = args.data_dir / "reports" / "tn_legislative_rdh_2012_audit.json"
    if audit_2012_path.exists():
        audit_2012 = load(audit_2012_path)
        audits.append(audit_2012)
        if audit_2012.get("unmatched_votes") or any(
            int(item.get("missing_block_votes_before_reconciliation", 0) or 0)
            for item in audit_2012.get("files", [])
        ):
            errors.append({"file": audit_2012_path.name, "error": "unresolved_2012_votes"})
    audit_items = [item for audit in audits for item in audit.get("files", [])]
    for item in audit_items:
        path = args.data_dir / "district_contests" / item["file"]
        payload = load(path)
        scope = payload["scope"]
        expected_count = 99 if scope == "state_house" else 33
        results = payload.get("general", {}).get("results", {})
        if len(results) != expected_count:
            errors.append({"file": path.name, "error": "district_count", "actual": len(results), "expected": expected_count})
        for district, row in results.items():
            votes = [row.get(field) for field in FIELDS]
            if any(not isinstance(value, int) or value < 0 for value in votes):
                errors.append({"file": path.name, "district": district, "error": "invalid_vote"})
                continue
            total, margin = sum(votes), row["rep_votes"] - row["dem_votes"]
            expected_pct = round(margin / total * 100 if total else 0.0, 4)
            if row.get("total_votes") != total or row.get("margin") != margin or row.get("margin_pct") != expected_pct:
                errors.append({"file": path.name, "district": district, "error": "margin_consistency"})
        targets = payload.get("meta", {}).get("certified_statewide_targets")
        if targets:
            actual = {field: sum(row[field] for row in results.values()) for field in FIELDS}
            if actual != targets:
                errors.append({"file": path.name, "error": "statewide_total", "actual": actual, "expected": targets})
        checked += 1

    # Direct 2024 State House files must remain byte-for-byte unchanged.
    if args.baseline_dir:
        for contest in ("president", "us_senate", "state_house"):
            name = f"state_house_{contest}_2024.json"
            current = args.data_dir / "district_contests" / name
            baseline = args.baseline_dir / name
            if current.exists() and baseline.exists() and current.read_bytes() != baseline.read_bytes():
                errors.append({"file": name, "error": "direct_2024_state_house_not_preserved"})

    # High-risk districts must be present and geographically nonempty.
    focus = {
        "state_house": {50, 52, 54, 56, 60, 61, 63, 65, 69, 83, 84, 85, 86, 87, 88, 90, 91, 93, 95, 96, 97, 98, 99},
        "state_senate": {20, 21, 23, 31, 33},
    }
    for item in audit_items:
        path = args.data_dir / "district_contests" / item["file"]
        payload = load(path)
        if "rdh" not in str(payload.get("meta", {}).get("source", "")).lower():
            continue
        results = payload["general"]["results"]
        for district in focus[payload["scope"]]:
            if str(district) not in results or results[str(district)]["total_votes"] <= 0:
                errors.append({"file": path.name, "district": district, "error": "focus_district_empty"})

    print(json.dumps({"valid": not errors, "files_checked": checked, "errors": errors}, indent=2))
    raise SystemExit(1 if errors else 0)


if __name__ == "__main__":
    main()
