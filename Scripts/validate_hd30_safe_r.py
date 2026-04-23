#!/usr/bin/env python3
"""Validate that TN HD-30 is GOP for most statewide contests.

Rule:
  - For scope=state_house and contest_type in {president, governor, us_senate},
    district 30 must have winner=REP, except:
      - us_senate 2006 is allowed to be non-REP.

This is intentionally opinionated; it's a guardrail for this repo's allocation assumptions.
"""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DISTRICT_DIR = ROOT / "Data" / "district_contests"
MANIFEST_PATH = DISTRICT_DIR / "manifest.json"

ALLOW_NON_REP = {
    ("us_senate", 2006),
}


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    if not MANIFEST_PATH.exists():
        raise SystemExit(f"Missing manifest: {MANIFEST_PATH}")

    manifest = load_json(MANIFEST_PATH)
    files = manifest.get("files", [])
    if not isinstance(files, list):
        raise SystemExit("Invalid district manifest format")

    targets = []
    for entry in files:
        if not isinstance(entry, dict):
            continue
        if entry.get("scope") != "state_house":
            continue
        contest_type = str(entry.get("contest_type") or "")
        if contest_type not in {"president", "governor", "us_senate"}:
            continue
        try:
            year = int(entry.get("year"))
        except (TypeError, ValueError):
            continue
        file_name = str(entry.get("file") or "")
        if not file_name:
            continue
        targets.append((contest_type, year, file_name))

    failures = []
    for contest_type, year, file_name in sorted(targets, key=lambda x: (x[0], x[1])):
        payload = load_json(DISTRICT_DIR / file_name)
        results = ((payload.get("general") or {}).get("results") or {})
        row = results.get("30") or results.get(30)
        winner = (row or {}).get("winner", "")
        if (contest_type, year) in ALLOW_NON_REP:
            ok = True
        else:
            ok = (str(winner).upper() == "REP")
        line = f"{contest_type} {year}: winner={winner}"
        if ok:
            print(line)
        else:
            failures.append(line)

    if failures:
        print("\nFAILURES:")
        for line in failures:
            print(line)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
