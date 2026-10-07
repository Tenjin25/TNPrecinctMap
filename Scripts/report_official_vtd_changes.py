#!/usr/bin/env python3
"""Compare published district slices with a Git baseline."""
import argparse, json, subprocess
from pathlib import Path


def load_git(ref: str, path: str):
    try:
        raw = subprocess.check_output(["git", "show", f"{ref}:{path}"])
    except subprocess.CalledProcessError:
        return None
    return json.loads(raw)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", default="origin/main")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    changed = []
    for folder in ("district_contests", "district_contests_2026"):
        for path in sorted((root / "Data" / folder).glob("*.json")):
            if path.name in {"manifest.json", "unresolved_unm_buckets.json", "calibration_overrides.json"}:
                continue
            rel = path.relative_to(root).as_posix()
            old = load_git(args.baseline, rel)
            if not old:
                continue
            new = json.loads(path.read_text(encoding="utf-8"))
            before = old.get("general", {}).get("results", {})
            after = new.get("general", {}).get("results", {})
            for district in sorted(set(before) | set(after), key=lambda x: int(x)):
                left, right = before.get(district, {}), after.get(district, {})
                fields = ("dem_votes", "rep_votes", "other_votes", "total_votes", "margin_pct", "winner", "rating")
                if any(left.get(field) != right.get(field) for field in fields):
                    changed.append({"mode": "2026" if folder.endswith("2026") else "2022_current", "file": path.name, "district": district, "before": {f: left.get(f) for f in fields}, "after": {f: right.get(f) for f in fields}})
    flips = sum(r["before"]["winner"] != r["after"]["winner"] for r in changed)
    ratings = sum(r["before"]["rating"] != r["after"]["rating"] for r in changed)
    payload = {"baseline": args.baseline, "changed_district_rows": len(changed), "winner_changes": flips, "rating_changes": ratings, "rows": changed}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps({k: payload[k] for k in ("changed_district_rows", "winner_changes", "rating_changes")}, indent=2))


if __name__ == "__main__":
    main()
