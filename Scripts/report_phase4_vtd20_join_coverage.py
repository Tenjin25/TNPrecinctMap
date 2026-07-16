#!/usr/bin/env python3
"""Phase-4 verification: contest join coverage vs official Census VTD20.

Writes Data/reports/phase4_vtd20_join_coverage.json summarizing:
  - contest row labels that join / miss official VTDST20
  - blockchain mapped rates by year
  - zero-vote row counts
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
CONTESTS = DATA / "contests"
XWALK = DATA / "crosswalks"
REPORTS = DATA / "reports"
VTD20 = DATA / "tn_vtd_2020.geojson"
COUNTY = DATA / "tl_2020_47_county20.geojson"
CHAIN_SUMMARY = XWALK / "tn_precinct_to_vtd20_blockchain_summary.json"


def county_by_fips() -> dict[str, str]:
    payload = json.loads(COUNTY.read_text(encoding="utf-8"))
    out = {}
    for feat in payload.get("features", []):
        props = feat.get("properties") or {}
        fp = str(props.get("COUNTYFP20") or "").zfill(3)
        name = " ".join(str(props.get("NAME20") or "").upper().split())
        if fp and name:
            out[fp] = name
    return out


def official_labels() -> set[str]:
    fp_map = county_by_fips()
    payload = json.loads(VTD20.read_text(encoding="utf-8"))
    labels = set()
    for feat in payload.get("features", []):
        props = feat.get("properties") or {}
        county = fp_map.get(str(props.get("COUNTYFP20") or "").zfill(3), "")
        vtd = str(props.get("VTDST20") or "").strip()
        if vtd.isdigit():
            vtd = vtd.zfill(6)
        if county and vtd:
            labels.add(f"{county} - {vtd}")
    return labels


def contest_coverage(official: set[str]) -> list[dict]:
    rows_out = []
    for path in sorted(CONTESTS.glob("*.json")):
        if path.name == "manifest.json":
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = payload.get("rows") or []
        joined = 0
        missing = 0
        zeros = 0
        votes_joined = 0
        votes_missing = 0
        missing_samples = []
        for row in rows:
            label = " ".join(str(row.get("county") or "").split())
            # normalize county part to upper
            if " - " in label:
                c, v = label.split(" - ", 1)
                label_n = f"{c.upper()} - {v.strip().zfill(6) if v.strip().isdigit() else v.strip()}"
            else:
                label_n = label.upper()
            votes = int(row.get("total_votes") or 0)
            if votes <= 0:
                zeros += 1
            if label_n in official:
                joined += 1
                votes_joined += votes
            else:
                missing += 1
                votes_missing += votes
                if len(missing_samples) < 5:
                    missing_samples.append(label_n)
        meta = payload.get("meta") or {}
        rows_out.append(
            {
                "file": path.name,
                "rows": len(rows),
                "joined_official": joined,
                "missing_official": missing,
                "join_pct": round(100.0 * joined / len(rows), 2) if rows else 0.0,
                "zero_vote_rows": zeros,
                "votes_joined": votes_joined,
                "votes_missing": votes_missing,
                "block_chain_allocation": bool(meta.get("block_chain_frontend_allocation")),
                "missing_samples": missing_samples,
            }
        )
    return rows_out


def blockchain_summary() -> list[dict]:
    if not CHAIN_SUMMARY.exists():
        return []
    payload = json.loads(CHAIN_SUMMARY.read_text(encoding="utf-8"))
    out = []
    for y in payload.get("years") or []:
        if y.get("status") != "ok":
            continue
        sp = int(y.get("source_precincts") or 0)
        mp = int(y.get("mapped_precincts") or 0)
        out.append(
            {
                "year": y.get("year"),
                "mapped_precincts": mp,
                "source_precincts": sp,
                "mapped_pct": round(100.0 * mp / sp, 2) if sp else 0.0,
                "drawable_labels_touched": y.get("drawable_labels_touched"),
                "drawable_labels_total": y.get("drawable_labels_total"),
                "transfer_method_counts": y.get("transfer_method_counts"),
            }
        )
    return out


def bedford_2024_ok() -> dict:
    path = XWALK / "tn_precinct_to_vtd20_blockweighted_2024.csv"
    if not path.exists():
        return {"ok": False, "reason": "missing blockweighted 2024"}
    rows = [
        r
        for r in csv.DictReader(path.open(encoding="utf-8-sig"))
        if r.get("county_norm") == "BEDFORD"
    ]
    pairs = [(r["from_precinct_norm"], r["dst_vtd20"]) for r in rows]
    expected = {
        ("1 1 WARTRACE", "000101"),
        ("1 2 BELL BUCKLE", "000102"),
    }
    hit = {(a, b.zfill(6) if b.isdigit() else b) for a, b in pairs}
    return {
        "ok": expected.issubset(hit),
        "rows": len(rows),
        "sample": pairs[:4],
    }


def main() -> None:
    official = official_labels()
    payload = {
        "official_vtd20_labels": len(official),
        "bedford_2024_spotcheck": bedford_2024_ok(),
        "blockchain_years": blockchain_summary(),
        "contests": contest_coverage(official),
    }
    REPORTS.mkdir(parents=True, exist_ok=True)
    out = REPORTS / "phase4_vtd20_join_coverage.json"
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    contests = payload["contests"]
    print(f"Official VTD20 labels: {len(official)}")
    print(f"Bedford 2024 spotcheck: {payload['bedford_2024_spotcheck']}")
    print("\nBlockchain mapped %:")
    for y in payload["blockchain_years"]:
        print(
            f"  {y['year']}: {y['mapped_pct']}% "
            f"({y['mapped_precincts']}/{y['source_precincts']}) "
            f"touched {y['drawable_labels_touched']}/{y['drawable_labels_total']}"
        )
    print("\nContest join % (official VTD20):")
    for c in contests:
        print(
            f"  {c['file']}: join={c['join_pct']}% "
            f"zeros={c['zero_vote_rows']} missing={c['missing_official']} "
            f"chain={c['block_chain_allocation']}"
        )
    print(f"\nWrote {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
