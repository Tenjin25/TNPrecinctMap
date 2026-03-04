#!/usr/bin/env python3
"""Build TN contest slice JSON files for the map UI."""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
CONTESTS_DIR = DATA_DIR / "contests"
DISTRICT_DIR = DATA_DIR / "district_contests"


COUNTY_CONTEST_TYPES = {"president", "governor", "us_senate"}
DISTRICT_SCOPE_BY_CONTEST = {
    "us_house": "congressional",
    "state_house": "state_house",
    "state_senate": "state_senate",
}


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def norm_county(county: str) -> str:
    return norm_space(county).upper()


def parse_votes(value) -> int:
    if value is None:
        return 0
    s = str(value).strip().replace(",", "")
    if not s:
        return 0
    try:
        return int(round(float(s)))
    except ValueError:
        return 0


def party_bucket(party_raw: str) -> str:
    p = norm_space((party_raw or "")).upper()
    if not p or p in {"NA", "N/A"}:
        return "OTHER"
    if p in {"D", "DEM", "DEMOCRAT", "DEMOCRATIC"} or p.startswith("DEMOCRAT"):
        return "DEM"
    if p in {"R", "REP", "REPUBLICAN"} or p.startswith("REPUBLICAN"):
        return "REP"
    return "OTHER"


def infer_contest_type(office_raw: str) -> Optional[str]:
    o = norm_space(office_raw).upper()
    if not o:
        return None
    if "PRESIDENT" in o:
        return "president"
    if "GOVERNOR" in o and "LIEUTENANT" not in o:
        return "governor"
    if "U.S. SENATE" in o or "UNITED STATES SENATE" in o:
        return "us_senate"
    if "U.S. HOUSE" in o or "UNITED STATES HOUSE OF REPRESENTATIVES" in o:
        return "us_house"
    if "STATE HOUSE" in o:
        return "state_house"
    if "STATE SENATE" in o:
        return "state_senate"
    return None


def parse_district(district_raw: str, office_raw: str) -> Optional[str]:
    d = norm_space(district_raw).upper()
    if d and d not in {"NA", "N/A", "NONE"}:
        m = re.search(r"(\d+)", d)
        if m:
            return str(int(m.group(1)))

    o = norm_space(office_raw)
    m2 = re.search(r"[Dd]istrict\s+(\d+)", o)
    if m2:
        return str(int(m2.group(1)))
    return None


@dataclass
class Totals:
    dem: int = 0
    rep: int = 0
    other: int = 0
    dem_cands: Counter = field(default_factory=Counter)
    rep_cands: Counter = field(default_factory=Counter)

    def add(self, party: str, candidate: str, votes: int) -> None:
        if votes <= 0:
            return
        cand = norm_space(candidate)
        if party == "DEM":
            self.dem += votes
            if cand:
                self.dem_cands[cand] += votes
        elif party == "REP":
            self.rep += votes
            if cand:
                self.rep_cands[cand] += votes
        else:
            self.other += votes

    def as_row(self, label: str) -> dict:
        total = self.dem + self.rep + self.other
        margin_votes = self.rep - self.dem
        margin_pct = (margin_votes / total * 100.0) if total else 0.0
        winner = "REP" if margin_votes > 0 else ("DEM" if margin_votes < 0 else "TIE")
        dem_cand = self.dem_cands.most_common(1)[0][0] if self.dem_cands else ""
        rep_cand = self.rep_cands.most_common(1)[0][0] if self.rep_cands else ""
        return {
            "county": label,
            "dem_votes": int(self.dem),
            "rep_votes": int(self.rep),
            "other_votes": int(self.other),
            "total_votes": int(total),
            "dem_candidate": dem_cand,
            "rep_candidate": rep_cand,
            "margin": int(margin_votes),
            "margin_pct": round(margin_pct, 4),
            "winner": winner,
            "color": "",
        }

    def as_district_result(self) -> dict:
        total = self.dem + self.rep + self.other
        margin_votes = self.rep - self.dem
        margin_pct = (margin_votes / total * 100.0) if total else 0.0
        winner = "REP" if margin_votes > 0 else ("DEM" if margin_votes < 0 else "TIE")
        dem_cand = self.dem_cands.most_common(1)[0][0] if self.dem_cands else ""
        rep_cand = self.rep_cands.most_common(1)[0][0] if self.rep_cands else ""
        return {
            "dem_votes": int(self.dem),
            "rep_votes": int(self.rep),
            "other_votes": int(self.other),
            "total_votes": int(total),
            "dem_candidate": dem_cand,
            "rep_candidate": rep_cand,
            "margin": int(margin_votes),
            "margin_pct": round(margin_pct, 4),
            "winner": winner,
        }


def iter_standard_rows(path: Path, year: int) -> Iterator[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            yield {
                "year": year,
                "county": r.get("county", ""),
                "office": r.get("office", ""),
                "district": r.get("district", ""),
                "party": r.get("party", ""),
                "candidate": r.get("candidate", ""),
                "votes": parse_votes(r.get("votes")),
            }


def iter_2024_rows(path: Path, year: int) -> Iterator[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            county = r.get("COUNTY", "")
            office = r.get("OFFICENAME", "")
            district = ""
            if office:
                m = re.search(r"[Dd]istrict\s+(\d+)", office)
                if m:
                    district = str(int(m.group(1)))
            for i in range(1, 11):
                cand = r.get(f"RNAME{i}", "")
                party = r.get(f"PARTY{i}", "")
                votes = parse_votes(r.get(f"PVTALLY{i}"))
                if not cand and votes <= 0:
                    continue
                yield {
                    "year": year,
                    "county": county,
                    "office": office,
                    "district": district,
                    "party": party,
                    "candidate": cand,
                    "votes": votes,
                }


def iter_all_rows(csv_files: List[Path]) -> Iterator[dict]:
    for path in csv_files:
        year = int(path.name[:4])
        if year == 2024:
            yield from iter_2024_rows(path, year)
        else:
            yield from iter_standard_rows(path, year)


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build() -> dict:
    csv_files = sorted(DATA_DIR.glob("*__tn__*__precinct.csv"))
    if not csv_files:
        raise RuntimeError("No TN precinct CSV files found in Data/")

    county_aggs: Dict[Tuple[str, int, str], Totals] = defaultdict(Totals)
    district_aggs: Dict[Tuple[str, str, int, str], Totals] = defaultdict(Totals)

    for row in iter_all_rows(csv_files):
        contest_type = infer_contest_type(row["office"])
        if not contest_type:
            continue

        county = norm_county(row["county"])
        if not county:
            continue
        party = party_bucket(row["party"])
        candidate = row["candidate"]
        votes = int(row["votes"])
        year = int(row["year"])

        if contest_type in COUNTY_CONTEST_TYPES:
            county_aggs[(contest_type, year, county)].add(party, candidate, votes)

        scope = DISTRICT_SCOPE_BY_CONTEST.get(contest_type)
        if scope:
            district = parse_district(row["district"], row["office"])
            if district:
                district_aggs[(scope, contest_type, year, district)].add(
                    party, candidate, votes
                )

    contests_manifest_files: List[dict] = []
    district_manifest_files: List[dict] = []

    for contest_type in sorted({k[0] for k in county_aggs.keys()}):
        years = sorted({k[1] for k in county_aggs.keys() if k[0] == contest_type})
        for year in years:
            rows = []
            dem_total = 0
            rep_total = 0
            for key, totals in sorted(county_aggs.items(), key=lambda x: x[0][2]):
                c_type, y, county = key
                if c_type != contest_type or y != year:
                    continue
                row = totals.as_row(county)
                rows.append(row)
                dem_total += row["dem_votes"]
                rep_total += row["rep_votes"]

            file_name = f"{contest_type}_{year}.json"
            payload = {
                "contest_type": contest_type,
                "year": year,
                "meta": {
                    "source": "tn_precinct_csv_aggregation",
                    "rows": len(rows),
                },
                "rows": rows,
            }
            write_json(CONTESTS_DIR / file_name, payload)
            contests_manifest_files.append(
                {
                    "year": year,
                    "contest_type": contest_type,
                    "file": file_name,
                    "rows": len(rows),
                    "dem_total": int(dem_total),
                    "rep_total": int(rep_total),
                    "major_party_contested": bool(dem_total > 0 and rep_total > 0),
                }
            )

    grouped_district: Dict[Tuple[str, str, int], Dict[str, Totals]] = defaultdict(dict)
    for (scope, contest_type, year, district), totals in district_aggs.items():
        grouped_district[(scope, contest_type, year)][district] = totals

    for (scope, contest_type, year), dmap in sorted(grouped_district.items()):
        results = {}
        dem_total = 0
        rep_total = 0
        for district in sorted(dmap.keys(), key=lambda d: int(d)):
            row = dmap[district].as_district_result()
            results[str(int(district))] = row
            dem_total += row["dem_votes"]
            rep_total += row["rep_votes"]

        file_name = f"{scope}_{contest_type}_{year}.json"
        payload = {
            "scope": scope,
            "contest_type": contest_type,
            "year": year,
            "meta": {
                "source": "tn_precinct_csv_direct_district_aggregation",
                "match_coverage_pct": 100.0,
                "districts": len(results),
            },
            "general": {"results": results},
        }
        write_json(DISTRICT_DIR / file_name, payload)
        district_manifest_files.append(
            {
                "scope": scope,
                "year": year,
                "contest_type": contest_type,
                "file": file_name,
                "districts": len(results),
                "dem_total": int(dem_total),
                "rep_total": int(rep_total),
                "major_party_contested": bool(dem_total > 0 and rep_total > 0),
            }
        )

    contests_manifest = {"files": sorted(contests_manifest_files, key=lambda x: (x["contest_type"], x["year"]))}
    district_manifest = {"files": sorted(district_manifest_files, key=lambda x: (x["scope"], x["contest_type"], x["year"]))}
    write_json(CONTESTS_DIR / "manifest.json", contests_manifest)
    write_json(DISTRICT_DIR / "manifest.json", district_manifest)

    summary = {
        "contest_files": len(contests_manifest_files),
        "district_files": len(district_manifest_files),
        "contest_manifest_path": str((CONTESTS_DIR / "manifest.json").relative_to(ROOT)),
        "district_manifest_path": str((DISTRICT_DIR / "manifest.json").relative_to(ROOT)),
    }
    return summary


def main() -> None:
    summary = build()
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
