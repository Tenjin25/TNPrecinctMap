#!/usr/bin/env python3
"""Build TN contest slices for county/precinct centroid and district views.

Produces:
  - Data/contests/*.json + manifest.json
  - Data/district_contests/*.json + manifest.json
"""

from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
CONTESTS_DIR = DATA_DIR / "contests"
DISTRICT_DIR = DATA_DIR / "district_contests"


COUNTY_PLUS_PRECINCT_CONTESTS = {"president", "us_senate", "governor"}
DISTRICT_SCOPE_BY_OFFICE_CONTEST = {
    "us_house": "congressional",
    "state_house": "state_house",
    "state_senate": "state_senate",
}
STATEWIDE_DISTRICT_SCOPES = ("congressional", "state_house", "state_senate")


def norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def norm_text(s: str) -> str:
    s = norm_space(s).upper()
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm_county(county: str) -> str:
    return norm_text(county)


def norm_precinct_name(precinct: str) -> str:
    return norm_text(precinct)


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


def is_non_geographic_precinct_name(precinct_raw: str) -> bool:
    p = norm_space(precinct_raw).upper()
    if not p:
        return True
    checks = (
        "ABSENTEE",
        "PROVISIONAL",
        "EARLY",
        "MAIL",
        "CURBSIDE",
        "VOTE CENTER",
        "VOTECENTER",
        "ONE STOP",
    )
    if any(x in p for x in checks):
        return True
    if p in {"EV", "TRANS"}:
        return True
    if p.startswith("OS"):
        return True
    return False


@dataclass
class Totals:
    dem: float = 0.0
    rep: float = 0.0
    other: float = 0.0
    dem_cands: Counter = field(default_factory=Counter)
    rep_cands: Counter = field(default_factory=Counter)

    def add(self, party: str, candidate: str, votes: float) -> None:
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

    def rounded(self) -> Tuple[int, int, int]:
        return int(round(self.dem)), int(round(self.rep)), int(round(self.other))

    def as_precinct_row(self, label: str) -> dict:
        dem, rep, other = self.rounded()
        total = dem + rep + other
        margin_votes = rep - dem
        margin_pct = (margin_votes / total * 100.0) if total else 0.0
        winner = "REP" if margin_votes > 0 else ("DEM" if margin_votes < 0 else "TIE")
        dem_cand = self.dem_cands.most_common(1)[0][0] if self.dem_cands else ""
        rep_cand = self.rep_cands.most_common(1)[0][0] if self.rep_cands else ""
        return {
            "county": label,
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": other,
            "total_votes": total,
            "dem_candidate": dem_cand,
            "rep_candidate": rep_cand,
            "margin": margin_votes,
            "margin_pct": round(margin_pct, 4),
            "winner": winner,
            "color": "",
        }

    def as_district_result(self) -> dict:
        dem, rep, other = self.rounded()
        total = dem + rep + other
        margin_votes = rep - dem
        margin_pct = (margin_votes / total * 100.0) if total else 0.0
        winner = "REP" if margin_votes > 0 else ("DEM" if margin_votes < 0 else "TIE")
        dem_cand = self.dem_cands.most_common(1)[0][0] if self.dem_cands else ""
        rep_cand = self.rep_cands.most_common(1)[0][0] if self.rep_cands else ""
        return {
            "dem_votes": dem,
            "rep_votes": rep,
            "other_votes": other,
            "total_votes": total,
            "dem_candidate": dem_cand,
            "rep_candidate": rep_cand,
            "margin": margin_votes,
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
                "precinct": r.get("precinct", ""),
                "office": r.get("office", ""),
                "district": r.get("district", ""),
                "party": r.get("party", ""),
                "candidate": r.get("candidate", ""),
                "votes": parse_votes(r.get("votes")),
                "prctseq": "",
            }


def iter_2024_rows(path: Path, year: int) -> Iterator[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            county = r.get("COUNTY", "")
            precinct = r.get("PRECINCT", "")
            prctseq = norm_space(r.get("PRCTSEQ", ""))
            office = r.get("OFFICENAME", "")
            district = ""
            m = re.search(r"[Dd]istrict\s+(\d+)", norm_space(office))
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
                    "precinct": precinct,
                    "office": office,
                    "district": district,
                    "party": party,
                    "candidate": cand,
                    "votes": votes,
                    "prctseq": prctseq,
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


def load_county_maps() -> Tuple[Dict[str, str], Dict[str, str]]:
    """Return (county_norm -> countyfp, countyfp -> county_norm)."""
    county_geojson = DATA_DIR / "tl_2020_47_county20.geojson"
    if not county_geojson.exists():
        raise FileNotFoundError("Missing Data/tl_2020_47_county20.geojson")
    with county_geojson.open("r", encoding="utf-8") as f:
        gj = json.load(f)
    norm_to_fp = {}
    fp_to_norm = {}
    for feat in gj.get("features", []):
        p = feat.get("properties", {})
        fp = str(p.get("COUNTYFP20", "")).zfill(3)
        nm = norm_county(p.get("NAME20", ""))
        if fp and nm:
            norm_to_fp[nm] = fp
            fp_to_norm[fp] = nm
    return norm_to_fp, fp_to_norm


def load_precinct_to_2024_map() -> Dict[Tuple[int, str, str], str]:
    """Map (from_year, county_norm, from_precinct_norm) -> 2024 PRCTSEQ (zfill 6)."""
    path = DATA_DIR / "crosswalks" / "tn_precinct_to_2024.csv"
    out: Dict[Tuple[int, str, str], str] = {}
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            from_year = int(r.get("from_year", "0") or 0)
            county_norm = norm_county(r.get("county_norm", ""))
            from_precinct_norm = norm_precinct_name(r.get("from_precinct_norm", ""))
            prct = norm_space(r.get("to_prctseq_2024", ""))
            if not (from_year and county_norm and from_precinct_norm and prct):
                continue
            out[(from_year, county_norm, from_precinct_norm)] = prct.zfill(6)
    return out


def load_2024_prctseq_by_county() -> Dict[str, set]:
    """Return county_norm -> set(int PRCTSEQ) from 2024 CSV."""
    path = DATA_DIR / "20241105__tn__general__precinct.csv"
    out: Dict[str, set] = defaultdict(set)
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            county = norm_county(r.get("COUNTY", ""))
            seq_raw = norm_space(r.get("PRCTSEQ", ""))
            if not county or not seq_raw or not seq_raw.isdigit():
                continue
            out[county].add(int(seq_raw))
    return out


def read_blockassign_table(zip_path: Path, suffix: str) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as zf:
        name = next((n for n in zf.namelist() if n.endswith(suffix)), None)
        if not name:
            raise RuntimeError(f"{suffix} not found in {zip_path.name}")
        raw = zf.read(name).decode("utf-8-sig", errors="replace")
    return pd.read_csv(io.StringIO(raw), sep="|", dtype=str)


def build_precinct_district_weights() -> Dict[str, Dict[Tuple[str, str], List[Tuple[str, float]]]]:
    """Build district weights by precinct key (countyfp, vtd_code) using block counts."""
    zip_path = DATA_DIR / "BlockAssign_ST47_TN.zip"
    if not zip_path.exists():
        raise FileNotFoundError("Missing Data/BlockAssign_ST47_TN.zip")

    vtd = read_blockassign_table(zip_path, "_VTD.txt").rename(
        columns={"BLOCKID": "BLOCKID", "COUNTYFP": "COUNTYFP", "DISTRICT": "VTD"}
    )[["BLOCKID", "COUNTYFP", "VTD"]]
    vtd["COUNTYFP"] = vtd["COUNTYFP"].astype(str).str.zfill(3)
    vtd["VTD"] = vtd["VTD"].astype(str).str.zfill(6)

    scopes = {
        "congressional": "_CD.txt",
        "state_house": "_SLDL.txt",
        "state_senate": "_SLDU.txt",
    }

    out = {}
    for scope, suffix in scopes.items():
        d = read_blockassign_table(zip_path, suffix).rename(
            columns={"BLOCKID": "BLOCKID", "DISTRICT": "DISTRICT"}
        )[["BLOCKID", "DISTRICT"]]
        d["DISTRICT"] = d["DISTRICT"].astype(str).str.strip()
        merged = vtd.merge(d, on="BLOCKID", how="left")
        merged = merged[(merged["DISTRICT"].notna()) & (merged["DISTRICT"] != "")]
        if merged.empty:
            out[scope] = {}
            continue
        counts = (
            merged.groupby(["COUNTYFP", "VTD", "DISTRICT"], dropna=False)
            .size()
            .reset_index(name="block_count")
        )
        totals = (
            counts.groupby(["COUNTYFP", "VTD"], dropna=False)["block_count"]
            .sum()
            .reset_index(name="total_blocks")
        )
        counts = counts.merge(totals, on=["COUNTYFP", "VTD"], how="left")
        counts["weight"] = counts["block_count"] / counts["total_blocks"]

        mapping: Dict[Tuple[str, str], List[Tuple[str, float]]] = defaultdict(list)
        for _, r in counts.iterrows():
            countyfp = str(r["COUNTYFP"]).zfill(3)
            vtd_code = str(r["VTD"]).zfill(6)
            district = str(r["DISTRICT"]).strip()
            m = re.search(r"(\d+)", district)
            if m:
                district = str(int(m.group(1)))
            mapping[(countyfp, vtd_code)].append((district, float(r["weight"])))
        out[scope] = dict(mapping)
    return out


def build_prctseq_offsets(
    county_norm_to_fp: Dict[str, str],
    district_weights: Dict[str, Dict[Tuple[str, str], List[Tuple[str, float]]]],
) -> Tuple[Dict[str, int], Dict[str, set]]:
    """Infer county-specific offset to map 2024 PRCTSEQ -> BlockAssign VTD code.

    Returns:
      offsets: county_fp -> additive offset
      vtd_ints_by_county: county_fp -> set(int vtd_code)
    """
    prctseq_by_county = load_2024_prctseq_by_county()
    vtd_keys = district_weights.get("congressional", {})
    vtd_ints_by_county: Dict[str, set] = defaultdict(set)
    for county_fp, vtd_code in vtd_keys.keys():
        if vtd_code.isdigit():
            vtd_ints_by_county[county_fp].add(int(vtd_code))

    offsets: Dict[str, int] = {}
    for county_norm, pset in prctseq_by_county.items():
        county_fp = county_norm_to_fp.get(county_norm, "")
        if not county_fp:
            continue
        vset = vtd_ints_by_county.get(county_fp, set())
        if not vset or not pset:
            continue

        # Candidate offsets from pairwise diffs.
        cands = Counter()
        for p in pset:
            for v in vset:
                diff = v - p
                if -100 <= diff <= 10000:
                    cands[diff] += 1
        best_k = 0
        best_score = -1
        for k, _cnt in cands.most_common(200):
            score = sum(1 for p in pset if (p + k) in vset)
            if score > best_score:
                best_score = score
                best_k = k
        if best_score >= 1:
            offsets[county_fp] = best_k
    return offsets, vtd_ints_by_county


def prctseq_to_vtd(
    county_fp: str,
    seq_code6: str,
    offsets: Dict[str, int],
    vtd_ints_by_county: Dict[str, set],
) -> str:
    if not seq_code6 or not seq_code6.isdigit():
        return seq_code6
    seq_int = int(seq_code6)
    vset = vtd_ints_by_county.get(county_fp, set())
    if not vset:
        return seq_code6
    if seq_int in vset:
        return str(seq_int).zfill(6)
    k = offsets.get(county_fp, 0)
    candidate = seq_int + k
    if candidate in vset:
        return str(candidate).zfill(6)
    return seq_code6


def resolve_precinct_code(
    year: int,
    county_norm: str,
    county_fp: str,
    precinct_raw: str,
    prctseq_raw: str,
    to2024: Dict[Tuple[int, str, str], str],
    offsets: Dict[str, int],
    vtd_ints_by_county: Dict[str, set],
) -> str:
    if year == 2024:
        p = norm_space(prctseq_raw)
        if p:
            return prctseq_to_vtd(county_fp, p.zfill(6), offsets, vtd_ints_by_county)
    prec_norm = norm_precinct_name(precinct_raw)
    if not prec_norm:
        return ""
    code = to2024.get((year, county_norm, prec_norm), "")
    if code:
        return prctseq_to_vtd(
            county_fp, code.zfill(6), offsets, vtd_ints_by_county
        )
    if is_non_geographic_precinct_name(precinct_raw):
        return f"NG-{prec_norm[:20]}".replace(" ", "_")
    return f"UNM-{prec_norm[:20]}".replace(" ", "_")


def build() -> dict:
    csv_files = sorted(DATA_DIR.glob("*__tn__*__precinct.csv"))
    if not csv_files:
        raise RuntimeError("No TN precinct CSV files found in Data/")

    county_norm_to_fp, _fp_to_county_norm = load_county_maps()
    to2024 = load_precinct_to_2024_map()
    district_weights = build_precinct_district_weights()
    prctseq_offsets, vtd_ints_by_county = build_prctseq_offsets(
        county_norm_to_fp, district_weights
    )

    # Contest rows keyed by (contest, year, "COUNTY - CODE")
    contest_precinct: Dict[Tuple[str, int, str], Totals] = defaultdict(Totals)
    # Direct district office rows keyed by (scope, contest, year, district)
    direct_district: Dict[Tuple[str, str, int, str], Totals] = defaultdict(Totals)

    # Keep a thin cache of statewide precinct rows for district reaggregation.
    statewide_precinct_rows: List[Tuple[str, int, str, str, str, Totals]] = []
    # tuple: contest, year, county_norm, countyfp, code, Totals ref

    for row in iter_all_rows(csv_files):
        contest_type = infer_contest_type(row["office"])
        if not contest_type:
            continue
        county_norm = norm_county(row["county"])
        if not county_norm:
            continue
        county_fp = county_norm_to_fp.get(county_norm, "")
        party = party_bucket(row["party"])
        votes = float(row["votes"])
        candidate = row["candidate"]
        year = int(row["year"])

        # County+precinct contest slices for statewide races.
        if contest_type in COUNTY_PLUS_PRECINCT_CONTESTS:
            code = resolve_precinct_code(
                year=year,
                county_norm=county_norm,
                county_fp=county_fp,
                precinct_raw=row["precinct"],
                prctseq_raw=row["prctseq"],
                to2024=to2024,
                offsets=prctseq_offsets,
                vtd_ints_by_county=vtd_ints_by_county,
            )
            if not code:
                continue
            label = f"{county_norm} - {code}"
            key = (contest_type, year, label)
            contest_precinct[key].add(party, candidate, votes)

        # Direct district office contest slices.
        scope = DISTRICT_SCOPE_BY_OFFICE_CONTEST.get(contest_type)
        if scope:
            district = parse_district(row["district"], row["office"])
            if district:
                dkey = (scope, contest_type, year, district)
                direct_district[dkey].add(party, candidate, votes)

    # Build contest JSON files + manifest.
    contest_manifest_files: List[dict] = []
    all_contest_rows_by_contest_year: Dict[Tuple[str, int], List[dict]] = {}

    contests_present = sorted({(k[0], k[1]) for k in contest_precinct.keys()})
    for contest_type, year in contests_present:
        rows = []
        dem_total = 0
        rep_total = 0
        for (c_type, y, label), totals in sorted(contest_precinct.items(), key=lambda x: x[0][2]):
            if c_type != contest_type or y != year:
                continue
            row_out = totals.as_precinct_row(label)
            rows.append(row_out)
            dem_total += row_out["dem_votes"]
            rep_total += row_out["rep_votes"]
        all_contest_rows_by_contest_year[(contest_type, year)] = rows

        file_name = f"{contest_type}_{year}.json"
        payload = {
            "contest_type": contest_type,
            "year": year,
            "meta": {"source": "tn_precinct_csv_to_2024_precinct_ids", "rows": len(rows)},
            "rows": rows,
        }
        write_json(CONTESTS_DIR / file_name, payload)
        contest_manifest_files.append(
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

    # Build statewide contest -> district scope files via precinct district-weights.
    statewide_district: Dict[Tuple[str, str, int, str], Totals] = defaultdict(Totals)

    for (contest_type, year), rows in all_contest_rows_by_contest_year.items():
        if contest_type not in COUNTY_PLUS_PRECINCT_CONTESTS:
            continue
        for r in rows:
            label = norm_space(r.get("county", ""))
            if " - " not in label:
                continue
            county_norm, code = label.split(" - ", 1)
            county_norm = norm_county(county_norm)
            code = norm_space(code).zfill(6) if re.fullmatch(r"\d+", norm_space(code)) else norm_space(code)
            county_fp = county_norm_to_fp.get(county_norm, "")
            if not county_fp or not re.fullmatch(r"\d{6}", code):
                continue

            dem_votes = float(r.get("dem_votes", 0))
            rep_votes = float(r.get("rep_votes", 0))
            other_votes = float(r.get("other_votes", 0))
            dem_cand = r.get("dem_candidate", "")
            rep_cand = r.get("rep_candidate", "")

            for scope in STATEWIDE_DISTRICT_SCOPES:
                wmap = district_weights.get(scope, {})
                allocs = wmap.get((county_fp, code), [])
                if not allocs:
                    continue
                for district, w in allocs:
                    key = (scope, contest_type, year, district)
                    node = statewide_district[key]
                    node.add("DEM", dem_cand, dem_votes * w)
                    node.add("REP", rep_cand, rep_votes * w)
                    node.add("OTHER", "", other_votes * w)

    # Build district files + manifest (direct + statewide-reallocated).
    district_manifest_files: List[dict] = []
    grouped: Dict[Tuple[str, str, int], Dict[str, Totals]] = defaultdict(dict)

    for (scope, contest_type, year, district), totals in direct_district.items():
        grouped[(scope, contest_type, year)][district] = totals
    for (scope, contest_type, year, district), totals in statewide_district.items():
        grouped[(scope, contest_type, year)][district] = totals

    for (scope, contest_type, year), dmap in sorted(grouped.items()):
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
                "source": "tn_precinct_csv_district_aggregation",
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

    contests_manifest = {
        "files": sorted(contest_manifest_files, key=lambda x: (x["contest_type"], x["year"]))
    }
    district_manifest = {
        "files": sorted(
            district_manifest_files,
            key=lambda x: (x["scope"], x["contest_type"], x["year"]),
        )
    }
    write_json(CONTESTS_DIR / "manifest.json", contests_manifest)
    write_json(DISTRICT_DIR / "manifest.json", district_manifest)

    summary = {
        "contest_files": len(contest_manifest_files),
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
