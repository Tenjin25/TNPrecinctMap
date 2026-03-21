#!/usr/bin/env python3
"""Build TN contest slices for county/precinct centroid and district views.

Produces:
  - Data/contests/*.json + manifest.json
  - Data/district_contests/*.json + manifest.json
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import geopandas as gpd


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
OVERLAP_MIN_SRC_WEIGHT = 0.001
SMALL_UNMAPPED_ROW_VOTE_FALLBACK_MAX = 2000.0
PRECINCT_OVERLAY_GEOJSON = DATA_DIR / "tn_voting_precincts.geojson"
VTD10_SHAPEFILE_ZIP = DATA_DIR / "tl_2012_47_vtd10.zip"
VTD00_COUNTY_ZIP_DIR = DATA_DIR / "tiger2008_vtd00_counties"
VTD20_NAME_ZIP = DATA_DIR / "tl_2020_47_vtd20.zip"
CONGRESSIONAL_DISTRICT_GEOJSON = DATA_DIR / "tl_2022_47_cd118.geojson"
STATE_HOUSE_DISTRICT_GEOJSON = DATA_DIR / "tl_2022_47_sldl.geojson"
STATE_SENATE_DISTRICT_GEOJSON = DATA_DIR / "tl_2022_47_sldu.geojson"


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
        "ABS ",
        " ABS",
        "PROVISIONAL",
        "EARLY",
        "ELECTION COMM",
        "ELECTION COMMISSION",
        "MAIL",
        "CURBSIDE",
        "SATELLITE",
        "MILITARY",
        "OVERSEAS",
        "PAPER BALLOT",
        "PROPERTY OWNER",
        "NURSING HOME",
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


def is_unmapped_non_geo_bucket(code_raw: str) -> bool:
    """Detect UNM-* labels that are administrative/non-precinct vote buckets."""
    s = norm_space(code_raw).upper()
    if s.startswith("UNM-"):
        s = s[4:]
    s = s.replace("_", " ")
    if not s:
        return False
    checks = (
        "ELECTION COMM",
        "ELECTION COMMISSION",
        "SAFETY PRECINCT",
        "ABSENTEE",
        "SATELLITE",
        "MILITARY",
        "OVERSEAS",
        "PAPER BALLOT",
        "PROPERTY OWNER",
        "NURSING HOME",
        "CURBSIDE",
        "PROVISIONAL",
    )
    if any(x in s for x in checks):
        return True
    if re.search(r"\bEV\b", s):
        return True
    if re.search(r"\bABS\b", s):
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


def build_precinct_split_key_maps(
    to2024: Dict[Tuple[int, str, str], str],
) -> Tuple[Dict[Tuple[int, str, str], str], Dict[Tuple[str, str], str]]:
    """Build unique precinct-key lookup maps from strict precinct->2024 mappings."""
    by_year_raw: Dict[Tuple[int, str, str], set] = defaultdict(set)
    any_year_raw: Dict[Tuple[str, str], set] = defaultdict(set)

    for (year, county_norm, from_precinct_norm), code in to2024.items():
        norm_code = norm_space(code).zfill(6)
        if not norm_code or not norm_code.isdigit():
            continue
        keys = extract_precinct_name_keys(from_precinct_norm)
        for key in keys:
            by_year_raw[(int(year), county_norm, key)].add(norm_code)
            any_year_raw[(county_norm, key)].add(norm_code)

    by_year = {
        k: next(iter(v))
        for k, v in by_year_raw.items()
        if len(v) == 1
    }
    any_year = {
        k: next(iter(v))
        for k, v in any_year_raw.items()
        if len(v) == 1
    }
    return by_year, any_year


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


def build_2024_prctseq_to_vtd_lookup(
    county_norm_to_fp: Dict[str, str],
    vtd20_name_key_map: Dict[Tuple[str, str], List[str]],
) -> Dict[Tuple[str, int], str]:
    """Map (countyfp, PRCTSEQ int) -> VTD20 code using 2024 precinct names."""
    path = DATA_DIR / "20241105__tn__general__precinct.csv"
    out_raw: Dict[Tuple[str, int], set] = defaultdict(set)
    if not path.exists() or not vtd20_name_key_map:
        return {}

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            county_norm = norm_county(r.get("COUNTY", ""))
            county_fp = county_norm_to_fp.get(county_norm, "")
            seq_raw = norm_space(r.get("PRCTSEQ", ""))
            precinct_raw = norm_space(r.get("PRECINCT", ""))
            if not (county_fp and seq_raw and seq_raw.isdigit() and precinct_raw):
                continue
            seq_int = int(seq_raw)
            codes = set()
            for key in extract_precinct_name_keys(precinct_raw):
                for code in vtd20_name_key_map.get((county_fp, key), []):
                    if code and code.isdigit():
                        codes.add(str(code).zfill(6))
            if len(codes) == 1:
                out_raw[(county_fp, seq_int)].update(codes)

    return {
        k: next(iter(v))
        for k, v in out_raw.items()
        if len(v) == 1
    }


def normalize_district_code(raw) -> str:
    s = norm_space(str(raw or ""))
    if not s:
        return ""
    m = re.search(r"(\d+)", s)
    if m:
        return str(int(m.group(1)))
    return s


def build_district_weight_maps_from_overlay() -> Tuple[
    Dict[str, Dict[Tuple[str, str], List[Tuple[str, float]]]],
    Dict[str, Dict[str, List[Tuple[str, float]]]],
]:
    """Build district weights by area overlap between precinct polygons and district polygons."""
    if not PRECINCT_OVERLAY_GEOJSON.exists():
        raise FileNotFoundError("Missing Data/tn_voting_precincts.geojson")

    scope_shapes = {
        "congressional": (CONGRESSIONAL_DISTRICT_GEOJSON, "CD118FP"),
        "state_house": (STATE_HOUSE_DISTRICT_GEOJSON, "SLDLST"),
        "state_senate": (STATE_SENATE_DISTRICT_GEOJSON, "SLDUST"),
    }
    for shp, _field in scope_shapes.values():
        if not shp.exists():
            raise FileNotFoundError(f"Missing {shp}")

    county_norm_to_fp, _ = load_county_maps()
    vtd = gpd.read_file(PRECINCT_OVERLAY_GEOJSON)[["county_norm", "prec_id", "geometry"]].copy()
    vtd["COUNTYFP"] = vtd["county_norm"].apply(
        lambda c: county_norm_to_fp.get(norm_county(str(c)), "")
    )
    vtd["VTD"] = vtd["prec_id"].astype(str).str.zfill(6)
    vtd = vtd[(vtd["COUNTYFP"] != "") & (vtd["VTD"] != "")].copy()
    vtd = vtd[vtd["geometry"].notna()].copy()
    vtd = vtd.to_crs(5070)
    vtd["vtd_area"] = vtd.geometry.area
    vtd = vtd[vtd["vtd_area"] > 0].copy()

    precinct_out: Dict[str, Dict[Tuple[str, str], List[Tuple[str, float]]]] = {}
    county_out: Dict[str, Dict[str, List[Tuple[str, float]]]] = {}

    for scope, (shape_path, district_field) in scope_shapes.items():
        districts = gpd.read_file(shape_path)[[district_field, "geometry"]].copy()
        districts["DISTRICT"] = districts[district_field].apply(normalize_district_code)
        districts = districts[(districts["DISTRICT"] != "") & districts["geometry"].notna()].copy()
        districts = districts.to_crs(5070)

        left = vtd[["COUNTYFP", "VTD", "vtd_area", "geometry"]].copy()
        right = districts[["DISTRICT", "geometry"]].copy()
        intersections = gpd.overlay(left, right, how="intersection", keep_geom_type=False)
        if intersections.empty:
            precinct_out[scope] = {}
            county_out[scope] = {}
            continue

        intersections["inter_area"] = intersections.geometry.area
        intersections = intersections[intersections["inter_area"] > 0].copy()
        intersections["weight"] = intersections["inter_area"] / intersections["vtd_area"]

        grouped = (
            intersections.groupby(["COUNTYFP", "VTD", "DISTRICT"], as_index=False)["weight"]
            .sum()
        )
        mapping: Dict[Tuple[str, str], List[Tuple[str, float]]] = defaultdict(list)
        for (countyfp, vtd_code), frame in grouped.groupby(["COUNTYFP", "VTD"]):
            total = float(frame["weight"].sum())
            if total <= 0:
                continue
            rows = sorted(
                (
                    (str(d), float(w) / total)
                    for d, w in zip(frame["DISTRICT"], frame["weight"])
                    if float(w) > 0
                ),
                key=lambda x: x[1],
                reverse=True,
            )
            if rows:
                mapping[(str(countyfp).zfill(3), str(vtd_code).zfill(6))] = rows
        precinct_out[scope] = dict(mapping)

        county_inter = (
            intersections.groupby(["COUNTYFP", "DISTRICT"], as_index=False)["inter_area"]
            .sum()
        )
        county_totals = (
            county_inter.groupby("COUNTYFP", as_index=False)["inter_area"]
            .sum()
            .rename(columns={"inter_area": "county_area"})
        )
        county_inter = county_inter.merge(county_totals, on="COUNTYFP", how="left")
        county_inter["weight"] = county_inter["inter_area"] / county_inter["county_area"]

        county_mapping: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        for countyfp, frame in county_inter.groupby("COUNTYFP"):
            rows = sorted(
                (
                    (str(d), float(w))
                    for d, w in zip(frame["DISTRICT"], frame["weight"])
                    if float(w) > 0
                ),
                key=lambda x: x[1],
                reverse=True,
            )
            if rows:
                county_mapping[str(countyfp).zfill(3)] = rows
        county_out[scope] = dict(county_mapping)

    return precinct_out, county_out


def build_district_weight_maps() -> Tuple[
    Dict[str, Dict[Tuple[str, str], List[Tuple[str, float]]]],
    Dict[str, Dict[str, List[Tuple[str, float]]]],
]:
    """Build district weights by precinct-polygon to district-polygon area overlap only."""
    required = [
        PRECINCT_OVERLAY_GEOJSON,
        CONGRESSIONAL_DISTRICT_GEOJSON,
        STATE_HOUSE_DISTRICT_GEOJSON,
        STATE_SENATE_DISTRICT_GEOJSON,
    ]
    missing = [str(p) for p in required if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing required overlay inputs for district weight join: " + ", ".join(missing)
        )
    return build_district_weight_maps_from_overlay()


def load_vtd_overlap_to_2020_map(
    path: Path,
    src_code_width: int,
    min_src_weight: float = OVERLAP_MIN_SRC_WEIGHT,
) -> Dict[Tuple[str, str], List[Tuple[str, float]]]:
    """Load src-year VTD -> 2020 VTD overlap weights.

    Expected CSV columns from scripts/build_tn_vtd_overlap_crosswalks.py:
      - src_countyfp, src_vtdst, dst_vtdst, src_weight
    """
    out: Dict[Tuple[str, str], List[Tuple[str, float]]] = {}
    if not path.exists():
        return out

    raw: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            countyfp = norm_space(r.get("src_countyfp", "")).zfill(3)
            src_code = norm_space(r.get("src_vtdst", ""))
            dst_code = norm_space(r.get("dst_vtdst", ""))
            try:
                src_weight = float(r.get("src_weight", "") or 0.0)
            except ValueError:
                src_weight = 0.0

            if not countyfp or not src_code or not dst_code or src_weight <= 0:
                continue
            if src_code.isdigit():
                src_code = src_code.zfill(src_code_width)
            if dst_code.isdigit():
                dst_code = dst_code.zfill(6)
            raw[(countyfp, src_code)][dst_code] += src_weight

    for key, dst_map in raw.items():
        if not dst_map:
            continue
        filtered = {d: w for d, w in dst_map.items() if w >= min_src_weight}
        if not filtered:
            # Keep dominant destination if everything is tiny slivers.
            dmax = max(dst_map.keys(), key=lambda d: dst_map[d])
            filtered = {dmax: dst_map[dmax]}
        total = sum(filtered.values())
        if total <= 0:
            continue
        out[key] = sorted(
            ((d, w / total) for d, w in filtered.items() if w > 0),
            key=lambda x: x[1],
            reverse=True,
        )
    return out


def overlap_source_year_for_election(year: int) -> Optional[int]:
    y = int(year)
    if y <= 2008:
        return 2000
    if 2009 <= y <= 2018:
        return 2010
    return None


def overlap_source_year_candidates(year: int) -> List[int]:
    """Preferred source-year lookup order for overlap remapping."""
    y = int(year)
    if y <= 2008:
        return [2000, 2010]
    if 2009 <= y <= 2018:
        return [2010, 2000]
    # 2020+ rows can still include legacy precinct labels that map via 2010/2000 VTDs.
    return [2010, 2000]


def source_code_candidates_for_overlap(code_numeric: str, src_year: int) -> List[str]:
    """Return ordered candidate source VTD codes for overlap lookup."""
    code = norm_space(code_numeric)
    if not code or not code.isdigit():
        return []
    candidates: List[str] = []

    def add(v: str) -> None:
        if v and v not in candidates:
            candidates.append(v)

    # Preserve raw representation first.
    add(code)
    # Add integer-normalized forms to handle 0001 vs 000001 mismatches.
    n = str(int(code))
    if src_year in {2000, 2010}:
        add(n.zfill(4))
        add(code[-4:].zfill(4))
    add(n.zfill(6))
    return candidates


def _precinct_text_variants(raw: str) -> List[str]:
    s = norm_text(raw)
    if not s:
        return []
    raw_words = [w for w in re.findall(r"[A-Z]+", s) if len(w) >= 2 and w not in {"OF", "THE", "AND"}]
    stop = {
        "SCHOOL",
        "ELEMENTARY",
        "MIDDLE",
        "HIGH",
        "CHURCH",
        "CENTER",
        "CENTRE",
        "COMMUNITY",
        "HALL",
        "FIRE",
        "STATION",
        "BAPTIST",
        "METHODIST",
        "PRESBYTERIAN",
        "CATHOLIC",
        "UNITED",
        "COUNTY",
        "CITY",
        "PRECINCT",
        "VOTE",
        "VOTING",
        "WARD",
        "DISTRICT",
        "LIBRARY",
        "ANNEX",
        "CLUB",
        "HOUSE",
        "BOARD",
        "EDUCATION",
        "CHAPEL",
        "PARK",
        "CHRIST",
        "OF",
        "THE",
        "AND",
    }
    variants: List[str] = []

    def add(v: str) -> None:
        if v and v not in variants:
            variants.append(v)

    def add_word_variants(words: List[str]) -> None:
        if not words:
            return
        add(" ".join(words))
        add(" ".join(w[:4] for w in words if w))
        add(" ".join(w[:2] for w in words if w))
        add(words[0])
        if len(words) >= 2:
            add(" ".join(words[:2]))

    def expand_compound_words(words: List[str]) -> List[str]:
        if not words:
            return []
        suffixes = (
            "VIEW",
            "CREEK",
            "CENTER",
            "SCHOOL",
            "HALL",
            "PARK",
            "CHURCH",
            "STATION",
            "COUNTY",
            "CITY",
        )
        out: List[str] = []
        for w in words:
            split_done = False
            for suf in suffixes:
                if len(w) > len(suf) + 2 and w.endswith(suf):
                    out.append(w[: -len(suf)])
                    out.append(suf)
                    split_done = True
                    break
            if not split_done:
                out.append(w)
        return out

    # Keep raw phrase variants so names composed of location words (e.g. CITY PARK)
    # can still match, while also adding filtered variants for robustness.
    add_word_variants(raw_words)
    add_word_variants(expand_compound_words(raw_words))
    filtered_words = [w for w in raw_words if w not in stop]
    add_word_variants(filtered_words)
    add_word_variants(expand_compound_words(filtered_words))
    return [v for v in variants if v]


def extract_precinct_name_keys(raw: str) -> List[str]:
    """Build robust precinct matching keys across split and name-based patterns."""
    s = norm_text(raw)
    if not s:
        return []
    nums = [int(n) for n in re.findall(r"\d+", s)]
    text_variants = _precinct_text_variants(s)
    keys: List[str] = []

    def add(k: str) -> None:
        if k and k not in keys:
            keys.append(k)

    if len(nums) >= 2:
        add(f"PAIR:{nums[0]:02d}:{nums[1]:02d}")
        add(f"PAIR:{nums[0]:02d}:{nums[-1]:02d}")
    if nums:
        add(f"NUM:{nums[0]:02d}")
        add(f"NUM:{nums[-1]:02d}")
    if nums and text_variants:
        for tv in text_variants:
            add(f"TXTNUM:{tv}:{nums[0]:02d}")
            add(f"NUMTXT:{nums[0]:02d}:{tv}")
            add(f"TXTNUM:{tv}:{nums[-1]:02d}")
            add(f"NUMTXT:{nums[-1]:02d}:{tv}")
    for tv in text_variants:
        add(f"TXT:{tv}")
    return keys


def source_name_keys_for_overlap(code_label: str) -> List[str]:
    """Return parsed precinct matching keys from non-numeric UNM/NG labels."""
    s = norm_space(code_label).upper()
    if not s:
        return []
    if s.startswith("UNM-"):
        s = s[4:]
    elif s.startswith("NG-"):
        s = s[3:]
    s = s.replace("_", " ")
    return extract_precinct_name_keys(s)


def _add_vtd_name_key_rows(
    rows: Iterable[dict],
    county_col: str,
    code_col: str,
    name_cols: List[str],
    code_width: int,
    out: Dict[Tuple[str, str], set],
) -> None:
    for r in rows:
        countyfp = norm_space(str(r.get(county_col, ""))).zfill(3)
        raw_code = norm_space(str(r.get(code_col, "")))
        if not countyfp or not raw_code:
            continue
        src_code = raw_code.zfill(code_width) if raw_code.isdigit() else raw_code
        for col in name_cols:
            for key in extract_precinct_name_keys(r.get(col, "")):
                out[(countyfp, key)].add(src_code)


def load_vtd_name_key_map(src_year: int) -> Dict[Tuple[str, str], List[str]]:
    """Map (countyfp, split_key like '01 3') -> source VTD codes for a source year."""
    out: Dict[Tuple[str, str], set] = defaultdict(set)

    if src_year == 2010:
        if not VTD10_SHAPEFILE_ZIP.exists():
            return {}
        gdf = gpd.read_file(VTD10_SHAPEFILE_ZIP)
        rows = gdf.drop(columns="geometry", errors="ignore").to_dict("records")
        _add_vtd_name_key_rows(
            rows=rows,
            county_col="COUNTYFP10",
            code_col="VTDST10",
            name_cols=["NAME10", "NAMELSAD10"],
            code_width=4,
            out=out,
        )
    elif src_year == 2000:
        if not VTD00_COUNTY_ZIP_DIR.exists():
            return {}
        for zip_path in sorted(VTD00_COUNTY_ZIP_DIR.glob("tl_2008_*_vtd00.zip")):
            gdf = gpd.read_file(zip_path)
            rows = gdf.drop(columns="geometry", errors="ignore").to_dict("records")
            _add_vtd_name_key_rows(
                rows=rows,
                county_col="COUNTYFP00",
                code_col="VTDST00",
                name_cols=["NAME00", "NAMELSAD00"],
                code_width=4,
                out=out,
            )
    else:
        return {}

    return {
        k: sorted(v)
        for k, v in out.items()
        if v
    }


def load_vtd20_name_key_map() -> Dict[Tuple[str, str], List[str]]:
    """Map (countyfp, split_key like '01 3') -> VTD20 code(s)."""
    if not VTD20_NAME_ZIP.exists():
        return {}
    out: Dict[Tuple[str, str], set] = defaultdict(set)
    gdf = gpd.read_file(VTD20_NAME_ZIP)
    rows = gdf.drop(columns="geometry", errors="ignore").to_dict("records")
    _add_vtd_name_key_rows(
        rows=rows,
        county_col="COUNTYFP20",
        code_col="VTDST20",
        name_cols=["NAME20", "NAMELSAD20"],
        code_width=6,
        out=out,
    )
    return {
        k: sorted(v)
        for k, v in out.items()
        if v
    }


def resolve_source_codes_for_overlap(
    year: int,
    county_fp: str,
    code_numeric: str,
    code_label: str,
    overlap_maps_by_src_year: Dict[int, Dict[Tuple[str, str], List[Tuple[str, float]]]],
    vtd_name_key_maps_by_src_year: Dict[int, Dict[Tuple[str, str], List[str]]],
) -> Tuple[Optional[int], List[str]]:
    """Resolve source-era VTD codes that exist in overlap maps for a precinct label."""
    tried_any = False
    for src_year in overlap_source_year_candidates(year):
        overlap_map = overlap_maps_by_src_year.get(src_year, {})
        if not overlap_map:
            continue
        tried_any = True

        source_codes: List[str] = []
        seen_codes = set()
        for c in source_code_candidates_for_overlap(code_numeric, src_year):
            if c in seen_codes:
                continue
            if overlap_map.get((county_fp, c)):
                seen_codes.add(c)
                source_codes.append(c)

        if not source_codes:
            name_map = vtd_name_key_maps_by_src_year.get(src_year, {})
            for key in source_name_keys_for_overlap(code_label):
                candidates = name_map.get((county_fp, key), [])
                if len(candidates) > 8:
                    continue
                for c in candidates:
                    if c in seen_codes:
                        continue
                    if overlap_map.get((county_fp, c)):
                        seen_codes.add(c)
                        source_codes.append(c)

        if source_codes:
            return src_year, source_codes

    if tried_any:
        return overlap_source_year_candidates(year)[0], []
    return None, []


def resolve_precinct_to_2020_code_from_overlap(
    year: int,
    county_fp: str,
    code_numeric: str,
    code_label: str,
    overlap_maps_by_src_year: Dict[int, Dict[Tuple[str, str], List[Tuple[str, float]]]],
    vtd_name_key_maps_by_src_year: Dict[int, Dict[Tuple[str, str], List[str]]],
) -> str:
    """Resolve a single best 2020 VTD code from overlap weights."""
    src_year, source_codes = resolve_source_codes_for_overlap(
        year=year,
        county_fp=county_fp,
        code_numeric=code_numeric,
        code_label=code_label,
        overlap_maps_by_src_year=overlap_maps_by_src_year,
        vtd_name_key_maps_by_src_year=vtd_name_key_maps_by_src_year,
    )
    if src_year is None or not source_codes:
        return ""

    overlap_map = overlap_maps_by_src_year.get(src_year, {})
    dst_accum: Dict[str, float] = defaultdict(float)
    source_weight = 1.0 / float(len(source_codes))
    for src_code in source_codes:
        src_to_2020 = overlap_map.get((county_fp, src_code), [])
        for dst_vtd20, src_w in src_to_2020:
            dst_accum[str(dst_vtd20).zfill(6)] += source_weight * float(src_w)
    if not dst_accum:
        return ""
    return max(sorted(dst_accum.keys()), key=lambda d: dst_accum[d])


def remap_precinct_code_to_2020_vtd_allocations(
    year: int,
    county_fp: str,
    code_numeric: str,
    code_label: str,
    scope_precinct_weights: Dict[Tuple[str, str], List[Tuple[str, float]]],
    overlap_maps_by_src_year: Dict[int, Dict[Tuple[str, str], List[Tuple[str, float]]]],
    vtd_name_key_maps_by_src_year: Dict[int, Dict[Tuple[str, str], List[str]]],
) -> List[Tuple[str, float]]:
    """Return district allocations via historical VTD overlap as fallback."""
    src_year, source_codes = resolve_source_codes_for_overlap(
        year=year,
        county_fp=county_fp,
        code_numeric=code_numeric,
        code_label=code_label,
        overlap_maps_by_src_year=overlap_maps_by_src_year,
        vtd_name_key_maps_by_src_year=vtd_name_key_maps_by_src_year,
    )
    if src_year is None or not source_codes:
        return []
    overlap_map = overlap_maps_by_src_year.get(src_year, {})

    district_accum: Dict[str, float] = defaultdict(float)
    source_weight = 1.0 / float(len(source_codes))
    for src_code in source_codes:
        src_to_2020 = overlap_map.get((county_fp, src_code), [])
        for dst_vtd20, src_w in src_to_2020:
            district_weights = scope_precinct_weights.get((county_fp, str(dst_vtd20).zfill(6)), [])
            for district, dw in district_weights:
                district_accum[district] += source_weight * float(src_w) * float(dw)

    total = sum(district_accum.values())
    if total <= 0:
        return []
    return sorted(
        ((d, w / total) for d, w in district_accum.items() if w > 0),
        key=lambda x: x[1],
        reverse=True,
    )


def build_prctseq_offsets(
    county_norm_to_fp: Dict[str, str],
    district_weights: Dict[str, Dict[Tuple[str, str], List[Tuple[str, float]]]],
) -> Tuple[Dict[str, List[int]], Dict[str, set]]:
    """Infer county-specific offset to map 2024 PRCTSEQ -> VTD20 code.

    Returns:
      offsets_by_county: county_fp -> ordered list[int] additive offsets
      vtd_ints_by_county: county_fp -> set(int vtd_code)
    """
    prctseq_by_county = load_2024_prctseq_by_county()
    vtd_keys = district_weights.get("congressional", {})
    vtd_ints_by_county: Dict[str, set] = defaultdict(set)
    for county_fp, vtd_code in vtd_keys.keys():
        if vtd_code.isdigit():
            vtd_ints_by_county[county_fp].add(int(vtd_code))

    offsets_by_county: Dict[str, List[int]] = {}
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
        unmatched = set(pset)
        ranked: List[int] = []
        for k, _cnt in cands.most_common(400):
            hit = {p for p in unmatched if (p + k) in vset}
            if not hit:
                continue
            ranked.append(k)
            unmatched -= hit
            if len(ranked) >= 8 or not unmatched:
                break
        if ranked:
            offsets_by_county[county_fp] = ranked
    return offsets_by_county, vtd_ints_by_county


def prctseq_to_vtd(
    county_fp: str,
    seq_code6: str,
    offsets_by_county: Dict[str, List[int]],
    vtd_ints_by_county: Dict[str, set],
    prctseq_exact_to_vtd: Dict[Tuple[str, int], str],
) -> str:
    if not seq_code6 or not seq_code6.isdigit():
        return seq_code6
    seq_int = int(seq_code6)
    exact = prctseq_exact_to_vtd.get((county_fp, seq_int), "")
    if exact:
        return str(exact).zfill(6)
    vset = vtd_ints_by_county.get(county_fp, set())
    if not vset:
        return seq_code6
    if seq_int in vset:
        return str(seq_int).zfill(6)
    for k in offsets_by_county.get(county_fp, []):
        candidate = seq_int + int(k)
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
    to2024_split_by_year: Dict[Tuple[int, str, str], str],
    to2024_split_any_year: Dict[Tuple[str, str], str],
    offsets_by_county: Dict[str, List[int]],
    vtd_ints_by_county: Dict[str, set],
    prctseq_exact_to_vtd: Dict[Tuple[str, int], str],
    overlap_maps_by_src_year: Dict[int, Dict[Tuple[str, str], List[Tuple[str, float]]]],
    vtd_name_key_maps_by_src_year: Dict[int, Dict[Tuple[str, str], List[str]]],
    vtd20_name_key_map: Dict[Tuple[str, str], List[str]],
) -> str:
    if year == 2024:
        p = norm_space(prctseq_raw)
        if p:
            return prctseq_to_vtd(
                county_fp, p.zfill(6), offsets_by_county, vtd_ints_by_county, prctseq_exact_to_vtd
            )
    prec_norm = norm_precinct_name(precinct_raw)
    if not prec_norm:
        return ""
    code = to2024.get((year, county_norm, prec_norm), "")
    if code:
        return prctseq_to_vtd(
            county_fp, code.zfill(6), offsets_by_county, vtd_ints_by_county, prctseq_exact_to_vtd
        )

    # Fallback: resolve by split key (e.g., "01-3") from year-specific then any-year crosswalk rows.
    split_keys = source_name_keys_for_overlap(precinct_raw)
    for split_key in split_keys:
        split_code = to2024_split_by_year.get((year, county_norm, split_key), "")
        if not split_code:
            split_code = to2024_split_any_year.get((county_norm, split_key), "")
        if split_code:
            return prctseq_to_vtd(
                county_fp, split_code.zfill(6), offsets_by_county, vtd_ints_by_county, prctseq_exact_to_vtd
            )

    # Fallback: leading numeric precinct labels (e.g., "25 MILLENNIUM", "04 WILL BLOUNT MID")
    # often correspond to PRCTSEQ identifiers.
    mnum = re.match(r"^\s*(\d{1,3})(?:[A-Z])?\b", prec_norm)
    if mnum:
        seq_guess = str(int(mnum.group(1))).zfill(6)
        guessed = prctseq_to_vtd(
            county_fp, seq_guess, offsets_by_county, vtd_ints_by_county, prctseq_exact_to_vtd
        )
        if guessed and guessed.isdigit() and int(guessed) in vtd_ints_by_county.get(county_fp, set()):
            return guessed

    # Fallback: historical VTD name -> 2020 VTD via overlap (2000/2010 sources).
    overlap_code = resolve_precinct_to_2020_code_from_overlap(
        year=year,
        county_fp=county_fp,
        code_numeric="",
        code_label=precinct_raw,
        overlap_maps_by_src_year=overlap_maps_by_src_year,
        vtd_name_key_maps_by_src_year=vtd_name_key_maps_by_src_year,
    )
    if overlap_code:
        return prctseq_to_vtd(
            county_fp, overlap_code.zfill(6), offsets_by_county, vtd_ints_by_county, prctseq_exact_to_vtd
        )

    # Fallback: direct 2020 VTD name-key lookup when 2020 VTD zip is present locally.
    if county_fp and vtd20_name_key_map:
        for split_key in split_keys:
            candidates = vtd20_name_key_map.get((county_fp, split_key), [])
            if len(candidates) == 1:
                return prctseq_to_vtd(
                    county_fp, candidates[0].zfill(6), offsets_by_county, vtd_ints_by_county, prctseq_exact_to_vtd
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
    to2024_split_by_year, to2024_split_any_year = build_precinct_split_key_maps(to2024)
    district_weights, county_district_weights = build_district_weight_maps()
    overlap_maps_by_src_year = {
        2000: load_vtd_overlap_to_2020_map(
            DATA_DIR / "crosswalks" / "tn_vtd00_to_vtd20_overlap.csv",
            src_code_width=4,
        ),
        2010: load_vtd_overlap_to_2020_map(
            DATA_DIR / "crosswalks" / "tn_vtd10_to_vtd20_overlap.csv",
            src_code_width=4,
        ),
    }
    vtd_name_key_maps_by_src_year = {
        2000: load_vtd_name_key_map(2000),
        2010: load_vtd_name_key_map(2010),
    }
    vtd20_name_key_map = load_vtd20_name_key_map()
    prctseq_exact_to_vtd = build_2024_prctseq_to_vtd_lookup(
        county_norm_to_fp=county_norm_to_fp,
        vtd20_name_key_map=vtd20_name_key_map,
    )
    prctseq_offsets_by_county, vtd_ints_by_county = build_prctseq_offsets(
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
                to2024_split_by_year=to2024_split_by_year,
                to2024_split_any_year=to2024_split_any_year,
                offsets_by_county=prctseq_offsets_by_county,
                vtd_ints_by_county=vtd_ints_by_county,
                prctseq_exact_to_vtd=prctseq_exact_to_vtd,
                overlap_maps_by_src_year=overlap_maps_by_src_year,
                vtd_name_key_maps_by_src_year=vtd_name_key_maps_by_src_year,
                vtd20_name_key_map=vtd20_name_key_map,
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
    statewide_alloc_stats: Dict[Tuple[str, str, int], dict] = defaultdict(
        lambda: {
            "rows": 0,
            "direct_rows": 0,
            "overlap_rows": 0,
            "non_geo_rows": 0,
            "county_fallback_rows": 0,
            "dropped_rows": 0,
            "votes_total": 0.0,
            "votes_direct": 0.0,
            "votes_overlap": 0.0,
            "votes_non_geo": 0.0,
            "votes_fallback": 0.0,
            "votes_dropped": 0.0,
        }
    )
    unresolved_unm_vote_by_bucket: Dict[Tuple[str, str, int, str, str], float] = defaultdict(float)

    for (contest_type, year), rows in all_contest_rows_by_contest_year.items():
        if contest_type not in COUNTY_PLUS_PRECINCT_CONTESTS:
            continue
        for r in rows:
            label = norm_space(r.get("county", ""))
            if " - " not in label:
                continue
            county_norm, code = label.split(" - ", 1)
            county_norm = norm_county(county_norm)
            code_raw = norm_space(code)
            code_numeric = code_raw.zfill(6) if re.fullmatch(r"\d+", code_raw) else ""
            county_fp = county_norm_to_fp.get(county_norm, "")
            if not county_fp:
                continue

            dem_votes = float(r.get("dem_votes", 0))
            rep_votes = float(r.get("rep_votes", 0))
            other_votes = float(r.get("other_votes", 0))
            dem_cand = r.get("dem_candidate", "")
            rep_cand = r.get("rep_candidate", "")

            for scope in STATEWIDE_DISTRICT_SCOPES:
                stat_key = (scope, contest_type, year)
                stat = statewide_alloc_stats[stat_key]
                stat["rows"] += 1

                wmap = district_weights.get(scope, {})
                allocs = wmap.get((county_fp, code_numeric), []) if code_numeric else []
                is_non_geo_bucket = code_raw.startswith("NG-")
                is_unmapped_label_bucket = code_raw.startswith("UNM-")
                is_unmapped_non_geo = is_unmapped_label_bucket and is_unmapped_non_geo_bucket(code_raw)
                is_low_seq_numeric = bool(
                    code_numeric and code_numeric.isdigit() and int(code_numeric) < 1000
                )
                county_allocs = county_district_weights.get(scope, {}).get(county_fp, [])
                is_single_district_county = len(county_allocs) == 1
                votes_total = dem_votes + rep_votes + other_votes
                stat["votes_total"] += votes_total

                source = "direct"
                if not allocs:
                    allocs = remap_precinct_code_to_2020_vtd_allocations(
                        year=year,
                        county_fp=county_fp,
                        code_numeric=code_numeric,
                        code_label=code_raw,
                        scope_precinct_weights=wmap,
                        overlap_maps_by_src_year=overlap_maps_by_src_year,
                        vtd_name_key_maps_by_src_year=vtd_name_key_maps_by_src_year,
                    )
                    source = "overlap" if allocs else "county_fallback"
                if not allocs and (is_non_geo_bucket or is_unmapped_non_geo):
                    allocs = county_allocs
                    source = "non_geo_fallback" if allocs else "dropped"
                if not allocs and (is_unmapped_label_bucket or is_low_seq_numeric) and is_single_district_county:
                    allocs = county_allocs
                    source = "county_fallback" if allocs else "dropped"
                if (
                    not allocs
                    and (is_unmapped_label_bucket or is_low_seq_numeric)
                    and votes_total <= SMALL_UNMAPPED_ROW_VOTE_FALLBACK_MAX
                ):
                    allocs = county_allocs
                    source = "county_fallback" if allocs else "dropped"
                allow_county_fallback = not (
                    is_non_geo_bucket or is_unmapped_label_bucket or is_low_seq_numeric
                )
                if not allocs and allow_county_fallback:
                    allocs = county_allocs
                    source = "county_fallback" if allocs else "dropped"
                if not allocs:
                    if is_unmapped_label_bucket:
                        unresolved_unm_vote_by_bucket[
                            (scope, contest_type, int(year), county_norm, code_raw)
                        ] += votes_total
                    stat["dropped_rows"] += 1
                    stat["votes_dropped"] += votes_total
                    continue

                if source == "direct":
                    stat["direct_rows"] += 1
                    stat["votes_direct"] += votes_total
                elif source == "overlap":
                    stat["overlap_rows"] += 1
                    stat["votes_overlap"] += votes_total
                elif source == "non_geo_fallback":
                    stat["non_geo_rows"] += 1
                    stat["votes_non_geo"] += votes_total
                else:
                    stat["county_fallback_rows"] += 1
                    stat["votes_fallback"] += votes_total

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
        alloc = statewide_alloc_stats.get((scope, contest_type, year))
        coverage_pct = 100.0
        direct_row_pct = 0.0
        overlap_row_pct = 0.0
        non_geo_row_pct = 0.0
        county_fallback_row_pct = 0.0
        dropped_row_pct = 0.0
        direct_vote_pct = 0.0
        overlap_vote_pct = 0.0
        non_geo_vote_pct = 0.0
        county_fallback_vote_pct = 0.0
        dropped_vote_pct = 0.0
        if alloc:
            rows_total = float(alloc["rows"])
            votes_total = float(alloc["votes_total"])
            votes_alloc = float(
                alloc["votes_direct"]
                + alloc["votes_overlap"]
                + alloc["votes_non_geo"]
                + alloc["votes_fallback"]
            )
            if rows_total > 0:
                direct_row_pct = (float(alloc["direct_rows"]) / rows_total) * 100.0
                overlap_row_pct = (float(alloc["overlap_rows"]) / rows_total) * 100.0
                non_geo_row_pct = (float(alloc["non_geo_rows"]) / rows_total) * 100.0
                county_fallback_row_pct = (
                    float(alloc["county_fallback_rows"]) / rows_total
                ) * 100.0
                dropped_row_pct = (float(alloc["dropped_rows"]) / rows_total) * 100.0
            if votes_total > 0:
                coverage_pct = (votes_alloc / votes_total) * 100.0
                direct_vote_pct = (float(alloc["votes_direct"]) / votes_total) * 100.0
                overlap_vote_pct = (float(alloc["votes_overlap"]) / votes_total) * 100.0
                non_geo_vote_pct = (float(alloc["votes_non_geo"]) / votes_total) * 100.0
                county_fallback_vote_pct = (
                    float(alloc["votes_fallback"]) / votes_total
                ) * 100.0
                dropped_vote_pct = (float(alloc["votes_dropped"]) / votes_total) * 100.0

        payload = {
            "scope": scope,
            "contest_type": contest_type,
            "year": year,
            "meta": {
                "source": "tn_precinct_csv_district_aggregation",
                "match_coverage_pct": round(coverage_pct, 4),
                "direct_precinct_row_pct": round(direct_row_pct, 4),
                "overlap_precinct_row_pct": round(overlap_row_pct, 4),
                "non_geo_row_pct": round(non_geo_row_pct, 4),
                "county_fallback_row_pct": round(county_fallback_row_pct, 4),
                "dropped_row_pct": round(dropped_row_pct, 4),
                "direct_precinct_vote_pct": round(direct_vote_pct, 4),
                "overlap_precinct_vote_pct": round(overlap_vote_pct, 4),
                "non_geo_vote_pct": round(non_geo_vote_pct, 4),
                "county_fallback_vote_pct": round(county_fallback_vote_pct, 4),
                "dropped_vote_pct": round(dropped_vote_pct, 4),
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

    unm_rows = [
        {
            "scope": scope,
            "contest_type": contest_type,
            "year": year,
            "county_norm": county_norm,
            "code": code,
            "dropped_votes": round(votes, 3),
        }
        for (scope, contest_type, year, county_norm, code), votes in sorted(
            unresolved_unm_vote_by_bucket.items(),
            key=lambda kv: kv[1],
            reverse=True,
        )
    ]
    write_json(
        DISTRICT_DIR / "unresolved_unm_buckets.json",
        {
            "generated_by": "build_tn_contests.py",
            "rows": unm_rows,
            "count": len(unm_rows),
        },
    )

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
