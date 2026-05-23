#!/usr/bin/env python3
"""Build DRA-style precinct->VTD20 crosswalks from source CSV + block overlap files.

This script is intentionally "phase 1":
- Input: one source precinct CSV (OpenElections/TN format) and a year.
- Uses block-derived VTD overlap CSVs (00->20 or 10->20) to transfer weights.
- Output: weighted mapping from source precinct labels to 2020 VTD codes.

Artifacts:
- Data/crosswalks/tn_precinct_to_vtd20_blockweighted_<year>.csv
- Data/crosswalks/tn_precinct_to_vtd20_blockweighted_<year>_unmatched.csv
- Data/crosswalks/tn_precinct_to_vtd20_blockweighted_<year>_summary.json
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"


HIGH_CONFIDENCE_METHODS = {
    "exact_name",
    "prefix_name",
    "code_token_name",
    "token_vtd",
    "shelby_alias_name",
    "alpha_code_name",
}
MEDIUM_CONFIDENCE_METHODS = {
    "simple_exact_name",
    "compact_exact_name",
    "tail_exact_name",
}
LOW_CONFIDENCE_METHODS = {
    "tail_fuzzy_name",
    "fuzzy_name",
    "forced_best_name",
}


def norm_text(value: str) -> str:
    s = (value or "").strip().upper()
    s = re.sub(r"[\u2018\u2019]", "'", s)
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm_county(value: str) -> str:
    s = norm_text(value)
    s = re.sub(r"\s+COUNTY$", "", s).strip()
    return s


def parse_year_from_path(path: Path) -> int:
    m = re.match(r"^(\d{4})", path.name)
    if not m:
        raise ValueError(f"Could not parse year from filename: {path.name}")
    return int(m.group(1))


def pick_source_vintage(year: int) -> int:
    if year <= 2009:
        return 2000
    if year <= 2019:
        return 2010
    return 2020


def source_overlap_path(vintage: int) -> Path:
    if vintage == 2000:
        return XWALK_DIR / "tn_vtd00_to_vtd20_overlap.csv"
    if vintage == 2010:
        return XWALK_DIR / "tn_vtd10_to_vtd20_overlap.csv"
    if vintage == 2020:
        # 2020+ sources are best matched using the curated precinct->2024 catalog.
        return XWALK_DIR / "tn_precinct_to_2024.csv"
    raise ValueError(f"Unsupported vintage: {vintage}")


def county_name_by_fips() -> Dict[str, str]:
    out: Dict[str, str] = {}
    path = DATA_DIR / "tl_2020_47_county20.geojson"
    if not path.exists():
        return out
    payload = json.loads(path.read_text(encoding="utf-8"))
    for feat in payload.get("features", []):
        props = feat.get("properties", {}) or {}
        county_fp = str(props.get("COUNTYFP20", "")).zfill(3)
        county_name = (props.get("NAME20", "") or "").strip()
        if county_fp and county_name and county_fp not in out:
            out[county_fp] = norm_county(county_name)
    return out


@dataclass(frozen=True)
class SourcePrecinctKey:
    county_norm: str
    precinct_norm: str


def read_rows(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def collect_source_precincts(source_csv: Path) -> Dict[SourcePrecinctKey, dict]:
    out: Dict[SourcePrecinctKey, dict] = {}
    for row in read_rows(source_csv):
        county = (row.get("county") or row.get("COUNTY") or "").strip()
        precinct = (row.get("precinct") or row.get("PRECINCT") or "").strip()
        if not county or not precinct:
            continue
        pnorm = norm_text(precinct)
        if is_non_geographic_label(pnorm):
            continue
        key = SourcePrecinctKey(norm_county(county), pnorm)
        if key not in out:
            out[key] = {
                "county_raw": county,
                "precinct_raw": precinct,
                "rows": 0,
            }
        out[key]["rows"] += 1
    return out


def is_non_geographic_label(pnorm: str) -> bool:
    if not pnorm:
        return True
    tokens = set(pnorm.split())
    blocked = {
        "ABSENTEE",
        "EARLY",
        "PROVISIONAL",
        "TOTAL",
        "COUNTY",
        "WIDE",
    }
    if tokens.intersection(blocked):
        return True
    if pnorm.startswith("NG ") or pnorm.startswith("UNM "):
        return True
    return False


def load_overlap_catalog(path: Path) -> Tuple[Dict[Tuple[str, str], List[dict]], Dict[Tuple[str, str], Dict[str, float]]]:
    """Return:
    1) source catalog by (county_norm, src_vtdst): source name candidates
    2) transfer map by (county_norm, src_vtdst): dst_vtdst -> summed src_weight
    """
    if not path.exists():
        raise FileNotFoundError(f"Missing overlap file: {path}")

    county_norm_from_fips = county_name_by_fips()
    source_catalog: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    transfers: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for row in read_rows(path):
        county_fp = str(row.get("src_countyfp", "")).zfill(3)
        county_norm = county_norm_from_fips.get(county_fp, "")
        if not county_norm:
            continue
        src_vtd = str(row.get("src_vtdst", "")).strip()
        dst_vtd = str(row.get("dst_vtdst", "")).strip()
        if not src_vtd or not dst_vtd:
            continue
        src_name = (row.get("src_name", "") or "").strip()
        src_name_norm = norm_text(src_name)
        source_catalog[(county_norm, src_vtd)].append(
            {
                "src_name": src_name,
                "src_name_norm": src_name_norm,
            }
        )
        try:
            src_weight = float(row.get("src_weight") or 0.0)
        except ValueError:
            src_weight = 0.0
        if src_weight > 0:
            transfers[(county_norm, src_vtd)][dst_vtd] += src_weight

    return source_catalog, transfers


def load_2024_precinct_catalog(path: Path) -> Tuple[Dict[Tuple[str, str], List[dict]], Dict[Tuple[str, str], Dict[str, float]]]:
    """Build an identity transfer catalog from curated precinct->2024 mappings.

    The `to_prctseq_2024` code aligns with the 2020-era precinct code used by
    downstream district joins, so we treat it as both src and dst VTD key.
    """
    if not path.exists():
        raise FileNotFoundError(f"Missing 2024 precinct catalog file: {path}")

    source_catalog: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    transfers: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for row in read_rows(path):
        county_norm = norm_county(str(row.get("county_norm", "")).strip())
        src_name_norm = norm_text(str(row.get("from_precinct_norm", "")).strip())
        src_vtd = str(row.get("to_prctseq_2024", "")).strip()
        if not county_norm or not src_name_norm or not src_vtd:
            continue
        source_catalog[(county_norm, src_vtd)].append(
            {
                "src_name": src_name_norm,
                "src_name_norm": src_name_norm,
            }
        )
        transfers[(county_norm, src_vtd)][src_vtd] += 1.0

    return source_catalog, transfers


def merge_catalogs(
    catalogs: List[Tuple[Dict[Tuple[str, str], List[dict]], Dict[Tuple[str, str], Dict[str, float]]]]
) -> Tuple[Dict[Tuple[str, str], List[dict]], Dict[Tuple[str, str], Dict[str, float]]]:
    merged_catalog: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    merged_transfers: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(lambda: defaultdict(float))

    for source_catalog, transfers in catalogs:
        for key, metas in source_catalog.items():
            merged_catalog[key].extend(metas)
        for key, dst_map in transfers.items():
            for dst_vtd, w in dst_map.items():
                merged_transfers[key][dst_vtd] += float(w or 0.0)

    return merged_catalog, merged_transfers


def code_token(value: str) -> str:
    s = norm_text(value)
    if not s:
        return ""
    tok = s.split(" ", 1)[0]
    tok = re.sub(r"[^A-Z0-9]", "", tok)
    return tok


def simplify_precinct_name(value: str) -> str:
    s = norm_text(value)
    if not s:
        return ""
    s = re.sub(r"\bSCH\b", "SCHOOL", s)
    s = re.sub(r"\bHGHTS\b", "HEIGHTS", s)
    s = re.sub(r"\bCTR\b", "CENTER", s)
    s = re.sub(r"\bAUD\b", "AUDITORIUM", s)
    s = re.sub(r"\bMID\b", "MIDDLE", s)
    s = re.sub(r"\bPRIM\b", "PRIMARY", s)
    s = re.sub(r"\bELEM\b", "ELEMENTARY", s)
    s = re.sub(r"\b(\d{1,3})([A-Z]{1,4})\b", r"\1 \2", s)
    s = re.sub(
        r"\b(PRECINCT|PCT|DISTRICT|DIST|WARD|VTD|BOX|VOTING|CENTER|CENTRE|CITY|COUNTY)\b",
        " ",
        s,
    )
    s = re.sub(r"\s+", " ", s).strip()
    return s


def strip_leading_locator_tokens(value: str) -> str:
    s = norm_text(value)
    if not s:
        return ""
    s = re.sub(r"\b(\d{1,3})([A-Z]{1,4})\b", r"\1 \2", s)
    for _ in range(3):
        n = re.sub(
            r"^\s*(?:\d{1,3}[A-Z]?)(?:\s*[- ]\s*\d{1,3}[A-Z]?)?\s+",
            "",
            s,
            count=1,
        )
        if n == s:
            break
        s = n
    return simplify_precinct_name(s)


def compact_match_key(value: str) -> str:
    s = simplify_precinct_name(value)
    return re.sub(r"[^A-Z0-9]", "", s)


def leading_alpha_code(value: str) -> str:
    s = norm_text(value)
    if not s:
        return ""
    m = re.match(r"^\s*(\d{1,3})\s*([A-Z]{1,4})\b", s)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    m2 = re.match(r"^\s*(\d{1,3}[A-Z]{1,4})\b", s)
    if m2:
        return m2.group(1)
    return ""


def shelby_aliases(precinct_norm: str) -> List[str]:
    """Generate deterministic Shelby-specific label aliases."""
    out: List[str] = []
    s = norm_text(precinct_norm)
    if not s:
        return out
    m = re.match(r"^(\d{3})$", s)
    if m:
        out.append(f"MEMPHIS {int(m.group(1))}")
    m = re.match(r"^(\d{3})\s+(\d{1,2})$", s)
    if m:
        out.append(f"MEMPHIS {int(m.group(1))}-{int(m.group(2))}")
    m = re.match(r"^ROSS\s+(\d{1,2})$", s)
    if m:
        out.append(f"ROSS STORE {int(m.group(1)):02d}")
    m = re.match(r"^LAKELAND\s+\d{1,2}$", s)
    if m:
        out.append("LAKELAND")
    m = re.match(r"^ARLINGTON\s+\d{1,2}$", s)
    if m:
        out.append("ARLINGTON")
    m = re.match(r"^LUCY\s+(\d{1,2})$", s)
    if m:
        out.append(f"LUCY {int(m.group(1))}")
    # Excel date-serial artifact seen in 2022 Shelby source.
    m = re.match(r"^(\d{5})$", s)
    if m:
        serial = int(m.group(1))
        if 43000 <= serial <= 50000:
            dt = datetime(1899, 12, 30) + timedelta(days=serial)
            mm = f"{dt.month:02d}"
            dd = f"{dt.day:02d}"
            out.append(f"{mm}-{dd}")
            out.append(f"{dd}-{mm}")
            out.append(f"{int(mm)}-{int(dd)}")
            out.append(f"{int(dd)}-{int(mm)}")

    dedup: List[str] = []
    seen = set()
    for v in out:
        n = norm_text(v)
        if n and n not in seen:
            seen.add(n)
            dedup.append(n)
    return dedup


def normalize_numeric_token(token: str) -> str:
    t = re.sub(r"[^A-Z0-9]", "", token.upper())
    if not t:
        return ""
    m = re.match(r"^0*(\d+)([A-Z]?)$", t)
    if m:
        return f"{int(m.group(1))}{m.group(2)}"
    return t


def leading_code_tokens(value: str) -> List[str]:
    s = (value or "").strip().upper()
    if not s:
        return []
    s = s.replace("_", " ")
    out: List[str] = []
    # examples: 25-3, 01-2, 2C, 10T, 001-006
    m = re.match(r"^\s*([0-9]{1,3}[A-Z]?)\s*[- ]\s*([0-9]{1,3}[A-Z]?)\b", s)
    if m:
        out.append(f"{normalize_numeric_token(m.group(1))}-{normalize_numeric_token(m.group(2))}")
    m2 = re.match(r"^\s*([0-9]{1,3}[A-Z]?)\b", s)
    if m2:
        out.append(normalize_numeric_token(m2.group(1)))
    m3 = re.match(r"^\s*([A-Z]+)\s+([0-9]{1,3}[A-Z]?)\b", s)
    if m3:
        out.append(f"{m3.group(1)} {normalize_numeric_token(m3.group(2))}")
    # unique, preserve order
    dedup = []
    seen = set()
    for tok in out:
        if tok and tok not in seen:
            seen.add(tok)
            dedup.append(tok)
    return dedup


def match_source_vtd(
    county_norm: str,
    precinct_norm: str,
    source_catalog: Dict[Tuple[str, str], List[dict]],
) -> Tuple[str, str, float]:
    """Return (src_vtdst, method, score)."""
    candidates = [
        (src_vtd, meta)
        for (c_norm, src_vtd), metas in source_catalog.items()
        if c_norm == county_norm
        for meta in metas
    ]
    if not candidates:
        return "", "no_county_catalog", 0.0

    # 1) Exact normalized-name match against any source VTD name variant.
    by_vtd_name_norms: Dict[str, set] = defaultdict(set)
    for src_vtd, meta in candidates:
        if meta["src_name_norm"]:
            by_vtd_name_norms[src_vtd].add(meta["src_name_norm"])
    for src_vtd, names in by_vtd_name_norms.items():
        if precinct_norm in names:
            return src_vtd, "exact_name", 1.0

    # 1a) Shelby-specific deterministic aliases for legacy coded labels.
    if county_norm == "SHELBY":
        aliases = shelby_aliases(precinct_norm)
        for alias in aliases:
            for src_vtd, names in by_vtd_name_norms.items():
                if alias in names:
                    return src_vtd, "shelby_alias_name", 0.997

    # 1b) Prefix name match: source labels often look like "10-6 Gateway School"
    # while election CSVs can carry only "10-6".
    for src_vtd, names in by_vtd_name_norms.items():
        for cand in names:
            if cand == precinct_norm or cand.startswith(f"{precinct_norm} "):
                return src_vtd, "prefix_name", 0.995

    # 1c) Leading code-token match (robust to zero-padding and minor formatting).
    q_codes = leading_code_tokens(precinct_norm)
    if q_codes:
        for src_vtd, names in by_vtd_name_norms.items():
            for cand in names:
                c_codes = leading_code_tokens(cand)
                if any(q == c for q in q_codes for c in c_codes):
                    return src_vtd, "code_token_name", 0.993

    # 2) Leading token/code match, e.g., "0008", "8A", etc.
    qtok = code_token(precinct_norm)
    if qtok:
        for src_vtd in by_vtd_name_norms.keys():
            vtok = re.sub(r"[^A-Z0-9]", "", src_vtd.upper())
            if qtok == vtok or qtok.lstrip("0") == vtok.lstrip("0"):
                return src_vtd, "token_vtd", 1.0

    # 2a) Leading numeric+alpha code match (e.g., 14CH, 11S).
    q_alpha = leading_alpha_code(precinct_norm)
    if q_alpha:
        alpha_hits: List[Tuple[str, float]] = []
        for src_vtd, names in by_vtd_name_norms.items():
            for cand in names:
                c_alpha = leading_alpha_code(cand)
                if c_alpha and c_alpha == q_alpha:
                    c_simple = simplify_precinct_name(cand)
                    q_simple_for_score = simplify_precinct_name(precinct_norm)
                    score = SequenceMatcher(None, q_simple_for_score or precinct_norm, c_simple or cand).ratio()
                    alpha_hits.append((src_vtd, score))
                    break
        if len(alpha_hits) == 1:
            return alpha_hits[0][0], "alpha_code_name", round(max(alpha_hits[0][1], 0.992), 6)
        if len(alpha_hits) > 1:
            alpha_hits.sort(key=lambda x: x[1], reverse=True)
            if alpha_hits[0][1] >= 0.72:
                return alpha_hits[0][0], "alpha_code_name", round(alpha_hits[0][1], 6)

    # 2b) Simplified exact-name match after removing common words.
    q_simple = simplify_precinct_name(precinct_norm)
    if q_simple:
        for src_vtd, names in by_vtd_name_norms.items():
            for cand in names:
                if q_simple and q_simple == simplify_precinct_name(cand):
                    return src_vtd, "simple_exact_name", 0.99

    # 2bb) Compact exact match (ignore spacing/punctuation differences).
    q_compact = compact_match_key(precinct_norm)
    if q_compact:
        for src_vtd, names in by_vtd_name_norms.items():
            for cand in names:
                if q_compact == compact_match_key(cand):
                    return src_vtd, "compact_exact_name", 0.988

    # 2c) Exact tail-name match after removing leading locator/code tokens.
    q_tail = strip_leading_locator_tokens(precinct_norm)
    if q_tail:
        for src_vtd, names in by_vtd_name_norms.items():
            for cand in names:
                if q_tail and q_tail == strip_leading_locator_tokens(cand):
                    return src_vtd, "tail_exact_name", 0.985

    # 2d) Tail-name fuzzy match. Helps labels like "167 COLLEGEDALE 3" where
    # code prefixes vary but the precinct name body still matches strongly.
    if q_tail and len(q_tail) >= 6:
        tail_best_vtd = ""
        tail_best_score = 0.0
        for src_vtd, names in by_vtd_name_norms.items():
            for cand in names:
                c_tail = strip_leading_locator_tokens(cand)
                if not c_tail:
                    continue
                score = SequenceMatcher(None, q_tail, c_tail).ratio()
                if score > tail_best_score:
                    tail_best_score = score
                    tail_best_vtd = src_vtd
        if tail_best_vtd and tail_best_score >= 0.74:
            return tail_best_vtd, "tail_fuzzy_name", round(tail_best_score, 6)

    # 3) Fuzzy best name, conservative threshold.
    best_vtd = ""
    best_score = 0.0
    for src_vtd, names in by_vtd_name_norms.items():
        for cand in names:
            score = SequenceMatcher(None, q_simple or precinct_norm, simplify_precinct_name(cand) or cand).ratio()
            if score > best_score:
                best_score = score
                best_vtd = src_vtd
    if best_vtd and best_score >= 0.86:
        return best_vtd, "fuzzy_name", round(best_score, 6)

    # Coverage-max fallback: keep every row mapped when a county catalog exists.
    if best_vtd:
        return best_vtd, "forced_best_name", round(best_score, 6)

    return "", "unmatched", round(best_score, 6)


def build_crosswalk(source_csv: Path, year: int) -> dict:
    vintage = pick_source_vintage(year)
    overlap_csv = source_overlap_path(vintage)
    cat_2024 = load_2024_precinct_catalog(XWALK_DIR / "tn_precinct_to_2024.csv")
    if vintage == 2020:
        source_catalog, transfers = merge_catalogs(
            [
                cat_2024,
                load_overlap_catalog(XWALK_DIR / "tn_vtd10_to_vtd20_overlap.csv"),
            ]
        )
    else:
        source_catalog, transfers = merge_catalogs(
            [
                load_overlap_catalog(overlap_csv),
                cat_2024,
            ]
        )
    src_precincts = collect_source_precincts(source_csv)

    out_rows: List[dict] = []
    low_confidence_rows: List[dict] = []
    unmatched_rows: List[dict] = []
    method_counts: Dict[str, int] = defaultdict(int)

    for key, meta in sorted(src_precincts.items(), key=lambda kv: (kv[0].county_norm, kv[0].precinct_norm)):
        src_vtd, method, score = match_source_vtd(key.county_norm, key.precinct_norm, source_catalog)
        method_counts[method] += 1
        if method in HIGH_CONFIDENCE_METHODS:
            confidence_tier = "high"
        elif method in MEDIUM_CONFIDENCE_METHODS:
            confidence_tier = "medium"
        elif method in LOW_CONFIDENCE_METHODS:
            confidence_tier = "low"
        else:
            confidence_tier = "unknown"
        if not src_vtd:
            unmatched_rows.append(
                {
                    "year": year,
                    "county_norm": key.county_norm,
                    "precinct_norm": key.precinct_norm,
                    "county_raw": meta["county_raw"],
                    "precinct_raw": meta["precinct_raw"],
                    "match_method": method,
                    "confidence_tier": confidence_tier,
                    "match_score": score,
                    "source_rows": meta["rows"],
                }
            )
            continue

        dst_map = transfers.get((key.county_norm, src_vtd), {})
        total = sum(dst_map.values())
        if total <= 0:
            unmatched_rows.append(
                {
                    "year": year,
                    "county_norm": key.county_norm,
                    "precinct_norm": key.precinct_norm,
                    "county_raw": meta["county_raw"],
                    "precinct_raw": meta["precinct_raw"],
                    "match_method": "matched_no_transfer",
                    "confidence_tier": confidence_tier,
                    "match_score": score,
                    "source_rows": meta["rows"],
                }
            )
            continue

        for dst_vtd, w in sorted(dst_map.items(), key=lambda it: it[1], reverse=True):
            weight = float(w) / total
            if weight <= 0:
                continue
            out_rows.append(
                {
                    "from_year": year,
                    "source_vintage": vintage,
                    "county_norm": key.county_norm,
                    "from_precinct_norm": key.precinct_norm,
                    "src_vtdst": src_vtd,
                    "dst_vtd20": dst_vtd,
                    "weight": round(weight, 8),
                    "match_method": method,
                    "confidence_tier": confidence_tier,
                    "match_score": score,
                }
            )
            if confidence_tier == "low":
                low_confidence_rows.append(
                    {
                        "from_year": year,
                        "source_vintage": vintage,
                        "county_norm": key.county_norm,
                        "from_precinct_norm": key.precinct_norm,
                        "src_vtdst": src_vtd,
                        "dst_vtd20": dst_vtd,
                        "weight": round(weight, 8),
                        "match_method": method,
                        "confidence_tier": confidence_tier,
                        "match_score": score,
                    }
                )

    return {
        "rows": out_rows,
        "low_confidence_rows": low_confidence_rows,
        "unmatched": unmatched_rows,
        "method_counts": dict(sorted(method_counts.items())),
        "source_csv": source_csv.name,
        "source_year": year,
        "source_vintage": vintage,
        "overlap_csv": overlap_csv.name,
        "input_precinct_keys": len(src_precincts),
    }


def write_csv(path: Path, fieldnames: List[str], rows: Iterable[dict]) -> int:
    count = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-csv", required=True, help="Path to source precinct CSV")
    parser.add_argument("--year", type=int, help="Election year (defaults from source filename)")
    args = parser.parse_args()

    source_csv = Path(args.source_csv)
    if not source_csv.is_absolute():
        source_csv = ROOT / source_csv
    if not source_csv.exists():
        raise FileNotFoundError(f"Missing source CSV: {source_csv}")

    year = int(args.year) if args.year else parse_year_from_path(source_csv)
    payload = build_crosswalk(source_csv, year)

    XWALK_DIR.mkdir(parents=True, exist_ok=True)
    stem = f"tn_precinct_to_vtd20_blockweighted_{year}"
    out_csv = XWALK_DIR / f"{stem}.csv"
    out_strict_csv = XWALK_DIR / f"{stem}_strict.csv"
    out_low_confidence_csv = XWALK_DIR / f"{stem}_low_confidence.csv"
    out_unmatched = XWALK_DIR / f"{stem}_unmatched.csv"
    out_summary = XWALK_DIR / f"{stem}_summary.json"

    strict_methods = set(HIGH_CONFIDENCE_METHODS)
    strict_rows = [r for r in payload["rows"] if r.get("match_method") in strict_methods]

    n_rows = write_csv(
        out_csv,
        [
            "from_year",
            "source_vintage",
            "county_norm",
            "from_precinct_norm",
            "src_vtdst",
            "dst_vtd20",
            "weight",
            "match_method",
            "confidence_tier",
            "match_score",
        ],
        payload["rows"],
    )
    n_strict_rows = write_csv(
        out_strict_csv,
        [
            "from_year",
            "source_vintage",
            "county_norm",
            "from_precinct_norm",
            "src_vtdst",
            "dst_vtd20",
            "weight",
            "match_method",
            "confidence_tier",
            "match_score",
        ],
        strict_rows,
    )
    n_low_confidence_rows = write_csv(
        out_low_confidence_csv,
        [
            "from_year",
            "source_vintage",
            "county_norm",
            "from_precinct_norm",
            "src_vtdst",
            "dst_vtd20",
            "weight",
            "match_method",
            "confidence_tier",
            "match_score",
        ],
        payload["low_confidence_rows"],
    )
    n_unmatched = write_csv(
        out_unmatched,
        [
            "year",
            "county_norm",
            "precinct_norm",
            "county_raw",
            "precinct_raw",
            "match_method",
            "confidence_tier",
            "match_score",
            "source_rows",
        ],
        payload["unmatched"],
    )

    summary = {
        "source_csv": payload["source_csv"],
        "source_year": payload["source_year"],
        "source_vintage": payload["source_vintage"],
        "overlap_csv": payload["overlap_csv"],
        "input_precinct_keys": payload["input_precinct_keys"],
        "crosswalk_rows": n_rows,
        "strict_crosswalk_rows": n_strict_rows,
        "low_confidence_crosswalk_rows": n_low_confidence_rows,
        "unmatched_rows": n_unmatched,
        "method_counts": payload["method_counts"],
        "outputs": {
            "crosswalk_csv": out_csv.name,
            "strict_crosswalk_csv": out_strict_csv.name,
            "low_confidence_crosswalk_csv": out_low_confidence_csv.name,
            "unmatched_csv": out_unmatched.name,
            "summary_json": out_summary.name,
        },
    }
    out_summary.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
