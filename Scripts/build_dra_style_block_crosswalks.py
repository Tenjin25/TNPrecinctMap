#!/usr/bin/env python3
"""Build DRA-style precinct->VTD20 crosswalks from source CSV + vintage transfers.

Historical years (≤2019) match only against vintage VTD00/VTD10 catalogs and
transfer via the full block chain (preferred) or overlap CSV. The modern 2024
precinct catalog is intentionally not merged into historical matching.

Modern years (2020+) still use the curated precinct->2024 catalog plus Census
VTD20 name groups.

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
from typing import Dict, Iterable, List, Optional, Tuple


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"
CENSUS_VTD20_GEOJSON = DATA_DIR / "tn_vtd_2020.geojson"
DRA_VTD20_CATALOG_CSV = XWALK_DIR / "tn_dra2020_vtd20_catalog.csv"
PRCTSEQ_TO_VTD20_OVERRIDES_CSV = XWALK_DIR / "tn_prctseq_to_vtd20_overrides.csv"
SOURCE_2024_PRECINCT_CSV = DATA_DIR / "20241105__tn__general__precinct.csv"


HIGH_CONFIDENCE_METHODS = {
    "exact_name",
    "manual_override",
    "prctseq_area_overlay",
    "prefix_name",
    "code_token_name",
    "shelby_alias_name",
    "knox_alias_name",
    "greene_alias_name",
    "alpha_code_name",
}
MEDIUM_CONFIDENCE_METHODS = {
    "token_vtd",
    "simple_exact_name",
    "compact_exact_name",
    "tail_exact_name",
    "core_exact_name",
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


def source_tag_from_path(path: Path) -> str:
    stem = path.stem
    stem = re.sub(r"[^A-Za-z0-9]+", "_", stem).strip("_").lower()
    return stem


def pick_source_vintage(year: int) -> int:
    if year <= 2009:
        return 2000
    if year <= 2019:
        return 2010
    return 2020


def source_overlap_path(vintage: int) -> Path:
    """Return preferred vintage transfer CSV (block chain first, then overlap)."""
    if vintage == 2000:
        chain = XWALK_DIR / "tn_vtd00_to_vtd20_block_chain.csv"
        if chain.exists():
            return chain
        return XWALK_DIR / "tn_vtd00_to_vtd20_overlap.csv"
    if vintage == 2010:
        chain = XWALK_DIR / "tn_vtd10_to_vtd20_block_chain.csv"
        if chain.exists():
            return chain
        return XWALK_DIR / "tn_vtd10_to_vtd20_overlap.csv"
    if vintage == 2020:
        # 2020+ sources are best matched using the curated precinct->2024 catalog.
        return XWALK_DIR / "tn_precinct_to_2024.csv"
    raise ValueError(f"Unsupported vintage: {vintage}")


def lookup_transfer_map(
    transfers: Dict[Tuple[str, str], Dict[str, float]],
    county_norm: str,
    src_vtd: str,
) -> Dict[str, float]:
    src = str(src_vtd or "").strip()
    candidates = [src]
    if src.isdigit():
        candidates.extend([src.zfill(4), src.zfill(6), str(int(src))])
    for key in candidates:
        hit = transfers.get((county_norm, key))
        if hit:
            return dict(hit)
    return {}


def token_vtd_name_agrees(body: str, names: Iterable[str]) -> bool:
    """True when a non-empty precinct name body agrees with a vintage VTD name."""
    if not body:
        return True
    body_simple = simplify_precinct_name(body) or body
    for name in names:
        cand = simplify_precinct_name(name) or name
        if not cand:
            continue
        if body_simple == cand or body_simple in cand or cand in body_simple:
            return True
        if SequenceMatcher(None, body_simple, cand).ratio() >= 0.72:
            return True
    return False


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
        pnorm = canonical_precinct_norm(precinct)
        if is_non_geographic_label(pnorm):
            continue
        key = SourcePrecinctKey(norm_county(county), pnorm)
        seq_raw = str(row.get("PRCTSEQ") or row.get("prctseq") or "").strip()
        prctseq = str(int(seq_raw)) if seq_raw.isdigit() else ""
        if key not in out:
            out[key] = {
                "county_raw": county,
                "precinct_raw": precinct,
                "prctseq": prctseq,
                "rows": 0,
            }
        elif prctseq and not out[key].get("prctseq"):
            out[key]["prctseq"] = prctseq
        out[key]["rows"] += 1
    return out


def resolve_prctseq_area_override(
    county_norm: str,
    prctseq: str,
    prctseq_overrides: Dict[Tuple[str, str], Dict[str, float]],
) -> Dict[str, float]:
    """Return boom/NYT PRCTSEQ→VTD20 weights when present."""
    if not prctseq or not prctseq.isdigit():
        return {}
    seq_int = str(int(prctseq))
    for key in (seq_int.zfill(4), seq_int.zfill(6), seq_int):
        hits = prctseq_overrides.get((county_norm, key), {})
        if hits:
            return {str(k): float(v) for k, v in hits.items() if float(v) > 0}
    return {}


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


def load_official_vtd20_by_county() -> Dict[str, set]:
    """county_norm -> set of official 6-digit Census VTDST20 codes."""
    out: Dict[str, set] = defaultdict(set)
    if not CENSUS_VTD20_GEOJSON.exists():
        return out
    county_norm_from_fips = county_name_by_fips()
    payload = json.loads(CENSUS_VTD20_GEOJSON.read_text(encoding="utf-8"))
    for feat in payload.get("features", []):
        props = feat.get("properties", {}) or {}
        county_fp = str(props.get("COUNTYFP20", "")).zfill(3)
        county_norm = county_norm_from_fips.get(county_fp, "")
        vtd_code = str(props.get("VTDST20", "")).strip().zfill(6)
        if county_norm and vtd_code.isdigit():
            out[county_norm].add(vtd_code)
    return out


def load_census_vtd20_leading_code_lookup() -> Dict[Tuple[str, str], Dict[str, float]]:
    """Map (county, leading NAME20 code token) -> official VTDST20 weights.

    Example: Bedford NAME20 '101 Wartrace ...' yields token '101' -> 000101.
    """
    out: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    if not CENSUS_VTD20_GEOJSON.exists():
        return out
    county_norm_from_fips = county_name_by_fips()
    payload = json.loads(CENSUS_VTD20_GEOJSON.read_text(encoding="utf-8"))
    for feat in payload.get("features", []):
        props = feat.get("properties", {}) or {}
        county_fp = str(props.get("COUNTYFP20", "")).zfill(3)
        county_norm = county_norm_from_fips.get(county_fp, "")
        vtd_code = str(props.get("VTDST20", "")).strip().zfill(6)
        name20 = norm_text(str(props.get("NAME20", "")).strip())
        if not (county_norm and vtd_code.isdigit() and name20):
            continue
        try:
            area = float(props.get("ALAND20") or 0.0) + float(props.get("AWATER20") or 0.0)
        except (TypeError, ValueError):
            area = 0.0
        weight = area if area > 0 else 1.0
        tokens = set(leading_code_tokens(name20))
        # After norm_text, Census "10-W" becomes "10 W" — also index compact/hyphen forms.
        spaced = re.match(r"^(\d{1,3})\s+([A-Z]{1,2})$", name20)
        if spaced:
            num = str(int(spaced.group(1)))
            suf = spaced.group(2)
            tokens.update({f"{num}-{suf}", f"{num}{suf}", f"{num} {suf}", num})
        bare_name = re.match(r"^(\d{1,3})$", name20)
        if bare_name:
            tokens.add(str(int(bare_name.group(1))))
        for tok in tokens:
            if tok:
                out[(county_norm, tok)][vtd_code] += weight
        # Also index bare integer form of the VTDST20 itself (e.g. 101 -> 000101).
        bare = str(int(vtd_code))
        out[(county_norm, bare)][vtd_code] += weight
        out[(county_norm, bare.zfill(4))][vtd_code] += weight
    return out


def _name_body_for_match(value: str) -> str:
    s = strip_leading_locator_tokens(value)
    s = simplify_precinct_name(s or value)
    return s


def resolve_name_to_official_vtd(
    county_norm: str,
    precinct_norm: str,
    exact_lookup: Dict[Tuple[str, str], Dict[str, float]],
    grouped_lookup: Dict[Tuple[str, str], Dict[str, float]],
    leading_lookup: Dict[Tuple[str, str], Dict[str, float]],
    official_by_county: Dict[str, set],
) -> Dict[str, float]:
    """Resolve a modern precinct label onto official Census VTDST20 codes only."""
    official = official_by_county.get(county_norm, set())
    if not official:
        return {}

    def filter_official(raw: Dict[str, float]) -> Dict[str, float]:
        clean = {zfill_maybe(k): float(v) for k, v in raw.items() if zfill_maybe(k) in official and float(v) > 0}
        total = sum(clean.values())
        if total <= 0:
            return {}
        return {k: v / total for k, v in clean.items()}

    exact = filter_official(exact_lookup.get((county_norm, precinct_norm), {}))
    if exact:
        return exact
    grouped = filter_official(grouped_lookup.get((county_norm, modern_group_key(precinct_norm)), {}))
    if grouped:
        return grouped

    # Rural dual-numeric labels ("2-1", "9 2"): bind via primary number, not subunit.
    dual_hits = resolve_dual_numeric_leading_hits(
        county_norm, precinct_norm, leading_lookup, filter_official
    )
    if dual_hits:
        return dual_hits

    # Leading code tokens from the election label (e.g. "101 WARTRACE", "10W HOWARD").
    # Prefer the most specific token with a unique official hit.
    for tok in leading_code_tokens(precinct_norm):
        hits = filter_official(leading_lookup.get((county_norm, tok), {}))
        if len(hits) == 1:
            return hits
    # Bare numeric parents (19, 68) may legitimately split across child VTDs.
    for tok in leading_code_tokens(precinct_norm):
        hits = filter_official(leading_lookup.get((county_norm, tok), {}))
        if hits and (len(hits) == 1 or re.fullmatch(r"\d{1,3}", tok)):
            return hits

    # Soft place-name match against Census NAME20 bodies (unique best only).
    body = _name_body_for_match(precinct_norm)
    if body and len(body) >= 4:
        scored: List[Tuple[float, str]] = []
        for name_key, dst_map in exact_lookup.items():
            if name_key[0] != county_norm:
                continue
            cand_body = _name_body_for_match(name_key[1])
            if not cand_body or len(cand_body) < 4:
                continue
            if body == cand_body or body in cand_body or cand_body in body:
                score = 1.0
            else:
                score = SequenceMatcher(None, body, cand_body).ratio()
            if score < 0.88:
                continue
            for dst, w in dst_map.items():
                if dst in official:
                    scored.append((score * float(w), dst))
        if scored:
            scored.sort(reverse=True)
            best_score = scored[0][0]
            best = {dst: score for score, dst in scored if score >= best_score * 0.98}
            if len(best) == 1:
                return {next(iter(best)): 1.0}

    return {}


def zfill_maybe(value: str, width: int = 6) -> str:
    s = str(value or "").strip()
    if s.isdigit():
        return s.zfill(width)
    return s


def build_prctseq_to_official_vtd_bridge(
    exact_lookup: Dict[Tuple[str, str], Dict[str, float]],
    grouped_lookup: Dict[Tuple[str, str], Dict[str, float]],
    leading_lookup: Dict[Tuple[str, str], Dict[str, float]],
    official_by_county: Dict[str, set],
) -> Dict[Tuple[str, str], Dict[str, float]]:
    """Build county-local PRCTSEQ -> official VTDST20 bridges.

    Uses 2024 precinct labels for name/code matches, then fills remaining
    PRCTSEQ values with injective offset matching against each county's
    official VTDST20 set (e.g. Bedford 1..11 -> 101..111 via +100).
    """
    out: Dict[Tuple[str, str], Dict[str, float]] = {}
    if not SOURCE_2024_PRECINCT_CSV.exists():
        return out

    prctseq_by_county: Dict[str, set] = defaultdict(set)
    label_by_key: Dict[Tuple[str, int], str] = {}
    for row in read_rows(SOURCE_2024_PRECINCT_CSV):
        county_norm = norm_county(str(row.get("COUNTY") or row.get("county") or "").strip())
        seq_raw = str(row.get("PRCTSEQ") or row.get("prctseq") or "").strip()
        precinct = norm_text(str(row.get("PRECINCT") or row.get("precinct") or "").strip())
        if not county_norm or not seq_raw.isdigit():
            continue
        seq_int = int(seq_raw)
        prctseq_by_county[county_norm].add(seq_int)
        if precinct and (county_norm, seq_int) not in label_by_key:
            label_by_key[(county_norm, seq_int)] = precinct

    # Pass 1: unique name/code resolutions from 2024 labels.
    exact_by_county: Dict[str, Dict[int, str]] = defaultdict(dict)
    for (county_norm, seq_int), precinct in label_by_key.items():
        resolved = resolve_name_to_official_vtd(
            county_norm,
            precinct,
            exact_lookup,
            grouped_lookup,
            leading_lookup,
            official_by_county,
        )
        if len(resolved) == 1:
            exact_by_county[county_norm][seq_int] = next(iter(resolved.keys()))

    # Pass 2: offset / injective fill for remaining PRCTSEQ values.
    for county_norm, pset in prctseq_by_county.items():
        official = official_by_county.get(county_norm, set())
        if not official:
            continue
        vset = {int(v) for v in official if str(v).isdigit()}
        if not vset:
            continue

        assigned: Dict[int, str] = dict(exact_by_county.get(county_norm, {}))
        used_vtds = {int(v) for v in assigned.values() if str(v).isdigit()}

        # Score additive offsets by PRCTSEQ hit count.
        pmin, pmax = min(pset), max(pset)
        vmin, vmax = min(vset), max(vset)
        scored: List[Tuple[int, float, int]] = []
        for k in range(max(-200, vmin - pmax), min(10000, vmax - pmin) + 1):
            hits = 0
            weight = 0.0
            for p in pset:
                if (p + k) in vset:
                    hits += 1
                    weight += 1.0 / (1.0 + float(p))
            if hits <= 0:
                continue
            # Bonus when exact name matches imply this offset.
            bonus = sum(
                1
                for p, v in assigned.items()
                if str(v).isdigit() and int(v) - int(p) == k
            )
            if bonus:
                hits += min(25, bonus)
                weight += 0.5 * float(bonus)
            scored.append((hits, weight, k))
        scored.sort(reverse=True)

        unmatched = sorted(p for p in pset if p not in assigned)
        for _hits, _weight, k in scored[:40]:
            if not unmatched:
                break
            still = []
            for p in unmatched:
                cand = p + int(k)
                if cand in vset and cand not in used_vtds:
                    code = str(cand).zfill(6)
                    assigned[p] = code
                    used_vtds.add(cand)
                else:
                    still.append(p)
            unmatched = still

        # Identity for any remaining codes that are already official VTDST20s.
        for p in list(unmatched):
            if p in vset and p not in used_vtds:
                assigned[p] = str(p).zfill(6)
                used_vtds.add(p)

        for seq_int, vtd in assigned.items():
            out[(county_norm, str(seq_int).zfill(4))] = {vtd: 1.0}
            out[(county_norm, str(seq_int).zfill(6))] = {vtd: 1.0}

    return out


def load_2024_precinct_catalog(path: Path) -> Tuple[Dict[Tuple[str, str], List[dict]], Dict[Tuple[str, str], Dict[str, float]]]:
    """Build a modern-name catalog retargeted into official Census/current VTD20 codes."""
    if not path.exists():
        raise FileNotFoundError(f"Missing 2024 precinct catalog file: {path}")

    source_catalog: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    transfers: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    exact_lookup, grouped_lookup = load_census_vtd20_name_lookup()
    leading_lookup = load_census_vtd20_leading_code_lookup()
    official_by_county = load_official_vtd20_by_county()
    dra_code_lookup = load_dra_vtd20_code_lookup()
    prctseq_override_lookup = load_prctseq_to_vtd20_override_lookup()
    prctseq_bridge = build_prctseq_to_official_vtd_bridge(
        exact_lookup,
        grouped_lookup,
        leading_lookup,
        official_by_county,
    )

    def resolved_dst_map(county_norm: str, precinct_norm: str, fallback_code: str) -> Dict[str, float]:
        official = official_by_county.get(county_norm, set())

        def filter_official(raw: Dict[str, float]) -> Dict[str, float]:
            clean = {
                zfill_maybe(d): float(w)
                for d, w in (raw or {}).items()
                if zfill_maybe(d) in official and float(w) > 0
            }
            total = sum(clean.values())
            if total <= 0:
                return {}
            return {k: v / total for k, v in clean.items()}

        # 1) Explicit PRCTSEQ overrides (Davidson/Shelby review, etc.).
        for key in (fallback_code.zfill(4), fallback_code.zfill(6), fallback_code):
            explicit = filter_official(prctseq_override_lookup.get((county_norm, key), {}))
            if explicit:
                return explicit

        # 2) Statewide PRCTSEQ -> official VTDST20 bridge (offsets + labels).
        for key in (fallback_code.zfill(4), fallback_code.zfill(6), fallback_code):
            bridged = prctseq_bridge.get((county_norm, key), {})
            if bridged:
                return dict(bridged)

        # 3) Strict Census NAME20 exact match.
        exact = filter_official(exact_lookup.get((county_norm, precinct_norm), {}))
        if exact:
            return exact

        # 3b) Rural dual-numeric labels ("2-1", "9 2"): primary number, not subunit.
        dual_hits = resolve_dual_numeric_leading_hits(
            county_norm, precinct_norm, leading_lookup, filter_official
        )
        if dual_hits:
            return dual_hits

        # 4) Prefer the most specific leading-code token with a unique official hit
        # (so 10W -> 10-W wins over bare 10 -> {10-N,10-S,10-W}).
        for tok in leading_code_tokens(precinct_norm):
            hits = filter_official(leading_lookup.get((county_norm, tok), {}))
            if len(hits) == 1:
                return hits
        for tok in leading_code_tokens(precinct_norm):
            hits = filter_official(leading_lookup.get((county_norm, tok), {}))
            if hits and (len(hits) == 1 or re.fullmatch(r"\d{1,3}", tok)):
                return hits

        # 5) Grouped Census name family, only when it collapses to one VTD.
        grouped = filter_official(grouped_lookup.get((county_norm, modern_group_key(precinct_norm)), {}))
        if len(grouped) == 1:
            return grouped

        # 6) DRA GEOID suffix lookup (helps Anderson-style codespaces).
        dra_match = filter_official(dra_code_lookup.get((county_norm, fallback_code.zfill(4)), {}))
        if dra_match:
            return dra_match

        # 7) Identity only when the padded code is a real official VTDST20.
        code = zfill_maybe(fallback_code, 6)
        if code in official:
            return {code: 1.0}

        # 8) Soft place-name fallback (unique hit only).
        soft = resolve_name_to_official_vtd(
            county_norm,
            precinct_norm,
            exact_lookup,
            grouped_lookup,
            leading_lookup,
            official_by_county,
        )
        if len(soft) == 1:
            return soft
        return {}

    for row in read_rows(path):
        county_norm = norm_county(str(row.get("county_norm", "")).strip())
        src_name_norm = norm_text(str(row.get("from_precinct_norm", "")).strip())
        dst_name_norm = norm_text(str(row.get("to_precinct_norm", "")).strip())
        src_vtd = str(row.get("to_prctseq_2024", "")).strip()
        if not county_norm or not src_name_norm or not src_vtd:
            continue
        names = [src_name_norm]
        if dst_name_norm and dst_name_norm not in names:
            names.append(dst_name_norm)
        for name_norm in names:
            source_catalog[(county_norm, src_vtd)].append(
                {
                    "src_name": name_norm,
                    "src_name_norm": name_norm,
                }
            )
        target_name_norm = dst_name_norm or src_name_norm
        for dst_vtd, weight in resolved_dst_map(county_norm, target_name_norm, src_vtd).items():
            transfers[(county_norm, src_vtd)][dst_vtd] += float(weight)

    for row in load_numeric_precinct_2022_bootstrap_rows():
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
        for dst_vtd, weight in resolved_dst_map(county_norm, src_name_norm, src_vtd).items():
            transfers[(county_norm, src_vtd)][dst_vtd] += float(weight)

    for row in load_precinct_alias_rows():
        county_norm = norm_county(str(row.get("county_norm", "")).strip())
        src_name_norm = norm_text(str(row.get("from_precinct_norm", "")).strip())
        dst_name_norm = norm_text(str(row.get("to_precinct_norm", "")).strip())
        src_vtd = str(row.get("to_prctseq_2024", "")).strip()
        if not county_norm or not src_name_norm or not src_vtd:
            continue
        names = [src_name_norm]
        if dst_name_norm and dst_name_norm not in names:
            names.append(dst_name_norm)
        for name_norm in names:
            source_catalog[(county_norm, src_vtd)].append(
                {
                    "src_name": name_norm,
                    "src_name_norm": name_norm,
                }
            )
        target_name_norm = dst_name_norm or src_name_norm
        for dst_vtd, weight in resolved_dst_map(county_norm, target_name_norm, src_vtd).items():
            transfers[(county_norm, src_vtd)][dst_vtd] += float(weight)

    return source_catalog, transfers


def modern_group_key(value: str) -> str:
    """Collapse numbered VTD20 names into a coarser modern precinct family name."""
    s = norm_text(value)
    if not s:
        return ""
    s = re.sub(r"\bST\b", "ST", s)
    s = re.sub(r"\b0+(\d)", r"\1", s)
    s = re.sub(r"\b(\d+)\s+0+(\d+)\b", r"\1 \2", s)
    # Drop trailing numeric pieces such as "1", "2", "3" to recover merged
    # modern precinct labels like "ALTON PARK" from Census VTD20 names like
    # "ALTON PARK 1" / "ALTON PARK 2".
    s = re.sub(r"\s+\d{1,2}$", "", s).strip()
    # Drop single-letter split suffixes like "AIRPORT A" / "COLLEGEDALE B".
    s = re.sub(r"\s+[A-Z]$", "", s).strip()
    return s


def load_census_vtd20_group_catalog() -> Tuple[
    Dict[Tuple[str, str], List[dict]],
    Dict[Tuple[str, str], Dict[str, float]],
]:
    """Build coarse modern-name -> weighted official VTD20 group transfers.

    This uses the official 2020 Census VTD statewide layer and groups numbered
    VTDs into coarser family names. The goal is not perfect historical truth,
    but better current-decade coverage when a modern reporting precinct name
    corresponds to several official VTD20 polygons.
    """
    source_catalog: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    transfers: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    if not CENSUS_VTD20_GEOJSON.exists():
        return source_catalog, transfers

    county_norm_from_fips = county_name_by_fips()
    payload = json.loads(CENSUS_VTD20_GEOJSON.read_text(encoding="utf-8"))
    grouped: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    for feat in payload.get("features", []):
        props = feat.get("properties", {}) or {}
        county_fp = str(props.get("COUNTYFP20", "")).zfill(3)
        county_norm = county_norm_from_fips.get(county_fp, "")
        vtd_code = str(props.get("VTDST20", "")).strip()
        name20 = str(props.get("NAME20", "")).strip()
        if not (county_norm and vtd_code and name20):
            continue
        key = modern_group_key(name20)
        if not key:
            continue
        try:
            area = float(props.get("ALAND20") or 0.0) + float(props.get("AWATER20") or 0.0)
        except (TypeError, ValueError):
            area = 0.0
        grouped[(county_norm, key)].append(
            {
                "vtd_code": vtd_code.zfill(6),
                "name20": norm_text(name20),
                "area": area if area > 0 else 1.0,
            }
        )

    for (county_norm, key), rows in grouped.items():
        if len(rows) < 2:
            continue
        synthetic_src = f"GRP::{county_norm}::{key}"
        source_catalog[(county_norm, synthetic_src)].append(
            {
                "src_name": key,
                "src_name_norm": key,
            }
        )
        for row in rows:
            transfers[(county_norm, synthetic_src)][row["vtd_code"]] += float(row["area"])

    # Also index every official VTD20 by its exact Census NAME20 with identity
    # transfer. This lets modern coded labels like "002 SIGNAL MOUNTAIN 2" bind via
    # tail/exact name instead of a colliding leading code token.
    for feat in payload.get("features", []):
        props = feat.get("properties", {}) or {}
        county_fp = str(props.get("COUNTYFP20", "")).zfill(3)
        county_norm = county_norm_from_fips.get(county_fp, "")
        vtd_code = str(props.get("VTDST20", "")).strip().zfill(6)
        name20 = norm_text(str(props.get("NAME20", "")).strip())
        if not (county_norm and vtd_code.isdigit() and name20):
            continue
        source_catalog[(county_norm, vtd_code)].append(
            {
                "src_name": name20,
                "src_name_norm": name20,
            }
        )
        transfers[(county_norm, vtd_code)][vtd_code] += 1.0

    return source_catalog, transfers


def load_census_vtd20_name_lookup() -> Tuple[
    Dict[Tuple[str, str], Dict[str, float]],
    Dict[Tuple[str, str], Dict[str, float]],
]:
    """Build exact-name and grouped-name lookups into official Census VTD20 codes."""
    exact_lookup: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    grouped_lookup: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    if not CENSUS_VTD20_GEOJSON.exists():
        return exact_lookup, grouped_lookup

    county_norm_from_fips = county_name_by_fips()
    payload = json.loads(CENSUS_VTD20_GEOJSON.read_text(encoding="utf-8"))
    for feat in payload.get("features", []):
        props = feat.get("properties", {}) or {}
        county_fp = str(props.get("COUNTYFP20", "")).zfill(3)
        county_norm = county_norm_from_fips.get(county_fp, "")
        vtd_code = str(props.get("VTDST20", "")).strip().zfill(6)
        name20 = norm_text(str(props.get("NAME20", "")).strip())
        if not (county_norm and vtd_code and name20):
            continue
        try:
            area = float(props.get("ALAND20") or 0.0) + float(props.get("AWATER20") or 0.0)
        except (TypeError, ValueError):
            area = 0.0
        weight = area if area > 0 else 1.0
        exact_lookup[(county_norm, name20)][vtd_code] += weight
        grouped_lookup[(county_norm, modern_group_key(name20))][vtd_code] += weight
    return exact_lookup, grouped_lookup


def load_dra_vtd20_code_lookup() -> Dict[Tuple[str, str], Dict[str, float]]:
    """Translate legacy DRA/current-decade PRCTSEQ-style codes into official Census VTD20 codes."""
    out: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    if not DRA_VTD20_CATALOG_CSV.exists():
        return out

    exact_lookup, grouped_lookup = load_census_vtd20_name_lookup()
    county_norm_from_fips = county_name_by_fips()

    for row in read_rows(DRA_VTD20_CATALOG_CSV):
        geoid20 = str(row.get("GEOID20", "")).strip()
        name20 = norm_text(str(row.get("Name", "")).strip())
        if len(geoid20) < 11 or not name20:
            continue
        county_fp = geoid20[2:5]
        county_norm = county_norm_from_fips.get(county_fp, "")
        legacy_code = geoid20[5:].zfill(6)
        legacy_key = legacy_code[-4:]
        if not county_norm:
            continue

        exact = exact_lookup.get((county_norm, name20), {})
        if exact:
            for dst_vtd, weight in exact.items():
                out[(county_norm, legacy_key)][dst_vtd] += float(weight)
            continue

        grouped = grouped_lookup.get((county_norm, modern_group_key(name20)), {})
        if grouped:
            for dst_vtd, weight in grouped.items():
                out[(county_norm, legacy_key)][dst_vtd] += float(weight)

    return out


def load_prctseq_to_vtd20_override_lookup() -> Dict[Tuple[str, str], Dict[str, float]]:
    """Load explicit county-local PRCTSEQ -> official VTD20 bridges."""
    out: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    if not PRCTSEQ_TO_VTD20_OVERRIDES_CSV.exists():
        return out

    county_norm_from_fips = county_name_by_fips()
    for row in read_rows(PRCTSEQ_TO_VTD20_OVERRIDES_CSV):
        county_fp = str(row.get("county_fp", "")).zfill(3)
        county_norm = county_norm_from_fips.get(county_fp, "")
        prctseq = str(row.get("prctseq", "")).strip()
        vtd20 = str(row.get("vtd20", "")).strip().zfill(6)
        try:
            weight = float(row.get("weight") or 1.0)
        except ValueError:
            weight = 1.0
        if not (county_norm and prctseq.isdigit() and vtd20.isdigit()):
            continue
        if weight <= 0:
            continue
        out[(county_norm, prctseq.zfill(4))][vtd20] += weight
        out[(county_norm, prctseq.zfill(6))][vtd20] += weight

    for key, mapping in list(out.items()):
        total = sum(mapping.values())
        if total > 0:
            for vtd20 in list(mapping.keys()):
                mapping[vtd20] = mapping[vtd20] / total
    return out


def load_numeric_precinct_2022_bootstrap_rows() -> List[dict]:
    """Reuse stable numeric 2022 labels from the current low-confidence report.

    The 2022 Tennessee source contains many county-local numeric labels. When the
    existing low-confidence output already collapses one label to a single 2024
    PRCTSEQ within a county, treat that as a bootstrap override so future crosswalk
    rebuilds can resolve it as an exact catalog match instead of a fuzzy fallback.

    Keep this conservative: do not bootstrap pure forced-best fallbacks, which can
    otherwise snowball weak placeholder matches into self-reinforcing overrides.
    """
    path = XWALK_DIR / "tn_precinct_to_vtd20_blockweighted_2022_low_confidence.csv"
    grouped_codes: Dict[Tuple[str, str], set] = defaultdict(set)
    if not path.exists():
        return []

    for row in read_rows(path):
        county_norm = norm_county(str(row.get("county_norm", "")).strip())
        precinct_norm = norm_text(str(row.get("from_precinct_norm", "")).strip())
        src_vtd = str(row.get("src_vtdst", "")).strip()
        method = str(row.get("match_method", "")).strip()
        try:
            score = float(str(row.get("match_score", "")).strip() or 0.0)
        except ValueError:
            score = 0.0
        if method not in {"tail_fuzzy_name", "fuzzy_name"} or score < 0.9:
            continue
        if not (county_norm and precinct_norm and precinct_norm.isdigit() and src_vtd and src_vtd.isdigit()):
            continue
        grouped_codes[(county_norm, precinct_norm)].add(src_vtd.zfill(6))

    out: List[dict] = []
    for (county_norm, precinct_norm), codes in sorted(grouped_codes.items()):
        if len(codes) != 1:
            continue
        out.append(
            {
                "from_year": "2022",
                "county_norm": county_norm,
                "from_precinct_norm": precinct_norm,
                "to_prctseq_2024": next(iter(codes)),
            }
        )
    return out


def load_numeric_precinct_2022_src_overrides() -> Dict[Tuple[str, str], str]:
    """Promote stable county-local numeric 2022 labels into exact source matches."""
    out: Dict[Tuple[str, str], str] = {}
    for row in load_numeric_precinct_2022_bootstrap_rows():
        county_norm = norm_county(str(row.get("county_norm", "")).strip())
        precinct_norm = norm_text(str(row.get("from_precinct_norm", "")).strip())
        src_vtd = str(row.get("to_prctseq_2024", "")).strip()
        if county_norm and precinct_norm and src_vtd:
            out[(county_norm, precinct_norm)] = src_vtd
    return out


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
    s = re.sub(r"\bST\b", "STREET", s)
    s = re.sub(r"\bSCH\b", "SCHOOL", s)
    s = re.sub(r"\bSCHL\b", "SCHOOL", s)
    s = re.sub(r"\bHGHTS\b", "HEIGHTS", s)
    s = re.sub(r"\bCTR\b", "CENTER", s)
    s = re.sub(r"\bAUD\b", "AUDITORIUM", s)
    s = re.sub(r"\bMID\b", "MIDDLE", s)
    s = re.sub(r"\bPRIM\b", "PRIMARY", s)
    s = re.sub(r"\bELEM\b", "ELEMENTARY", s)
    s = re.sub(r"\bINTER\b", "INTERMEDIATE", s)
    s = re.sub(r"\bINT\b", "INTERMEDIATE", s)
    s = re.sub(r"\bMS\b", "MIDDLE SCHOOL", s)
    s = re.sub(r"\bFD\b", "FIRE DEPARTMENT", s)
    s = re.sub(r"\bFIREDEPT\b", "FIRE DEPARTMENT", s)
    s = re.sub(r"\bDEPT\b", "DEPARTMENT", s)
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
    # Historical files sometimes encode sub-precinct letters as "10A Foo",
    # which becomes "A Foo" after the numeric split above.
    s = re.sub(r"^\s*[A-Z]\s+", "", s, count=1)
    return simplify_precinct_name(s)


def load_precinct_alias_rows() -> List[dict]:
    """Load exact alias->current precinct mappings when a 2024 PRCTSEQ exists."""
    path = XWALK_DIR / "tn_precinct_aliases.csv"
    if not path.exists():
        return []

    out: List[dict] = []
    seen = set()
    for row in read_rows(path):
        county_norm = norm_county(str(row.get("county_norm", "")).strip())
        precinct_norm = norm_text(str(row.get("precinct_norm", "")).strip())
        prctseq = str(row.get("prctseq_2024", "")).strip()
        has_2024 = str(row.get("has_2024", "")).strip()
        if not (county_norm and precinct_norm and prctseq):
            continue
        if has_2024 not in {"1", "TRUE", "True", "true"}:
            continue
        key = (county_norm, precinct_norm, prctseq)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "county_norm": county_norm,
                "from_precinct_norm": precinct_norm,
                "to_prctseq_2024": prctseq,
            }
        )
    return out


def _override_status_accepted(review_status_raw: str) -> bool:
    """Accept approved reviews and Phase-3 auto seeds."""
    status = norm_text(str(review_status_raw or "").strip())
    if not status:
        return True
    if status in {"APPROVED", "CONFIRMED", "DONE"}:
        return True
    # norm_text turns phase3_auto_src -> "PHASE3 AUTO SRC"
    return status.startswith("PHASE3")


def load_manual_src_overrides() -> Dict[Tuple[int, str, str], str]:
    """Load reviewed source-VTD overrides for hard historical labels."""
    path = XWALK_DIR / "tn_crosswalk_manual_overrides.csv"
    out: Dict[Tuple[int, str, str], str] = {}
    if not path.exists():
        return out

    for row in read_rows(path):
        enabled = str(row.get("enabled", "")).strip()
        if enabled not in {"1", "TRUE", "True", "true"}:
            continue
        if not _override_status_accepted(row.get("review_status", "")):
            continue
        try:
            year = int(str(row.get("year", "")).strip())
        except ValueError:
            continue
        county_norm = norm_county(str(row.get("county_norm", "")).strip())
        precinct_norm = norm_text(str(row.get("from_precinct_norm", "")).strip())
        src_vtd = str(row.get("override_src_vtdst", "")).strip()
        if not (year and county_norm and precinct_norm and src_vtd):
            continue
        out[(year, county_norm, precinct_norm)] = src_vtd
    return out


def load_manual_dst_overrides() -> Dict[Tuple[int, str, str], str]:
    """Load reviewed direct destination overrides for no-transfer cases."""
    path = XWALK_DIR / "tn_crosswalk_manual_overrides.csv"
    out: Dict[Tuple[int, str, str], str] = {}
    if not path.exists():
        return out

    for row in read_rows(path):
        enabled = str(row.get("enabled", "")).strip()
        if enabled not in {"1", "TRUE", "True", "true"}:
            continue
        if not _override_status_accepted(row.get("review_status", "")):
            continue
        try:
            year = int(str(row.get("year", "")).strip())
        except ValueError:
            continue
        county_norm = norm_county(str(row.get("county_norm", "")).strip())
        precinct_norm = norm_text(str(row.get("from_precinct_norm", "")).strip())
        dst_vtd20 = str(row.get("override_dst_vtd20", "")).strip()
        if dst_vtd20.isdigit():
            dst_vtd20 = dst_vtd20.zfill(6)
        if not (year and county_norm and precinct_norm and dst_vtd20):
            continue
        out[(year, county_norm, precinct_norm)] = dst_vtd20
    return out


def compact_match_key(value: str) -> str:
    s = simplify_precinct_name(value)
    return re.sub(r"[^A-Z0-9]", "", s)


def core_place_name(value: str) -> str:
    s = strip_leading_locator_tokens(value)
    if not s:
        return ""
    s = re.sub(
        r"\b(SCHOOL|ELEMENTARY|MIDDLE|HIGH|PRIMARY|INTERMEDIATE|COMMUNITY|CENTER|HALL|CHURCH|FIRE|DEPARTMENT|OFFICE|BUILDING)\b",
        " ",
        s,
    )
    s = re.sub(r"\s+", " ", s).strip()
    return s


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


def excel_serial_aliases(precinct_norm: str) -> List[str]:
    """Decode Excel date-serial artifacts like 44562 -> 1-1 / 01-01.

    Only emits month-day in the decoded date order. Emitting day-month swaps
    causes collisions (1-2 vs 2-1 both claim each other).
    """
    out: List[str] = []
    s = norm_text(precinct_norm)
    if not s:
        return out
    m = re.match(r"^(\d{5})$", s)
    if m:
        serial = int(m.group(1))
        if 43000 <= serial <= 50000:
            dt = datetime(1899, 12, 30) + timedelta(days=serial)
            # Unpadded first so canonical keys match OpenElections labels (1-2 not 01-02).
            out.append(f"{int(dt.month)}-{int(dt.day)}")
            out.append(f"{dt.month:02d}-{dt.day:02d}")
    dedup: List[str] = []
    seen = set()
    for v in out:
        n = norm_text(v)
        if n and n not in seen:
            seen.add(n)
            dedup.append(n)
    return dedup


def canonical_precinct_norm(precinct_norm: str) -> str:
    """Prefer decoded Excel month-day labels over raw serial keys."""
    s = norm_text(precinct_norm)
    aliases = excel_serial_aliases(s)
    if not aliases:
        return s
    return aliases[0]


SHELBY_SUBURB_ABBREV = {
    "ARL": "ARLINGTON",
    "BAR": "BARTLETT",
    "BRU": "BRUNSWICK",
    "CAP": "CAPLEVILLE",
    "COL": "COLLIERVILLE",
    "COR": "CORDOVA",
    "EAD": "EADS",
    "FOR": "FOREST HILLS",
    "GER": "GERMANTOWN",
    "KER": "KERRVILLE",
    "LAK": "LAKELAND",
    "LOC": "LOCKE",
    "LUC": "LUCY",
    "MCC": "MCCONNELL S",
    "MIL": "MILLINGTON",
    "MOR": "MORNING SUN",
    "ROS": "ROSS STORE",
    "STE": "STEWARTVILLE",
    "WOO": "WOODSTOCK",
}


def _shelby_place_num_aliases(place: str, num: int) -> List[str]:
    """Emit zero-padded and bare numbered place labels used across Shelby vintages."""
    out = [f"{place} {num}", f"{place} {num:02d}"]
    if place in {"BARTLETT", "GERMANTOWN", "ROSS STORE"}:
        out.append(f"{place} {num:02d}")
    if place in {"BRUNSWICK", "CAPLEVILLE", "COLLIERVILLE", "CORDOVA", "LUCY", "MILLINGTON", "WOODSTOCK"}:
        out.append(f"{place} {num}")
    return out


def shelby_aliases(precinct_norm: str) -> List[str]:
    """Generate deterministic Shelby-specific label aliases."""
    out: List[str] = excel_serial_aliases(precinct_norm)
    s = norm_text(precinct_norm)
    if not s:
        return out
    # Bare Memphis ward codes in older exports: 001 -> MEMPHIS 01
    m = re.match(r"^(\d{3})$", s)
    if m:
        n = int(m.group(1))
        out.extend([f"MEMPHIS {n}", f"MEMPHIS {n:02d}", f"MEMPHIS {n:03d}"])
    m = re.match(r"^(\d{3})\s+(\d{1,2})$", s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        out.extend(
            [
                f"MEMPHIS {a}-{b}",
                f"MEMPHIS {a} {b}",
                f"MEMPHIS {a:02d} {b}",
                f"MEMPHIS {a:02d}-{b}",
            ]
        )
    # 2000 suburb codes: "101 01 BAR 01", "102 1 BRU 1", "100 ARL", "106 EAD"
    m = re.match(r"^(\d{3})\s+(\d{1,2})\s+([A-Z]{3})\s+(\d{1,2})$", s)
    if m:
        place = SHELBY_SUBURB_ABBREV.get(m.group(3), "")
        if place:
            out.extend(_shelby_place_num_aliases(place, int(m.group(4))))
            if place in {"ARLINGTON", "EADS", "FOREST HILLS", "KERRVILLE", "LAKELAND", "LOCKE", "MORNING SUN", "STEWARTVILLE", "MCCONNELL S"}:
                out.append(place)
    m = re.match(r"^(\d{3})\s+([A-Z]{3})$", s)
    if m:
        place = SHELBY_SUBURB_ABBREV.get(m.group(2), "")
        if place:
            out.append(place)
    # Compact suburb form without inner index: "118 WOO 1", "119 WOO 2"
    m = re.match(r"^(\d{3})\s+([A-Z]{3})\s+(\d{1,2})$", s)
    if m:
        place = SHELBY_SUBURB_ABBREV.get(m.group(2), "")
        if place:
            out.extend(_shelby_place_num_aliases(place, int(m.group(3))))
            if place in {
                "ARLINGTON",
                "EADS",
                "FOREST HILLS",
                "KERRVILLE",
                "LAKELAND",
                "LOCKE",
                "MORNING SUN",
                "STEWARTVILLE",
                "MCCONNELL S",
            }:
                out.append(place)
    m = re.match(r"^ROSS\s+(\d{1,2})$", s)
    if m:
        n = int(m.group(1))
        out.extend([f"ROSS STORE {n:02d}", f"ROSS STORE {n}"])
    m = re.match(r"^LAKELAND\s+\d{1,2}$", s)
    if m:
        out.append("LAKELAND")
    m = re.match(r"^ARLINGTON\s+\d{1,2}$", s)
    if m:
        out.append("ARLINGTON")
    m = re.match(r"^LUCY\s+(\d{1,2})$", s)
    if m:
        out.append(f"LUCY {int(m.group(1))}")

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


def parse_dual_numeric_label(value: str) -> Optional[Tuple[int, int]]:
    """Parse rural-style precinct labels like '2-1' / '9 2' into (primary, subunit)."""
    s = norm_text(value)
    if not s:
        return None
    m = re.fullmatch(r"(\d{1,3})[- ](\d{1,3})", s)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def resolve_dual_numeric_leading_hits(
    county_norm: str,
    precinct_norm: str,
    leading_lookup: Dict[Tuple[str, str], Dict[str, float]],
    filter_official,
) -> Dict[str, float]:
    """Resolve 'N-1' / 'N 2' labels via the primary number, not the subunit.

    Election exports often use district/precinct + subunit ('2-1', '10-1'). The
    trailing '1' is not a Census VTD code, but a naive token scan can bind every
    such label to whatever VTD owns leading token '1' (Haywood collapse bug).

    When the primary number alone is ambiguous (09A/09B), use subunit 1/2 as A/B.
    Leaves Knox-style dual codes (second token > 2) to the generic path.
    """
    parsed = parse_dual_numeric_label(precinct_norm)
    if not parsed:
        return {}
    primary, subunit = parsed
    if subunit > 2:
        return {}
    primary_tok = str(primary)
    hits = filter_official(leading_lookup.get((county_norm, primary_tok), {}))
    if len(hits) == 1:
        return hits
    if len(hits) > 1 and 1 <= subunit <= 2:
        letter = chr(ord("A") + subunit - 1)
        for tok in (f"{primary_tok}{letter}", f"{primary_tok}-{letter}", f"{primary_tok} {letter}"):
            letter_hits = filter_official(leading_lookup.get((county_norm, tok), {}))
            if len(letter_hits) == 1:
                return letter_hits
    return {}


def leading_code_tokens(value: str) -> List[str]:
    s = (value or "").strip().upper()
    if not s:
        return []
    s = s.replace("_", " ")
    # Normalize hyphenated codes so "10-W" and "10 W" share the same token path.
    s = re.sub(r"\b(\d{1,3})\s*-\s*([A-Z]{1,2})\b", r"\1 \2", s)
    out: List[str] = []
    # examples: 25-3, 01-2, 2C, 10T, 001-006
    m = re.match(r"^\s*([0-9]{1,3}[A-Z]?)\s*[- ]\s*([0-9]{1,3}[A-Z]?)\b", s)
    if m:
        out.append(f"{normalize_numeric_token(m.group(1))}-{normalize_numeric_token(m.group(2))}")
        digits2 = re.sub(r"\D", "", m.group(2))
        second_n = int(digits2) if digits2 else None
        # Rural labels like "2-1" / "9-2": first number is the precinct id and the
        # second is only a subunit. Emitting bare "1" here collapses whole counties
        # onto whatever VTD owns leading token 1 (see Haywood).
        # Knox-style dual codes ("001-006") keep the meaningful second token.
        if second_n is not None and second_n > 2:
            out.append(normalize_numeric_token(m.group(2)))
            out.append(str(second_n))
        else:
            out.append(normalize_numeric_token(m.group(1)))
            digits1 = re.sub(r"\D", "", m.group(1))
            if digits1:
                out.append(str(int(digits1)))
    # Compact or spaced directional: 10W / 10 W / 65SW
    m2 = re.match(r"^\s*([0-9]{1,3})([A-Z]{1,2})\b", s)
    m2_space = re.match(r"^\s*([0-9]{1,3})\s+([A-Z]{1,2})\b", s)
    if m2 or m2_space:
        mm = m2 or m2_space
        num = str(int(mm.group(1)))
        suf = mm.group(2)
        out.extend(
            [
                f"{num}{suf}",
                f"{num}-{suf}",
                f"{num} {suf}",
                num,
            ]
        )
        # Multi-letter suffixes: 66NE -> 66N / 66-N; 65SW -> 65S / 65-W candidates.
        if len(suf) >= 2:
            out.extend([f"{num}{suf[0]}", f"{num}-{suf[0]}", f"{num}{suf[-1]}", f"{num}-{suf[-1]}"])
    else:
        m2b = re.match(r"^\s*([0-9]{1,3}[A-Z]?)\b", s)
        if m2b:
            out.append(normalize_numeric_token(m2b.group(1)))
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


def knox_label_aliases(precinct_norm: str) -> List[str]:
    """Knox election labels -> candidate vintage/Census name keys."""
    s = norm_text(precinct_norm)
    out: List[str] = []
    # 2000 dual codes: 001 006 -> VTD00 NAME00 "6"
    m = re.match(r"^(\d{1,3})\s+(\d{1,3})$", s)
    if m:
        b = m.group(2)
        out.extend([str(int(b)), b, b.lstrip("0") or "0"])
    # Compact / spaced directional codes: 63N / 10 W -> 63N / 63-N / 10W
    m = re.match(r"^(\d{1,3})\s*([A-Z]{1,2})\b(?:\s+(.*))?$", s)
    if not m:
        m = re.match(r"^(\d{1,3})\s+([A-Z]{1,2})\b(?:\s+(.*))?$", s)
    if m:
        num = str(int(m.group(1)))
        suf = m.group(2)
        rest = (m.group(3) or "").strip()
        out.extend([f"{num}{suf}", f"{num}-{suf}", f"{num} {suf}", num])
        if len(suf) >= 2:
            out.extend([f"{num}{suf[0]}", f"{num}-{suf[0]}"])
        if rest:
            out.extend([f"{num}{suf} {rest}", f"{num}-{suf} {rest}", rest])
    dedup: List[str] = []
    seen = set()
    for v in out:
        n = norm_text(v)
        if n and n not in seen:
            seen.add(n)
            dedup.append(n)
    return dedup


_GREENE_PLACE_TO_VTDST: Optional[Dict[str, str]] = None
_GREENE_PLACE_ALIASES = {
    # 2000/2002 label that predates the 2010 "04 MCDONALD" rename.
    "WARRENSBURG": "MCDONALD",
    "COURTHOUSE": "COURT HOUSE",
    "COURT HOUSE": "COURT HOUSE",
    "MT CARMEL": "MT CARMEL",
    "MOUNT CARMEL": "MT CARMEL",
    "MT PLEASANT": "MT PLEASANT",
    "MOUNT PLEASANT": "MT PLEASANT",
    # Spelling / split variants in early election exports.
    "CHUCKY": "CHUCKY DOAK",
    "CHUCKEY": "CHUCKY DOAK",
    "CHUCKEY DOAK": "CHUCKY DOAK",
    # Union Temple is a 2010 split; VTD00 only has Lost Mountain (3128).
    "UNION TEMPLE": "LOST MOUNTAIN",
}


def load_greene_place_to_vtdst() -> Dict[str, str]:
    """Map Greene place labels -> shared VTD00/VTD10 codes via VTD10 NAME10.

    Greene VTD00 NAME00 values are bare numeric codes (3000, 3004, ...), while
    election files and VTD10 use place names. Codes are stable across 2000/2010.
    """
    global _GREENE_PLACE_TO_VTDST
    if _GREENE_PLACE_TO_VTDST is not None:
        return _GREENE_PLACE_TO_VTDST
    out: Dict[str, str] = {}
    path = ROOT / "Data" / "tn_vtd_2010.geojson"
    if not path.exists():
        _GREENE_PLACE_TO_VTDST = out
        return out
    payload = json.loads(path.read_text(encoding="utf-8"))
    for feat in payload.get("features", []):
        props = feat.get("properties", {}) or {}
        if str(props.get("COUNTYFP10", "")).zfill(3) != "059":
            continue
        code = str(props.get("VTDST10", "")).strip()
        name = norm_text(str(props.get("NAME10", "")).strip())
        if not (code and name):
            continue
        out[name] = code
        # Strip leading precinct index tokens: "01 FOREST HILLS" / "10 1 EAST VIEW"
        body = re.sub(r"^\d{1,2}(?:\s+\d{1,2})?\s+", "", name).strip()
        if body:
            out.setdefault(body, code)
        # Compact body without spaces for light matching helpers.
        compact = re.sub(r"\s+", "", body or name)
        if compact:
            out.setdefault(compact, code)
    for src, dst_name in _GREENE_PLACE_ALIASES.items():
        dst_key = norm_text(dst_name)
        if dst_key in out:
            # Force alias overrides (e.g. UNION TEMPLE -> LOST MOUNTAIN for VTD00).
            out[norm_text(src)] = out[dst_key]
    _GREENE_PLACE_TO_VTDST = out
    return out


def greene_label_to_vtdst(precinct_norm: str) -> str:
    """Resolve a Greene election label to a vintage VTDST code, if unique."""
    lookup = load_greene_place_to_vtdst()
    s = norm_text(precinct_norm)
    if not s:
        return ""
    candidates = [s]
    # Drop leading index: "01 FOREST HILLS", "10 1 EAST VIEW", "10 2 COURTHOUSE"
    stripped = re.sub(r"^\d{1,2}(?:\s+\d{1,2})?\s+", "", s).strip()
    if stripped and stripped != s:
        candidates.append(stripped)
    # Drop trailing ward/split numbers: "HARDINS 11", "WOODLAWN 5"
    bare = re.sub(r"\s+\d{1,2}$", "", stripped or s).strip()
    if bare and bare not in candidates:
        candidates.append(bare)
    # Prefer alias targets first so 2010-only splits (UNION TEMPLE) remap for VTD00.
    ordered: List[str] = []
    for key in candidates:
        alias = _GREENE_PLACE_ALIASES.get(key)
        if alias:
            n = norm_text(alias)
            if n and n not in ordered:
                ordered.append(n)
    for key in candidates:
        if key and key not in ordered:
            ordered.append(key)
    for key in ordered:
        code = lookup.get(key)
        if code:
            return code
    # Truncated OCR/export labels: "10 2 ANDREW JOHNSO" -> ANDREW JOHNSON
    for body in (bare, stripped, s):
        if len(body) < 6:
            continue
        hits = sorted(
            {
                code
                for name, code in lookup.items()
                if not name[:1].isdigit()
                and (name.startswith(body) or body.startswith(name[: max(6, min(len(name), len(body)))]))
            }
        )
        if len(hits) == 1:
            return hits[0]
    return ""


def match_source_vtd(
    county_norm: str,
    precinct_norm: str,
    source_catalog: Dict[Tuple[str, str], List[dict]],
    allow_forced: bool = True,
) -> Tuple[str, str, float]:
    """Return (src_vtdst, method, score).

    token_vtd only accepts codes that exist in the provided catalog. When the
    precinct label also has a place-name body, that body must agree with the
    vintage VTD name so sequential election codes do not bind to unrelated
    Census VTD codes.
    """
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
    # Prefer concrete PRCTSEQ/catalog codes over synthetic GRP:: family splits so
    # boom/NYT PRCTSEQ overlays are not stolen by equal-weight Census group splits
    # (Hamilton Alton Park 50/50 vs geometry ~85/15).
    exact_name_hits = [src_vtd for src_vtd, names in by_vtd_name_norms.items() if precinct_norm in names]
    if exact_name_hits:
        exact_name_hits.sort(key=lambda src: (str(src).startswith("GRP::"), str(src)))
        return exact_name_hits[0], "exact_name", 1.0

    # 1a) Generic Excel date-serial aliases used by several 2022 county exports.
    for alias in excel_serial_aliases(precinct_norm):
        for src_vtd, names in by_vtd_name_norms.items():
            if alias in names:
                return src_vtd, "prefix_name", 0.996

    # 1b) Shelby-specific deterministic aliases for legacy coded labels.
    if county_norm == "SHELBY":
        for alias in shelby_aliases(precinct_norm):
            for src_vtd, names in by_vtd_name_norms.items():
                if alias in names:
                    return src_vtd, "shelby_alias_name", 0.997

    # 1bb) Knox dual-code / compact directional aliases (001 006 -> "6", 63N -> "63N").
    if county_norm == "KNOX":
        for alias in knox_label_aliases(precinct_norm):
            for src_vtd, names in by_vtd_name_norms.items():
                if alias in names:
                    return src_vtd, "knox_alias_name", 0.997
            for src_vtd, names in by_vtd_name_norms.items():
                for cand in names:
                    if cand == alias or cand.startswith(f"{alias} "):
                        return src_vtd, "knox_alias_name", 0.996

    # 1bc) Greene place names: VTD00 NAME00 is numeric, so resolve via VTD10 labels.
    if county_norm == "GREENE":
        greene_code = greene_label_to_vtdst(precinct_norm)
        if greene_code:
            catalog_codes = {src_vtd for src_vtd, _ in candidates}
            if greene_code in catalog_codes:
                return greene_code, "greene_alias_name", 0.997
            # Some overlap tables zero-pad inconsistently.
            padded = greene_code.zfill(4)
            if padded in catalog_codes:
                return padded, "greene_alias_name", 0.997

    # 1c) Prefix name match: source labels often look like "10-6 Gateway School"
    # while election CSVs can carry only "10-6".
    for src_vtd, names in by_vtd_name_norms.items():
        for cand in names:
            if cand == precinct_norm or cand.startswith(f"{precinct_norm} "):
                return src_vtd, "prefix_name", 0.995

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

    # 2d) Core place-name exact match after dropping facility words. This helps
    # cases like "BROAD ST" vs "BROAD STREET SCHOOL" or "RURAL VALE" vs
    # "RURAL VALE FIRE DEPARTMENT" without collapsing unrelated place names.
    q_core = core_place_name(precinct_norm)
    if q_core:
        core_hits: List[str] = []
        for src_vtd, names in by_vtd_name_norms.items():
            for cand in names:
                if q_core == core_place_name(cand):
                    core_hits.append(src_vtd)
                    break
        core_hits = list(dict.fromkeys(core_hits))
        if len(core_hits) == 1:
            return core_hits[0], "core_exact_name", 0.982

    # 1d) Leading code-token match. Runs AFTER place-name methods so labels like
    # "002 SIGNAL MOUNTAIN 2" do not bind to an unrelated VTD that happens to
    # share leading code "2" (Hamilton 2020 collapse onto Alton Park, etc.).
    # When a place-name body is present, require name agreement.
    q_codes = leading_code_tokens(precinct_norm)
    if q_codes:
        code_hits: List[str] = []
        for src_vtd, names in by_vtd_name_norms.items():
            code_ok = False
            for cand in names:
                c_codes = leading_code_tokens(cand)
                if any(q == c for q in q_codes for c in c_codes):
                    code_ok = True
                    break
            if not code_ok:
                continue
            if q_tail and len(q_tail) >= 3 and not token_vtd_name_agrees(q_tail, names):
                continue
            code_hits.append(src_vtd)
        code_hits = list(dict.fromkeys(code_hits))
        if len(code_hits) == 1:
            return code_hits[0], "code_token_name", 0.993

    # 2e) Catalog-gated token/code match. Runs after name methods so labels like
    # "02 ANDERSONVILLE" prefer the named VTD over a sequential code collision.
    qtok = code_token(precinct_norm)
    if qtok:
        body = q_tail if q_tail and q_tail != precinct_norm else ""
        if body and len(body) < 3:
            body = ""
        for src_vtd, names in by_vtd_name_norms.items():
            vtok = re.sub(r"[^A-Z0-9]", "", src_vtd.upper())
            if not (qtok == vtok or qtok.lstrip("0") == vtok.lstrip("0")):
                continue
            if body and not token_vtd_name_agrees(body, names):
                continue
            return src_vtd, "token_vtd", 0.98

    # 2f) Tail-name fuzzy match. Helps labels like "167 COLLEGEDALE 3" where
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

    # Coverage-max fallback (modern years only). Historical matching leaves
    # weak rows unmatched so the block chain is not fed bogus src_vtdst codes.
    if allow_forced and best_vtd:
        return best_vtd, "forced_best_name", round(best_score, 6)

    return "", "unmatched", round(best_score, 6)


def build_crosswalk(source_csv: Path, year: int) -> dict:
    vintage = pick_source_vintage(year)
    overlap_csv = source_overlap_path(vintage)
    manual_src_overrides = load_manual_src_overrides()
    manual_dst_overrides = load_manual_dst_overrides()
    numeric_2022_overrides = load_numeric_precinct_2022_src_overrides() if year == 2022 else {}
    # Davidson (and later boom counties): NYT area overlays keyed by PRCTSEQ beat
    # Census name-identity matches when labels collide (10-2 ballot ≠ 10-2 VTD).
    prctseq_area_override_counties = {"DAVIDSON"} if year >= 2020 else set()
    prctseq_overrides = (
        load_prctseq_to_vtd20_override_lookup() if prctseq_area_override_counties else {}
    )
    if vintage == 2020:
        cat_2024 = load_2024_precinct_catalog(XWALK_DIR / "tn_precinct_to_2024.csv")
        census_group_catalog = load_census_vtd20_group_catalog()
        source_catalog, transfers = merge_catalogs(
            [
                census_group_catalog,
                cat_2024,
                load_overlap_catalog(XWALK_DIR / "tn_vtd10_to_vtd20_overlap.csv"),
            ]
        )
        allow_forced = True
    else:
        # Historical: vintage catalog + transfers only (no 2024 name bleed).
        source_catalog, transfers = load_overlap_catalog(overlap_csv)
        allow_forced = False
    src_precincts = collect_source_precincts(source_csv)

    out_rows: List[dict] = []
    low_confidence_rows: List[dict] = []
    unmatched_rows: List[dict] = []
    method_counts: Dict[str, int] = defaultdict(int)

    for key, meta in sorted(src_precincts.items(), key=lambda kv: (kv[0].county_norm, kv[0].precinct_norm)):
        override_key = (year, key.county_norm, key.precinct_norm)
        # Direct destination override can resolve labels even when no vintage src match exists
        # (common when VTD00/10 NAME fields are numeric codes but election labels match VTD20).
        if override_key in manual_dst_overrides and override_key not in manual_src_overrides:
            manual_dst = manual_dst_overrides[override_key]
            out_rows.append(
                {
                    "from_year": year,
                    "source_vintage": vintage,
                    "county_norm": key.county_norm,
                    "from_precinct_norm": key.precinct_norm,
                    "src_vtdst": manual_dst,
                    "dst_vtd20": manual_dst,
                    "weight": 1.0,
                    "match_method": "manual_override",
                    "confidence_tier": "high",
                    "match_score": 1.0,
                }
            )
            method_counts["manual_override"] += 1
            continue

        if key.county_norm in prctseq_area_override_counties:
            area_map = resolve_prctseq_area_override(
                key.county_norm,
                str(meta.get("prctseq") or ""),
                prctseq_overrides,
            )
            if area_map:
                total = sum(area_map.values())
                method_counts["prctseq_area_overlay"] += 1
                for dst_vtd, w in sorted(area_map.items(), key=lambda it: it[1], reverse=True):
                    weight = float(w) / total
                    if weight <= 0:
                        continue
                    out_rows.append(
                        {
                            "from_year": year,
                            "source_vintage": vintage,
                            "county_norm": key.county_norm,
                            "from_precinct_norm": key.precinct_norm,
                            "src_vtdst": str(meta.get("prctseq") or "").zfill(4),
                            "dst_vtd20": dst_vtd if not str(dst_vtd).isdigit() else str(dst_vtd).zfill(6),
                            "weight": round(weight, 8),
                            "match_method": "prctseq_area_overlay",
                            "confidence_tier": "high",
                            "match_score": 1.0,
                        }
                    )
                continue

        if override_key in manual_src_overrides:
            src_vtd = manual_src_overrides[override_key]
            method = "manual_override"
            score = 1.0
        elif year == 2022 and (key.county_norm, key.precinct_norm) in numeric_2022_overrides:
            src_vtd = numeric_2022_overrides[(key.county_norm, key.precinct_norm)]
            method = "manual_override"
            score = 1.0
        else:
            src_vtd, method, score = match_source_vtd(
                key.county_norm,
                key.precinct_norm,
                source_catalog,
                allow_forced=allow_forced,
            )
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

        dst_map = lookup_transfer_map(transfers, key.county_norm, src_vtd)
        total = sum(dst_map.values())
        if total <= 0:
            manual_dst = manual_dst_overrides.get(override_key)
            if manual_dst:
                out_rows.append(
                    {
                        "from_year": year,
                        "source_vintage": vintage,
                        "county_norm": key.county_norm,
                        "from_precinct_norm": key.precinct_norm,
                        "src_vtdst": src_vtd,
                        "dst_vtd20": manual_dst,
                        "weight": 1.0,
                        "match_method": "manual_override",
                        "confidence_tier": "high",
                        "match_score": 1.0,
                    }
                )
                continue
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
    parser.add_argument("--source-tag", help="Optional tag to make output filenames source-specific")
    parser.add_argument(
        "--no-source-tag",
        action="store_true",
        help="Write generic year-level output filenames without a source tag",
    )
    args = parser.parse_args()

    source_csv = Path(args.source_csv)
    if not source_csv.is_absolute():
        source_csv = ROOT / source_csv
    if not source_csv.exists():
        raise FileNotFoundError(f"Missing source CSV: {source_csv}")

    year = int(args.year) if args.year else parse_year_from_path(source_csv)
    payload = build_crosswalk(source_csv, year)
    if args.no_source_tag:
        source_tag = ""
    else:
        source_tag = (args.source_tag or "").strip() or source_tag_from_path(source_csv)

    XWALK_DIR.mkdir(parents=True, exist_ok=True)
    stem = f"tn_precinct_to_vtd20_blockweighted_{year}"
    if source_tag:
        stem = f"{stem}__{source_tag}"
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
