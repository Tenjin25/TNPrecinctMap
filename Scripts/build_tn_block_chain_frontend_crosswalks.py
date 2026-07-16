#!/usr/bin/env python3
"""Build precinct→official VTD20 crosswalks using the full block chain.

Destination universe is the official Census VTD20 layer
(Data/tn_vtd_2020.geojson from tl_2020_47_vtd20.zip). Those VTDST20 codes also
match the frontend dra_v07 precinct layer, so contest rows still join the map.

Transfer priority for historical vintages:
  1. Full block chain (VTD00->block00->VTD10->block10->VTD20 via NHGIS bridges)
  2. Direct VTD overlap
  3. Collapsed block_fallback (legacy shortcut)
  4. Modern catalog for 2020+ sources

Reuses source-precinct→src_vtd matches from the existing DRA-style blockweighted
CSVs so matching logic stays unchanged; only the weight-transfer step is
replaced/normalized to official 6-digit VTDST20 IDs.

Outputs under Data/crosswalks/:
  tn_precinct_to_vtd20_blockchain_{year}.csv
  tn_precinct_to_vtd20_blockchain_{year}_unmatched.csv
  tn_precinct_to_vtd20_blockchain_summary.json
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"

OFFICIAL_VTD20_GEOJSON = DATA_DIR / "tn_vtd_2020.geojson"
FRONTEND_GEOJSON = DATA_DIR / "tn_voting_precincts_dra_v07.geojson"
COUNTY_GEOJSON = DATA_DIR / "tl_2020_47_county20.geojson"

OVERLAP_00 = XWALK_DIR / "tn_vtd00_to_vtd20_overlap.csv"
OVERLAP_10 = XWALK_DIR / "tn_vtd10_to_vtd20_overlap.csv"
CHAIN_00 = XWALK_DIR / "tn_vtd00_to_vtd20_block_chain.csv"
CHAIN_10 = XWALK_DIR / "tn_vtd10_to_vtd20_block_chain.csv"
FALLBACK_00 = XWALK_DIR / "tn_vtd00_to_vtd20_block_fallback.csv"
FALLBACK_10 = XWALK_DIR / "tn_vtd10_to_vtd20_block_fallback.csv"

YEARS_DEFAULT = [
    2000,
    2002,
    2004,
    2006,
    2008,
    2010,
    2012,
    2014,
    2016,
    2018,
    2020,
    2022,
    2024,
]

OUT_FIELDS = [
    "from_year",
    "source_vintage",
    "county_norm",
    "from_precinct_norm",
    "src_vtdst",
    "dst_vtd20",
    "name20",
    "weight",
    "match_method",
    "transfer_method",
    "confidence_tier",
    "match_score",
]


def norm_county(value: str) -> str:
    return " ".join(str(value or "").strip().upper().split())


def norm_text(value: str) -> str:
    return " ".join(str(value or "").strip().upper().split())


def numeric(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def zfill_vtd(value: str, width: int = 6) -> str:
    s = str(value or "").strip()
    if s.isdigit():
        return s.zfill(width)
    return s


def pick_source_vintage(year: int) -> int:
    if year <= 2009:
        return 2000
    if year <= 2019:
        return 2010
    return 2020


def county_name_by_fips() -> Dict[str, str]:
    if not COUNTY_GEOJSON.exists():
        return {}
    payload = json.loads(COUNTY_GEOJSON.read_text(encoding="utf-8"))
    out = {}
    for feat in payload.get("features", []):
        props = feat.get("properties") or {}
        fp = str(props.get("COUNTYFP20") or "").zfill(3)
        name = norm_county(props.get("NAME20") or "")
        if fp and name:
            out[fp] = name
    return out


def load_drawable_labels() -> Tuple[Dict[str, set], Dict[Tuple[str, str], str], str]:
    """Load official Census VTD20 destination universe.

    Returns:
      - county_norm -> set of VTDST20 codes
      - (county_norm, vtdst20) -> NAME20
      - source path label
    """
    if not OFFICIAL_VTD20_GEOJSON.exists():
        raise FileNotFoundError(
            f"Missing official VTD20 GeoJSON: {OFFICIAL_VTD20_GEOJSON}. "
            "Run Scripts/export_tn_vtd2020_geojson.py with tl_2020_47_vtd20.zip present."
        )
    county_by_fips = county_name_by_fips()
    payload = json.loads(OFFICIAL_VTD20_GEOJSON.read_text(encoding="utf-8"))
    by_county: Dict[str, set] = defaultdict(set)
    names: Dict[Tuple[str, str], str] = {}
    source = "tn_vtd_2020.geojson"
    for feat in payload.get("features", []):
        props = feat.get("properties") or {}
        if props.get("vtd20_name_source"):
            source = str(props.get("vtd20_name_source"))
        county_fp = str(props.get("COUNTYFP20") or "").zfill(3)
        county = county_by_fips.get(county_fp, "")
        vtd = zfill_vtd(props.get("VTDST20") or "", 6)
        name = str(props.get("NAME20") or props.get("NAMELSAD20") or "").strip()
        if not county or not vtd:
            continue
        by_county[county].add(vtd)
        if name:
            names[(county, vtd)] = name
    if not by_county:
        raise RuntimeError(f"No official VTD20 labels found in {OFFICIAL_VTD20_GEOJSON}")

    # Sanity: frontend dra_v07 should use the same VTDST20 codes.
    if FRONTEND_GEOJSON.exists():
        frontend = json.loads(FRONTEND_GEOJSON.read_text(encoding="utf-8"))
        front_set = set()
        for feat in frontend.get("features", []):
            props = feat.get("properties") or {}
            county = norm_county(props.get("county_norm") or "")
            prec = zfill_vtd(props.get("prec_id") or "", 6)
            if county and prec:
                front_set.add((county, prec))
        official_set = {(c, v) for c, vs in by_county.items() for v in vs}
        if front_set != official_set:
            print(
                f"Warning: official VTD20 vs frontend ID mismatch: "
                f"only_official={len(official_set - front_set)} "
                f"only_frontend={len(front_set - official_set)}",
                flush=True,
            )
        else:
            print("Official VTD20 IDs match frontend dra_v07 prec_id set.", flush=True)

    return dict(by_county), names, source


def load_transfer_map(path: Path, county_by_fips: Dict[str, str]) -> Dict[Tuple[str, str], Dict[str, float]]:
    clean: Dict[Tuple[str, str], Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            county_fp = str(row.get("src_countyfp") or "").zfill(3)
            county = county_by_fips.get(county_fp, "")
            if not county:
                continue
            src = str(row.get("src_vtdst") or "").strip()
            dst = zfill_vtd(row.get("dst_vtdst") or "", 6)
            weight = numeric(row.get("src_weight"))
            if not src or not dst or weight <= 0:
                continue
            clean[(county, src)][dst] += weight
            if src.isdigit():
                clean[(county, src.zfill(4))][dst] += weight
                clean[(county, str(int(src)))][dst] += weight
    return {k: dict(v) for k, v in clean.items()}


def lookup_transfer(
    transfers: Dict[Tuple[str, str], Dict[str, float]],
    county: str,
    src_vtd: str,
) -> Dict[str, float]:
    src = str(src_vtd or "").strip()
    candidates = [src]
    if src.isdigit():
        candidates.extend([src.zfill(4), src.zfill(6), str(int(src))])
    for key in candidates:
        hit = transfers.get((county, key))
        if hit:
            return dict(hit)
    return {}


def merge_transfers(
    chain: Dict[str, float],
    overlap: Dict[str, float],
    fallback: Dict[str, float],
) -> Tuple[Dict[str, float], str]:
    if chain:
        return dict(chain), "block_chain"
    if overlap:
        return dict(overlap), "overlap"
    if fallback:
        return dict(fallback), "block_fallback"
    return {}, "none"


def load_blockweighted_matches(year: int) -> List[dict]:
    path = XWALK_DIR / f"tn_precinct_to_vtd20_blockweighted_{year}.csv"
    if not path.exists():
        return []
    by_src: Dict[Tuple[str, str], dict] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            county = norm_county(row.get("county_norm") or "")
            precinct = norm_text(row.get("from_precinct_norm") or "")
            src = str(row.get("src_vtdst") or "").strip()
            dst = zfill_vtd(row.get("dst_vtd20") or "", 6)
            if not county or not precinct or not src:
                continue
            key = (county, precinct)
            if key not in by_src:
                by_src[key] = {
                    "county_norm": county,
                    "from_precinct_norm": precinct,
                    "src_vtdst": src,
                    "match_method": str(row.get("match_method") or "").strip(),
                    "confidence_tier": str(row.get("confidence_tier") or "").strip(),
                    "match_score": numeric(row.get("match_score")),
                    "existing_dst_weights": defaultdict(float),
                }
            by_src[key]["existing_dst_weights"][dst] += numeric(row.get("weight"))
    return list(by_src.values())


def filter_to_drawable(
    weights: Dict[str, float],
    drawable: set,
) -> Dict[str, float]:
    out = {dst: w for dst, w in weights.items() if dst in drawable and w > 0}
    total = sum(out.values())
    if total <= 0:
        return {}
    return {dst: w / total for dst, w in out.items()}


def resolve_modern_weights(
    county: str,
    src_vtd: str,
    existing: Dict[str, float],
    modern_transfers: Dict[Tuple[str, str], Dict[str, float]],
    drawable: set,
) -> Tuple[Dict[str, float], str]:
    """Resolve modern-year destinations onto drawable dra_v07 VTD20 IDs."""
    raw = {zfill_vtd(k, 6): numeric(v) for k, v in existing.items()}
    filtered = filter_to_drawable(raw, drawable)
    if filtered:
        return filtered, "existing_blockweighted_drawable"

    candidates = [src_vtd]
    if str(src_vtd).isdigit():
        candidates.extend([str(src_vtd).zfill(4), str(src_vtd).zfill(6), str(int(src_vtd))])
    for key in candidates:
        hit = modern_transfers.get((county, key))
        if not hit:
            continue
        filtered = filter_to_drawable({zfill_vtd(d, 6): w for d, w in hit.items()}, drawable)
        if filtered:
            return filtered, "modern_catalog_drawable"

    src6 = zfill_vtd(src_vtd, 6)
    if src6 in drawable:
        return {src6: 1.0}, "identity_drawable"
    return {}, "none"


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def build_year(
    year: int,
    drawable_by_county: Dict[str, set],
    name20_by_key: Dict[Tuple[str, str], str],
    chain_00: Dict[Tuple[str, str], Dict[str, float]],
    chain_10: Dict[Tuple[str, str], Dict[str, float]],
    overlap_00: Dict[Tuple[str, str], Dict[str, float]],
    overlap_10: Dict[Tuple[str, str], Dict[str, float]],
    fallback_00: Dict[Tuple[str, str], Dict[str, float]],
    fallback_10: Dict[Tuple[str, str], Dict[str, float]],
    modern_transfers: Dict[Tuple[str, str], Dict[str, float]],
) -> dict:
    vintage = pick_source_vintage(year)
    matches = load_blockweighted_matches(year)
    if not matches:
        return {
            "year": year,
            "source_vintage": vintage,
            "status": "skipped_missing_blockweighted",
            "rows": 0,
        }

    if vintage == 2000:
        chain, overlap, fallback = chain_00, overlap_00, fallback_00
    elif vintage == 2010:
        chain, overlap, fallback = chain_10, overlap_10, fallback_10
    else:
        chain, overlap, fallback = {}, {}, {}

    out_rows: List[dict] = []
    unmatched: List[dict] = []
    transfer_counts = defaultdict(int)
    drawable_hit_precincts = 0

    for item in matches:
        county = item["county_norm"]
        drawable = drawable_by_county.get(county, set())
        src = item["src_vtdst"]

        if vintage == 2020:
            filtered, transfer_method = resolve_modern_weights(
                county,
                src,
                item["existing_dst_weights"],
                modern_transfers,
                drawable,
            )
        else:
            ch = lookup_transfer(chain, county, src)
            ov = lookup_transfer(overlap, county, src)
            fb = lookup_transfer(fallback, county, src)
            merged, transfer_method = merge_transfers(ch, ov, fb)
            filtered = filter_to_drawable(merged, drawable)
            if not filtered and ch:
                filtered = filter_to_drawable(ch, drawable)
                if filtered:
                    transfer_method = "block_chain_drawable"
            elif not filtered and ov:
                filtered = filter_to_drawable(ov, drawable)
                if filtered:
                    transfer_method = "overlap_drawable"
            elif not filtered and fb:
                filtered = filter_to_drawable(fb, drawable)
                if filtered:
                    transfer_method = "block_fallback_drawable"
            if not filtered:
                # Phase-3 dst-only manual overrides already store official VTDST20
                # weights in the blockweighted CSV (src may be the dst code itself).
                existing_filtered = filter_to_drawable(
                    {zfill_vtd(k, 6): numeric(v) for k, v in item["existing_dst_weights"].items()},
                    drawable,
                )
                if existing_filtered:
                    filtered = existing_filtered
                    transfer_method = "existing_blockweighted_drawable"

        if not filtered:
            unmatched.append(
                {
                    "year": year,
                    "county_norm": county,
                    "from_precinct_norm": item["from_precinct_norm"],
                    "src_vtdst": src,
                    "match_method": item["match_method"],
                    "transfer_method": transfer_method,
                    "reason": "no_drawable_destination",
                }
            )
            transfer_counts[transfer_method or "none"] += 1
            continue

        transfer_counts[transfer_method] += 1
        drawable_hit_precincts += 1
        for dst, weight in sorted(filtered.items(), key=lambda kv: (-kv[1], kv[0])):
            out_rows.append(
                {
                    "from_year": year,
                    "source_vintage": vintage,
                    "county_norm": county,
                    "from_precinct_norm": item["from_precinct_norm"],
                    "src_vtdst": src,
                    "dst_vtd20": dst,
                    "name20": name20_by_key.get((county, dst), ""),
                    "weight": f"{weight:.10f}".rstrip("0").rstrip(".") if weight != 1 else "1.0",
                    "match_method": item["match_method"],
                    "transfer_method": transfer_method,
                    "confidence_tier": item["confidence_tier"],
                    "match_score": item["match_score"],
                }
            )

    out_csv = XWALK_DIR / f"tn_precinct_to_vtd20_blockchain_{year}.csv"
    unmatched_csv = XWALK_DIR / f"tn_precinct_to_vtd20_blockchain_{year}_unmatched.csv"
    write_csv(out_csv, out_rows, OUT_FIELDS)
    write_csv(
        unmatched_csv,
        unmatched,
        [
            "year",
            "county_norm",
            "from_precinct_norm",
            "src_vtdst",
            "match_method",
            "transfer_method",
            "reason",
        ],
    )

    # Join quality vs frontend labels.
    joined_labels = {
        f"{r['county_norm']} - {r['dst_vtd20']}" for r in out_rows
    }
    all_drawable = {
        f"{county} - {prec}"
        for county, precs in drawable_by_county.items()
        for prec in precs
    }

    return {
        "year": year,
        "source_vintage": vintage,
        "status": "ok",
        "source_precincts": len(matches),
        "mapped_precincts": drawable_hit_precincts,
        "unmatched_precincts": len(unmatched),
        "output_rows": len(out_rows),
        "transfer_method_counts": dict(transfer_counts),
        "drawable_labels_touched": len(joined_labels),
        "drawable_labels_total": len(all_drawable),
        "output_csv": str(out_csv.relative_to(DATA_DIR)),
        "unmatched_csv": str(unmatched_csv.relative_to(DATA_DIR)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--years",
        nargs="*",
        type=int,
        default=YEARS_DEFAULT,
        help="Election years to rebuild (default: all statewide years)",
    )
    args = parser.parse_args()

    drawable, name20_by_key, drawable_source = load_drawable_labels()
    county_by_fips = county_name_by_fips()
    print(f"Official VTD20: {OFFICIAL_VTD20_GEOJSON.name} (feature source hint: {drawable_source})", flush=True)
    print(f"Drawable counties: {len(drawable)} VTDs: {sum(len(v) for v in drawable.values())}", flush=True)

    chain_00 = load_transfer_map(CHAIN_00, county_by_fips)
    chain_10 = load_transfer_map(CHAIN_10, county_by_fips)
    overlap_00 = load_transfer_map(OVERLAP_00, county_by_fips)
    overlap_10 = load_transfer_map(OVERLAP_10, county_by_fips)
    fallback_00 = load_transfer_map(FALLBACK_00, county_by_fips)
    fallback_10 = load_transfer_map(FALLBACK_10, county_by_fips)

    modern_transfers: Dict[Tuple[str, str], Dict[str, float]] = {}
    try:
        import build_dra_style_block_crosswalks as dra

        _catalog, modern_transfers = dra.load_2024_precinct_catalog(
            XWALK_DIR / "tn_precinct_to_2024.csv"
        )
        # Normalize destination codes to 6-digit.
        normalized = {}
        for key, dst_map in modern_transfers.items():
            normalized[key] = {zfill_vtd(d, 6): float(w) for d, w in dst_map.items() if float(w) > 0}
        modern_transfers = normalized
        print(f"Modern catalog transfers: {len(modern_transfers)}", flush=True)
    except Exception as exc:  # noqa: BLE001
        print(f"Warning: modern catalog unavailable ({exc})", flush=True)

    print(
        f"Transfers loaded: chain00={len(chain_00)} chain10={len(chain_10)} "
        f"overlap00={len(overlap_00)} fallback00={len(fallback_00)} "
        f"overlap10={len(overlap_10)} fallback10={len(fallback_10)}",
        flush=True,
    )

    summaries = []
    for year in args.years:
        print(f"Building {year}...", flush=True)
        summary = build_year(
            year,
            drawable,
            name20_by_key,
            chain_00,
            chain_10,
            overlap_00,
            overlap_10,
            fallback_00,
            fallback_10,
            modern_transfers,
        )
        summaries.append(summary)
        print(json.dumps(summary, indent=2), flush=True)

    payload = {
        "official_vtd20_geojson": str(OFFICIAL_VTD20_GEOJSON.relative_to(DATA_DIR)),
        "frontend_geojson": str(FRONTEND_GEOJSON.relative_to(DATA_DIR)),
        "drawable_source_hint": drawable_source,
        "years": summaries,
    }
    out_summary = XWALK_DIR / "tn_precinct_to_vtd20_blockchain_summary.json"
    out_summary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {out_summary.relative_to(DATA_DIR)}", flush=True)


if __name__ == "__main__":
    main()
