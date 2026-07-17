#!/usr/bin/env python3
"""Overlay NYT 2024 election precinct polygons onto Census VTD20 for boom counties.

Targets: Davidson, Knox, Hamilton, Rutherford, Williamson, Montgomery, Sumner —
counties where election geography diverges from 2020 VTDs and mapped_zero /
gap-fill density is high.

Uses cached Data/raw/nyt_tn_precincts_2024.geojson when present.

Outputs:
  Data/reports/boom_election_precinct_to_vtd20_area_overlay.csv
  Updates Data/crosswalks/tn_prctseq_to_vtd20_overrides.csv
    (replaces prior boom_nyt_* / davidson_current_label_exact / focus_nyt rows
     for covered PRCTSEQs)
"""

from __future__ import annotations

import csv
import gzip
import json
import re
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import geopandas as gpd
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
RAW = DATA / "raw"
REPORTS = DATA / "reports"
XWALK = DATA / "crosswalks"

NYT_URL = "https://int.nyt.com/newsgraphics/elections/map-data/2024/national/TN-precincts-with-results.geojson.gz"
NYT_CACHE = RAW / "nyt_tn_precincts_2024.geojson"
SOURCE_2024 = DATA / "20241105__tn__general__precinct.csv"
OVERLAY_CSV = REPORTS / "boom_election_precinct_to_vtd20_area_overlay.csv"
OVERRIDES = XWALK / "tn_prctseq_to_vtd20_overrides.csv"
VTD20 = DATA / "tn_vtd_2020.geojson"
SOURCE = "boom_nyt_precinct_area_overlay"
MIN_OVERLAY_SHARE = 0.02
MAX_OVERLAY_TARGETS = 8
LETTER_TO_NUM = {"A": "1", "B": "2", "C": "3", "D": "4", "E": "5"}

FOCUS = {
    "DAVIDSON": "037",
    "KNOX": "093",
    "HAMILTON": "065",
    "RUTHERFORD": "149",
    "WILLIAMSON": "187",
    "MONTGOMERY": "125",
    "SUMNER": "165",
}

# Replace these override sources for focus counties when NYT covers the PRCTSEQ.
REPLACE_SOURCES = {
    "boom_nyt_precinct_area_overlay",
    "davidson_current_label_exact",
    "focus_nyt_precinct_area_overlay",
}


def ensure_nyt_cache() -> Path:
    RAW.mkdir(parents=True, exist_ok=True)
    if NYT_CACHE.exists() and NYT_CACHE.stat().st_size > 1_000_000:
        return NYT_CACHE
    print(f"Downloading {NYT_URL}")
    with urllib.request.urlopen(NYT_URL, timeout=300) as resp:
        raw = gzip.decompress(resp.read())
    NYT_CACHE.write_bytes(raw)
    return NYT_CACHE


def norm_county(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().upper())


def norm_text(value: str) -> str:
    text = str(value or "").upper()
    text = text.replace("-", " ").replace("/", " ").replace("_", " ")
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def nyt_suffix(geoid: str) -> str:
    return geoid.split("-", 1)[1] if "-" in geoid else geoid


def load_election_labels() -> Dict[str, Dict[str, str]]:
    """county_norm -> {prctseq_int_str: raw precinct label}"""
    out: Dict[str, Dict[str, str]] = defaultdict(dict)
    with SOURCE_2024.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            county = norm_county(row.get("COUNTY") or "")
            if county not in FOCUS:
                continue
            seq = str(row.get("PRCTSEQ") or "").strip()
            label = str(row.get("PRECINCT") or "").strip()
            if not seq.isdigit() or not label:
                continue
            key = str(int(seq))
            if key not in out[county]:
                out[county][key] = label
    return out


def code_pair(text: str) -> Optional[Tuple[int, int]]:
    """Parse leading N-N / N N precinct codes into ints."""
    m = re.match(r"^\s*0*(\d+)\s*[- ]\s*0*(\d+)\b", str(text or ""))
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def code_triple(text: str) -> Optional[Tuple[int, int, int]]:
    """Parse leading N-N-N codes (e.g. Williamson 1-4-5)."""
    m = re.match(r"^\s*0*(\d+)\s*[- ]\s*0*(\d+)\s*[- ]\s*0*(\d+)\b", str(text or ""))
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def match_davidson(nyt_key: str, label: str) -> bool:
    a = code_pair(nyt_key)
    b = code_pair(label)
    return a is not None and a == b


def match_rutherford(nyt_key: str, label: str) -> bool:
    a = code_pair(nyt_key)
    b = code_pair(label)
    if a is not None and a == b:
        return True
    return norm_text(nyt_key) == norm_text(label)


def match_williamson(nyt_key: str, label: str) -> bool:
    t = code_triple(nyt_key)
    u = code_triple(label)
    if t is not None and t == u:
        return True
    a = code_pair(nyt_key)
    b = code_pair(label)
    return a is not None and a == b


def match_montgomery(nyt_key: str, label: str) -> bool:
    # NYT: "10 Minglewood..." / "11A Mosaic..."; election: "10 ..." / "1A St B CC"
    kn = norm_text(nyt_key)
    ln = norm_text(label)
    if not kn or not ln:
        return False
    return kn.split()[0] == ln.split()[0]


def match_sumner(nyt_key: str, label: str) -> bool:
    # NYT: "101 Westmoreland Middle" / "1402 Birdwell..."; election: "1-1 Westmoreland"
    kn = norm_text(nyt_key)
    m = re.match(r"^(\d{3,4})\b", kn)
    if not m:
        return False
    code = m.group(1)
    pair = code_pair(label)
    if not pair:
        return False
    a, b = pair
    return code == f"{a}{b:02d}"


def match_hamilton(nyt_key: str, label: str) -> bool:
    return norm_text(nyt_key) == norm_text(label)


def match_knox(nyt_key: str, label: str) -> bool:
    kn = norm_text(nyt_key)
    ln = norm_text(label)
    if not kn or not ln:
        return False
    if ln == kn:
        return True
    # Leading election token must equal NYT key (06, 10A, 65S, …).
    tok = ln.split()[0]
    return tok == kn


MATCHERS = {
    "DAVIDSON": match_davidson,
    "RUTHERFORD": match_rutherford,
    "WILLIAMSON": match_williamson,
    "MONTGOMERY": match_montgomery,
    "SUMNER": match_sumner,
    "HAMILTON": match_hamilton,
    "KNOX": match_knox,
}


def resolve_prctseq(county: str, nyt_key: str, labels: Dict[str, str]) -> Optional[Tuple[str, str]]:
    matcher = MATCHERS[county]
    hits = [(seq, lab) for seq, lab in labels.items() if matcher(nyt_key, lab)]
    if len(hits) == 1:
        return hits[0]
    if len(hits) > 1 and county == "KNOX":
        # Prefer exact leading-token equality already enforced; if still ambiguous,
        # prefer the label whose leading token length equals the NYT key and has no
        # longer sibling that also matched (e.g. 63 over 63N when key is 63).
        exact = [(s, lab) for s, lab in hits if norm_text(lab).split()[0] == norm_text(nyt_key)]
        if len(exact) == 1:
            return exact[0]
        # Prefer the shorter precinct name (parent without N/E/W suffix ward).
        exact.sort(key=lambda item: len(norm_text(item[1])))
        if exact and len(norm_text(exact[0][1])) < len(norm_text(exact[1][1])):
            return exact[0]
    return None


def apply_letter_family_correction(
    label: str,
    ranked: List[dict],
    name_to_vtd: Dict[str, Tuple[str, str]],
) -> List[dict]:
    """Prefer Census 'Family N' when election label is 'Family X' but geometry hit a stranger.

    Example: Hamilton 'Mountain Creek B' overlays mostly onto Valdeau; remap the
    primary share to Census 'Mountain Creek 2' while keeping in-family secondaries.
    """
    ln = norm_text(label)
    m = re.fullmatch(r"(.+?) ([A-E])", ln)
    if not m or not ranked:
        return ranked
    family, letter = m.group(1), m.group(2)
    want = f"{family} {LETTER_TO_NUM[letter]}"
    top_name = norm_text(ranked[0].get("vtd_name") or "")
    if top_name.startswith(f"{family} "):
        return ranked
    if want not in name_to_vtd:
        return ranked
    code, nice_name = name_to_vtd[want]
    primary_w = float(ranked[0]["weight"])
    rest = [
        r
        for r in ranked[1:]
        if norm_text(r.get("vtd_name") or "").startswith(f"{family} ")
        or float(r["weight"]) >= MIN_OVERLAY_SHARE
    ]
    return [{"vtd20": code, "vtd_name": nice_name, "weight": primary_w}, *rest]


def select_overlay_targets(ranked: List[dict]) -> Tuple[List[dict], str]:
    """Keep all meaningful area shares instead of majority-only collapse."""
    if not ranked:
        return [], "area_core"
    keep = [r for r in ranked if float(r["weight"]) >= MIN_OVERLAY_SHARE][:MAX_OVERLAY_TARGETS]
    if not keep:
        keep = ranked[:1]
    conf = "area_core" if len(keep) == 1 else "area_split"
    return keep, conf


def overlay_county(
    left: gpd.GeoDataFrame,
    right: gpd.GeoDataFrame,
) -> List[dict]:
    left = left.to_crs(3857).copy()
    right = right.to_crs(3857).copy()
    left["prec_area"] = left.geometry.area
    keep_cols = [c for c in ["geoid", "nyt_key", "prctseq", "label", "prec_area", "geometry"] if c in left.columns]
    inter = gpd.overlay(
        left[keep_cols],
        right[["vtd20", "vtd_name", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )
    if inter.empty:
        return []
    inter["share"] = inter.geometry.area / inter["prec_area"]
    inter = inter[inter["share"] >= 0.005].copy()
    if inter.empty:
        return []

    rows: List[dict] = []
    group_cols = [c for c in ["geoid", "nyt_key", "prctseq", "label"] if c in inter.columns]
    for keys, grp in inter.groupby(group_cols, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        meta = dict(zip(group_cols, keys))
        total = float(grp["share"].sum())
        agg = (
            grp.assign(weight=grp["share"] / total if total > 0 else 0.0)
            .groupby(["vtd20", "vtd_name"], as_index=False)["weight"]
            .sum()
            .sort_values("weight", ascending=False)
        )
        for _, row in agg.iterrows():
            rows.append(
                {
                    **meta,
                    "vtd20": str(row["vtd20"]),
                    "vtd_name": str(row["vtd_name"]),
                    "weight": float(row["weight"]),
                }
            )
    return rows


def main() -> None:
    ensure_nyt_cache()
    election = load_election_labels()
    nyt = gpd.read_file(NYT_CACHE)
    nyt["GEOID"] = nyt["GEOID"].astype(str)
    vtd_all = gpd.read_file(VTD20)
    vtd_all["county_fp"] = vtd_all["COUNTYFP20"].astype(str).str.zfill(3)
    vtd_all["vtd20"] = vtd_all["VTDST20"].astype(str).str.zfill(6)
    vtd_all["vtd_name"] = vtd_all["NAME20"].astype(str)

    overlay_rows: List[dict] = []
    override_rows: List[dict] = []
    match_summary: Dict[str, dict] = {}

    for county, fips in FOCUS.items():
        sub = nyt[nyt["GEOID"].str.startswith(f"47{fips}-")].copy()
        labels = election.get(county, {})
        matched_feats: List[dict] = []
        unmatched_nyt: List[str] = []
        for _, feat in sub.iterrows():
            geoid = str(feat["GEOID"])
            key = nyt_suffix(geoid)
            resolved = resolve_prctseq(county, key, labels)
            if not resolved:
                unmatched_nyt.append(key)
                continue
            seq, label = resolved
            matched_feats.append(
                {
                    "geoid": geoid,
                    "nyt_key": key,
                    "prctseq": seq,
                    "label": label,
                    "geometry": feat.geometry,
                }
            )

        match_summary[county] = {
            "nyt_features": int(len(sub)),
            "election_labels": len(labels),
            "matched": len(matched_feats),
            "unmatched_nyt": unmatched_nyt,
            "unused_prctseq": sorted(
                set(labels) - {m["prctseq"] for m in matched_feats},
                key=lambda s: int(s),
            ),
        }
        if not matched_feats:
            continue

        left = gpd.GeoDataFrame(matched_feats, geometry="geometry", crs=sub.crs)
        right = vtd_all[vtd_all["county_fp"] == fips].copy()
        name_to_vtd = {
            norm_text(str(row.vtd_name)): (str(row.vtd20), str(row.vtd_name))
            for row in right.itertuples()
            if str(row.vtd_name or "").strip()
        }
        pieces = overlay_county(left, right)
        for row in pieces:
            overlay_rows.append({**row, "county_norm": county, "county_fp": fips})

        by_seq: Dict[str, List[dict]] = defaultdict(list)
        for row in pieces:
            by_seq[str(row["prctseq"])].append(row)

        for seq, seq_pieces in by_seq.items():
            # Collapse duplicate vtd20s if multiple NYT features somehow share a seq.
            collapsed: Dict[Tuple[str, str], float] = defaultdict(float)
            label = str(seq_pieces[0].get("label") or "")
            for p in seq_pieces:
                collapsed[(p["vtd20"], p["vtd_name"])] += float(p["weight"])
            ranked = sorted(
                ({"vtd20": k[0], "vtd_name": k[1], "weight": w} for k, w in collapsed.items()),
                key=lambda r: -r["weight"],
            )
            total = sum(r["weight"] for r in ranked) or 1.0
            for r in ranked:
                r["weight"] = r["weight"] / total
            ranked = apply_letter_family_correction(label, ranked, name_to_vtd)
            total = sum(float(r["weight"]) for r in ranked) or 1.0
            for r in ranked:
                r["weight"] = float(r["weight"]) / total
            keep, conf = select_overlay_targets(ranked)
            wsum = sum(r["weight"] for r in keep) or 1.0
            for r in keep:
                override_rows.append(
                    {
                        "county_fp": fips,
                        "county_norm": county,
                        "prctseq": str(int(seq)),
                        "vtd20": r["vtd20"],
                        "vtd_name": r["vtd_name"],
                        "weight": round(r["weight"] / wsum, 6),
                        "source": SOURCE,
                        "confidence": conf,
                    }
                )

    REPORTS.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(overlay_rows).to_csv(OVERLAY_CSV, index=False)

    existing: List[dict] = []
    if OVERRIDES.exists():
        with OVERRIDES.open("r", encoding="utf-8-sig", newline="") as handle:
            existing = list(csv.DictReader(handle))

    covered = {(r["county_norm"], str(int(r["prctseq"]))) for r in override_rows}
    keep = []
    for row in existing:
        county = str(row.get("county_norm") or "").upper()
        seq = str(row.get("prctseq") or "").strip()
        src = str(row.get("source") or "")
        if county in FOCUS and seq.isdigit() and (county, str(int(seq))) in covered:
            continue
        if src == SOURCE and county in FOCUS:
            continue
        keep.append(row)

    final = keep + override_rows
    final.sort(key=lambda r: (r["county_fp"], int(r["prctseq"]), -float(r.get("weight") or 0)))
    with OVERRIDES.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["county_fp", "county_norm", "prctseq", "vtd20", "vtd_name", "weight", "source", "confidence"],
        )
        writer.writeheader()
        writer.writerows(final)

    # VTD coverage after overlay weights
    vtd_touched: Dict[str, set] = defaultdict(set)
    for row in override_rows:
        vtd_touched[row["county_norm"]].add(row["vtd20"])

    summary = {
        "overlay_csv": str(OVERLAY_CSV.relative_to(ROOT)).replace("\\", "/"),
        "override_rows_added": len(override_rows),
        "override_rows_total": len(final),
        "match_summary": match_summary,
        "vtd20_touched": {k: len(v) for k, v in vtd_touched.items()},
        "vtd20_official": {
            county: int((vtd_all["county_fp"] == fips).sum()) for county, fips in FOCUS.items()
        },
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
