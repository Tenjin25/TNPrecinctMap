from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
SHIFT_OUTPUT_CSV = DATA_DIR / "reports" / "shelby_cd8_from_cd5_cd9_shift_review.csv"
SUMMARY_OUTPUT_JSON = DATA_DIR / "reports" / "shelby_cd589_vote_weighted_summary_2024_president.json"
CATALOG_CSV = DATA_DIR / "crosswalks" / "tn_blockassign_vtd_with_names.csv"


def load_modules():
    sys.path.insert(0, str(ROOT / "Scripts"))
    import build_tn_congressional_2026_district_contests as cd26  # type: ignore

    tn = cd26.load_build_module()
    return tn, cd26


def load_catalog_names() -> dict[str, str]:
    out: dict[str, str] = {}
    with CATALOG_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if str(row.get("county_fips", "")).strip() != "157":
                continue
            code = str(row.get("vtd_code", "")).strip()
            name = str(row.get("vtd_name", "")).strip()
            if code:
                out[code] = name
    return out


def dominant_label(d5: float, d8: float, d9: float) -> str:
    ordered = sorted([("5", d5), ("8", d8), ("9", d9)], key=lambda item: item[1], reverse=True)
    return ordered[0][0]


def main() -> None:
    tn, cd26 = load_modules()
    catalog_names = load_catalog_names()

    old_district_weights, _ = tn.build_district_weight_maps()
    old_congressional_weights = old_district_weights.get("congressional", {})
    new_congressional_weights, _ = cd26.build_congressional_weight_maps_2026(tn)

    county_norm_to_fp, _ = tn.load_county_maps()
    to2024 = tn.load_precinct_to_2024_map()
    strict_to2020 = tn.load_blockweighted_strict_to_vtd20_map()
    to2024_split_by_year, to2024_split_any_year = tn.build_precinct_split_key_maps(to2024)
    to2024_fuzzy_candidates = tn.build_precinct_fuzzy_candidates(to2024)
    overlap_maps_by_src_year = {
        2000: tn.load_vtd_overlap_to_2020_map(
            DATA_DIR / "crosswalks" / "tn_vtd00_to_vtd20_overlap.csv", src_code_width=4
        ),
        2010: tn.load_vtd_overlap_to_2020_map(
            DATA_DIR / "crosswalks" / "tn_vtd10_to_vtd20_overlap.csv", src_code_width=4
        ),
    }
    vtd_name_key_maps_by_src_year = {
        2000: tn.load_vtd_name_key_map(2000),
        2010: tn.load_vtd_name_key_map(2010),
    }
    vtd20_name_key_map = tn.load_vtd20_name_key_map()
    vtd20_leading_code_map = tn.load_vtd20_leading_code_map()
    prctseq_exact_to_vtd = tn.build_2024_prctseq_to_vtd_lookup(
        county_norm_to_fp=county_norm_to_fp,
        vtd20_name_key_map=vtd20_name_key_map,
        vtd20_leading_code_map=vtd20_leading_code_map,
    )
    prctseq_name_candidates = tn.build_2024_prctseq_to_vtd_candidates(
        county_norm_to_fp=county_norm_to_fp,
        vtd20_name_key_map=vtd20_name_key_map,
        vtd20_leading_code_map=vtd20_leading_code_map,
    )
    prctseq_exact_to_vtd.update(tn.load_prctseq_to_vtd20_overrides())
    prctseq_offsets_by_county, vtd_ints_by_county, prctseq_offset_candidates_by_county = tn.build_prctseq_offsets(
        county_norm_to_fp, {"congressional": new_congressional_weights}, prctseq_exact_to_vtd
    )
    prctseq_unique_to_vtd = tn.build_prctseq_unique_to_vtd_map(
        county_norm_to_fp=county_norm_to_fp,
        vtd_ints_by_county=vtd_ints_by_county,
        offset_candidates_by_county=prctseq_offset_candidates_by_county,
        prctseq_exact_to_vtd=prctseq_exact_to_vtd,
        prctseq_name_candidates=prctseq_name_candidates,
    )
    prctseq_exact_to_vtd.update(prctseq_unique_to_vtd)

    # Precinct-level share comparison between old and new lines.
    shift_rows = []
    shelby_codes = sorted({code for county_fp, code in new_congressional_weights.keys() if county_fp == "157"})
    for code in shelby_codes:
        old_allocs = {str(d): float(w) for d, w in old_congressional_weights.get(("157", code), [])}
        new_allocs = {str(d): float(w) for d, w in new_congressional_weights.get(("157", code), [])}
        old5 = old_allocs.get("5", 0.0)
        old8 = old_allocs.get("8", 0.0)
        old9 = old_allocs.get("9", 0.0)
        new5 = new_allocs.get("5", 0.0)
        new8 = new_allocs.get("8", 0.0)
        new9 = new_allocs.get("9", 0.0)
        shift_rows.append(
            {
                "county": "SHELBY",
                "code": code,
                "vtd_name": catalog_names.get(code, ""),
                "old_cd5_share": old5,
                "old_cd8_share": old8,
                "old_cd9_share": old9,
                "new_cd5_share": new5,
                "new_cd8_share": new8,
                "new_cd9_share": new9,
                "delta_cd8_share": new8 - old8,
                "old_dominant_cd": dominant_label(old5, old8, old9),
                "new_dominant_cd": dominant_label(new5, new8, new9),
                "moved_into_cd8_from_5_or_9": "yes"
                if (new8 - old8) > 0.25 and dominant_label(old5, old8, old9) in {"5", "9"} and dominant_label(new5, new8, new9) == "8"
                else "no",
            }
        )

    shift_rows.sort(
        key=lambda row: (
            row["moved_into_cd8_from_5_or_9"] != "yes",
            -float(row["delta_cd8_share"]),
            row["code"],
        )
    )

    SHIFT_OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with SHIFT_OUTPUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(shift_rows[0].keys()) if shift_rows else [])
        writer.writeheader()
        writer.writerows(shift_rows)

    # Vote-weighted Shelby-only summary for 2024 president across 5/8/9 on new lines.
    district_vote_summary = {
        "5": {"dem_votes": 0.0, "rep_votes": 0.0, "other_votes": 0.0},
        "8": {"dem_votes": 0.0, "rep_votes": 0.0, "other_votes": 0.0},
        "9": {"dem_votes": 0.0, "rep_votes": 0.0, "other_votes": 0.0},
    }
    csv_files = sorted(DATA_DIR.glob("*__tn__*__precinct.csv"))
    for row in tn.iter_all_rows(csv_files):
        if int(row["year"]) != 2024 or tn.infer_contest_type(row["office"]) != "president":
            continue
        county_norm = tn.norm_county(row["county"])
        if county_norm != "SHELBY":
            continue
        county_fp = county_norm_to_fp.get(county_norm, "")
        code = tn.resolve_precinct_code(
            year=2024,
            county_norm=county_norm,
            county_fp=county_fp,
            precinct_raw=row["precinct"],
            prctseq_raw=row["prctseq"],
            to2024=to2024,
            strict_to2020=strict_to2020,
            to2024_split_by_year=to2024_split_by_year,
            to2024_split_any_year=to2024_split_any_year,
            to2024_fuzzy_candidates=to2024_fuzzy_candidates,
            offsets_by_county=prctseq_offsets_by_county,
            vtd_ints_by_county=vtd_ints_by_county,
            prctseq_exact_to_vtd=prctseq_exact_to_vtd,
            overlap_maps_by_src_year=overlap_maps_by_src_year,
            vtd_name_key_maps_by_src_year=vtd_name_key_maps_by_src_year,
            vtd20_name_key_map=vtd20_name_key_map,
            vtd20_leading_code_map=vtd20_leading_code_map,
        )
        if not code or not code.isdigit():
            continue
        allocs = {str(d): float(w) for d, w in new_congressional_weights.get(("157", code.zfill(6)), [])}
        party = str(row["party"]).upper()
        votes = float(row["votes"])
        for district in ("5", "8", "9"):
            share = allocs.get(district, 0.0)
            if share <= 0:
                continue
            if party.startswith("DEM"):
                district_vote_summary[district]["dem_votes"] += votes * share
            elif party.startswith("REP"):
                district_vote_summary[district]["rep_votes"] += votes * share
            else:
                district_vote_summary[district]["other_votes"] += votes * share

    payload = {
        "scope": "shelby_only",
        "contest_type": "president",
        "year": 2024,
        "districts": {},
    }
    for district, node in district_vote_summary.items():
        dem_votes = float(node["dem_votes"])
        rep_votes = float(node["rep_votes"])
        other_votes = float(node["other_votes"])
        total_votes = dem_votes + rep_votes + other_votes
        margin = rep_votes - dem_votes
        payload["districts"][district] = {
            "dem_votes": round(dem_votes, 3),
            "rep_votes": round(rep_votes, 3),
            "other_votes": round(other_votes, 3),
            "total_votes": round(total_votes, 3),
            "margin_r_minus_d": round(margin, 3),
            "margin_pct": round((margin / total_votes * 100.0), 4) if total_votes else 0.0,
        }

    with SUMMARY_OUTPUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    moved_count = sum(1 for row in shift_rows if row["moved_into_cd8_from_5_or_9"] == "yes")
    print(
        json.dumps(
            {
                "shift_output": str(SHIFT_OUTPUT_CSV.relative_to(ROOT)),
                "summary_output": str(SUMMARY_OUTPUT_JSON.relative_to(ROOT)),
                "moved_into_cd8_from_5_or_9_count": moved_count,
                "top10_moved_into_cd8": [row for row in shift_rows if row["moved_into_cd8_from_5_or_9"] == "yes"][:10],
                "districts": payload["districts"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
