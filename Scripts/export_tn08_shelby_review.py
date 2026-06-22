from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
OUTPUT_PATH = DATA_DIR / "reports" / "tn08_shelby_review_2024_president.csv"


def load_modules():
    sys.path.insert(0, str(ROOT / "Scripts"))
    import build_tn_congressional_2026_district_contests as cd26  # type: ignore

    tn = cd26.load_build_module()
    return tn, cd26


def build_rows():
    tn, cd26 = load_modules()

    district_weights, _county_district_weights = cd26.build_congressional_weight_maps_2026(tn)
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
        county_norm_to_fp, {"congressional": district_weights}, prctseq_exact_to_vtd
    )
    prctseq_unique_to_vtd = tn.build_prctseq_unique_to_vtd_map(
        county_norm_to_fp=county_norm_to_fp,
        vtd_ints_by_county=vtd_ints_by_county,
        offset_candidates_by_county=prctseq_offset_candidates_by_county,
        prctseq_exact_to_vtd=prctseq_exact_to_vtd,
        prctseq_name_candidates=prctseq_name_candidates,
    )
    prctseq_exact_to_vtd.update(prctseq_unique_to_vtd)

    agg = {}
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
        allocs = district_weights.get((county_fp, code.zfill(6)), [])
        share8 = sum(float(w) for district, w in allocs if str(district) == "8")
        if share8 <= 0:
            continue
        key = (row["precinct"], row["prctseq"], code.zfill(6))
        node = agg.setdefault(
            key,
            {
                "county": county_norm,
                "precinct": row["precinct"],
                "prctseq": row["prctseq"],
                "code": code.zfill(6),
                "district8_share": share8,
                "dem_votes_to_8": 0.0,
                "rep_votes_to_8": 0.0,
                "other_votes_to_8": 0.0,
                "total_votes_to_8": 0.0,
            },
        )
        votes_to_8 = float(row["votes"]) * share8
        party = str(row["party"]).upper()
        if party.startswith("DEM"):
            node["dem_votes_to_8"] += votes_to_8
        elif party.startswith("REP"):
            node["rep_votes_to_8"] += votes_to_8
        else:
            node["other_votes_to_8"] += votes_to_8
        node["total_votes_to_8"] += votes_to_8

    rows = []
    for node in agg.values():
        total_votes = float(node["total_votes_to_8"])
        margin = float(node["rep_votes_to_8"]) - float(node["dem_votes_to_8"])
        margin_pct = (margin / total_votes * 100.0) if total_votes else 0.0
        partisan_lean = "REP" if margin > 0 else "DEM" if margin < 0 else "TIE"
        rows.append(
            {
                **node,
                "margin_r_minus_d_to_8": margin,
                "margin_pct_to_8": margin_pct,
                "lean_to_8": partisan_lean,
            }
        )

    rows.sort(key=lambda item: (item["margin_pct_to_8"], -item["total_votes_to_8"]))
    return rows


def write_csv(rows):
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "county",
        "precinct",
        "prctseq",
        "code",
        "district8_share",
        "dem_votes_to_8",
        "rep_votes_to_8",
        "other_votes_to_8",
        "total_votes_to_8",
        "margin_r_minus_d_to_8",
        "margin_pct_to_8",
        "lean_to_8",
    ]
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    rows = build_rows()
    write_csv(rows)
    summary = {
        "output_path": str(OUTPUT_PATH.relative_to(ROOT)),
        "rows": len(rows),
        "top_dem_to_8": rows[:10],
        "top_rep_to_8": sorted(rows, key=lambda item: (item["margin_pct_to_8"], item["total_votes_to_8"]), reverse=True)[:10],
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
