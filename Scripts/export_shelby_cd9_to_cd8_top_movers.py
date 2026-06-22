from __future__ import annotations

import csv
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
SHIFT_CSV = DATA_DIR / "reports" / "shelby_cd8_from_cd5_cd9_shift_review.csv"
SUMMARY_JSON = DATA_DIR / "reports" / "shelby_cd589_vote_weighted_summary_2024_president.json"
OUTPUT_CSV = DATA_DIR / "reports" / "shelby_cd9_to_cd8_top_movers_2024_president.csv"


def load_modules():
    sys.path.insert(0, str(ROOT / "Scripts"))
    import build_tn_congressional_2026_district_contests as cd26  # type: ignore

    tn = cd26.load_build_module()
    return tn, cd26


def build_vote_lookup():
    tn, cd26 = load_modules()
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
    district_weights, _ = cd26.build_congressional_weight_maps_2026(tn)
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

    lookup: dict[str, dict[str, float]] = {}
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
        node = lookup.setdefault(
            code.zfill(6),
            {
                "dem_votes": 0.0,
                "rep_votes": 0.0,
                "other_votes": 0.0,
                "total_votes": 0.0,
            },
        )
        party = str(row["party"]).upper()
        votes = float(row["votes"])
        if party.startswith("DEM"):
            node["dem_votes"] += votes
        elif party.startswith("REP"):
            node["rep_votes"] += votes
        else:
            node["other_votes"] += votes
        node["total_votes"] += votes
    return lookup


def main() -> None:
    vote_lookup = build_vote_lookup()
    rows = []
    with SHIFT_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("old_dominant_cd") != "9" or row.get("new_dominant_cd") != "8":
                continue
            code = str(row.get("code", "")).strip()
            vote_node = vote_lookup.get(code, {})
            dem_votes = float(vote_node.get("dem_votes", 0.0) or 0.0)
            rep_votes = float(vote_node.get("rep_votes", 0.0) or 0.0)
            other_votes = float(vote_node.get("other_votes", 0.0) or 0.0)
            total_votes = float(vote_node.get("total_votes", 0.0) or 0.0)
            margin = rep_votes - dem_votes
            rows.append(
                {
                    **row,
                    "dem_votes_2024_pres": round(dem_votes, 3),
                    "rep_votes_2024_pres": round(rep_votes, 3),
                    "other_votes_2024_pres": round(other_votes, 3),
                    "total_votes_2024_pres": round(total_votes, 3),
                    "margin_r_minus_d_2024_pres": round(margin, 3),
                    "margin_pct_2024_pres": round((margin / total_votes * 100.0), 4) if total_votes else 0.0,
                    "net_dem_contribution_sort": round(dem_votes - rep_votes, 3),
                }
            )

    rows.sort(
        key=lambda item: (
            -float(item["net_dem_contribution_sort"]),
            -float(item["total_votes_2024_pres"]),
            item["code"],
        )
    )

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {OUTPUT_CSV.relative_to(ROOT)} with {len(rows)} rows")


if __name__ == "__main__":
    main()
