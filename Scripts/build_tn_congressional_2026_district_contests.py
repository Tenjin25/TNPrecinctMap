#!/usr/bin/env python3
"""Build Tennessee congressional district contest slices for the 2026 lines.

This script reuses the existing precinct parsing and crosswalk helpers from
``build_tn_contests.py`` but writes only congressional outputs into
``Data/district_contests_2026/`` so the app can load a separate 2026 lines set
without disturbing the historical 2022/2024 district contest files.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from collections import defaultdict
from pathlib import Path

import geopandas as gpd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
OUTPUT_DIR = DATA_DIR / "district_contests_2026"
LEGACY_DISTRICT_CONTEST_DIR = DATA_DIR / "district_contests"
CONTESTS_DIR = DATA_DIR / "contests"
BUILD_SCRIPT = ROOT / "Scripts" / "build_tn_contests.py"


def load_build_module():
    spec = importlib.util.spec_from_file_location("tn_build_tn_contests", BUILD_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {BUILD_SCRIPT}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_legacy_unaffected_district_results(contest_type: str, year: int) -> dict[str, dict]:
    """Reuse 2022-line district results for districts that did not change."""
    legacy_path = LEGACY_DISTRICT_CONTEST_DIR / f"congressional_{contest_type}_{year}.json"
    if not legacy_path.exists():
        return {}
    try:
        payload = json.loads(legacy_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    results = payload.get("general", {}).get("results", {}) or {}
    out = {}
    for district in ("1", "2"):
        row = results.get(district)
        if isinstance(row, dict):
            out[district] = row
    return out


def build_congressional_weight_maps_2026(tn):
    county_norm_to_fp, _ = tn.load_county_maps()
    precincts = gpd.read_file(DATA_DIR / "tn_voting_precincts.geojson")[
        ["county_norm", "prec_id", "geometry"]
    ].copy()
    precincts["COUNTYFP"] = precincts["county_norm"].apply(
        lambda c: county_norm_to_fp.get(tn.norm_county(str(c)), "")
    )
    precincts["VTD"] = precincts["prec_id"].astype(str).str.zfill(6)
    precincts = precincts[(precincts["COUNTYFP"] != "") & (precincts["VTD"] != "")].copy()
    precincts = precincts[precincts["geometry"].notna()].copy()
    precincts = precincts.to_crs(5070)
    precincts["vtd_area"] = precincts.geometry.area
    precincts = precincts[precincts["vtd_area"] > 0].copy()

    districts = gpd.read_file(DATA_DIR / "tl_2026_47_cd2026.geojson")[["DISTRICT", "geometry"]].copy()
    districts["DISTRICT"] = districts["DISTRICT"].apply(tn.normalize_district_code)
    districts = districts[(districts["DISTRICT"] != "") & districts["geometry"].notna()].copy()
    districts = districts.to_crs(5070)

    left = precincts[["COUNTYFP", "VTD", "vtd_area", "geometry"]].copy()
    right = districts[["DISTRICT", "geometry"]].copy()
    joined = gpd.overlay(left, right, how="intersection")
    if joined.empty:
        raise RuntimeError("No precinct-district intersections found for 2026 congressional lines")

    joined["int_area"] = joined.geometry.area
    joined["weight"] = joined["int_area"] / joined["vtd_area"]
    joined = joined[joined["weight"] > 0].copy()

    district_weights = defaultdict(list)
    county_weights = defaultdict(lambda: defaultdict(float))
    for row in joined.itertuples():
        county_fp = str(row.COUNTYFP).zfill(3)
        vtd = str(row.VTD).zfill(6)
        district = str(row.DISTRICT).strip()
        weight = float(row.weight)
        if not county_fp or not vtd or not district or weight <= 0:
            continue
        district_weights[(county_fp, vtd)].append((district, weight))
        county_weights[county_fp][district] += weight

    district_weights_out = {}
    for key, allocs in district_weights.items():
        total = sum(float(w) for _d, w in allocs)
        if total <= 0:
            continue
        district_weights_out[key] = sorted(
            ((str(d), float(w) / total) for d, w in allocs if float(w) > 0),
            key=lambda x: x[1],
            reverse=True,
        )

    county_district_weights_out = {}
    for county_fp, dmap in county_weights.items():
        total = sum(float(w) for w in dmap.values() if w > 0)
        if total <= 0:
            continue
        county_district_weights_out[county_fp] = sorted(
            ((str(d), float(w) / total) for d, w in dmap.items() if float(w) > 0),
            key=lambda x: x[1],
            reverse=True,
        )

    return district_weights_out, county_district_weights_out


def build_congressional_2026():
    tn = load_build_module()
    district_weights, county_district_weights = build_congressional_weight_maps_2026(tn)

    county_norm_to_fp, _ = tn.load_county_maps()
    to2024 = tn.load_precinct_to_2024_map()
    strict_to2020 = tn.load_blockweighted_strict_to_vtd20_map()
    to2024_split_by_year, to2024_split_any_year = tn.build_precinct_split_key_maps(to2024)
    to2024_fuzzy_candidates = tn.build_precinct_fuzzy_candidates(to2024)
    overlap_maps_by_src_year = {
        2000: tn.load_vtd_overlap_to_2020_map(DATA_DIR / "crosswalks" / "tn_vtd00_to_vtd20_overlap.csv", src_code_width=4),
        2010: tn.load_vtd_overlap_to_2020_map(DATA_DIR / "crosswalks" / "tn_vtd10_to_vtd20_overlap.csv", src_code_width=4),
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
        county_norm_to_fp, district_weights, prctseq_exact_to_vtd
    )
    prctseq_unique_to_vtd = tn.build_prctseq_unique_to_vtd_map(
        county_norm_to_fp=county_norm_to_fp,
        vtd_ints_by_county=vtd_ints_by_county,
        offset_candidates_by_county=prctseq_offset_candidates_by_county,
        prctseq_exact_to_vtd=prctseq_exact_to_vtd,
        prctseq_name_candidates=prctseq_name_candidates,
    )
    prctseq_exact_to_vtd.update(prctseq_unique_to_vtd)

    csv_files = sorted(DATA_DIR.glob("*__tn__*__precinct.csv"))
    if not csv_files:
        raise RuntimeError("No TN precinct CSV files found in Data/")

    contest_precinct = defaultdict(tn.Totals)
    statewide_2024_prctseq = defaultdict(tn.Totals)

    for row in tn.iter_all_rows(csv_files):
      contest_type = tn.infer_contest_type(row["office"])
      if not contest_type:
        continue
      county_norm = tn.norm_county(row["county"])
      if not county_norm:
        continue
      county_fp = county_norm_to_fp.get(county_norm, "")
      party = tn.party_bucket(row["party"])
      votes = float(row["votes"])
      candidate = row["candidate"]
      year = int(row["year"])

      if contest_type in tn.COUNTY_PLUS_PRECINCT_CONTESTS:
        code = tn.resolve_precinct_code(
          year=year,
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
        if not code:
          continue
        label = f"{county_norm} - {code}"
        contest_precinct[(contest_type, year, label)].add(party, candidate, votes)
        if year == 2024 and county_fp:
          seq_raw = tn.norm_space(row["prctseq"])
          if seq_raw.isdigit():
            statewide_2024_prctseq[(contest_type, county_fp, seq_raw.zfill(6))].add(
              party, candidate, votes
            )

    all_contest_rows_by_contest_year = defaultdict(list)
    for (contest_type, year, label), totals in contest_precinct.items():
      all_contest_rows_by_contest_year[(contest_type, year)].append(
        totals.as_precinct_row(label)
      )

    statewide_district = defaultdict(tn.Totals)
    statewide_alloc_stats = defaultdict(
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

    for (contest_type, year), rows in sorted(all_contest_rows_by_contest_year.items()):
      if contest_type not in tn.COUNTY_PLUS_PRECINCT_CONTESTS:
        continue
      county_scope_geo_vote_accum = defaultdict(lambda: defaultdict(float))
      for r in rows:
        label = tn.norm_space(r.get("county", ""))
        if " - " not in label:
          continue
        county_norm, code = label.split(" - ", 1)
        county_norm = tn.norm_county(county_norm)
        code_raw = tn.norm_space(code)
        code_numeric = code_raw.zfill(6) if code_raw.isdigit() else ""
        county_fp = county_norm_to_fp.get(county_norm, "")
        if not county_fp:
          continue

        dem_votes = float(r.get("dem_votes", 0))
        rep_votes = float(r.get("rep_votes", 0))
        other_votes = float(r.get("other_votes", 0))
        dem_cand = r.get("dem_candidate", "")
        rep_cand = r.get("rep_candidate", "")
        stat_key = ("congressional", contest_type, year)
        stat = statewide_alloc_stats[stat_key]
        stat["rows"] += 1
        votes_total = dem_votes + rep_votes + other_votes
        stat["votes_total"] += votes_total

        wmap = district_weights
        allocs = wmap.get((county_fp, code_numeric), []) if code_numeric else []
        source = "overlay_direct" if allocs else ""
        is_non_geo_bucket = code_raw.startswith("NG-")
        is_unmapped_label_bucket = code_raw.startswith("UNM-")
        is_unmapped_non_geo = is_unmapped_label_bucket and tn.is_unmapped_non_geo_bucket(code_raw)
        is_low_seq_numeric = bool(code_numeric and int(code_numeric) < 1000)
        county_allocs = county_district_weights.get(county_fp, [])
        is_single_district_county = len(county_allocs) == 1

        if not allocs:
          allocs = tn.remap_precinct_code_to_2020_vtd_allocations(
            year=year,
            county_fp=county_fp,
            code_numeric=code_numeric,
            code_label=code_raw,
            scope_precinct_weights=wmap,
            overlap_maps_by_src_year=overlap_maps_by_src_year,
            vtd_name_key_maps_by_src_year=vtd_name_key_maps_by_src_year,
          )
          source = "overlay_overlap" if allocs else "county_fallback"

        if not allocs and (is_non_geo_bucket or is_unmapped_non_geo):
          allocs = county_allocs
          source = "non_geo_fallback" if allocs else "dropped"
        # Some legacy 2024 precinct rows resolve to low numeric codes that are
        # valid precinct identifiers but are not present in the 2026 district
        # overlay. For congressional 2026 aggregation, county fallback is
        # better than dropping those votes entirely.
        allow_county_fallback = not (is_non_geo_bucket or is_unmapped_label_bucket)
        if not allocs and allow_county_fallback:
          if is_low_seq_numeric and county_allocs:
            top_district, top_share = county_allocs[0]
            if float(top_share) >= 0.40:
              allocs = [(str(top_district), 1.0)]
              source = "county_dominant_fallback"
            else:
              allocs = county_allocs
              source = "county_fallback" if allocs else "dropped"
          else:
            allocs = county_allocs
            source = "county_fallback" if allocs else "dropped"
        if not allocs:
          stat["dropped_rows"] += 1
          stat["votes_dropped"] += votes_total
          continue

        allocs = tn.maybe_apply_overlap_dominant_assignment(
          scope="congressional",
          county_fp=county_fp,
          year=int(year),
          source=source,
          allocs=allocs,
        )

        if source in {"overlay_direct"}:
          stat["direct_rows"] += 1
          stat["votes_direct"] += votes_total
        elif source == "overlay_overlap":
          stat["overlap_rows"] += 1
          stat["votes_overlap"] += votes_total
        elif source == "non_geo_fallback":
          stat["non_geo_rows"] += 1
          stat["votes_non_geo"] += votes_total
        else:
          stat["county_fallback_rows"] += 1
          stat["votes_fallback"] += votes_total

        for district, w in allocs:
          key = ("congressional", contest_type, year, district)
          node = statewide_district[key]
          node.add("DEM", dem_cand, dem_votes * w)
          node.add("REP", rep_cand, rep_votes * w)
          node.add("OTHER", "", other_votes * w)
          if source in {"overlay_direct", "overlay_overlap"} and not (is_non_geo_bucket or is_unmapped_non_geo):
            county_scope_geo_vote_accum[("congressional", county_fp)][str(district)] += votes_total * float(w)

    # Build output files into the dedicated 2026 directory.
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    district_manifest_files = []
    grouped = defaultdict(dict)
    for (scope, contest_type, year, district), totals in statewide_district.items():
      grouped[(scope, contest_type, year)][district] = totals

    for (_scope, contest_type, year), dmap in sorted(grouped.items()):
      results = {}
      alloc = statewide_alloc_stats.get(("congressional", contest_type, year))
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
          county_fallback_row_pct = (float(alloc["county_fallback_rows"]) / rows_total) * 100.0
          dropped_row_pct = (float(alloc["dropped_rows"]) / rows_total) * 100.0
        if votes_total > 0:
          coverage_pct = (votes_alloc / votes_total) * 100.0
          direct_vote_pct = (float(alloc["votes_direct"]) / votes_total) * 100.0
          overlap_vote_pct = (float(alloc["votes_overlap"]) / votes_total) * 100.0
          non_geo_vote_pct = (float(alloc["votes_non_geo"]) / votes_total) * 100.0
          county_fallback_vote_pct = (float(alloc["votes_fallback"]) / votes_total) * 100.0
          dropped_vote_pct = (float(alloc["votes_dropped"]) / votes_total) * 100.0
      for district in sorted(dmap.keys(), key=lambda d: int(d)):
        row = dmap[district].as_district_result()
        results[str(int(district))] = row

      legacy_overrides = load_legacy_unaffected_district_results(contest_type, year)
      if legacy_overrides:
        results.update({k: v for k, v in legacy_overrides.items() if k in {"1", "2"}})

      dem_total = 0
      rep_total = 0
      for row in results.values():
        dem_total += int(row.get("dem_votes", 0) or 0)
        rep_total += int(row.get("rep_votes", 0) or 0)

      file_name = f"congressional_{contest_type}_{year}.json"
      payload = {
        "scope": "congressional",
        "contest_type": contest_type,
        "year": year,
        "meta": {
          "source": "tn_precinct_csv_district_aggregation_2026_lines_with_2022_line_transfer_for_districts_1_2",
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
      write_json(OUTPUT_DIR / file_name, payload)
      district_manifest_files.append(
        {
          "scope": "congressional",
          "year": year,
          "contest_type": contest_type,
          "file": file_name,
          "districts": len(results),
          "dem_total": int(dem_total),
          "rep_total": int(rep_total),
          "major_party_contested": bool(dem_total > 0 and rep_total > 0),
          "legacy_line_transfer": [1, 2],
        }
      )

    manifest = {
      "files": sorted(district_manifest_files, key=lambda x: (x["contest_type"], x["year"]))
    }

    write_json(OUTPUT_DIR / "manifest.json", manifest)
    return {
      "output_dir": str(OUTPUT_DIR.relative_to(ROOT)),
      "district_files": len(district_manifest_files),
      "manifest_path": str((OUTPUT_DIR / "manifest.json").relative_to(ROOT)),
    }


def main() -> None:
    summary = build_congressional_2026()
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
