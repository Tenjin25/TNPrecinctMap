#!/usr/bin/env python3
"""Audit Tennessee's official historical VTD archive and build NHGIS-backed joins.

The Tennessee Comptroller election-date polygons are the precinct authority.  Census
and NHGIS data are used only to carry blocks forward to 2020 VTDs/district lines.
Large source and normalized geometry files stay local; compact reports and crosswalks
are suitable for version control.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.validation import make_valid

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "Data"
PRIORITY = (2008, 2012, 2014, 2016, 2018, 2020, 2022, 2024)
ANALYSIS_CRS = "EPSG:5070"
NON_GEO = re.compile(
    r"\b(ABSENTEE|EARLY|PROVISIONAL|ELECTION COMM(?:ISSION)?|MAIL|MILITARY|"
    r"OVERSEAS|PAPER BALLOT|CURBSIDE|VOTE ?CENTER|ALL COUNTY)\b", re.I
)


def norm(value: object) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]+", " ", str(value or "").upper())).strip()


def norm_county(value: object) -> str:
    return re.sub(r"\s+COUNTY$", "", norm(value)).strip()


def precinct_parts(value: object) -> tuple[str, str]:
    raw = str(value or "").strip()
    hit = re.match(r"^\s*([0-9]+(?:[-A-Z][0-9A-Z]*)?)\s+(.+)$", raw, re.I)
    code = norm(hit.group(1)) if hit else ""
    name = norm(hit.group(2) if hit else raw)
    return code, name


def directional_alias(value: str) -> str:
    tokens = {"EAST": "E", "WEST": "W", "NORTH": "N", "SOUTH": "S"}
    return " ".join(tokens.get(part, part) for part in norm(value).split())


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def layer_names(archive: Path) -> list[str]:
    with zipfile.ZipFile(archive) as source:
        return sorted(Path(n).stem for n in source.namelist() if n.lower().endswith(".shp"))


def election_date(layer: str) -> str:
    hit = re.search(r"VTD_(\d{4})_([A-Za-z]+)", layer)
    if not hit:
        return ""
    year, month = hit.groups()
    days = {"January": "01", "March": "01", "April": "01", "May": "01", "June": "01", "August": "01", "October": "01", "November": "01"}
    return f"{year}-{days.get(month, '01')}-01"


def read_layer(archive: Path, layer: str) -> gpd.GeoDataFrame:
    return gpd.read_file(archive, layer=layer)


def repair(gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, dict]:
    empty_before = int(gdf.geometry.is_empty.sum() + gdf.geometry.isna().sum())
    invalid = ~gdf.geometry.is_valid & gdf.geometry.notna() & ~gdf.geometry.is_empty
    repaired = gdf.copy()
    repaired.loc[invalid, "geometry"] = repaired.loc[invalid, "geometry"].map(make_valid)
    repaired = repaired[repaired.geometry.notna() & ~repaired.geometry.is_empty].copy()
    remaining = int((~repaired.geometry.is_valid).sum())
    return repaired.to_crs(ANALYSIS_CRS), {
        "invalid_before": int(invalid.sum()),
        "repaired": int(invalid.sum()) - remaining,
        "invalid_after": remaining,
        "empty_or_null_removed": empty_before,
    }


def county_lookup() -> tuple[dict[str, str], gpd.GeoDataFrame]:
    counties = gpd.read_file(DATA / "tl_2020_47_county20.geojson").to_crs(ANALYSIS_CRS)
    names = {}
    for _, row in counties.iterrows():
        fips = str(row.get("COUNTYFP20", "")).zfill(3)
        names[fips] = norm_county(row.get("NAME20", ""))
    return names, counties[["COUNTYFP20", "NAME20", "geometry"]]


def official_catalog(gdf: gpd.GeoDataFrame, counties: gpd.GeoDataFrame, fips_names: dict[str, str]) -> pd.DataFrame:
    rows = []
    missing_county = []
    for idx, row in gdf.iterrows():
        county = ""
        for field in ("NAME20", "COUNTY", "COUNTYNAME", "COUNTY_NAM"):
            if field in gdf.columns and pd.notna(row.get(field)) and str(row.get(field)).strip():
                raw_county = str(row.get(field)).strip()
                numeric_county = str(int(float(raw_county))).zfill(3) if re.fullmatch(r"\d+(?:\.0+)?", raw_county) else ""
                county = fips_names.get(numeric_county, "") if numeric_county else norm_county(raw_county)
                break
        for field in ("COUNTYFP20", "COUNTYFP10"):
            if not county and field in gdf.columns:
                county = fips_names.get(str(row.get(field, "")).zfill(3), "")
        raw_name = ""
        for field in ("NEWVOTINGP", "PRECINCT", "Precinct", "Precinct_N", "NAME", "NAME10"):
            if field in gdf.columns and pd.notna(row.get(field)) and str(row.get(field)).strip():
                raw_name = str(row.get(field)); break
        raw_code = ""
        for field in ("VTDST10", "VTD", "CCD"):
            if field in gdf.columns and pd.notna(row.get(field)) and str(row.get(field)).strip():
                raw_code = str(row.get(field)); break
        parsed_code, parsed_name = precinct_parts(raw_name)
        rows.append({"feature_id": int(idx), "county": county, "code": norm(raw_code) or parsed_code, "name": parsed_name, "raw_name": raw_name})
        if not county:
            missing_county.append(int(idx))
    frame = pd.DataFrame(rows).set_index("feature_id", drop=False)
    if missing_county:
        points = gpd.GeoDataFrame({"feature_id": missing_county}, geometry=gdf.loc[missing_county].representative_point(), crs=gdf.crs)
        joined = gpd.sjoin(points, counties, predicate="within", how="left")
        for _, row in joined.iterrows():
            frame.loc[int(row.feature_id), "county"] = norm_county(row.get("NAME20", ""))
    return frame


def result_precincts(path: Path) -> list[dict]:
    found = {}
    with path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            county = norm_county(row.get("COUNTY") or row.get("county"))
            raw = (row.get("PRECINCT") or row.get("precinct") or "").strip()
            seq = (row.get("PRCTSEQ") or row.get("prctseq") or "").strip()
            key = (county, norm(raw), seq)
            found.setdefault(key, {"county": county, "precinct": raw, "precinct_norm": norm(raw), "prctseq": seq})
    return list(found.values())


def match_results(rows: list[dict], catalog: pd.DataFrame) -> tuple[list[dict], list[dict], list[dict]]:
    by_county: dict[str, list[dict]] = defaultdict(list)
    for rec in catalog.to_dict("records"):
        by_county[rec["county"]].append(rec)
    matched, unmatched, nongeo = [], [], []
    for row in rows:
        if NON_GEO.search(row["precinct"] or ""):
            nongeo.append({**row, "bucket_type": norm(row["precinct"]).lower().replace(" ", "_")}); continue
        code, name = precinct_parts(row["precinct"])
        candidates = by_county.get(row["county"], [])
        scored = []
        for cand in candidates:
            code_exact = bool(code and cand["code"] and (code.lstrip("0") == cand["code"].lstrip("0")))
            name_exact = bool(name and cand["name"] and name == cand["name"])
            ratio = SequenceMatcher(None, directional_alias(name), directional_alias(cand["name"])).ratio() if name and cand["name"] else 0.0
            score = max(1.0 if code_exact and name_exact else 0, 0.97 if code_exact else 0, 0.95 if name_exact else 0, ratio * 0.9)
            method = "code_and_name" if code_exact and name_exact else "exact_code" if code_exact else "exact_name" if name_exact else "fuzzy_name"
            scored.append((score, method, cand))
        scored.sort(key=lambda item: item[0], reverse=True)
        best = scored[0] if scored else None
        runner = scored[1][0] if len(scored) > 1 else 0
        if not best or best[0] < 0.78 or best[0] - runner < 0.03:
            unmatched.append({**row, "best_score": round(best[0], 4) if best else 0, "reason": "ambiguous" if best else "county_not_found"}); continue
        matched.append({**row, "feature_id": best[2]["feature_id"], "official_name": best[2]["raw_name"], "match_method": best[1], "confidence": round(best[0], 4), "review_required": best[0] < 0.90})
    return matched, unmatched, nongeo


def read_transfer(vintage: int) -> dict[str, list[tuple[str, float]]]:
    chain = DATA / "crosswalks" / "block_chain"
    direct = {}
    if vintage == 2020:
        path = chain / "hop_tabblock20_to_vtd20_blockassign.csv"
        with path.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                direct.setdefault(row["block_geoid"], []).append((row["dst_vtdst"], float(row["weight"])))
        return direct
    first_path = chain / ("hop_tabblock00_to_tabblock10_nhgis.csv" if vintage == 2000 else "hop_tabblock10_to_tabblock20_nhgis.csv")
    second_path = chain / "hop_tabblock10_to_tabblock20_nhgis.csv"
    first = defaultdict(list)
    with first_path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            src = row[f"block_geoid_{vintage}"]
            dst = row["block_geoid_2010" if vintage == 2000 else "block_geoid_2020"]
            first[src].append((dst, float(row["xwalk_weight"])))
    if vintage == 2000:
        hop = defaultdict(list)
        with second_path.open(encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle): hop[row["block_geoid_2010"]].append((row["block_geoid_2020"], float(row["xwalk_weight"])))
        first = {src: [(b20, w1 * w2) for b10, w1 in vals for b20, w2 in hop.get(b10, [])] for src, vals in first.items()}
    vtd = read_transfer(2020)
    return {src: [(code, w1 * w2) for b20, w1 in vals for code, w2 in vtd.get(b20, [])] for src, vals in first.items()}


def block_path(block_dir: Path, vintage: int) -> Path:
    names = {2000: "tl_2008_47_tabblock00.zip", 2010: "tl_2012_47_tabblock10.zip", 2020: "tl_2020_47_tabblock20.zip"}
    path = block_dir / names[vintage]
    if not path.exists(): raise FileNotFoundError(f"Missing local block geometry archive: {path}")
    return path


def official_to_vtd20(gdf: gpd.GeoDataFrame, block_zip: Path, vintage: int) -> tuple[dict[int, dict[str, float]], dict]:
    blocks = gpd.read_file(block_zip)
    suffix = str(vintage)[-2:]
    geoid_col = next(c for c in (f"GEOID{suffix}", f"BLKIDFP{suffix}", "GEOID20", "GEOID10", "GEOID00", "GEOID") if c in blocks.columns)
    pop_col = next((c for c in (f"POP{str(vintage)[-2:]}", "POP20", "POP10", "POP00") if c in blocks.columns), None)
    blocks = blocks[[geoid_col] + ([pop_col] if pop_col else []) + ["geometry"]].to_crs(gdf.crs)
    blocks["block_weight"] = (pd.to_numeric(blocks[pop_col], errors="coerce").fillna(0).astype(float) if pop_col else 0.0)
    zero = blocks["block_weight"] <= 0
    blocks.loc[zero, "block_weight"] = blocks.loc[zero].geometry.area.clip(lower=1)
    pts = blocks.copy(); pts.geometry = pts.geometry.representative_point()
    joined = gpd.sjoin(pts, gpd.GeoDataFrame({"feature_id": gdf.index}, geometry=gdf.geometry, crs=gdf.crs), predicate="within", how="inner")
    transfer = read_transfer(vintage)
    accum: dict[int, Counter] = defaultdict(Counter)
    missing = 0
    for _, row in joined.iterrows():
        links = transfer.get(str(row[geoid_col]), [])
        if not links: missing += 1; continue
        weight = float(row["block_weight"])
        for vtd, share in links: accum[int(row["feature_id"])][vtd] += weight * share
    out = {}
    for feature_id, counts in accum.items():
        total = sum(counts.values())
        weights = {k: v / total for k, v in counts.items() if v > 0} if total else {}
        if weights and max(weights.values()) >= 0.999:
            winner = max(weights, key=weights.get); weights = {winner: 1.0}
        out[feature_id] = weights
    return out, {"block_vintage": vintage, "blocks": len(blocks), "blocks_joined": len(joined), "joined_blocks_without_nhgis_link": missing, "features_with_block_weights": len(out)}


def write_csv(path: Path, rows: list[dict], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = fields or sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore"); writer.writeheader(); writer.writerows(rows)


def save_baseline(output: Path) -> None:
    if output.exists():
        return
    rows = []
    for folder in ("contests", "district_contests", "district_contests_2026"):
        for path in sorted((DATA / folder).glob("*.json")):
            rows.append({"path": str(path.relative_to(ROOT)).replace("\\", "/"), "sha256": sha256(path), "bytes": path.stat().st_size})
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({"captured_utc": datetime.now(timezone.utc).isoformat(), "files": rows}, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--block-dir", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=DATA)
    parser.add_argument("--normalized-dir", type=Path, default=DATA / "raw" / "official_vtd_normalized")
    parser.add_argument("--inventory-only", action="store_true")
    parser.add_argument("--years", nargs="*", type=int, help="Limit crosswalk rebuilds; inventory still covers every layer")
    args = parser.parse_args()
    if not args.archive.exists(): raise FileNotFoundError(args.archive)
    save_baseline(args.output_root / "reports" / "official_vtd_baseline.json")
    fips_names, counties = county_lookup()
    inventory, repairs, match_summary, block_summary = [], [], [], []
    layers = layer_names(args.archive)
    for layer in layers:
        raw = read_layer(args.archive, layer)
        fixed, stats = repair(raw)
        catalog = official_catalog(fixed, counties, fips_names)
        year_hit = re.search(r"VTD_(\d{4})_", layer); year = int(year_hit.group(1)) if year_hit else None
        covered = sorted(c for c in catalog.county.unique() if c)
        inventory.append({"layer": layer, "election_date": election_date(layer), "feature_count": len(raw), "source_crs": str(raw.crs), "analysis_crs": ANALYSIS_CRS, "fields": [c for c in raw.columns if c != "geometry"], "county_count": len(covered), "counties": covered, **stats, "provenance": "Tennessee Comptroller of the Treasury; Tennessee county election commissions"})
        repairs.append({"layer": layer, **stats})
        selected = set(args.years or PRIORITY)
        if year not in selected or "November_State_General" not in layer or args.inventory_only: continue
        args.normalized_dir.mkdir(parents=True, exist_ok=True)
        fixed.to_file(args.normalized_dir / f"{layer}.gpkg", layer="vtd", driver="GPKG")
        source = args.output_root / f"{year}110{4 if year == 2008 else 6 if year in (2012,) else 4 if year == 2014 else 8 if year == 2016 else 6 if year == 2018 else 3 if year == 2020 else 8 if year == 2022 else 5}__tn__general__precinct.csv"
        # Prefer filename discovery; the explicit expression only makes failures obvious.
        hits = sorted(args.output_root.glob(f"{year}*__tn__general__precinct.csv"))
        if not hits: raise FileNotFoundError(f"No precinct results CSV for {year}")
        source = hits[0]
        matched, unmatched, nongeo = match_results(result_precincts(source), catalog)
        vintage = 2000 if year <= 2008 else 2010 if year <= 2018 else 2020
        weights, bstats = official_to_vtd20(fixed, block_path(args.block_dir, vintage), vintage)
        base = args.output_root / "crosswalks" / f"tn_precinct_to_vtd20_blockweighted_{year}"
        legacy_rows = []
        if base.with_suffix(".csv").exists():
            with base.with_suffix(".csv").open(encoding="utf-8-sig", newline="") as handle:
                legacy_rows = list(csv.DictReader(handle))
        legacy_strict = []
        strict_path = base.with_name(base.name + "_strict.csv")
        if strict_path.exists():
            with strict_path.open(encoding="utf-8-sig", newline="") as handle:
                legacy_strict = list(csv.DictReader(handle))
        output_rows = []
        no_weight = []
        for row in matched:
            links = weights.get(int(row["feature_id"]), {})
            if not links: no_weight.append({**row, "reason": "no_nhgis_block_weight"}); continue
            for vtd, weight in sorted(links.items()):
                output_rows.append({"from_year": year, "source_vintage": vintage, "county_norm": row["county"], "from_precinct_norm": row["precinct_norm"], "src_vtdst": row["feature_id"], "dst_vtd20": vtd, "weight": round(weight, 12), "match_method": f"official_vtd_{row['match_method']}_nhgis_blocks", "confidence_tier": "high" if row["confidence"] >= .90 else "medium", "match_score": row["confidence"], "official_name": row["official_name"]})
        official_keys = {(str(r["county_norm"]), str(r["from_precinct_norm"])) for r in output_rows}
        fallback_rows = []
        for legacy in legacy_rows:
            key = (norm_county(legacy.get("county_norm")), norm(legacy.get("from_precinct_norm")))
            if key in official_keys: continue
            legacy = dict(legacy); legacy["match_method"] = "legacy_fallback_pending_official_review"
            fallback_rows.append(legacy)
        output_rows.extend(fallback_rows)
        write_csv(base.with_suffix(".csv"), output_rows)
        strict = [r for r in output_rows if str(r.get("match_method", "")).startswith("official_vtd_") and str(r.get("confidence_tier")) == "high" and float(r["weight"]) >= .999]
        official_strict_keys = {(norm_county(r.get("county_norm")), norm(r.get("from_precinct_norm"))) for r in strict}
        for legacy in legacy_strict:
            key = (norm_county(legacy.get("county_norm")), norm(legacy.get("from_precinct_norm")))
            if key not in official_keys and key not in official_strict_keys:
                legacy = dict(legacy); legacy["match_method"] = "legacy_fallback_pending_official_review"
                strict.append(legacy)
        write_csv(base.with_name(base.name + "_strict.csv"), strict)
        low = [{**r} for r in matched if r["review_required"]] + no_weight
        write_csv(base.with_name(base.name + "_low_confidence.csv"), low)
        write_csv(base.with_name(base.name + "_unmatched.csv"), unmatched)
        write_csv(base.with_name(base.name + "_non_geographic.csv"), nongeo)
        summary = {"year": year, "source_csv": source.name, "official_layer": layer, "source_precincts": len(matched)+len(unmatched)+len(nongeo), "matched": len(matched), "unmatched": len(unmatched), "non_geographic": len(nongeo), "low_confidence": len(low), "official_crosswalk_rows": len(output_rows)-len(fallback_rows), "legacy_fallback_rows": len(fallback_rows), "crosswalk_rows": len(output_rows), **bstats}
        base.with_name(base.name + "_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        match_summary.append(summary); block_summary.append({"year": year, **bstats})
    reports = args.output_root / "reports"; reports.mkdir(parents=True, exist_ok=True)
    all_match_summaries = []
    for summary_year in PRIORITY:
        summary_path = args.output_root / "crosswalks" / f"tn_precinct_to_vtd20_blockweighted_{summary_year}_summary.json"
        if summary_path.exists():
            all_match_summaries.append(json.loads(summary_path.read_text(encoding="utf-8")))
    payload = {"archive_expected_path": str(args.archive), "archive_sha256": sha256(args.archive), "archive_bytes": args.archive.stat().st_size, "analysis_crs": ANALYSIS_CRS, "authoritative_source": "Tennessee Comptroller of the Treasury and Tennessee county election commissions", "layers": inventory}
    (reports / "official_vtd_inventory.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    write_csv(reports / "official_vtd_inventory.csv", [{**r, "fields": "|".join(r["fields"]), "counties": "|".join(r["counties"])} for r in inventory])
    (reports / "official_vtd_geometry_repairs.json").write_text(json.dumps(repairs, indent=2), encoding="utf-8")
    (reports / "official_vtd_match_summary.json").write_text(json.dumps(all_match_summaries, indent=2), encoding="utf-8")
    (reports / "official_vtd_block_crosswalk_summary.json").write_text(json.dumps([{k: row[k] for k in ("year", "block_vintage", "blocks", "blocks_joined", "joined_blocks_without_nhgis_link", "features_with_block_weights") if k in row} for row in all_match_summaries], indent=2), encoding="utf-8")
    print(json.dumps({"layers": len(inventory), "priority_layers_built": len(match_summary), "archive_sha256": payload["archive_sha256"]}, indent=2))


if __name__ == "__main__":
    main()
