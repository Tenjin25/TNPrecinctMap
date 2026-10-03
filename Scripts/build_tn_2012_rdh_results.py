#!/usr/bin/env python3
"""Disaggregate Tennessee's 2012 statewide results to 2010 blocks.

Modified VAP is Census 2010 P003001 minus SF1 P042003 (adult correctional
facilities). Election precincts are matched to Census 2010 VTDs by the reviewed
crosswalk, allocated to blocks, and aggregated into the enacted 2022 House and
Senate plans. Final party totals use largest remainder against certified totals.
"""

from __future__ import annotations

import csv
import io
import json
import re
import zipfile
from collections import Counter, defaultdict
from pathlib import Path

import shapefile
from shapely.geometry import Point, shape
from shapely.strtree import STRtree

from build_tn_legislative_rdh_results import (
    DATA, FIELDS, SCOPES, certified_targets, compare, largest_remainder,
    load_json, refresh_manifest, result_row,
)
from build_dra_style_block_crosswalks import (
    HIGH_CONFIDENCE_METHODS, MEDIUM_CONFIDENCE_METHODS,
    is_non_geographic_label, match_source_vtd,
)


ROOT = Path(__file__).resolve().parents[1]
YEAR = 2012
SOURCE = DATA / "canonical_precinct_csvs" / "20121106__tn__general__precinct.csv"
CROSSWALK = DATA / "crosswalks" / "tn_precinct_to_vtd20_blockweighted_2012__20121106_tn_general_precinct.csv"
PL_ZIP = DATA / "tn2010.pl.zip"
SF1_ZIP = DATA / "tn2010.sf1.zip"
BLOCK_ZIP = DATA / "tl_2012_47_tabblock10.zip"
VTD_ZIP = DATA / "tl_2012_47_vtd10.zip"
OUTPUT_DIR = DATA / "district_contests"
AUDIT_PATH = DATA / "reports" / "tn_legislative_rdh_2012_audit.json"


def norm(value: str) -> str:
    value = re.sub(r"[^A-Z0-9 ]+", " ", str(value or "").upper())
    return re.sub(r"\s+", " ", value).strip()


def norm_county(value: str) -> str:
    return re.sub(r"\s+COUNTY$", "", norm(value)).strip()


def source_precinct_to_vtd():
    """Match directly to the official 2012 TIGER VTD codes and names.

    The older generic VTD20 crosswalk is not used here because its merged
    multi-vintage catalog can select a modern look-alike code. The block build
    requires the actual election-vintage VTD identifier.
    """
    county_names = {}
    county_geo = load_json(DATA / "tl_2020_47_county20.geojson")
    for feature in county_geo["features"]:
        props = feature["properties"]
        county_names[str(props["COUNTYFP20"]).zfill(3)] = norm_county(props["NAME20"])
    catalog = defaultdict(list)
    with zipfile.ZipFile(VTD_ZIP) as archive:
        dbf = next(n for n in archive.namelist() if n.lower().endswith(".dbf"))
        reader = shapefile.Reader(dbf=io.BytesIO(archive.read(dbf)))
        fields = [item[0] for item in reader.fields[1:]]
        for values in reader.iterRecords():
            record = dict(zip(fields, values))
            county = county_names[str(record["COUNTYFP10"]).zfill(3)]
            vtd = str(record["VTDST10"]).zfill(4)
            name = norm(record["NAME10"])
            catalog[(county, vtd)].append({"src_name": record["NAME10"], "src_name_norm": name})
        reader.close()
    keys = set()
    with SOURCE.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            key = (norm_county(row["county"]), norm(row["precinct"]))
            if key[0] and key[1] and not is_non_geographic_label(key[1]):
                keys.add(key)
    mapping, confidence = {}, {}
    for key in sorted(keys):
        vtd, method, score = match_source_vtd(key[0], key[1], catalog)
        if not vtd:
            continue
        tier = "high" if method in HIGH_CONFIDENCE_METHODS else ("medium" if method in MEDIUM_CONFIDENCE_METHODS else "low")
        mapping[key] = vtd
        confidence[key] = ((score, score), tier, method)
    return mapping, confidence


def geography_logrec_to_block(archive: zipfile.ZipFile, name: str):
    out = {}
    for raw in archive.open(name):
        line = raw.decode("latin1").rstrip("\r\n")
        if line[8:11] not in {"750", "101"}:
            continue
        logrec = line[18:25]
        geoid = line[27:32] + line[54:60] + line[61:65]
        out[logrec] = geoid
    return out


def read_modified_vap():
    with zipfile.ZipFile(PL_ZIP) as archive:
        geoid = geography_logrec_to_block(archive, "tngeo2010.pl")
        vap = {}
        for row in csv.reader(io.TextIOWrapper(archive.open("tn000022010.pl"), encoding="ascii")):
            if row[4] in geoid:
                vap[geoid[row[4]]] = int(row[5] or 0)  # P003001
    # Segment 6 contains P31-P49. P31-P41 contain 163 cells, so
    # P042003 is CSV field 170 after the five link fields.
    with zipfile.ZipFile(SF1_ZIP) as archive:
        geoid = geography_logrec_to_block(archive, "tngeo2010.sf1")
        correctional = {}
        for row in csv.reader(io.TextIOWrapper(archive.open("tn000062010.sf1"), encoding="ascii")):
            if row[4] in geoid:
                correctional[geoid[row[4]]] = int(row[170] or 0)  # P042003
    keys = set(vap) | set(correctional)
    return {key: max(0, vap.get(key, 0) - correctional.get(key, 0)) for key in keys}, vap, correctional


def polygon_index_from_zip(path: Path, field: str):
    with zipfile.ZipFile(path) as archive:
        shp = next(n for n in archive.namelist() if n.lower().endswith(".shp"))
        dbf = next(n for n in archive.namelist() if n.lower().endswith(".dbf"))
        reader = shapefile.Reader(shp=io.BytesIO(archive.read(shp)), dbf=io.BytesIO(archive.read(dbf)))
        fields = [item[0] for item in reader.fields[1:]]
        geometries, values = [], []
        for item in reader.iterShapeRecords():
            record = dict(zip(fields, item.record))
            geometries.append(shape(item.shape.__geo_interface__))
            values.append(str(record[field]))
        reader.close()
    return STRtree(geometries), geometries, values


def polygon_index_from_geojson(path: Path, field: str):
    payload = load_json(path)
    geometries = [shape(feature["geometry"]) for feature in payload["features"]]
    values = [str(int(str(feature["properties"][field]))) for feature in payload["features"]]
    return STRtree(geometries), geometries, values


def locate(point: Point, index) -> str:
    tree, geometries, values = index
    for candidate in tree.query(point):
        if geometries[candidate].covers(point):
            return values[candidate]
    return values[tree.nearest(point)]


def block_assignments(modified_vap: dict[str, int]):
    vtd_index = polygon_index_from_zip(VTD_ZIP, "VTDST10")
    district_indexes = {
        scope: polygon_index_from_geojson(DATA / name, field)
        for scope, (name, field, _prefix) in SCOPES.items()
    }
    blocks = defaultdict(list)
    with zipfile.ZipFile(BLOCK_ZIP) as archive:
        dbf = next(n for n in archive.namelist() if n.lower().endswith(".dbf"))
        reader = shapefile.Reader(dbf=io.BytesIO(archive.read(dbf)))
        fields = [item[0] for item in reader.fields[1:]]
        for values in reader.iterRecords():
            record = dict(zip(fields, values))
            geoid = str(record["GEOID"])
            point = Point(float(record["INTPTLON"]), float(record["INTPTLAT"]))
            county = str(record["COUNTYFP10"]).zfill(3)
            vtd = str(locate(point, vtd_index)).zfill(4)
            districts = {scope: locate(point, index) for scope, index in district_indexes.items()}
            blocks[(county, vtd)].append((modified_vap.get(geoid, 0), districts))
        reader.close()
    return blocks


def election_rows(mapping):
    county_fips = {}
    county_geo = load_json(DATA / "tl_2020_47_county20.geojson")
    for feature in county_geo["features"]:
        county_fips[norm_county(feature["properties"]["NAME20"])] = str(feature["properties"]["COUNTYFP20"]).zfill(3)
    votes = defaultdict(lambda: {field: 0 for field in FIELDS})
    unmatched = Counter()
    nongeographic = defaultdict(lambda: {field: 0 for field in FIELDS})
    with SOURCE.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            office = norm(row["office"])
            contest = "president" if office == "PRESIDENT" else ("us_senate" if office == "UNITED STATES SENATE" else "")
            if not contest:
                continue
            county = norm_county(row["county"])
            key = (county, norm(row["precinct"]))
            vtd = mapping.get(key)
            party = norm(row["party"])
            field = "dem_votes" if party == "D" else ("rep_votes" if party == "R" else "other_votes")
            if not vtd or county not in county_fips:
                if county in county_fips and (is_non_geographic_label(key[1]) or key[1] in {"EV", "EARLY VOTING"}):
                    nongeographic[(contest, county_fips[county])][field] += int(row["votes"])
                else:
                    unmatched[key] += int(row["votes"])
                continue
            votes[(contest, county_fips[county], vtd, key)][field] += int(row["votes"])
    return votes, nongeographic, unmatched


def main():
    mapping, confidence = source_precinct_to_vtd()
    modified_vap, vap, correctional = read_modified_vap()
    blocks = block_assignments(modified_vap)
    source_votes, nongeographic, unmatched = election_rows(mapping)
    audit = {
        "year": YEAR,
        "methodology": "2010-block modified VAP (P003001 minus P042003), enacted-2022 TIGER representative-point containment",
        "source_precinct_keys": len(mapping),
        "unmatched_vote_rows": len(unmatched),
        "unmatched_votes": sum(unmatched.values()),
        "county_cluster_fallback_votes": sum(sum(row.values()) for row in nongeographic.values()),
        "confidence_tiers": dict(Counter(item[1] for item in confidence.values())),
        "blocks_with_vap": sum(value > 0 for value in vap.values()),
        "blocks_with_correctional_population": sum(value > 0 for value in correctional.values()),
        "modified_vap_total": sum(modified_vap.values()),
        "vtd_block_groups": len(blocks),
        "vtd_collisions": {},
        "files": [],
    }
    collision_counts = Counter((county, vtd) for _contest, county, vtd, _key in source_votes)
    audit["vtd_collisions"] = {f"{county}-{vtd}": count for (county, vtd), count in collision_counts.items() if count > 2}
    method_votes, tier_votes = Counter(), Counter()
    for (_contest, _county, _vtd, key), row in source_votes.items():
        votes = sum(row.values())
        method_votes[confidence[key][2]] += votes
        tier_votes[confidence[key][1]] += votes
    matched_votes = sum(method_votes.values())
    audit["match_methods"] = dict(method_votes)
    audit["confidence_tier_votes"] = dict(tier_votes)
    audit["low_confidence_vote_pct"] = round(tier_votes["low"] / matched_votes * 100.0 if matched_votes else 0.0, 4)
    audit["county_cluster_fallback_vote_pct"] = round(
        audit["county_cluster_fallback_votes"] /
        (matched_votes + audit["county_cluster_fallback_votes"]) * 100.0
        if matched_votes + audit["county_cluster_fallback_votes"] else 0.0,
        4,
    )
    audit["unresolved_anomalies"] = [
        "Low-confidence VTD name matches remain explicitly estimated; review forced_best_name rows before treating 2012 as direct data.",
        "Only non-geographic absentee/early buckets use county-level modified-VAP allocation.",
    ]

    for scope in SCOPES:
        districts = [str(value) for value in range(1, 100 if scope == "state_house" else 34)]
        for contest in ("president", "us_senate"):
            floats = {field: defaultdict(float) for field in FIELDS}
            missing_block_votes = 0
            for (row_contest, county, vtd, _key), row in source_votes.items():
                if row_contest != contest:
                    continue
                members = blocks.get((county, vtd), [])
                if not members:
                    missing_block_votes += sum(row.values())
                    continue
                district_weights = defaultdict(float)
                for weight, district_map in members:
                    district_weights[district_map[scope]] += float(weight)
                if sum(district_weights.values()) <= 0:
                    for _weight, district_map in members:
                        district_weights[district_map[scope]] += 1.0
                denom = sum(district_weights.values())
                for district, weight in district_weights.items():
                    for field in FIELDS:
                        floats[field][district] += row[field] * weight / denom
            # Non-geographic absentee/early buckets have no defensible precinct
            # polygon. Allocate only these residuals within their own county by
            # modified VAP—the source hierarchy's last-resort county cluster.
            county_members = defaultdict(list)
            for (county, _vtd), members in blocks.items():
                county_members[county].extend(members)
            for (row_contest, county), row in nongeographic.items():
                if row_contest != contest:
                    continue
                district_weights = defaultdict(float)
                for weight, district_map in county_members.get(county, []):
                    district_weights[district_map[scope]] += float(weight)
                if sum(district_weights.values()) <= 0:
                    for _weight, district_map in county_members.get(county, []):
                        district_weights[district_map[scope]] += 1.0
                denom = sum(district_weights.values())
                if denom <= 0:
                    missing_block_votes += sum(row.values())
                    continue
                for district, weight in district_weights.items():
                    for field in FIELDS:
                        floats[field][district] += row[field] * weight / denom
            targets = certified_targets(contest, YEAR)
            allocation = {field: largest_remainder(targets[field], floats[field], districts) for field in FIELDS}
            results = {d: result_row({field: allocation[field][d] for field in FIELDS}) for d in districts}
            path = OUTPUT_DIR / f"{scope}_{contest}_{YEAR}.json"
            old = load_json(path) if path.exists() else {}
            payload = {
                "scope": scope, "contest_type": contest, "year": YEAR,
                "meta": {
                    "source": "estimated_2012_Census2010_block_modified_VAP",
                    "estimated": True,
                    "source_quality_rank": 3,
                    "precinct_source": SOURCE.name,
                    "precinct_geography": VTD_ZIP.name,
                    "block_geography": BLOCK_ZIP.name,
                    "modified_vap": "P003001_minus_P042003",
                    "block_assignment": "2010_VTD_and_enacted_2022_TIGER_representative_point",
                    "reconciliation": "party_specific_statewide_largest_remainder",
                    "certified_statewide_targets": targets,
                    "districts": len(districts),
                },
                "general": {"results": results},
            }
            path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
            audit["files"].append({
                "file": path.name, "missing_block_votes_before_reconciliation": missing_block_votes,
                "statewide_targets": targets,
                "statewide_after": {field: sum(row[field] for row in results.values()) for field in FIELDS},
                **compare(old.get("general", {}).get("results", {}), results),
            })
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_PATH.write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    refresh_manifest(OUTPUT_DIR)
    print(json.dumps({
        "valid_source_join": not unmatched,
        "unmatched_votes": sum(unmatched.values()),
        "files": len(audit["files"]),
        "missing_block_votes": sum(item["missing_block_votes_before_reconciliation"] for item in audit["files"]),
    }, indent=2))


if __name__ == "__main__":
    main()
