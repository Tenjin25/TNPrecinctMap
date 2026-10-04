#!/usr/bin/env python3
"""Pilot an RDH-style 2012 election disaggregation through the NHGIS block crosswalk."""

from __future__ import annotations

import argparse
import json
import re
import sys
import zipfile
from collections import defaultdict
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio


FIELDS = ("dem_votes", "rep_votes", "other_votes")
CVAP_SOURCE_FIELDS = ("CVAP_TOT24", "CVAP_WHT24", "CVAP_BLA24", "CVAP_HSP24")
CVAP_FEATURES = ("CVAP_WHT24", "CVAP_BLA24", "CVAP_HSP24", "CVAP_OTH24")
CONTEST_OFFICES = {
    "President": "president",
    "United States Senate": "us_senate",
}


def norm_text(value: str) -> str:
    value = str(value or "").strip().upper()
    value = re.sub(r"[\u2018\u2019]", "'", value)
    value = re.sub(r"[^A-Z0-9 ]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def norm_county(value: str) -> str:
    return re.sub(r"\s+COUNTY$", "", norm_text(value)).strip()


def party_field(value: str) -> str:
    party = str(value or "").strip().upper()
    return "dem_votes" if party == "D" else "rep_votes" if party == "R" else "other_votes"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def largest_remainder(target: int, values: dict[str, float]) -> dict[str, int]:
    keys = sorted(values, key=int)
    total = sum(max(0.0, float(values[key])) for key in keys)
    if total <= 0:
        return {key: 0 for key in keys}
    exact = {key: target * max(0.0, float(values[key])) / total for key in keys}
    out = {key: int(exact[key]) for key in keys}
    order = sorted(keys, key=lambda key: (exact[key] - out[key], -int(key)), reverse=True)
    for key in order[: target - sum(out.values())]:
        out[key] += 1
    return out


def certified_targets(data_dir: Path, contest: str) -> dict[str, int]:
    payload = load_json(data_dir / "contests" / f"{contest}_2012.json")
    return {
        field: sum(int(row.get(field, 0) or 0) for row in payload.get("rows", []))
        for field in FIELDS
    }


def build_target_block_to_vtd10(data_dir: Path) -> tuple[pd.DataFrame, dict]:
    blocks10 = pyogrio.read_dataframe(
        f"zip://{(data_dir / 'tl_2012_47_tabblock10.zip').resolve()}",
        columns=["GEOID", "COUNTYFP10", "ALAND", "AWATER"],
    )
    blocks10["source_block_geoid"] = blocks10["GEOID"].astype(str).str.zfill(15)
    blocks10["source_area"] = (
        pd.to_numeric(blocks10["ALAND"], errors="coerce").fillna(0)
        + pd.to_numeric(blocks10["AWATER"], errors="coerce").fillna(0)
    ).clip(lower=1)
    points = blocks10[["source_block_geoid", "source_area", "geometry"]].copy()
    points.geometry = points.geometry.representative_point()
    vtd10 = pyogrio.read_dataframe(
        data_dir / "tn_vtd_2010_census_county_merged.geojson",
        columns=["COUNTYFP10", "VTDST10"],
    ).to_crs(points.crs)
    assigned = gpd.sjoin(
        points, vtd10[["COUNTYFP10", "VTDST10", "geometry"]], how="left", predicate="within"
    )
    missing = assigned["VTDST10"].isna()
    nearest_fallbacks = int(missing.sum())
    if nearest_fallbacks:
        missing_points = assigned.loc[missing, ["source_block_geoid", "source_area", "geometry"]].to_crs(5070)
        nearest = gpd.sjoin_nearest(
            missing_points,
            vtd10[["COUNTYFP10", "VTDST10", "geometry"]].to_crs(5070),
            how="left",
        )
        nearest = nearest.drop_duplicates("source_block_geoid").set_index("source_block_geoid")
        assigned.loc[missing, "COUNTYFP10"] = assigned.loc[missing, "source_block_geoid"].map(nearest["COUNTYFP10"])
        assigned.loc[missing, "VTDST10"] = assigned.loc[missing, "source_block_geoid"].map(nearest["VTDST10"])
    if assigned["VTDST10"].isna().any():
        raise RuntimeError(f"2010 VTD assignment still missed {int(assigned['VTDST10'].isna().sum())} blocks")

    crosswalk = pd.read_csv(
        data_dir / "crosswalks" / "nhgis_blk2010_blk2020_47_tn_to_tn.csv",
        dtype={"source_block_geoid": str, "target_block_geoid": str},
    )
    crosswalk["source_block_geoid"] = crosswalk["source_block_geoid"].str.zfill(15)
    crosswalk["target_block_geoid"] = crosswalk["target_block_geoid"].str.zfill(15)
    crosswalk["weight"] = pd.to_numeric(crosswalk["weight"], errors="coerce").fillna(0)
    joined = crosswalk.merge(
        assigned[["source_block_geoid", "source_area", "COUNTYFP10", "VTDST10"]],
        on="source_block_geoid",
        how="left",
        validate="many_to_one",
    )
    joined["overlap_area_proxy"] = joined["source_area"] * joined["weight"]
    grouped = (
        joined.groupby(["target_block_geoid", "COUNTYFP10", "VTDST10"], as_index=False)["overlap_area_proxy"]
        .sum()
    )
    totals = grouped.groupby("target_block_geoid")["overlap_area_proxy"].transform("sum")
    grouped["vtd_membership"] = grouped["overlap_area_proxy"] / totals
    audit = {
        "source_2010_blocks": int(blocks10["source_block_geoid"].nunique()),
        "nearest_vtd_fallback_blocks": nearest_fallbacks,
        "nhgis_crosswalk_rows": int(len(crosswalk)),
        "target_2020_blocks": int(grouped["target_block_geoid"].nunique()),
        "fractionally_split_target_blocks": int(
            (grouped.groupby("target_block_geoid").size() > 1).sum()
        ),
    }
    return grouped[["target_block_geoid", "COUNTYFP10", "VTDST10", "vtd_membership"]], audit


def precinct_to_vtd10(data_dir: Path, crosswalk_data_dir: Path) -> tuple[pd.DataFrame, dict[str, str]]:
    path = crosswalk_data_dir / "crosswalks" / "tn_precinct_to_vtd20_blockweighted_2012__20121106_tn_general_precinct.csv"
    frame = pd.read_csv(path, dtype=str)
    frame = frame[[
        "county_norm", "from_precinct_norm", "src_vtdst", "match_method", "confidence_tier"
    ]].drop_duplicates()
    frame["county_norm"] = frame["county_norm"].map(norm_county)
    frame["from_precinct_norm"] = frame["from_precinct_norm"].map(norm_text)
    # Full-chain artifacts store a statewide six-character code; Census VTD10
    # uses the county-local four-character suffix.
    frame["src_vtdst"] = frame["src_vtdst"].astype(str).str.zfill(6).str[-4:]
    counties = pyogrio.read_dataframe(
        data_dir / "tl_2020_47_county20.geojson", read_geometry=False
    )
    county_map = {
        norm_county(row.NAME20): str(row.COUNTYFP20).zfill(3)
        for row in counties.itertuples()
    }
    frame["COUNTYFP10"] = frame["county_norm"].map(county_map)
    # Montgomery County's official GIS precinct service exposes the same numbered
    # 2010 VTDs as Census, while the 2012 result export subdivides those numbers
    # with A/B suffixes and polling-place labels.  Represent each 2012 label as the
    # union of every Census VTD sharing its leading number (not a forced single VTD).
    election = pd.read_csv(data_dir / "20121106__tn__general__precinct.csv", usecols=["county", "precinct"])
    montgomery_labels = election[election["county"].map(norm_county).eq("MONTGOMERY")]["precinct"].map(norm_text).unique()
    vtd10 = pyogrio.read_dataframe(
        data_dir / "tn_vtd_2010_census_county_merged.geojson",
        columns=["COUNTYFP10", "VTDST10", "NAME10"],
        read_geometry=False,
    )
    montgomery_vtds = vtd10[vtd10["COUNTYFP10"].astype(str).str.zfill(3).eq("125")].copy()
    montgomery_vtds["primary"] = montgomery_vtds["NAME10"].astype(str).str.extract(r"^(\d+)")[0].map(
        lambda value: str(int(value)) if pd.notna(value) else ""
    )
    additions = []
    for label in montgomery_labels:
        match = re.match(r"^(\d+)", label)
        if not match:
            continue
        primary = str(int(match.group(1)))
        for row in montgomery_vtds[montgomery_vtds["primary"].eq(primary)].itertuples():
            additions.append({
                "county_norm": "MONTGOMERY",
                "from_precinct_norm": label,
                "src_vtdst": str(row.VTDST10).zfill(4),
                "match_method": "montgomery_official_numeric_vtd_group",
                "confidence_tier": "high",
                "COUNTYFP10": "125",
            })
    if additions:
        frame = frame[frame["county_norm"].ne("MONTGOMERY")]
        frame = pd.concat([frame, pd.DataFrame(additions)], ignore_index=True)
    wilson_labels = election[election["county"].map(norm_county).eq("WILSON")]["precinct"].map(norm_text).unique()
    wilson_vtds = vtd10[vtd10["COUNTYFP10"].astype(str).str.zfill(3).eq("189")].copy()
    wilson_vtds["primary"] = wilson_vtds["NAME10"].astype(str).str.extract(r"^(\d+)")[0].map(
        lambda value: str(int(value)) if pd.notna(value) else ""
    )
    wilson_additions = []
    for label in wilson_labels:
        match = re.match(r"^(\d+)", label)
        if not match:
            continue
        primary = str(int(match.group(1)))
        for row in wilson_vtds[wilson_vtds["primary"].eq(primary)].itertuples():
            wilson_additions.append({
                "county_norm": "WILSON",
                "from_precinct_norm": label,
                "src_vtdst": str(row.VTDST10).zfill(4),
                "match_method": "wilson_numeric_vtd_group",
                "confidence_tier": "high",
                "COUNTYFP10": "189",
            })
    if wilson_additions:
        frame = frame[frame["county_norm"].ne("WILSON")]
        frame = pd.concat([frame, pd.DataFrame(wilson_additions)], ignore_index=True)
    # Sullivan's official precinct list retains the 2012 lettered codes and names
    # polling locations that unambiguously match these 2010 Census VTD names.
    # Keep uncertain renamed locations on county fallback rather than guessing.
    sullivan_aliases = {
        "1A": "8696",   # South Holston Ruritan
        "2B": "8700",   # Holston View School
        "2C": "8712",   # Avoca School
        "3A": "8864",   # Anderson School
        "4A": "8720",   # Sullivan County Offices
        "4B": "8884",   # East High School
        "4C": "8740",   # Buffalo Ruritan
        "5C": "8708",   # Hickory Tree Firehall
        "6A": "8732",   # Indian Springs School
        "6C": "8728",   # Central Heights School
        "7A": "8828",   # Colonial Heights
        "7B": "8832",   # Miller Perry
        "7C": "8868",   # Holston School
        "9B": "8800",   # Clouds Bend UMC
        "10A": "8768",  # Traders Village
        "11A": "8788",  # Civic Auditorium
        "11B": "8792",  # Kingsport core
    }
    frame = frame[
        ~(
            frame["county_norm"].eq("SULLIVAN")
            & frame["from_precinct_norm"].isin(sullivan_aliases)
        )
    ]
    sullivan_rows = [{
        "county_norm": "SULLIVAN",
        "from_precinct_norm": label,
        "src_vtdst": vtd,
        "match_method": "sullivan_official_polling_place_vtd",
        "confidence_tier": "high",
        "COUNTYFP10": "163",
    } for label, vtd in sullivan_aliases.items()]
    frame = pd.concat([frame, pd.DataFrame(sullivan_rows)], ignore_index=True)
    # Washington County's official precinct directory and linked maps preserve
    # these polling-place names.  Match only unique names/city-side variants.
    washington_aliases = {
        "03 GRAY EAST": "9516",
        "05 B C EAST": "9508",
        "07 GRAY WEST": "9518",
        "08 B C WEST": "9515",
        "10 LAKERIDGE": "9513",
        "14 BOWMANTOWN": "9540",
        "19 LEESBURG": "9536",
        "21 ASBURY": "9464",
        "23 WOODLAND": "9468",
        "31 CHEROKEE CITY": "9432",
        "35 LIMESTONE": "9544",
        "39 CONKLIN": "9396",
    }
    frame = frame[
        ~(
            frame["county_norm"].eq("WASHINGTON")
            & frame["from_precinct_norm"].isin(washington_aliases)
        )
    ]
    washington_rows = [{
        "county_norm": "WASHINGTON",
        "from_precinct_norm": label,
        "src_vtdst": vtd,
        "match_method": "washington_official_polling_place_vtd",
        "confidence_tier": "high",
        "COUNTYFP10": "179",
    } for label, vtd in washington_aliases.items()]
    frame = pd.concat([frame, pd.DataFrame(washington_rows)], ignore_index=True)
    grouped_aliases = {
        # Knox: exact polling-place matches, plus code-defined VTD unions.
        ("KNOX", "72 DANTE"): ("4732", "4733", "4734"),
        ("KNOX", "16 LARRY COX SR CTR"): ("4552", "4556"),
        ("KNOX", "25 SOUTH KNOX CC"): ("4828",),
        ("KNOX", "11 CENTRAL UMC"): ("4544",),
        ("KNOX", "34 FOUNT CITY LIB"): ("4748",),
        # Madison: exact 2010 VTD code and polling-place matches.
        ("MADISON", "10 3 NORTH SIDE"): ("5520",),
        ("MADISON", "4 1 MASONIC LODGE"): ("5476",),
        ("MADISON", "10 2 NORTH EAST"): ("5532",),
        ("MADISON", "3 3 J CIL"): ("5528",),
        ("MADISON", "1 2 MT MORIAH"): ("5442",),
        ("MADISON", "7 2 TN TECH CTR"): ("5502",),
        ("MADISON", "8 2 BROWNS"): ("5512",),
        ("MADISON", "3 2 OLD BELLS RD 10"): ("5462",),
    }
    grouped_keys = set(grouped_aliases)
    frame = frame[
        ~frame.apply(
            lambda row: (row["county_norm"], row["from_precinct_norm"]) in grouped_keys,
            axis=1,
        )
    ]
    grouped_rows = []
    for (county, label), vtds in grouped_aliases.items():
        fips = county_map[county]
        for vtd in vtds:
            grouped_rows.append({
                "county_norm": county,
                "from_precinct_norm": label,
                "src_vtdst": vtd,
                "match_method": "official_code_polling_place_vtd_group",
                "confidence_tier": "high",
                "COUNTYFP10": fips,
            })
    frame = pd.concat([frame, pd.DataFrame(grouped_rows)], ignore_index=True)
    return frame, county_map


def load_2016_proxy_block_membership(proxy_zip: Path | None) -> pd.DataFrame:
    """Load conservative 2016 block proxies for unresolved 2012 precincts.

    These labels are present in both election vintages with the same precinct code
    (or, for Hamilton/Washington, the same distinctive name).  The proxy is kept
    separate from the VTD10 crosswalk so its medium-confidence vintage assumption
    remains visible in the audit.
    """
    columns = ["county_norm", "from_precinct_norm", "GEOID20", "proxy_source"]
    if proxy_zip is None or not proxy_zip.exists():
        return pd.DataFrame(columns=columns)
    shp_name = proxy_zip.stem + ".shp"
    uri = f"/vsizip/{proxy_zip.as_posix()}/{shp_name}"
    blocks = pyogrio.read_dataframe(
        uri,
        columns=["GEOID20", "COUNTYFP", "PRECINCTID"],
        read_geometry=False,
    )
    blocks["COUNTYFP"] = blocks["COUNTYFP"].astype(str).str.zfill(3)
    blocks["GEOID20"] = blocks["GEOID20"].astype(str).str.zfill(15)
    blocks["precinct_id"] = blocks["PRECINCTID"].map(norm_text)

    davidson_codes = {
        "25 3", "26 2", "18 4", "28 2", "07 6", "16 1", "03 3",
        "20 1", "14 4", "18 5", "23 5", "19 6", "12 5",
    }
    sullivan_codes = {
        # Only the eight codes still unresolved after official polling-place/VTD
        # matches; retain the 17 high-confidence official matches above.
        "2A", "5A", "5B", "6B", "8A", "8B", "9A", "10B",
    }
    exact_ids = {
        ("065", "3365 SIGNAL MOUNTAIN 1"): ("HAMILTON", "207 SIGNAL MTN 1"),
        ("065", "3402 SIGNAL MOUNTAIN 2"): ("HAMILTON", "200 SIGNAL MTN 2"),
        ("179", "9334 9 BOONES CREEK CITY"): ("WASHINGTON", "09 B C CITY"),
        ("179", "9317 30 GRACE FELLOWSHIP CHURCH"): ("WASHINGTON", "30 GRACE"),
    }
    rows = []
    for row in blocks.itertuples(index=False):
        key = (row.COUNTYFP, row.precinct_id)
        target = exact_ids.get(key)
        if row.COUNTYFP == "037":
            match = re.search(r"(\d{2})\s+(\d+)$", row.precinct_id)
            code = f"{match.group(1)} {match.group(2)}" if match else ""
            if code in davidson_codes:
                target = ("DAVIDSON", code)
        elif row.COUNTYFP == "163":
            match = re.search(r"(\d{1,2}[A-Z])$", row.precinct_id)
            code = match.group(1) if match else ""
            if code in sullivan_codes:
                target = ("SULLIVAN", code)
        if target:
            rows.append({
                "county_norm": target[0],
                "from_precinct_norm": target[1],
                "GEOID20": row.GEOID20,
                "proxy_source": "vest_rdh_2016_block_assignment",
            })
    return pd.DataFrame(rows, columns=columns).drop_duplicates()


def load_2012_house_ballot_constraints(
    data_dir: Path, election: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return old-plan block districts and per-bucket House-ballot weights.

    The December 2020 Census BlockAssign SLDL file represents the legislative
    plan used from 2012 through 2020.  State House votes reported within an
    early/absentee/safety bucket reveal which old-plan district ballots made up
    that bucket, allowing a constrained allocation instead of a countywide one.
    """
    archive_path = data_dir / "BlockAssign_ST47_TN.zip"
    if not archive_path.exists():
        return (
            pd.DataFrame(columns=["GEOID20", "old_house_district"]),
            pd.DataFrame(columns=["county_norm", "from_precinct_norm", "old_house_district", "district_share"]),
        )
    with zipfile.ZipFile(archive_path) as archive:
        member = "BlockAssign_ST47_TN_SLDL.txt"
        with archive.open(member) as source:
            assignments = pd.read_csv(source, sep="|", dtype=str)
    assignments = assignments.rename(columns={"BLOCKID": "GEOID20", "DISTRICT": "old_house_district"})
    assignments["GEOID20"] = assignments["GEOID20"].astype(str).str.zfill(15)
    assignments["old_house_district"] = (
        pd.to_numeric(assignments["old_house_district"], errors="coerce").astype("Int64").astype(str)
    )

    house = election[election["office"].eq("State House District")].copy()
    house["county_norm"] = house["county"].map(norm_county)
    house["from_precinct_norm"] = house["precinct"].map(norm_text)
    house["old_house_district"] = (
        pd.to_numeric(house["district"], errors="coerce").astype("Int64").astype(str)
    )
    house["votes"] = pd.to_numeric(house["votes"], errors="coerce").fillna(0)
    ballot = house.groupby(
        ["county_norm", "from_precinct_norm", "old_house_district"], as_index=False
    )["votes"].sum()
    totals = ballot.groupby(["county_norm", "from_precinct_norm"])["votes"].transform("sum")
    ballot = ballot[totals > 0].copy()
    ballot["district_share"] = ballot["votes"] / totals[totals > 0]
    return assignments[["GEOID20", "old_house_district"]], ballot[
        ["county_norm", "from_precinct_norm", "old_house_district", "district_share"]
    ]


def load_allocation_weights(
    data_dir: Path, weight_scheme: str, cvap_csv: Path | None
) -> pd.DataFrame:
    import zipfile

    path = data_dir / "tn_2016_gen_2020_blocks.zip"
    with zipfile.ZipFile(path) as archive:
        shp = next(name for name in archive.namelist() if name.lower().endswith(".shp"))
    frame = pyogrio.read_dataframe(
        f"zip://{path.resolve()}!{shp}", columns=["GEOID20", "COUNTYFP", "VAP_MOD"], read_geometry=False
    )
    frame["GEOID20"] = frame["GEOID20"].astype(str).str.zfill(15)
    frame["COUNTYFP"] = frame["COUNTYFP"].astype(str).str.zfill(3)
    frame["VAP_MOD"] = pd.to_numeric(frame["VAP_MOD"], errors="coerce").fillna(0).clip(lower=0)
    if weight_scheme == "cvap":
        if cvap_csv is None:
            raise RuntimeError("--cvap-csv is required for --weight-scheme cvap")
        cvap = pd.read_csv(cvap_csv, usecols=["GEOID20", "CVAP_TOT24"], dtype={"GEOID20": str})
        cvap["GEOID20"] = cvap["GEOID20"].str.zfill(15)
        cvap["CVAP_TOT24"] = pd.to_numeric(cvap["CVAP_TOT24"], errors="coerce").fillna(0).clip(lower=0)
        frame = frame.merge(cvap, on="GEOID20", how="left", validate="one_to_one")
        if frame["CVAP_TOT24"].isna().any():
            raise RuntimeError(f"CVAP file missed {int(frame['CVAP_TOT24'].isna().sum())} blocks")
        frame["allocation_weight"] = frame["CVAP_TOT24"]
    else:
        frame["allocation_weight"] = frame["VAP_MOD"]
    return frame


def build_demographic_party_weights(
    membership: pd.DataFrame,
    votes: pd.DataFrame,
    cvap_csv: Path,
    block_counties: pd.DataFrame,
) -> tuple[pd.DataFrame, dict, pd.DataFrame]:
    cvap = pd.read_csv(cvap_csv, usecols=["GEOID20", *CVAP_SOURCE_FIELDS], dtype={"GEOID20": str})
    cvap["GEOID20"] = cvap["GEOID20"].str.zfill(15)
    for column in CVAP_SOURCE_FIELDS:
        cvap[column] = pd.to_numeric(cvap[column], errors="coerce").fillna(0).clip(lower=0)
    cvap["CVAP_OTH24"] = (
        cvap["CVAP_TOT24"] - cvap["CVAP_WHT24"] - cvap["CVAP_BLA24"] - cvap["CVAP_HSP24"]
    ).clip(lower=0)
    member_features = membership.merge(
        cvap,
        left_on="target_block_geoid",
        right_on="GEOID20",
        how="inner",
        validate="many_to_one",
    )
    for column in CVAP_FEATURES:
        member_features[column] *= member_features["vtd_membership"]
    vtd_features = member_features.groupby(["COUNTYFP10", "VTDST10"], as_index=False)[list(CVAP_FEATURES)].sum()
    unique_training = votes[~votes["source_vote_id"].duplicated(keep=False)]
    training = unique_training[unique_training["confidence_tier"].eq("high")].merge(
        vtd_features,
        left_on=["COUNTYFP10", "src_vtdst"],
        right_on=["COUNTYFP10", "VTDST10"],
        how="inner",
    )
    aggregations = {"votes": ("votes", "sum")}
    aggregations.update({column: (column, "first") for column in CVAP_FEATURES})
    training = training.groupby(
        ["COUNTYFP10", "src_vtdst", "contest", "field"], as_index=False
    ).agg(**aggregations)

    block_features = block_counties.merge(cvap, on="GEOID20", how="left", validate="one_to_one")
    if block_features[list(CVAP_FEATURES)].isna().any().any():
        raise RuntimeError("Race-specific CVAP file does not cover every allocation block")
    outputs = []
    coefficient_audit = {}
    for (contest, field), frame in training.groupby(["contest", "field"]):
        coefficients = np.linalg.lstsq(
            frame[list(CVAP_FEATURES)].to_numpy(dtype=float),
            frame["votes"].to_numpy(dtype=float),
            rcond=1e-8,
        )[0]
        coefficients = np.clip(coefficients, 0, None)
        predicted = np.maximum(
            block_features[list(CVAP_FEATURES)].to_numpy(dtype=float) @ coefficients, 0
        )
        # Empty demographic cells retain a tiny total-CVAP proxy so a populated
        # county/VTD cannot become mathematically unallocatable.
        total_cvap = block_features[list(CVAP_FEATURES)].sum(axis=1).to_numpy(dtype=float)
        predicted = np.where(predicted > 0, predicted, total_cvap)
        out = block_features[["GEOID20", "COUNTYFP"]].copy()
        out["contest"] = contest
        out["field"] = field
        out["allocation_weight"] = predicted
        outputs.append(out)
        coefficient_audit[f"{contest}:{field}"] = {
            column: round(float(value), 8)
            for column, value in zip(CVAP_FEATURES, coefficients)
        }
    return pd.concat(outputs, ignore_index=True), coefficient_audit


def allocate_votes_to_blocks(
    data_dir: Path,
    crosswalk_data_dir: Path,
    weight_scheme: str = "vap_mod",
    cvap_csv: Path | None = None,
    proxy_block_zip: Path | None = None,
) -> tuple[pd.DataFrame, dict]:
    membership, audit = build_target_block_to_vtd10(data_dir)
    precinct_map, county_map = precinct_to_vtd10(data_dir, crosswalk_data_dir)
    base_blocks = load_allocation_weights(data_dir, "vap_mod", None)

    all_election = pd.read_csv(data_dir / "20121106__tn__general__precinct.csv")
    election = all_election[all_election["office"].isin(CONTEST_OFFICES)].copy()
    election["county_norm"] = election["county"].map(norm_county)
    election["from_precinct_norm"] = election["precinct"].map(norm_text)
    election["contest"] = election["office"].map(CONTEST_OFFICES)
    election["field"] = election["party"].map(party_field)
    election["votes"] = pd.to_numeric(election["votes"], errors="coerce").fillna(0)
    source_vote_rows = election.groupby(
        ["county_norm", "from_precinct_norm", "contest", "field"], as_index=False
    )["votes"].sum()
    source_vote_rows["source_vote_id"] = range(len(source_vote_rows))
    direct_membership = load_2016_proxy_block_membership(proxy_block_zip)
    direct_keys = direct_membership[["county_norm", "from_precinct_norm"]].drop_duplicates()
    direct_votes = source_vote_rows.merge(
        direct_keys, on=["county_norm", "from_precinct_norm"], how="inner"
    ).merge(
        direct_membership,
        on=["county_norm", "from_precinct_norm"],
        how="inner",
        validate="many_to_many",
    )
    direct_ids = set(direct_votes["source_vote_id"])
    votes = source_vote_rows[~source_vote_rows["source_vote_id"].isin(direct_ids)].merge(
        precinct_map,
        on=["county_norm", "from_precinct_norm"],
        how="left",
        validate="many_to_many",
    )
    mapped_audit_rows = votes.drop_duplicates("source_vote_id")[
        ["source_vote_id", "confidence_tier"]
    ]
    direct_audit_rows = direct_votes.drop_duplicates("source_vote_id")[["source_vote_id"]].assign(
        confidence_tier="medium"
    )
    confidence_rows = source_vote_rows.merge(
        pd.concat([mapped_audit_rows, direct_audit_rows], ignore_index=True),
        on="source_vote_id",
        how="left",
        validate="one_to_one",
    )
    coefficient_audit = None
    if weight_scheme == "cvap_demographic":
        if cvap_csv is None:
            raise RuntimeError("--cvap-csv is required for demographic CVAP")
        block_weights, coefficient_audit = build_demographic_party_weights(
            membership,
            votes,
            cvap_csv,
            base_blocks[["GEOID20", "COUNTYFP"]],
        )
    else:
        block_weights = load_allocation_weights(data_dir, weight_scheme, cvap_csv)[
            ["GEOID20", "COUNTYFP", "allocation_weight"]
        ]
    membership = membership.merge(
        block_weights, left_on="target_block_geoid", right_on="GEOID20", how="right"
    )
    membership["COUNTYFP10"] = membership["COUNTYFP10"].fillna(membership["COUNTYFP"])
    membership["vtd_membership"] = membership["vtd_membership"].fillna(1.0)
    membership["allocation_mass"] = membership["allocation_weight"] * membership["vtd_membership"]

    direct_weight_keys = ["GEOID20"]
    if weight_scheme == "cvap_demographic":
        direct_weight_keys += ["contest", "field"]
    direct_join = direct_votes.merge(block_weights, on=direct_weight_keys, how="left")
    direct_mass = direct_join.groupby("source_vote_id")["allocation_weight"].transform("sum")
    direct_count = direct_join.groupby("source_vote_id")["GEOID20"].transform("count")
    direct_join["share"] = (direct_join["allocation_weight"] / direct_mass).where(
        direct_mass > 0, 1.0 / direct_count
    )
    direct_join["block_votes"] = direct_join["votes"] * direct_join["share"]
    geographic = votes[votes["src_vtdst"].notna()].copy()
    non_geo = votes[votes["src_vtdst"].isna()].copy()

    block_rows = membership[[
        column for column in (
            "GEOID20", "COUNTYFP10", "VTDST10", "contest", "field",
            "allocation_mass", "vtd_membership"
        ) if column in membership.columns
    ]].copy()
    geo_keys_left = ["COUNTYFP10", "src_vtdst"]
    geo_keys_right = ["COUNTYFP10", "VTDST10"]
    if weight_scheme == "cvap_demographic":
        geo_keys_left += ["contest", "field"]
        geo_keys_right += ["contest", "field"]
    geo_join = geographic.merge(
        block_rows,
        left_on=geo_keys_left,
        right_on=geo_keys_right,
        how="left",
    )
    geo_mass = geo_join.groupby("source_vote_id")["allocation_mass"].transform("sum")
    geo_fallback = geo_join.groupby("source_vote_id")["vtd_membership"].transform("sum")
    successful_geo = geo_join[geo_join["GEOID20"].notna() & ((geo_mass > 0) | (geo_fallback > 0))].copy()
    failed_geo_ids = set(geographic["source_vote_id"]) - set(successful_geo["source_vote_id"])
    failed_geo = geographic[geographic["source_vote_id"].isin(failed_geo_ids)].drop_duplicates(
        "source_vote_id"
    ).copy()
    geo_join = successful_geo
    geo_mass = geo_join.groupby("source_vote_id")["allocation_mass"].transform("sum")
    geo_fallback = geo_join.groupby("source_vote_id")["vtd_membership"].transform("sum")
    geo_join["share"] = (geo_join["allocation_mass"] / geo_mass).where(
        geo_mass > 0, geo_join["vtd_membership"] / geo_fallback
    )
    geo_join["block_votes"] = geo_join["votes"] * geo_join["share"]

    fallback = pd.concat([non_geo, failed_geo], ignore_index=True)
    fallback["COUNTYFP10"] = fallback["county_norm"].map(county_map)
    county_blocks = block_weights.copy()
    old_house_assignment, house_ballot = load_2012_house_ballot_constraints(data_dir, all_election)
    county_blocks = county_blocks.merge(old_house_assignment, on="GEOID20", how="left")
    county_group = ["COUNTYFP"]
    fallback_keys_left = ["COUNTYFP10"]
    fallback_keys_right = ["COUNTYFP"]
    if weight_scheme == "cvap_demographic":
        county_group += ["contest", "field"]
        fallback_keys_left += ["contest", "field"]
        fallback_keys_right += ["contest", "field"]
    county_blocks["county_mass"] = county_blocks.groupby(county_group)["allocation_weight"].transform("sum")
    county_blocks["county_count"] = county_blocks.groupby(county_group)["GEOID20"].transform("count")
    county_blocks["county_share"] = (county_blocks["allocation_weight"] / county_blocks["county_mass"]).where(
        county_blocks["county_mass"] > 0, 1.0 / county_blocks["county_count"]
    )
    constrained = fallback.merge(
        house_ballot,
        on=["county_norm", "from_precinct_norm"],
        how="inner",
        validate="many_to_many",
    )
    constrained_ids = set(constrained["source_vote_id"])
    unconstrained = fallback[~fallback["source_vote_id"].isin(constrained_ids)].copy()
    constrained_keys_left = [*fallback_keys_left, "old_house_district"]
    constrained_keys_right = [*fallback_keys_right, "old_house_district"]
    constrained_join = constrained.merge(
        county_blocks,
        left_on=constrained_keys_left,
        right_on=constrained_keys_right,
        how="left",
    )
    constrained_group = ["source_vote_id", "old_house_district"]
    constrained_mass = constrained_join.groupby(constrained_group)["allocation_weight"].transform("sum")
    constrained_count = constrained_join.groupby(constrained_group)["GEOID20"].transform("count")
    constrained_join["within_district_share"] = (
        constrained_join["allocation_weight"] / constrained_mass
    ).where(constrained_mass > 0, 1.0 / constrained_count)
    constrained_join["block_votes"] = (
        constrained_join["votes"]
        * constrained_join["district_share"]
        * constrained_join["within_district_share"]
    )
    non_join = unconstrained.merge(
        county_blocks, left_on=fallback_keys_left, right_on=fallback_keys_right, how="left"
    )
    fallback_joins = pd.concat([constrained_join, non_join], ignore_index=True)
    if fallback_joins["GEOID20"].isna().any():
        missing = sorted(fallback_joins.loc[fallback_joins["GEOID20"].isna(), "county_norm"].unique())
        raise RuntimeError(f"County fallback has no 2020 blocks for: {missing}")
    non_join["block_votes"] = non_join["votes"] * non_join["county_share"]

    allocation_parts = []
    for frame, method in (
        (direct_join, "stable_2016_block_proxy"),
        (geo_join, "historical_vtd_geometry"),
        (constrained_join, "house_ballot_constrained_fallback"),
        (non_join, "countywide_fallback"),
    ):
        part = frame[[
            "GEOID20", "contest", "field", "block_votes", "source_vote_id",
            "county_norm", "from_precinct_norm",
        ]].copy()
        part["allocation_method"] = method
        allocation_parts.append(part)
    source_allocated = pd.concat(allocation_parts, ignore_index=True)
    allocated = source_allocated[["GEOID20", "contest", "field", "block_votes"]].copy()
    allocated = allocated.groupby(["GEOID20", "contest", "field"], as_index=False)["block_votes"].sum()
    source_totals = source_vote_rows.groupby(["contest", "field"])["votes"].sum().sort_index()
    allocated_totals = allocated.groupby(["contest", "field"])["block_votes"].sum().sort_index()
    deltas = source_totals.subtract(allocated_totals, fill_value=0)
    if (deltas.abs() > 1e-6).any():
        raise RuntimeError(f"Vote allocation failed conservation check: {deltas.to_dict()}")
    audit.update({
        "weight_scheme": weight_scheme,
        "cvap_source": str(cvap_csv) if cvap_csv else None,
        "demographic_coefficients": coefficient_audit,
        "source_precinct_labels": int(source_vote_rows[["county_norm", "from_precinct_norm"]].drop_duplicates().shape[0]),
        "geographic_vote_rows": int(len(geographic)),
        "countywide_non_geographic_vote_rows": int(len(non_geo)),
        "mapped_vote_rows_requiring_county_fallback": int(len(failed_geo)),
        "house_ballot_constrained_fallback_vote_rows": int(len(constrained_ids)),
        "house_ballot_constrained_votes_by_county": {
            f"{contest}:{county}": round(float(value), 6)
            for (contest, county), value in constrained.drop_duplicates("source_vote_id").groupby(
                ["contest", "county_norm"]
            )["votes"].sum().items()
        },
        "unconstrained_countywide_fallback_vote_rows": int(
            unconstrained["source_vote_id"].nunique()
        ),
        "direct_2016_proxy_vote_rows": int(direct_votes["source_vote_id"].nunique()),
        "direct_2016_proxy_precincts": int(
            direct_votes[["county_norm", "from_precinct_norm"]].drop_duplicates().shape[0]
        ),
        "direct_2016_proxy_votes_by_county": {
            f"{contest}:{county}": round(float(value), 6)
            for (contest, county), value in direct_votes.drop_duplicates("source_vote_id").groupby(
                ["contest", "county_norm"]
            )["votes"].sum().items()
        },
        "confidence_vote_totals": {
            f"{contest}:{tier}": round(float(value), 6)
            for (contest, tier), value in confidence_rows.assign(
                confidence_tier=confidence_rows["confidence_tier"].fillna("unmatched")
            ).groupby(["contest", "confidence_tier"])["votes"].sum().items()
        },
        "fallback_vote_totals_by_county": {
            f"{contest}:{county}": round(float(value), 6)
            for (contest, county), value in fallback.groupby(["contest", "county_norm"])["votes"].sum().items()
        },
        "allocated_2020_blocks": int(allocated["GEOID20"].nunique()),
        "raw_source_totals": {
            f"{contest}:{field}": round(float(value), 6)
            for (contest, field), value in source_vote_rows.groupby(["contest", "field"])["votes"].sum().items()
        },
        "allocated_totals": {
            f"{contest}:{field}": round(float(value), 6)
            for (contest, field), value in allocated.groupby(["contest", "field"])["block_votes"].sum().items()
        },
    })
    return allocated, audit, source_allocated


def finalize(row: dict) -> None:
    row["total_votes"] = sum(int(row[field]) for field in FIELDS)
    row["margin"] = int(row["rep_votes"]) - int(row["dem_votes"])
    row["margin_pct"] = round(100 * row["margin"] / row["total_votes"], 4) if row["total_votes"] else 0
    row["winner"] = "REP" if row["margin"] > 0 else "DEM" if row["margin"] < 0 else "TIE"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--published-data-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--weight-scheme", choices=("vap_mod", "cvap", "cvap_demographic"), default="vap_mod"
    )
    parser.add_argument("--cvap-csv", type=Path)
    parser.add_argument(
        "--proxy-block-zip",
        type=Path,
        help="Optional RDH/VEST 2016-on-2020-block ZIP for conservative stable-precinct proxies.",
    )
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    sys.path.insert(0, str((Path(__file__).parent).resolve()))
    from build_tn_legislative_from_blocks import current_plan_assignment

    allocated, audit, source_allocated = allocate_votes_to_blocks(
        args.data_dir,
        args.published_data_dir,
        args.weight_scheme,
        args.cvap_csv,
        args.proxy_block_zip,
    )
    specs = (
        ("state_house", 2022, "district_contests"),
        ("state_senate", 2022, "district_contests"),
        ("congressional", 2022, "district_contests"),
        ("congressional", 2026, "district_contests_2026"),
    )
    comparisons = []
    sensitivity_attribution = []
    for scope, lines_year, subdir in specs:
        assignment, assignment_audit = current_plan_assignment(
            args.data_dir, args.published_data_dir, scope, lines_year
        )
        joined = allocated.merge(assignment, on="GEOID20", how="inner", validate="many_to_one")
        if scope == "state_house" and lines_year == 2022:
            source_joined = source_allocated.merge(
                assignment, on="GEOID20", how="inner", validate="many_to_one"
            )
            for target_district, target_contest in (("75", "president"), ("80", "us_senate")):
                focus = source_joined[
                    source_joined["district"].eq(target_district)
                    & source_joined["contest"].eq(target_contest)
                ].copy()
                pivot = focus.pivot_table(
                    index=["county_norm", "from_precinct_norm", "allocation_method"],
                    columns="field",
                    values="block_votes",
                    aggfunc="sum",
                    fill_value=0,
                ).reset_index()
                for field in FIELDS:
                    if field not in pivot:
                        pivot[field] = 0.0
                pivot["total_votes"] = pivot[list(FIELDS)].sum(axis=1)
                pivot["margin"] = pivot["rep_votes"] - pivot["dem_votes"]
                top_sources = []
                for row in pivot.sort_values("total_votes", ascending=False).head(30).to_dict("records"):
                    top_sources.append({
                        "county": row["county_norm"],
                        "precinct": row["from_precinct_norm"],
                        "allocation_method": row["allocation_method"],
                        "dem_votes": round(float(row["dem_votes"]), 3),
                        "rep_votes": round(float(row["rep_votes"]), 3),
                        "other_votes": round(float(row["other_votes"]), 3),
                        "total_votes": round(float(row["total_votes"]), 3),
                        "margin": round(float(row["margin"]), 3),
                    })
                method_totals = focus.groupby("allocation_method")["block_votes"].sum().sort_values(
                    ascending=False
                )
                sensitivity_attribution.append({
                    "district": target_district,
                    "contest": target_contest,
                    "raw_allocated_votes": round(float(focus["block_votes"].sum()), 3),
                    "allocation_method_votes": {
                        method: round(float(value), 3) for method, value in method_totals.items()
                    },
                    "top_sources": top_sources,
                })
        for contest in CONTEST_OFFICES.values():
            template_path = args.published_data_dir / subdir / f"{scope}_{contest}_2012.json"
            if not template_path.exists():
                continue
            payload = load_json(template_path)
            old = payload.get("general", {}).get("results", {})
            districts = sorted(old, key=int)
            targets = certified_targets(args.published_data_dir, contest)
            allocations = {}
            for field in FIELDS:
                selected = joined[(joined["contest"] == contest) & (joined["field"] == field)]
                raw = selected.groupby("district")["block_votes"].sum().to_dict()
                if scope == "congressional" and lines_year == 2026:
                    anchors = {d: int(old[d].get(field, 0) or 0) for d in ("1", "2")}
                    remainder = targets[field] - sum(anchors.values())
                    adjustable = {d: raw.get(d, 0.0) for d in districts if d not in anchors}
                    allocations[field] = {**anchors, **largest_remainder(remainder, adjustable)}
                else:
                    allocations[field] = largest_remainder(
                        targets[field], {d: raw.get(d, 0.0) for d in districts}
                    )
            corrected = {}
            for district in districts:
                row = dict(old[district])
                for field in FIELDS:
                    row[field] = int(allocations[field][district])
                finalize(row)
                corrected[district] = row
            payload["general"] = {"results": corrected}
            payload.setdefault("meta", {})["nhgis_2012_block_pilot"] = {
                "estimated": True,
                "source_precinct_results": "20121106__tn__general__precinct.csv",
                "source_geography": "2010 Census VTD",
                "block_crosswalk": "NHGIS 2010 block to 2020 block",
                "disaggregator": (
                    "party-specific model trained on high-confidence 2012 VTDs using RDH 2020-2024 race/ethnicity CVAP"
                    if args.weight_scheme == "cvap_demographic"
                    else "RDH 2020-2024 ACS CVAP_TOT24 disaggregated to 2020 blocks"
                    if args.weight_scheme == "cvap"
                    else "RDH VAP_MOD from Tennessee 2016 2020-block file"
                ),
                "non_geographic_method": (
                    f"2012 State House ballot-constrained {args.weight_scheme} allocation within the prior-plan district"
                ),
                "reconciliation": "party-specific certified statewide largest remainder",
                "target_lines_year": lines_year,
                "assignment_audit": assignment_audit,
            }
            output_path = args.output_dir / f"{subdir}__{template_path.name}"
            output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
            deltas = [
                {
                    "district": d,
                    "before_margin_pct": old[d].get("margin_pct"),
                    "after_margin_pct": corrected[d]["margin_pct"],
                    "change_pp": round(corrected[d]["margin_pct"] - float(old[d].get("margin_pct", 0)), 4),
                    "flip": old[d].get("winner") != corrected[d]["winner"],
                }
                for d in districts
            ]
            comparisons.append({
                "scope": scope,
                "lines_year": lines_year,
                "contest": contest,
                "file": output_path.name,
                "districts": len(districts),
                "flips": [row["district"] for row in deltas if row["flip"]],
                "changes_ge_1pp": sum(abs(row["change_pp"]) >= 1 for row in deltas),
                "max_abs_change_pp": max(abs(row["change_pp"]) for row in deltas),
                "largest_changes": sorted(deltas, key=lambda row: abs(row["change_pp"]), reverse=True)[:12],
            })

    report = {
        "method": f"2012 NHGIS block-crosswalk {args.weight_scheme} pilot",
        "allocation_audit": audit,
        "comparisons": comparisons,
        "sensitivity_attribution": sensitivity_attribution,
    }
    (args.output_dir / "pilot_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (args.output_dir / "state_house_75_80_source_attribution.json").write_text(
        json.dumps(sensitivity_attribution, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
