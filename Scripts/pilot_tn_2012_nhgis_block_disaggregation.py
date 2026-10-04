#!/usr/bin/env python3
"""Pilot an RDH-style 2012 election disaggregation through the NHGIS block crosswalk."""

from __future__ import annotations

import argparse
import json
import re
import sys
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
) -> tuple[pd.DataFrame, dict]:
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
) -> tuple[pd.DataFrame, dict]:
    membership, audit = build_target_block_to_vtd10(data_dir)
    precinct_map, county_map = precinct_to_vtd10(data_dir, crosswalk_data_dir)
    base_blocks = load_allocation_weights(data_dir, "vap_mod", None)

    election = pd.read_csv(data_dir / "20121106__tn__general__precinct.csv")
    election = election[election["office"].isin(CONTEST_OFFICES)].copy()
    election["county_norm"] = election["county"].map(norm_county)
    election["from_precinct_norm"] = election["precinct"].map(norm_text)
    election["contest"] = election["office"].map(CONTEST_OFFICES)
    election["field"] = election["party"].map(party_field)
    election["votes"] = pd.to_numeric(election["votes"], errors="coerce").fillna(0)
    votes = election.groupby(
        ["county_norm", "from_precinct_norm", "contest", "field"], as_index=False
    )["votes"].sum()
    votes["source_vote_id"] = range(len(votes))
    votes = votes.merge(
        precinct_map,
        on=["county_norm", "from_precinct_norm"],
        how="left",
        validate="many_to_many",
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
    non_join = fallback.merge(
        county_blocks, left_on=fallback_keys_left, right_on=fallback_keys_right, how="left"
    )
    if non_join["GEOID20"].isna().any():
        missing = sorted(non_join.loc[non_join["GEOID20"].isna(), "county_norm"].unique())
        raise RuntimeError(f"County fallback has no 2020 blocks for: {missing}")
    non_join["block_votes"] = non_join["votes"] * non_join["county_share"]

    allocated = pd.concat(
        [
            geo_join[["GEOID20", "contest", "field", "block_votes"]],
            non_join[["GEOID20", "contest", "field", "block_votes"]],
        ],
        ignore_index=True,
    )
    allocated = allocated.groupby(["GEOID20", "contest", "field"], as_index=False)["block_votes"].sum()
    source_vote_rows = votes.drop_duplicates("source_vote_id")
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
        "confidence_vote_totals": {
            f"{contest}:{tier}": round(float(value), 6)
            for (contest, tier), value in source_vote_rows.assign(
                confidence_tier=source_vote_rows["confidence_tier"].fillna("unmatched")
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
    return allocated, audit


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
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    sys.path.insert(0, str((Path(__file__).parent).resolve()))
    from build_tn_legislative_from_blocks import current_plan_assignment

    allocated, audit = allocate_votes_to_blocks(
        args.data_dir, args.published_data_dir, args.weight_scheme, args.cvap_csv
    )
    specs = (
        ("state_house", 2022, "district_contests"),
        ("state_senate", 2022, "district_contests"),
        ("congressional", 2022, "district_contests"),
        ("congressional", 2026, "district_contests_2026"),
    )
    comparisons = []
    for scope, lines_year, subdir in specs:
        assignment, assignment_audit = current_plan_assignment(
            args.data_dir, args.published_data_dir, scope, lines_year
        )
        joined = allocated.merge(assignment, on="GEOID20", how="inner", validate="many_to_one")
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
                "non_geographic_method": f"county-constrained {args.weight_scheme} allocation",
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
    }
    (args.output_dir / "pilot_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
