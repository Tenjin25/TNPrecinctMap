#!/usr/bin/env python3
"""Backtest candidate block weights against high-confidence 2012 precinct totals."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from pilot_tn_2012_nhgis_block_disaggregation import (
    CONTEST_OFFICES,
    build_target_block_to_vtd10,
    load_allocation_weights,
    norm_county,
    norm_text,
    party_field,
    precinct_to_vtd10,
)


def evaluate_scheme(
    data_dir: Path,
    membership: pd.DataFrame,
    observations: pd.DataFrame,
    scheme: str,
    cvap_csv: Path | None,
) -> dict:
    weights = load_allocation_weights(data_dir, scheme, cvap_csv)
    joined = membership.merge(
        weights[["GEOID20", "allocation_weight"]],
        left_on="target_block_geoid",
        right_on="GEOID20",
        how="inner",
        validate="many_to_one",
    )
    joined["mass"] = joined["vtd_membership"] * joined["allocation_weight"]
    vtd_mass = joined.groupby(["COUNTYFP10", "VTDST10"], as_index=False)["mass"].sum()
    sample = observations.merge(
        vtd_mass,
        left_on=["COUNTYFP10", "src_vtdst"],
        right_on=["COUNTYFP10", "VTDST10"],
        how="inner",
    )
    # Multiple election labels can legitimately roll into one Census VTD.
    sample = sample.groupby(
        ["COUNTYFP10", "src_vtdst", "contest", "field"], as_index=False
    ).agg(votes=("votes", "sum"), mass=("mass", "first"))
    group = ["COUNTYFP10", "contest", "field"]
    sample["group_votes"] = sample.groupby(group)["votes"].transform("sum")
    sample["group_mass"] = sample.groupby(group)["mass"].transform("sum")
    sample["group_vtds"] = sample.groupby(group)["src_vtdst"].transform("nunique")
    sample = sample[(sample["group_vtds"] >= 2) & (sample["group_mass"] > 0)].copy()
    sample["predicted"] = sample["group_votes"] * sample["mass"] / sample["group_mass"]
    sample["abs_error"] = (sample["predicted"] - sample["votes"]).abs()
    sample["actual_share"] = sample["votes"] / sample["group_votes"]
    sample["predicted_share"] = sample["predicted"] / sample["group_votes"]
    sample["share_error_pp"] = 100 * (sample["predicted_share"] - sample["actual_share"]).abs()

    details = []
    for (contest, field), frame in sample.groupby(["contest", "field"]):
        total = float(frame["votes"].sum())
        details.append({
            "contest": contest,
            "field": field,
            "vtd_observations": int(len(frame)),
            "counties": int(frame["COUNTYFP10"].nunique()),
            "total_variation_pct": round(50 * float(frame["abs_error"].sum()) / total, 4),
            "mean_absolute_share_error_pp": round(float(frame["share_error_pp"].mean()), 4),
            "weighted_absolute_vote_error_pct": round(100 * float(frame["abs_error"].sum()) / total, 4),
        })
    total = float(sample["votes"].sum())
    return {
        "scheme": scheme,
        "vtd_observations": int(len(sample)),
        "counties": int(sample["COUNTYFP10"].nunique()),
        "total_variation_pct": round(50 * float(sample["abs_error"].sum()) / total, 4),
        "mean_absolute_share_error_pp": round(float(sample["share_error_pp"].mean()), 4),
        "by_contest_field": details,
    }


def evaluate_demographic_cv(
    membership: pd.DataFrame,
    observations: pd.DataFrame,
    cvap_csv: Path,
) -> dict:
    """Five-fold held-out-county test of race-specific CVAP party propensities."""
    source_fields = ["CVAP_TOT24", "CVAP_WHT24", "CVAP_BLA24", "CVAP_HSP24"]
    features = ["CVAP_WHT24", "CVAP_BLA24", "CVAP_HSP24", "CVAP_OTH24"]
    cvap = pd.read_csv(cvap_csv, usecols=["GEOID20", *source_fields], dtype={"GEOID20": str})
    cvap["GEOID20"] = cvap["GEOID20"].str.zfill(15)
    for column in source_fields:
        cvap[column] = pd.to_numeric(cvap[column], errors="coerce").fillna(0).clip(lower=0)
    cvap["CVAP_OTH24"] = (
        cvap["CVAP_TOT24"] - cvap["CVAP_WHT24"] - cvap["CVAP_BLA24"] - cvap["CVAP_HSP24"]
    ).clip(lower=0)
    joined = membership.merge(
        cvap,
        left_on="target_block_geoid",
        right_on="GEOID20",
        how="inner",
        validate="many_to_one",
    )
    for column in features:
        joined[column] *= joined["vtd_membership"]
    vtd_features = joined.groupby(["COUNTYFP10", "VTDST10"], as_index=False)[features].sum()
    sample = observations.merge(
        vtd_features,
        left_on=["COUNTYFP10", "src_vtdst"],
        right_on=["COUNTYFP10", "VTDST10"],
        how="inner",
    )
    aggregations = {"votes": ("votes", "sum")}
    aggregations.update({column: (column, "first") for column in features})
    sample = sample.groupby(
        ["COUNTYFP10", "src_vtdst", "contest", "field"], as_index=False
    ).agg(**aggregations)
    group = ["COUNTYFP10", "contest", "field"]
    sample["group_vtds"] = sample.groupby(group)["src_vtdst"].transform("nunique")
    sample = sample[sample["group_vtds"] >= 2].copy()
    sample["fold"] = pd.to_numeric(sample["COUNTYFP10"]).astype(int) % 5
    predictions = []
    for (contest, field), contest_frame in sample.groupby(["contest", "field"]):
        for fold in range(5):
            train = contest_frame[contest_frame["fold"] != fold]
            test = contest_frame[contest_frame["fold"] == fold].copy()
            if test.empty:
                continue
            x_train = train[features].to_numpy(dtype=float)
            y_train = train["votes"].to_numpy(dtype=float)
            # Nonnegative clipped least squares, with no intercept: each coefficient
            # is an estimated votes-per-CVAP propensity learned outside the county.
            coefficients = np.linalg.lstsq(x_train, y_train, rcond=1e-8)[0]
            coefficients = np.clip(coefficients, 0, None)
            test["raw_prediction"] = np.maximum(
                test[features].to_numpy(dtype=float) @ coefficients, 1e-12
            )
            test["group_votes"] = test.groupby("COUNTYFP10")["votes"].transform("sum")
            test["group_prediction"] = test.groupby("COUNTYFP10")["raw_prediction"].transform("sum")
            test["predicted"] = test["group_votes"] * test["raw_prediction"] / test["group_prediction"]
            predictions.append(test)
    result = pd.concat(predictions, ignore_index=True)
    result["abs_error"] = (result["predicted"] - result["votes"]).abs()
    result["actual_share"] = result["votes"] / result["group_votes"]
    result["predicted_share"] = result["predicted"] / result["group_votes"]
    result["share_error_pp"] = 100 * (result["predicted_share"] - result["actual_share"]).abs()
    details = []
    for (contest, field), frame in result.groupby(["contest", "field"]):
        total = float(frame["votes"].sum())
        details.append({
            "contest": contest,
            "field": field,
            "vtd_observations": int(len(frame)),
            "counties": int(frame["COUNTYFP10"].nunique()),
            "total_variation_pct": round(50 * float(frame["abs_error"].sum()) / total, 4),
            "mean_absolute_share_error_pp": round(float(frame["share_error_pp"].mean()), 4),
            "weighted_absolute_vote_error_pct": round(100 * float(frame["abs_error"].sum()) / total, 4),
        })
    total = float(result["votes"].sum())
    return {
        "scheme": "race_specific_cvap_out_of_county_cv",
        "vtd_observations": int(len(result)),
        "counties": int(result["COUNTYFP10"].nunique()),
        "total_variation_pct": round(50 * float(result["abs_error"].sum()) / total, 4),
        "mean_absolute_share_error_pp": round(float(result["share_error_pp"].mean()), 4),
        "by_contest_field": details,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--published-data-dir", type=Path, required=True)
    parser.add_argument("--cvap-csv", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    membership, membership_audit = build_target_block_to_vtd10(args.data_dir)
    precinct_map, _ = precinct_to_vtd10(args.data_dir, args.published_data_dir)
    precinct_map = precinct_map[precinct_map["confidence_tier"].eq("high")].copy()

    election = pd.read_csv(args.data_dir / "20121106__tn__general__precinct.csv")
    election = election[election["office"].isin(CONTEST_OFFICES)].copy()
    election["county_norm"] = election["county"].map(norm_county)
    election["from_precinct_norm"] = election["precinct"].map(norm_text)
    election["contest"] = election["office"].map(CONTEST_OFFICES)
    election["field"] = election["party"].map(party_field)
    election["votes"] = pd.to_numeric(election["votes"], errors="coerce").fillna(0)
    observations = election.groupby(
        ["county_norm", "from_precinct_norm", "contest", "field"], as_index=False
    )["votes"].sum()
    observations = observations.merge(
        precinct_map,
        on=["county_norm", "from_precinct_norm"],
        how="inner",
        validate="many_to_one",
    )

    report = {
        "method": "within-county high-confidence 2012 VTD distribution backtest",
        "interpretation": "lower error is better; this tests population weighting, not precinct-boundary matching",
        "membership_audit": membership_audit,
        "schemes": [
            evaluate_scheme(args.data_dir, membership, observations, "vap_mod", None),
            evaluate_scheme(args.data_dir, membership, observations, "cvap", args.cvap_csv),
            evaluate_demographic_cv(membership, observations, args.cvap_csv),
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
