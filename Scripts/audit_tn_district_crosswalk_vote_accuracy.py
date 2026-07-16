#!/usr/bin/env python3
"""Audit district carryover crosswalks against current district contest outputs.

The audit reallocates precinct-level contest JSON rows through each generated
district carryover crosswalk, then compares those totals with the district JSONs
already used by the app. Large deltas identify places where area weights are a
poor proxy and vote-weighted or direct district-source allocation may be needed.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
CONTESTS_DIR = DATA_DIR / "contests"
DISTRICT_CONTESTS_DIR = DATA_DIR / "district_contests"
DISTRICT_CONTESTS_2026_DIR = DATA_DIR / "district_contests_2026"
XWALK_DIR = DATA_DIR / "crosswalks"
OUT_DIR = DATA_DIR / "reports" / "district_crosswalk_comparison"
COUNTY_GEOJSON = DATA_DIR / "tl_2020_47_county20.geojson"
PRECINCT_CVAP_JSON = DATA_DIR / "tn_cvap_by_precinct_2020.json"

AUDITS = [
    {
        "label": "congressional_2022",
        "scope": "congressional",
        "lines_year": 2022,
        "crosswalk": XWALK_DIR / "tn_congressional_2022_precinct_crosswalk.csv",
        "district_dir": DISTRICT_CONTESTS_DIR,
    },
    {
        "label": "congressional_2026",
        "scope": "congressional",
        "lines_year": 2026,
        "crosswalk": XWALK_DIR / "tn_congressional_2026_precinct_crosswalk.csv",
        "district_dir": DISTRICT_CONTESTS_2026_DIR,
    },
    {
        "label": "state_house_2022",
        "scope": "state_house",
        "lines_year": 2022,
        "crosswalk": XWALK_DIR / "tn_state_house_2022_precinct_crosswalk.csv",
        "district_dir": DISTRICT_CONTESTS_DIR,
    },
    {
        "label": "state_senate_2022",
        "scope": "state_senate",
        "lines_year": 2022,
        "crosswalk": XWALK_DIR / "tn_state_senate_2022_precinct_crosswalk.csv",
        "district_dir": DISTRICT_CONTESTS_DIR,
    },
]

PREFERRED_CONTESTS = [
    ("president", 2024),
    ("us_senate", 2024),
    ("governor", 2022),
    ("president", 2020),
    ("us_senate", 2020),
]

VOTE_FIELDS = ["dem_votes", "rep_votes", "other_votes", "total_votes"]


def read_csv_rows(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: List[dict], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def county_name_by_fips() -> Dict[str, str]:
    payload = load_json(COUNTY_GEOJSON)
    out = {}
    for feature in payload.get("features", []) or []:
        props = feature.get("properties", {}) or {}
        raw_fips = str(props.get("COUNTYFP20") or props.get("COUNTYFP") or props.get("GEOID") or "")
        fips = "".join(ch for ch in raw_fips if ch.isdigit())[-3:].zfill(3)
        name = normalize_precinct_key(
            props.get("NAME20")
            or props.get("CountyName")
            or props.get("COUNTYNAME")
            or props.get("county_nam")
            or props.get("NAME")
            or ""
        )
        if fips and name:
            out[fips] = name
    return out


def normalize_precinct_key(value: str) -> str:
    return " ".join(str(value or "").strip().upper().split())


def normalize_district(value: str) -> str:
    s = str(value or "").strip()
    if s.endswith(".0"):
        s = s[:-2]
    if s.isdigit():
        return str(int(s))
    return s.lstrip("0") or s


def numeric(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def load_crosswalk_rows(path: Path) -> List[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")
    out = []
    for row in read_csv_rows(path):
        precinct = normalize_precinct_key(row.get("precinct_key") or "")
        district = normalize_district(row.get("district_num") or "")
        weight = numeric(row.get("area_weight"))
        if precinct and district and weight > 0:
            out.append(
                {
                    "precinct_key": precinct,
                    "county_norm": normalize_precinct_key(row.get("county_norm") or ""),
                    "prec_id": str(row.get("prec_id") or "").strip(),
                    "district_num": district,
                    "area_weight": weight,
                }
            )
    return out


def load_crosswalk(path: Path) -> Dict[str, List[Tuple[str, float]]]:
    out: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
    for row in load_crosswalk_rows(path):
        out[row["precinct_key"]].append((row["district_num"], row["area_weight"]))
    return dict(out)


def load_precinct_cvap() -> Dict[str, dict]:
    if not PRECINCT_CVAP_JSON.exists():
        return {}
    payload = load_json(PRECINCT_CVAP_JSON)
    county_names = county_name_by_fips()
    out = {}
    for raw_key, row in (payload.get("precincts", {}) or {}).items():
        parts = str(raw_key or "").strip().split("-", 1)
        if len(parts) != 2:
            continue
        county_fips = "".join(ch for ch in parts[0] if ch.isdigit())[-3:].zfill(3)
        county = county_names.get(county_fips, "")
        prec_id = parts[1].strip().upper()
        if not county or not prec_id:
            continue
        precinct_key = normalize_precinct_key(f"{county} - {prec_id}")
        out[precinct_key] = row
    return out


def contest_path(contest_type: str, year: int) -> Path:
    return CONTESTS_DIR / f"{contest_type}_{year}.json"


def district_path(district_dir: Path, scope: str, contest_type: str, year: int) -> Path:
    return district_dir / f"{scope}_{contest_type}_{year}.json"


def available_contests(district_dir: Path, scope: str) -> List[Tuple[str, int]]:
    out = []
    for contest_type, year in PREFERRED_CONTESTS:
        if contest_path(contest_type, year).exists() and district_path(district_dir, scope, contest_type, year).exists():
            out.append((contest_type, year))
    return out


def allocate_precinct_contest(contest: dict, crosswalk: Dict[str, List[Tuple[str, float]]]) -> Tuple[Dict[str, dict], dict]:
    totals: Dict[str, dict] = defaultdict(lambda: {field: 0.0 for field in VOTE_FIELDS})
    matched_rows = 0
    unmatched_rows = 0
    matched_votes = 0.0
    unmatched_votes = 0.0
    for row in contest.get("rows", []) or []:
        precinct = normalize_precinct_key(row.get("county") or "")
        allocs = crosswalk.get(precinct, [])
        votes = {field: numeric(row.get(field)) for field in VOTE_FIELDS}
        total_votes = votes["total_votes"]
        if not allocs:
            unmatched_rows += 1
            unmatched_votes += total_votes
            continue
        matched_rows += 1
        matched_votes += total_votes
        for district, weight in allocs:
            node = totals[district]
            for field, value in votes.items():
                node[field] += value * weight
    meta = {
        "matched_rows": matched_rows,
        "unmatched_rows": unmatched_rows,
        "matched_votes": matched_votes,
        "unmatched_votes": unmatched_votes,
    }
    return totals, meta


def aggregate_precinct_vote_rows(contest: dict) -> Dict[str, dict]:
    rows: Dict[str, dict] = defaultdict(lambda: {field: 0.0 for field in VOTE_FIELDS})
    for row in contest.get("rows", []) or []:
        precinct = normalize_precinct_key(row.get("county") or "")
        if not precinct:
            continue
        node = rows[precinct]
        for field in VOTE_FIELDS:
            node[field] += numeric(row.get(field))
    return dict(rows)


def load_reference_results(path: Path) -> Dict[str, dict]:
    payload = load_json(path)
    return {
        normalize_district(district): row
        for district, row in (payload.get("general", {}).get("results", {}) or {}).items()
        if normalize_district(district)
    }


def margin_pct(row: dict) -> float:
    total = numeric(row.get("total_votes"))
    if total <= 0:
        return 0.0
    return ((numeric(row.get("rep_votes")) - numeric(row.get("dem_votes"))) / total) * 100.0


def vote_share_pct(row: dict, field: str) -> float:
    total = numeric(row.get("total_votes"))
    if total <= 0:
        return 0.0
    return (numeric(row.get(field)) / total) * 100.0


def priority_for_split(total_votes: float, non_dominant_share: float, exposure: float) -> str:
    if exposure >= 1000 or (total_votes >= 1000 and non_dominant_share >= 0.25):
        return "high"
    if exposure >= 250 or (total_votes >= 500 and non_dominant_share >= 0.10):
        return "medium"
    return "low"


def build_split_vote_diagnostics(
    audit: dict,
    crosswalk_rows: List[dict],
    contest: dict,
    contest_type: str,
    year: int,
    precinct_cvap: Dict[str, dict],
) -> List[dict]:
    precinct_votes = aggregate_precinct_vote_rows(contest)
    grouped: Dict[str, List[dict]] = defaultdict(list)
    for row in crosswalk_rows:
        grouped[row["precinct_key"]].append(row)

    rows = []
    for precinct, allocs in grouped.items():
        if len(allocs) < 2:
            continue
        total_weight = sum(numeric(row["area_weight"]) for row in allocs)
        if total_weight <= 0:
            continue
        allocs = sorted(allocs, key=lambda row: numeric(row["area_weight"]), reverse=True)
        dominant = allocs[0]
        dominant_weight = numeric(dominant["area_weight"])
        non_dominant_weight = max(0.0, total_weight - dominant_weight)
        normalized_dominant_share = dominant_weight / total_weight
        normalized_non_dominant_share = max(0.0, 1.0 - normalized_dominant_share)
        unallocated_weight = max(0.0, 1.0 - total_weight)
        votes = precinct_votes.get(precinct, {field: 0.0 for field in VOTE_FIELDS})
        total_votes = numeric(votes.get("total_votes"))
        cvap = precinct_cvap.get(precinct, {})
        cvap_total = numeric(cvap.get("cvap_tot"))
        exposure = total_votes * normalized_non_dominant_share
        unallocated_exposure = total_votes * unallocated_weight
        cvap_exposure = cvap_total * normalized_non_dominant_share
        cvap_unallocated_exposure = cvap_total * unallocated_weight
        district_weights = ";".join(f"{row['district_num']}:{numeric(row['area_weight']):.6f}" for row in allocs)
        rows.append(
            {
                "audit_label": audit["label"],
                "scope": audit["scope"],
                "lines_year": audit["lines_year"],
                "contest_type": contest_type,
                "year": year,
                "precinct_key": precinct,
                "county_norm": allocs[0]["county_norm"],
                "prec_id": allocs[0]["prec_id"],
                "split_count": len(allocs),
                "district_weights": district_weights,
                "dominant_district": dominant["district_num"],
                "dominant_area_weight": round(dominant_weight, 8),
                "total_area_weight": round(total_weight, 8),
                "non_dominant_area_weight": round(non_dominant_weight, 8),
                "normalized_dominant_share": round(normalized_dominant_share, 8),
                "normalized_non_dominant_share": round(normalized_non_dominant_share, 8),
                "unallocated_area_weight": round(unallocated_weight, 8),
                "dem_votes": round(numeric(votes.get("dem_votes")), 4),
                "rep_votes": round(numeric(votes.get("rep_votes")), 4),
                "other_votes": round(numeric(votes.get("other_votes")), 4),
                "total_votes": round(total_votes, 4),
                "cvap_total": round(cvap_total, 4),
                "cvap_to_total_vote_ratio": round(cvap_total / total_votes, 4) if total_votes > 0 else 0.0,
                "dem_share_pct": round(vote_share_pct(votes, "dem_votes"), 4),
                "rep_share_pct": round(vote_share_pct(votes, "rep_votes"), 4),
                "margin_pct": round(margin_pct(votes), 4),
                "non_dominant_vote_exposure": round(exposure, 4),
                "unallocated_vote_exposure": round(unallocated_exposure, 4),
                "non_dominant_cvap_exposure": round(cvap_exposure, 4),
                "unallocated_cvap_exposure": round(cvap_unallocated_exposure, 4),
                "diagnostic_priority": priority_for_split(total_votes, normalized_non_dominant_share, exposure),
                "has_precinct_votes": precinct in precinct_votes,
                "has_precinct_cvap": precinct in precinct_cvap,
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            numeric(row["non_dominant_vote_exposure"]),
            numeric(row["total_votes"]),
            numeric(row["normalized_non_dominant_share"]),
        ),
        reverse=True,
    )


def summarize_split_vote_diagnostics(rows: List[dict], contest: dict) -> dict:
    contest_votes = sum(numeric(row.get("total_votes")) for row in contest.get("rows", []) or [])
    split_votes = sum(numeric(row["total_votes"]) for row in rows)
    exposure = sum(numeric(row["non_dominant_vote_exposure"]) for row in rows)
    cvap_total = sum(numeric(row["cvap_total"]) for row in rows)
    cvap_exposure = sum(numeric(row["non_dominant_cvap_exposure"]) for row in rows)
    high_rows = [row for row in rows if row["diagnostic_priority"] == "high"]
    medium_rows = [row for row in rows if row["diagnostic_priority"] == "medium"]
    top = rows[0] if rows else {}
    return {
        "split_precincts": len(rows),
        "split_precincts_with_votes": sum(1 for row in rows if row["has_precinct_votes"]),
        "split_precincts_with_cvap": sum(1 for row in rows if row["has_precinct_cvap"]),
        "contest_total_votes": round(contest_votes, 4),
        "split_total_votes": round(split_votes, 4),
        "split_total_votes_pct": round((split_votes / contest_votes) * 100.0, 4) if contest_votes > 0 else 0.0,
        "non_dominant_vote_exposure": round(exposure, 4),
        "non_dominant_vote_exposure_pct": round((exposure / contest_votes) * 100.0, 4) if contest_votes > 0 else 0.0,
        "split_cvap_total": round(cvap_total, 4),
        "non_dominant_cvap_exposure": round(cvap_exposure, 4),
        "non_dominant_cvap_exposure_pct": round((cvap_exposure / cvap_total) * 100.0, 4) if cvap_total > 0 else 0.0,
        "high_priority_split_precincts": len(high_rows),
        "medium_priority_split_precincts": len(medium_rows),
        "top_precinct_key": top.get("precinct_key", ""),
        "top_precinct_total_votes": top.get("total_votes", 0),
        "top_precinct_non_dominant_exposure": top.get("non_dominant_vote_exposure", 0),
        "top_precinct_district_weights": top.get("district_weights", ""),
    }


def compare_rows(
    audit_label: str,
    scope: str,
    lines_year: int,
    contest_type: str,
    year: int,
    allocated: Dict[str, dict],
    reference: Dict[str, dict],
) -> List[dict]:
    rows = []
    for district in sorted(set(allocated.keys()).union(reference.keys()), key=lambda d: (not d.isdigit(), int(d) if d.isdigit() else d)):
        calc = allocated.get(district, {})
        ref = reference.get(district, {})
        total_delta = numeric(calc.get("total_votes")) - numeric(ref.get("total_votes"))
        dem_delta = numeric(calc.get("dem_votes")) - numeric(ref.get("dem_votes"))
        rep_delta = numeric(calc.get("rep_votes")) - numeric(ref.get("rep_votes"))
        other_delta = numeric(calc.get("other_votes")) - numeric(ref.get("other_votes"))
        ref_total = numeric(ref.get("total_votes"))
        rows.append(
            {
                "audit_label": audit_label,
                "scope": scope,
                "lines_year": lines_year,
                "contest_type": contest_type,
                "year": year,
                "district": district,
                "has_allocated_result": bool(calc),
                "has_reference_result": bool(ref),
                "calc_dem_votes": round(numeric(calc.get("dem_votes")), 4),
                "ref_dem_votes": round(numeric(ref.get("dem_votes")), 4),
                "dem_delta": round(dem_delta, 4),
                "calc_rep_votes": round(numeric(calc.get("rep_votes")), 4),
                "ref_rep_votes": round(numeric(ref.get("rep_votes")), 4),
                "rep_delta": round(rep_delta, 4),
                "calc_other_votes": round(numeric(calc.get("other_votes")), 4),
                "ref_other_votes": round(numeric(ref.get("other_votes")), 4),
                "other_delta": round(other_delta, 4),
                "calc_total_votes": round(numeric(calc.get("total_votes")), 4),
                "ref_total_votes": round(ref_total, 4),
                "total_delta": round(total_delta, 4),
                "abs_total_delta": round(abs(total_delta), 4),
                "total_delta_pct_of_ref": round((total_delta / ref_total) * 100.0, 4) if ref_total > 0 else 0.0,
                "calc_margin_pct": round(margin_pct(calc), 4),
                "ref_margin_pct": round(margin_pct(ref), 4),
                "margin_pct_delta": round(margin_pct(calc) - margin_pct(ref), 4),
                "abs_margin_pct_delta": round(abs(margin_pct(calc) - margin_pct(ref)), 4),
            }
        )
    return rows


def summarize_detail(rows: List[dict], allocation_meta: dict) -> dict:
    if not rows:
        return {}
    allocated_without_reference = [r for r in rows if r["has_allocated_result"] and not r["has_reference_result"]]
    reference_without_allocated = [r for r in rows if r["has_reference_result"] and not r["has_allocated_result"]]
    return {
        "districts": len(rows),
        "matched_rows": allocation_meta["matched_rows"],
        "unmatched_rows": allocation_meta["unmatched_rows"],
        "matched_votes": round(allocation_meta["matched_votes"], 4),
        "unmatched_votes": round(allocation_meta["unmatched_votes"], 4),
        "allocated_without_reference_districts": len(allocated_without_reference),
        "allocated_without_reference_votes": round(sum(numeric(r["calc_total_votes"]) for r in allocated_without_reference), 4),
        "reference_without_allocated_districts": len(reference_without_allocated),
        "reference_without_allocated_votes": round(sum(numeric(r["ref_total_votes"]) for r in reference_without_allocated), 4),
        "max_abs_total_delta": round(max(abs(numeric(r["total_delta"])) for r in rows), 4),
        "max_abs_total_delta_pct_of_ref": round(max(abs(numeric(r["total_delta_pct_of_ref"])) for r in rows), 4),
        "max_abs_margin_pct_delta": round(max(abs(numeric(r["margin_pct_delta"])) for r in rows), 4),
        "mean_abs_total_delta": round(sum(abs(numeric(r["total_delta"])) for r in rows) / len(rows), 4),
        "mean_abs_margin_pct_delta": round(sum(abs(numeric(r["margin_pct_delta"])) for r in rows) / len(rows), 4),
    }


def margin_sensitivity(exposure: float, margin_pct_value: float) -> float:
    # Prioritize close precinct margins, but still let large exposure matter.
    closeness = max(0.0, 30.0 - min(abs(margin_pct_value), 30.0)) / 30.0
    return exposure * closeness


def split_review_priority(cvap_exposure: float, vote_2024_exposure: float, sensitivity: float) -> str:
    if cvap_exposure >= 3000 or vote_2024_exposure >= 2000 or sensitivity >= 1000:
        return "high"
    if cvap_exposure >= 1000 or vote_2024_exposure >= 500 or sensitivity >= 250:
        return "medium"
    return "low"


def split_review_reasons(row: dict) -> str:
    reasons = []
    cvap_exposure = numeric(row["non_dominant_cvap_exposure"])
    vote_exposure = numeric(row["max_2024_non_dominant_vote_exposure"])
    sensitivity = numeric(row["max_2024_margin_sensitivity"])
    if cvap_exposure >= 3000:
        reasons.append("high_cvap_exposure")
    elif cvap_exposure >= 1000:
        reasons.append("medium_cvap_exposure")
    if vote_exposure >= 2000:
        reasons.append("high_2024_vote_exposure")
    elif vote_exposure >= 500:
        reasons.append("medium_2024_vote_exposure")
    if sensitivity >= 1000:
        reasons.append("high_close_margin_sensitivity")
    elif sensitivity >= 250:
        reasons.append("medium_close_margin_sensitivity")
    closest_margin = numeric(row["closest_2024_abs_margin_pct"])
    if closest_margin > 0 and closest_margin <= 10:
        reasons.append("close_2024_precinct_margin")
    if numeric(row["split_count"]) >= 3:
        reasons.append("multiway_split")
    if numeric(row["unallocated_area_weight"]) > 0.001:
        reasons.append("unallocated_area")
    return ";".join(reasons) or "review_if_needed"


def build_split_review_queue(split_diag_rows: List[dict]) -> List[dict]:
    grouped: Dict[Tuple[str, str], dict] = {}
    for row in split_diag_rows:
        key = (str(row["audit_label"]), str(row["precinct_key"]))
        node = grouped.get(key)
        if node is None:
            node = {
                "audit_label": row["audit_label"],
                "scope": row["scope"],
                "lines_year": row["lines_year"],
                "precinct_key": row["precinct_key"],
                "county_norm": row["county_norm"],
                "prec_id": row["prec_id"],
                "split_count": row["split_count"],
                "district_weights": row["district_weights"],
                "dominant_district": row["dominant_district"],
                "dominant_area_weight": row["dominant_area_weight"],
                "total_area_weight": row["total_area_weight"],
                "normalized_non_dominant_share": row["normalized_non_dominant_share"],
                "unallocated_area_weight": row["unallocated_area_weight"],
                "cvap_total": row["cvap_total"],
                "non_dominant_cvap_exposure": row["non_dominant_cvap_exposure"],
                "unallocated_cvap_exposure": row["unallocated_cvap_exposure"],
                "has_precinct_cvap": row["has_precinct_cvap"],
                "contests_reviewed": set(),
                "max_non_dominant_vote_exposure": 0.0,
                "top_vote_exposure_contest": "",
                "max_2024_total_votes": 0.0,
                "max_2024_non_dominant_vote_exposure": 0.0,
                "top_2024_vote_exposure_contest": "",
                "closest_2024_abs_margin_pct": None,
                "closest_2024_margin_contest": "",
                "max_2024_margin_sensitivity": 0.0,
                "top_2024_margin_sensitivity_contest": "",
            }
            grouped[key] = node

        contest_label = f"{row['contest_type']}_{row['year']}"
        node["contests_reviewed"].add(contest_label)

        exposure = numeric(row["non_dominant_vote_exposure"])
        if exposure > numeric(node["max_non_dominant_vote_exposure"]):
            node["max_non_dominant_vote_exposure"] = exposure
            node["top_vote_exposure_contest"] = contest_label

        if int(row["year"]) == 2024:
            total_votes = numeric(row["total_votes"])
            if total_votes > numeric(node["max_2024_total_votes"]):
                node["max_2024_total_votes"] = total_votes

            if exposure > numeric(node["max_2024_non_dominant_vote_exposure"]):
                node["max_2024_non_dominant_vote_exposure"] = exposure
                node["top_2024_vote_exposure_contest"] = contest_label

            abs_margin = abs(numeric(row["margin_pct"]))
            closest = node["closest_2024_abs_margin_pct"]
            if closest is None or abs_margin < numeric(closest):
                node["closest_2024_abs_margin_pct"] = abs_margin
                node["closest_2024_margin_contest"] = contest_label

            sensitivity = margin_sensitivity(exposure, numeric(row["margin_pct"]))
            if sensitivity > numeric(node["max_2024_margin_sensitivity"]):
                node["max_2024_margin_sensitivity"] = sensitivity
                node["top_2024_margin_sensitivity_contest"] = contest_label

    rows = []
    for node in grouped.values():
        closest = node["closest_2024_abs_margin_pct"]
        out = {
            **{k: v for k, v in node.items() if k != "contests_reviewed" and k != "closest_2024_abs_margin_pct"},
            "contests_reviewed": ";".join(sorted(node["contests_reviewed"])),
            "closest_2024_abs_margin_pct": round(numeric(closest), 4) if closest is not None else "",
            "max_non_dominant_vote_exposure": round(numeric(node["max_non_dominant_vote_exposure"]), 4),
            "max_2024_total_votes": round(numeric(node["max_2024_total_votes"]), 4),
            "max_2024_non_dominant_vote_exposure": round(numeric(node["max_2024_non_dominant_vote_exposure"]), 4),
            "max_2024_margin_sensitivity": round(numeric(node["max_2024_margin_sensitivity"]), 4),
        }
        out["review_priority"] = split_review_priority(
            numeric(out["non_dominant_cvap_exposure"]),
            numeric(out["max_2024_non_dominant_vote_exposure"]),
            numeric(out["max_2024_margin_sensitivity"]),
        )
        out["review_reasons"] = split_review_reasons(out)
        rows.append(out)

    rows.sort(
        key=lambda row: (
            numeric(row["non_dominant_cvap_exposure"]),
            numeric(row["max_2024_non_dominant_vote_exposure"]),
            numeric(row["max_2024_margin_sensitivity"]),
            numeric(row["split_count"]),
        ),
        reverse=True,
    )
    for idx, row in enumerate(rows, start=1):
        row["review_rank"] = idx
    return rows


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    all_detail: List[dict] = []
    summary_rows: List[dict] = []
    split_diag_rows: List[dict] = []
    split_summary_rows: List[dict] = []
    precinct_cvap = load_precinct_cvap()

    for audit in AUDITS:
        crosswalk_rows = load_crosswalk_rows(audit["crosswalk"])
        crosswalk: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        for row in crosswalk_rows:
            crosswalk[row["precinct_key"]].append((row["district_num"], row["area_weight"]))
        crosswalk = dict(crosswalk)
        contests = available_contests(audit["district_dir"], audit["scope"])
        for contest_type, year in contests:
            contest = load_json(contest_path(contest_type, year))
            split_rows = build_split_vote_diagnostics(audit, crosswalk_rows, contest, contest_type, year, precinct_cvap)
            split_diag_rows.extend(split_rows)
            split_summary = summarize_split_vote_diagnostics(split_rows, contest)
            split_summary.update(
                {
                    "audit_label": audit["label"],
                    "scope": audit["scope"],
                    "lines_year": audit["lines_year"],
                    "contest_type": contest_type,
                    "year": year,
                }
            )
            split_summary_rows.append(split_summary)
            allocated, allocation_meta = allocate_precinct_contest(contest, crosswalk)
            reference = load_reference_results(district_path(audit["district_dir"], audit["scope"], contest_type, year))
            detail = compare_rows(
                audit_label=audit["label"],
                scope=audit["scope"],
                lines_year=int(audit["lines_year"]),
                contest_type=contest_type,
                year=year,
                allocated=allocated,
                reference=reference,
            )
            all_detail.extend(detail)
            summary = summarize_detail(detail, allocation_meta)
            summary.update(
                {
                    "audit_label": audit["label"],
                    "scope": audit["scope"],
                    "lines_year": audit["lines_year"],
                    "contest_type": contest_type,
                    "year": year,
                    "crosswalk_csv": str(audit["crosswalk"].relative_to(DATA_DIR)),
                    "reference_json": str(district_path(audit["district_dir"], audit["scope"], contest_type, year).relative_to(DATA_DIR)),
                }
            )
            summary_rows.append(summary)

    detail_path = OUT_DIR / "district_crosswalk_vote_accuracy_detail.csv"
    summary_path = OUT_DIR / "district_crosswalk_vote_accuracy_summary.csv"
    worst_path = OUT_DIR / "district_crosswalk_vote_accuracy_worst_deltas.csv"
    split_diag_path = OUT_DIR / "district_crosswalk_split_vote_exposure.csv"
    split_cvap_path = OUT_DIR / "district_crosswalk_split_cvap_exposure.csv"
    split_summary_path = OUT_DIR / "district_crosswalk_split_vote_exposure_summary.csv"
    split_review_queue_path = OUT_DIR / "district_split_review_queue.csv"
    detail_fieldnames = [
        "audit_label",
        "scope",
        "lines_year",
        "contest_type",
        "year",
        "district",
        "has_allocated_result",
        "has_reference_result",
        "calc_dem_votes",
        "ref_dem_votes",
        "dem_delta",
        "calc_rep_votes",
        "ref_rep_votes",
        "rep_delta",
        "calc_other_votes",
        "ref_other_votes",
        "other_delta",
        "calc_total_votes",
        "ref_total_votes",
        "total_delta",
        "abs_total_delta",
        "total_delta_pct_of_ref",
        "calc_margin_pct",
        "ref_margin_pct",
        "margin_pct_delta",
        "abs_margin_pct_delta",
    ]
    split_diag_fieldnames = [
        "audit_label",
        "scope",
        "lines_year",
        "contest_type",
        "year",
        "precinct_key",
        "county_norm",
        "prec_id",
        "split_count",
        "district_weights",
        "dominant_district",
        "dominant_area_weight",
        "total_area_weight",
        "non_dominant_area_weight",
        "normalized_dominant_share",
        "normalized_non_dominant_share",
        "unallocated_area_weight",
        "dem_votes",
        "rep_votes",
        "other_votes",
        "total_votes",
        "cvap_total",
        "cvap_to_total_vote_ratio",
        "dem_share_pct",
        "rep_share_pct",
        "margin_pct",
        "non_dominant_vote_exposure",
        "unallocated_vote_exposure",
        "non_dominant_cvap_exposure",
        "unallocated_cvap_exposure",
        "diagnostic_priority",
        "has_precinct_votes",
        "has_precinct_cvap",
    ]
    split_review_queue_fieldnames = [
        "review_rank",
        "review_priority",
        "review_reasons",
        "audit_label",
        "scope",
        "lines_year",
        "precinct_key",
        "county_norm",
        "prec_id",
        "split_count",
        "district_weights",
        "dominant_district",
        "dominant_area_weight",
        "total_area_weight",
        "normalized_non_dominant_share",
        "unallocated_area_weight",
        "cvap_total",
        "non_dominant_cvap_exposure",
        "unallocated_cvap_exposure",
        "has_precinct_cvap",
        "max_non_dominant_vote_exposure",
        "top_vote_exposure_contest",
        "max_2024_total_votes",
        "max_2024_non_dominant_vote_exposure",
        "top_2024_vote_exposure_contest",
        "closest_2024_abs_margin_pct",
        "closest_2024_margin_contest",
        "max_2024_margin_sensitivity",
        "top_2024_margin_sensitivity_contest",
        "contests_reviewed",
    ]
    write_csv(
        detail_path,
        all_detail,
        detail_fieldnames,
    )
    write_csv(
        worst_path,
        sorted(
            all_detail,
            key=lambda r: (
                max(abs(numeric(r["total_delta_pct_of_ref"])), abs(numeric(r["abs_margin_pct_delta"]))),
                abs(numeric(r["abs_total_delta"])),
            ),
            reverse=True,
        )[:100],
        detail_fieldnames,
    )
    write_csv(
        summary_path,
        sorted(summary_rows, key=lambda r: (r["audit_label"], r["contest_type"], r["year"])),
        [
            "audit_label",
            "scope",
            "lines_year",
            "contest_type",
            "year",
            "districts",
            "matched_rows",
            "unmatched_rows",
            "matched_votes",
            "unmatched_votes",
            "allocated_without_reference_districts",
            "allocated_without_reference_votes",
            "reference_without_allocated_districts",
            "reference_without_allocated_votes",
            "max_abs_total_delta",
            "max_abs_total_delta_pct_of_ref",
            "max_abs_margin_pct_delta",
            "mean_abs_total_delta",
            "mean_abs_margin_pct_delta",
            "crosswalk_csv",
            "reference_json",
        ],
    )
    write_csv(
        split_diag_path,
        sorted(
            split_diag_rows,
            key=lambda r: (
                numeric(r["non_dominant_vote_exposure"]),
                numeric(r["total_votes"]),
                numeric(r["normalized_non_dominant_share"]),
            ),
            reverse=True,
        ),
        split_diag_fieldnames,
    )
    write_csv(
        split_cvap_path,
        sorted(
            split_diag_rows,
            key=lambda r: (
                numeric(r["non_dominant_cvap_exposure"]),
                numeric(r["cvap_total"]),
                numeric(r["normalized_non_dominant_share"]),
            ),
            reverse=True,
        ),
        split_diag_fieldnames,
    )
    write_csv(
        split_summary_path,
        sorted(split_summary_rows, key=lambda r: (r["audit_label"], r["contest_type"], r["year"])),
        [
            "audit_label",
            "scope",
            "lines_year",
            "contest_type",
            "year",
            "split_precincts",
            "split_precincts_with_votes",
            "split_precincts_with_cvap",
            "contest_total_votes",
            "split_total_votes",
            "split_total_votes_pct",
            "non_dominant_vote_exposure",
            "non_dominant_vote_exposure_pct",
            "split_cvap_total",
            "non_dominant_cvap_exposure",
            "non_dominant_cvap_exposure_pct",
            "high_priority_split_precincts",
            "medium_priority_split_precincts",
            "top_precinct_key",
            "top_precinct_total_votes",
            "top_precinct_non_dominant_exposure",
            "top_precinct_district_weights",
        ],
    )
    split_review_queue = build_split_review_queue(split_diag_rows)
    write_csv(
        split_review_queue_path,
        split_review_queue,
        split_review_queue_fieldnames,
    )
    print(
        json.dumps(
            {
                "summary_csv": str(summary_path.relative_to(DATA_DIR)),
                "detail_csv": str(detail_path.relative_to(DATA_DIR)),
                "worst_deltas_csv": str(worst_path.relative_to(DATA_DIR)),
                "split_vote_exposure_csv": str(split_diag_path.relative_to(DATA_DIR)),
                "split_cvap_exposure_csv": str(split_cvap_path.relative_to(DATA_DIR)),
                "split_vote_exposure_summary_csv": str(split_summary_path.relative_to(DATA_DIR)),
                "split_review_queue_csv": str(split_review_queue_path.relative_to(DATA_DIR)),
                "audits": len(summary_rows),
                "detail_rows": len(all_detail),
                "split_vote_exposure_rows": len(split_diag_rows),
                "split_review_queue_rows": len(split_review_queue),
                "precinct_cvap_rows": len(precinct_cvap),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
