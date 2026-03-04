#!/usr/bin/env python3
"""Build Tennessee precinct and block crosswalk artifacts.

Outputs:
  - Data/crosswalks/tn_precinct_inventory.csv
  - Data/crosswalks/tn_precinct_aliases.csv
  - Data/crosswalks/tn_precinct_year_links.csv
  - Data/crosswalks/blockassign_tn_vtd.csv
  - Data/crosswalks/nhgis_*_tn_to_tn.csv
  - Data/crosswalks/tn_crosswalk_summary.json
"""

from __future__ import annotations

import csv
import json
import re
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
OUT_DIR = DATA_DIR / "crosswalks"


def norm_text(value: str) -> str:
    s = (value or "").strip().upper()
    s = re.sub(r"[\u2018\u2019]", "'", s)
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def canonical_precinct_token(raw: str) -> str:
    """Extract a compact precinct token for cross-year matching."""
    s = norm_text(raw)
    if not s:
        return ""
    token = s.split(" ", 1)[0]
    token = token.replace("_", "")
    token = token.replace("-", "")
    return token or s


def read_rows(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            yield row


def year_from_filename(path: Path) -> int:
    m = re.match(r"^(\d{4})", path.name)
    if not m:
        raise ValueError(f"Could not parse year from {path.name}")
    return int(m.group(1))


def parse_int(value) -> int:
    if value is None:
        return 0
    s = str(value).strip().replace(",", "")
    if not s:
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


@dataclass(frozen=True)
class PrecinctKey:
    year: int
    county_norm: str
    precinct_norm: str


def collect_precinct_inventory(csv_files: List[Path]):
    inventory = {}
    variants = defaultdict(set)
    year_county_to_norms = defaultdict(set)
    county_prctseq_2024 = {}

    for path in csv_files:
        year = year_from_filename(path)
        is_2024 = year == 2024

        for row in read_rows(path):
            county_raw = (
                row.get("county")
                or row.get("COUNTY")
                or ""
            ).strip()
            precinct_raw = (
                row.get("precinct")
                or row.get("PRECINCT")
                or ""
            ).strip()

            if not county_raw or not precinct_raw:
                continue

            county_norm = norm_text(county_raw)
            precinct_norm = norm_text(precinct_raw)
            if not county_norm or not precinct_norm:
                continue

            pk = PrecinctKey(year, county_norm, precinct_norm)
            if pk not in inventory:
                inventory[pk] = {
                    "year": year,
                    "county": county_raw.strip(),
                    "county_norm": county_norm,
                    "precinct": precinct_raw.strip(),
                    "precinct_norm": precinct_norm,
                    "precinct_token": canonical_precinct_token(precinct_raw),
                    "source_file": path.name,
                    "rows": 0,
                    "votes_sum": 0,
                }

            rec = inventory[pk]
            rec["rows"] += 1
            votes_raw = row.get("votes") if "votes" in row else row.get("PVTALLY1")
            rec["votes_sum"] += parse_int(votes_raw)
            variants[(county_norm, precinct_norm)].add(precinct_raw.strip())
            year_county_to_norms[(year, county_norm)].add(precinct_norm)

            if is_2024:
                prctseq = (row.get("PRCTSEQ") or "").strip()
                if prctseq:
                    county_prctseq_2024[(county_norm, precinct_norm)] = prctseq

    return inventory, variants, year_county_to_norms, county_prctseq_2024


def pick_best_match(source_norm: str, targets: Iterable[str]) -> Tuple[str, float]:
    best_target = ""
    best_score = 0.0
    for target in targets:
        score = SequenceMatcher(None, source_norm, target).ratio()
        if score > best_score:
            best_score = score
            best_target = target
    return best_target, best_score


def build_year_links(inventory, year_county_to_norms):
    years = sorted({k.year for k in inventory.keys()})
    links = []
    if not years:
        return links

    for year in years:
        next_year = next((y for y in years if y > year), None)
        prev_year = next((y for y in reversed(years) if y < year), None)
        if next_year is None and prev_year is None:
            continue

        counties = sorted({k.county_norm for k in inventory.keys() if k.year == year})
        for county_norm in counties:
            source_norms = year_county_to_norms.get((year, county_norm), set())
            for source_norm in sorted(source_norms):
                for to_year in [next_year, prev_year]:
                    if to_year is None:
                        continue
                    target_norms = year_county_to_norms.get((to_year, county_norm), set())
                    if not target_norms:
                        continue

                    if source_norm in target_norms:
                        links.append(
                            {
                                "from_year": year,
                                "to_year": to_year,
                                "county_norm": county_norm,
                                "from_precinct_norm": source_norm,
                                "to_precinct_norm": source_norm,
                                "method": "exact_norm",
                                "score": 1.0,
                            }
                        )
                        continue

                    match, score = pick_best_match(source_norm, target_norms)
                    if match and score >= 0.92:
                        links.append(
                            {
                                "from_year": year,
                                "to_year": to_year,
                                "county_norm": county_norm,
                                "from_precinct_norm": source_norm,
                                "to_precinct_norm": match,
                                "method": "fuzzy_norm",
                                "score": round(score, 6),
                            }
                        )
    return links


def build_to_2024_links(year_county_to_norms, county_prctseq_2024):
    links = []
    target_year = 2024
    counties_2024 = {
        county_norm
        for (year, county_norm) in year_county_to_norms.keys()
        if year == target_year
    }
    for (year, county_norm), source_norms in sorted(year_county_to_norms.items()):
        if year == target_year:
            continue
        if county_norm not in counties_2024:
            continue

        target_norms = year_county_to_norms.get((target_year, county_norm), set())
        if not target_norms:
            continue

        for source_norm in sorted(source_norms):
            if source_norm in target_norms:
                links.append(
                    {
                        "from_year": year,
                        "to_year": target_year,
                        "county_norm": county_norm,
                        "from_precinct_norm": source_norm,
                        "to_precinct_norm": source_norm,
                        "to_prctseq_2024": county_prctseq_2024.get((county_norm, source_norm), ""),
                        "method": "exact_norm",
                        "score": 1.0,
                    }
                )
                continue

            match, score = pick_best_match(source_norm, target_norms)
            if match and score >= 0.92:
                links.append(
                    {
                        "from_year": year,
                        "to_year": target_year,
                        "county_norm": county_norm,
                        "from_precinct_norm": source_norm,
                        "to_precinct_norm": match,
                        "to_prctseq_2024": county_prctseq_2024.get((county_norm, match), ""),
                        "method": "fuzzy_norm",
                        "score": round(score, 6),
                    }
                )
    return links


def write_csv(path: Path, fieldnames: List[str], rows: Iterable[dict]) -> int:
    count = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def normalize_nhgis_crosswalks():
    outputs = []
    for zpath in sorted(DATA_DIR.glob("nhgis_*_47.zip")):
        with zipfile.ZipFile(zpath) as zf:
            csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not csv_names:
                continue
            name = csv_names[0]
            with zf.open(name, "r") as raw:
                text = (line.decode("utf-8-sig", errors="replace") for line in raw)
                reader = csv.DictReader(text)
                cols = reader.fieldnames or []
                ge_cols = [c for c in cols if c.endswith("ge")]
                if len(ge_cols) < 2:
                    continue

                src_ge, dst_ge = ge_cols[0], ge_cols[1]
                out_rows = []
                total_rows = 0
                tn_to_tn = 0
                for row in reader:
                    total_rows += 1
                    src_val = (row.get(src_ge) or "").strip()
                    dst_val = (row.get(dst_ge) or "").strip()
                    if not (src_val.startswith("47") and dst_val.startswith("47")):
                        continue
                    tn_to_tn += 1
                    out_rows.append(
                        {
                            "source_block_geoid": src_val,
                            "target_block_geoid": dst_val,
                            "parea": row.get("parea", ""),
                            "weight": row.get("weight", ""),
                        }
                    )

                out_name = zpath.stem + "_tn_to_tn.csv"
                out_path = OUT_DIR / out_name
                write_csv(
                    out_path,
                    ["source_block_geoid", "target_block_geoid", "parea", "weight"],
                    out_rows,
                )
                outputs.append(
                    {
                        "input_zip": zpath.name,
                        "input_csv": name,
                        "output_csv": out_name,
                        "source_ge_col": src_ge,
                        "target_ge_col": dst_ge,
                        "input_rows": total_rows,
                        "output_rows_tn_to_tn": tn_to_tn,
                    }
                )
    return outputs


def extract_blockassign_vtd():
    zpath = DATA_DIR / "BlockAssign_ST47_TN.zip"
    if not zpath.exists():
        return {"exists": False, "rows": 0, "distinct_vtd": 0, "output_csv": None}

    rows = []
    distinct_vtd = set()
    with zipfile.ZipFile(zpath) as zf:
        txt_name = next((n for n in zf.namelist() if n.endswith("_VTD.txt")), None)
        if not txt_name:
            return {"exists": True, "rows": 0, "distinct_vtd": 0, "output_csv": None}

        with zf.open(txt_name, "r") as raw:
            text = (line.decode("utf-8-sig", errors="replace") for line in raw)
            reader = csv.DictReader(text, delimiter="|")
            for row in reader:
                blockid = (row.get("BLOCKID") or "").strip()
                countyfp = (row.get("COUNTYFP") or "").strip()
                district = (row.get("DISTRICT") or "").strip()
                if not blockid or not district:
                    continue
                rows.append(
                    {
                        "block_geoid_2020": blockid,
                        "county_fips": countyfp,
                        "vtd_code": district,
                    }
                )
                distinct_vtd.add((countyfp, district))

    out_name = "blockassign_tn_vtd.csv"
    out_path = OUT_DIR / out_name
    write_csv(out_path, ["block_geoid_2020", "county_fips", "vtd_code"], rows)
    return {
        "exists": True,
        "rows": len(rows),
        "distinct_vtd": len(distinct_vtd),
        "output_csv": out_name,
    }


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(DATA_DIR.glob("*__tn__*__precinct.csv"))
    if not csv_files:
        raise SystemExit("No Tennessee precinct CSV files were found in Data/")

    inventory, variants, year_county_to_norms, county_prctseq_2024 = collect_precinct_inventory(csv_files)

    inventory_rows = sorted(
        inventory.values(),
        key=lambda r: (r["year"], r["county_norm"], r["precinct_norm"]),
    )
    inventory_count = write_csv(
        OUT_DIR / "tn_precinct_inventory.csv",
        [
            "year",
            "county",
            "county_norm",
            "precinct",
            "precinct_norm",
            "precinct_token",
            "source_file",
            "rows",
            "votes_sum",
        ],
        inventory_rows,
    )

    years_by_norm = defaultdict(set)
    preferred_label = {}
    for (county_norm, precinct_norm), varset in variants.items():
        years_present = {
            pk.year
            for pk in inventory.keys()
            if pk.county_norm == county_norm and pk.precinct_norm == precinct_norm
        }
        years_by_norm[(county_norm, precinct_norm)] = years_present
        preferred = sorted(varset, key=lambda s: (-len(s), s))[0]
        preferred_label[(county_norm, precinct_norm)] = preferred

    alias_rows = []
    for (county_norm, precinct_norm), years_present in sorted(years_by_norm.items()):
        years_sorted = sorted(years_present)
        alias_rows.append(
            {
                "county_norm": county_norm,
                "precinct_norm": precinct_norm,
                "canonical_label": preferred_label[(county_norm, precinct_norm)],
                "years_present": ";".join(str(y) for y in years_sorted),
                "first_year": years_sorted[0],
                "last_year": years_sorted[-1],
                "has_2024": int(2024 in years_present),
                "prctseq_2024": county_prctseq_2024.get((county_norm, precinct_norm), ""),
            }
        )
    alias_count = write_csv(
        OUT_DIR / "tn_precinct_aliases.csv",
        [
            "county_norm",
            "precinct_norm",
            "canonical_label",
            "years_present",
            "first_year",
            "last_year",
            "has_2024",
            "prctseq_2024",
        ],
        alias_rows,
    )

    year_links = build_year_links(inventory, year_county_to_norms)
    year_link_count = write_csv(
        OUT_DIR / "tn_precinct_year_links.csv",
        [
            "from_year",
            "to_year",
            "county_norm",
            "from_precinct_norm",
            "to_precinct_norm",
            "method",
            "score",
        ],
        sorted(
            year_links,
            key=lambda r: (
                r["from_year"],
                r["to_year"],
                r["county_norm"],
                r["from_precinct_norm"],
            ),
        ),
    )

    to_2024_links = build_to_2024_links(year_county_to_norms, county_prctseq_2024)
    to_2024_link_count = write_csv(
        OUT_DIR / "tn_precinct_to_2024.csv",
        [
            "from_year",
            "to_year",
            "county_norm",
            "from_precinct_norm",
            "to_precinct_norm",
            "to_prctseq_2024",
            "method",
            "score",
        ],
        sorted(
            to_2024_links,
            key=lambda r: (
                r["from_year"],
                r["county_norm"],
                r["from_precinct_norm"],
            ),
        ),
    )

    nhgis_outputs = normalize_nhgis_crosswalks()
    blockassign_summary = extract_blockassign_vtd()

    summary = {
        "source_precinct_csv_files": [p.name for p in csv_files],
        "years": sorted({year_from_filename(p) for p in csv_files}),
        "inventory_rows": inventory_count,
        "alias_rows": alias_count,
        "year_link_rows": year_link_count,
        "to_2024_link_rows": to_2024_link_count,
        "nhgis_outputs": nhgis_outputs,
        "blockassign_vtd": blockassign_summary,
    }

    with (OUT_DIR / "tn_crosswalk_summary.json").open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
