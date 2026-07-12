#!/usr/bin/env python3
"""Build a Shelby PRCTSEQ review table from current results, overrides, and PDFs."""

from __future__ import annotations

import csv
import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List

try:
    from pypdf import PdfReader
except ModuleNotFoundError as exc:  # pragma: no cover - runtime guard
    raise SystemExit(
        "Missing dependency 'pypdf'. Run this script with the bundled Codex Python runtime."
    ) from exc


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
XWALK_DIR = DATA_DIR / "crosswalks"
RAW_PDF_DIR = DATA_DIR / "raw" / "shelby_precinct_pdfs"
CURRENT_2024_CSV = DATA_DIR / "20241105__tn__general__precinct.csv"
OVERRIDES_CSV = XWALK_DIR / "tn_prctseq_to_vtd20_overrides.csv"
BLOCKASSIGN_NAMES_CSV = XWALK_DIR / "tn_blockassign_vtd_with_names.csv"
PDF_MANIFEST_CSV = XWALK_DIR / "shelby_precinct_pdf_manifest.csv"
OUT_CSV = XWALK_DIR / "shelby_prctseq_review.csv"
OUT_JSON = XWALK_DIR / "shelby_prctseq_review_summary.json"

SELF_LABEL_RE = re.compile(r"(\d{4})\s*-\s*([^\r\n]+)")
MENTIONED_CODE_RE = re.compile(r"\b(\d{4})\b")


def read_rows(path: Path) -> Iterable[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        yield from csv.DictReader(f)


def norm_space(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def load_pdf_manifest() -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for row in read_rows(PDF_MANIFEST_CSV):
        code4 = str(row.get("code4", "")).strip()
        if code4:
            out[code4] = row
    return out


def extract_pdf_metadata(pdf_path: Path, code4: str) -> dict:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(pdf_path)).pages)
    matches = SELF_LABEL_RE.findall(text)
    polling_location = ""
    for found_code, label in matches:
        if found_code == code4:
            polling_location = norm_space(label)
            break
    if not polling_location and matches:
        polling_location = norm_space(matches[0][1])

    mentioned_codes: List[str] = []
    seen = set()
    for found in MENTIONED_CODE_RE.findall(text):
        if found not in seen:
            seen.add(found)
            mentioned_codes.append(found)

    return {
        "polling_location": polling_location,
        "mentioned_codes": mentioned_codes,
    }


def load_current_shelby_prctseq_rows() -> List[dict]:
    seen = set()
    rows = []
    for row in read_rows(CURRENT_2024_CSV):
        county = norm_space(str(row.get("COUNTY", ""))).upper()
        prctseq = str(row.get("PRCTSEQ", "")).zfill(4)
        precinct = norm_space(str(row.get("PRECINCT", "")))
        if county != "SHELBY" or not prctseq or not precinct:
            continue
        pair = (prctseq, precinct)
        if pair in seen:
            continue
        seen.add(pair)
        code4 = precinct.replace("-", "").replace(" ", "")
        rows.append({"prctseq": prctseq, "display_code": precinct, "code4": code4})
    rows.sort(key=lambda r: int(r["prctseq"]))
    return rows


def load_override_lookup() -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = defaultdict(list)
    for row in read_rows(OVERRIDES_CSV):
        if norm_space(str(row.get("county_norm", ""))).upper() != "SHELBY":
            continue
        prctseq = str(row.get("prctseq", "")).zfill(4)
        if not prctseq.isdigit():
            continue
        out[prctseq].append(
            {
                "vtd20": str(row.get("vtd20", "")).zfill(6),
                "vtd_name": norm_space(str(row.get("vtd_name", ""))),
                "weight": str(row.get("weight", "")).strip() or "1.0",
                "source": norm_space(str(row.get("source", ""))),
                "confidence": norm_space(str(row.get("confidence", ""))),
            }
        )
    return out


def load_official_vtd_inventory() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for row in read_rows(BLOCKASSIGN_NAMES_CSV):
        if str(row.get("county_fips", "")).zfill(3) != "157":
            continue
        out[str(row.get("vtd_code", "")).zfill(6)] = norm_space(str(row.get("vtd_name", "")))
    return out


def build_review_rows() -> List[dict]:
    manifest = load_pdf_manifest()
    overrides = load_override_lookup()
    rows = []
    for row in load_current_shelby_prctseq_rows():
        code4 = row["code4"]
        manifest_row = manifest.get(code4, {})
        override_rows = overrides.get(row["prctseq"], [])
        meta = {"polling_location": "", "mentioned_codes": []}
        if not override_rows:
            local_path = str(manifest_row.get("local_path", "")).replace("\\", "/")
            pdf_path = ROOT / local_path
            if pdf_path.exists():
                meta = extract_pdf_metadata(pdf_path, code4)
        rows.append(
            {
                "prctseq": row["prctseq"],
                "display_code": row["display_code"],
                "code4": code4,
                "polling_location": meta.get("polling_location", ""),
                "mentioned_codes": ";".join(meta.get("mentioned_codes", [])),
                "pdf_url": str(manifest_row.get("url", "")).strip(),
                "pdf_filename": str(manifest_row.get("filename", "")).strip(),
                "override_targets": len(override_rows),
                "override_vtd20s": ";".join(r["vtd20"] for r in override_rows),
                "override_vtd_names": ";".join(r["vtd_name"] for r in override_rows),
                "override_weights": ";".join(str(r["weight"]) for r in override_rows),
                "override_source": ";".join(r["source"] for r in override_rows),
                "override_confidence": ";".join(r["confidence"] for r in override_rows),
                "status": "resolved" if override_rows else "needs_review",
            }
        )
    return rows


def write_rows(rows: List[dict]) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "prctseq",
                "display_code",
                "code4",
                "polling_location",
                "mentioned_codes",
                "pdf_url",
                "pdf_filename",
                "override_targets",
                "override_vtd20s",
                "override_vtd_names",
                "override_weights",
                "override_source",
                "override_confidence",
                "status",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def write_summary(rows: List[dict]) -> None:
    official_inventory = load_official_vtd_inventory()
    assigned = set()
    weighted_rows = 0
    for row in rows:
        for vtd20 in str(row["override_vtd20s"]).split(";"):
            vtd20 = vtd20.strip()
            if vtd20:
                assigned.add(vtd20)
        if int(row["override_targets"] or 0) > 1:
            weighted_rows += 1

    unresolved_codes = [row["display_code"] for row in rows if row["status"] != "resolved"]
    summary = {
        "output_csv": str(OUT_CSV),
        "row_count": len(rows),
        "resolved_rows": sum(1 for row in rows if row["status"] == "resolved"),
        "needs_review_rows": sum(1 for row in rows if row["status"] != "resolved"),
        "weighted_override_rows": weighted_rows,
        "official_shelby_vtd20_total": len(official_inventory),
        "official_shelby_vtd20_assigned_in_overrides": len(assigned),
        "official_shelby_vtd20_unassigned": len(set(official_inventory) - assigned),
        "sample_unresolved_display_codes": unresolved_codes[:20],
    }
    OUT_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


def main() -> None:
    rows = build_review_rows()
    write_rows(rows)
    write_summary(rows)


if __name__ == "__main__":
    main()
