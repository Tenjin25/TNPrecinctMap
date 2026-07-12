#!/usr/bin/env python3
"""Download Shelby County precinct polling-location PDFs and build a manifest.

This fetcher scrapes the Shelby County Elections precinct polling-locations page,
finds linked PDF files, downloads them into ``Data/raw/shelby_precinct_pdfs``,
and writes CSV/JSON manifests that preserve the current county-local precinct
code system. Those manifests can be reused later to build Shelby-specific
PRCTSEQ-to-VTD bridges without manual clicking.

Examples:
  python Scripts/fetch_shelby_precinct_pdfs.py
  python Scripts/fetch_shelby_precinct_pdfs.py --force
  python Scripts/fetch_shelby_precinct_pdfs.py --skip-download
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from pathlib import Path
from typing import List, Optional


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "Data"
RAW_DIR = DATA_DIR / "raw" / "shelby_precinct_pdfs"
MANIFEST_CSV = DATA_DIR / "crosswalks" / "shelby_precinct_pdf_manifest.csv"
MANIFEST_JSON = DATA_DIR / "crosswalks" / "shelby_precinct_pdf_manifest.json"
SUMMARY_JSON = DATA_DIR / "crosswalks" / "shelby_precinct_pdf_fetch_summary.json"
PAGE_URL = "https://www.electionsshelbytn.gov/precinct-polling-locations/"
REQUEST_HEADERS = {
    "User-Agent": "TNPrecinctMap/1.0 (+https://github.com/Tenjin25/TNPrecinctMap)"
}


class PdfLinkParser(HTMLParser):
    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url
        self.links: List[dict] = []
        self._current_href: Optional[str] = None
        self._current_text: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "a":
            return
        attr_map = {k.lower(): (v or "") for k, v in attrs}
        href = attr_map.get("href", "").strip()
        if not href:
            return
        self._current_href = urllib.parse.urljoin(self.base_url, href)
        self._current_text = []

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._current_href is None:
            return
        href = self._current_href
        text = " ".join(part.strip() for part in self._current_text if part.strip()).strip()
        if href.lower().endswith(".pdf"):
            self.links.append({"url": href, "text": text})
        self._current_href = None
        self._current_text = []


def fetch_html(url: str) -> str:
    req = urllib.request.Request(url, headers=REQUEST_HEADERS)
    with urllib.request.urlopen(req, timeout=120) as resp:
        return resp.read().decode("utf-8", errors="replace")


def extract_code_candidates(value: str) -> List[str]:
    s = (value or "").strip()
    out: List[str] = []

    for m in re.finditer(r"\b(\d{2})[- ]?(\d{2})\b", s):
        out.append(f"{m.group(1)}{m.group(2)}")
    for m in re.finditer(r"\b(\d{4})\b", s):
        out.append(m.group(1))

    dedup: List[str] = []
    seen = set()
    for code in out:
        if code not in seen:
            seen.add(code)
            dedup.append(code)
    return dedup


def detect_precinct_code(link_text: str, url: str) -> str:
    basename = urllib.parse.unquote(Path(urllib.parse.urlparse(url).path).name)
    for value in (link_text, basename):
        codes = extract_code_candidates(value)
        if codes:
            return codes[0]
    return ""


def sanitized_filename(code4: str, url: str) -> str:
    basename = urllib.parse.unquote(Path(urllib.parse.urlparse(url).path).name)
    stem = Path(basename).stem
    suffix = Path(basename).suffix or ".pdf"
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", stem).strip("._-") or "precinct"
    if code4 and code4 not in stem:
        stem = f"{code4}_{stem}"
    return f"{stem}{suffix}"


def parse_links(html: str) -> List[dict]:
    parser = PdfLinkParser(PAGE_URL)
    parser.feed(html)

    rows: List[dict] = []
    seen_urls = set()
    for item in parser.links:
        url = item["url"].strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        text = item["text"].strip()
        code4 = detect_precinct_code(text, url)
        normalized_text = re.sub(r"\s+", " ", text).strip().upper()
        if not code4 and "PRECINCT POLLING LOCATION" not in normalized_text:
            continue
        rows.append(
            {
                "code4": code4,
                "prctseq": code4,
                "code_display": f"{code4[:2]}-{code4[2:]}" if len(code4) == 4 else "",
                "link_text": text,
                "url": url,
                "filename": sanitized_filename(code4, url),
            }
        )

    rows.sort(key=lambda r: (r["code4"] or "9999", r["filename"]))
    return rows


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    req = urllib.request.Request(url, headers=REQUEST_HEADERS)
    with urllib.request.urlopen(req, timeout=180) as response, dest.open("wb") as handle:
        shutil.copyfileobj(response, handle)


def write_manifest_csv(rows: List[dict]) -> None:
    MANIFEST_CSV.parent.mkdir(parents=True, exist_ok=True)
    with MANIFEST_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "code4",
                "prctseq",
                "code_display",
                "link_text",
                "url",
                "filename",
                "local_path",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--force", action="store_true", help="Redownload PDFs even if they exist locally.")
    ap.add_argument(
        "--skip-download",
        action="store_true",
        help="Only refresh the HTML-derived manifest; do not download PDFs.",
    )
    args = ap.parse_args()

    html = fetch_html(PAGE_URL)
    rows = parse_links(html)

    downloaded = 0
    existing = 0
    missing_code = 0

    for row in rows:
        if not row["code4"]:
            missing_code += 1
        local_path = RAW_DIR / row["filename"]
        row["local_path"] = str(local_path.relative_to(ROOT))
        if args.skip_download:
            continue
        if local_path.exists() and not args.force:
            existing += 1
            continue
        download(row["url"], local_path)
        downloaded += 1

    write_manifest_csv(rows)
    MANIFEST_JSON.write_text(json.dumps(rows, indent=2), encoding="utf-8")

    summary = {
        "source_page": PAGE_URL,
        "pdf_links_found": len(rows),
        "downloaded_this_run": downloaded,
        "already_present": existing,
        "skip_download": bool(args.skip_download),
        "missing_code_count": missing_code,
        "raw_dir": str(RAW_DIR),
        "manifest_csv": str(MANIFEST_CSV),
        "manifest_json": str(MANIFEST_JSON),
    }
    SUMMARY_JSON.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
