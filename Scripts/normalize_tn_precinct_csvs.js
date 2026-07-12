#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT, "Data");
const OUT_DIR = path.join(DATA_DIR, "canonical_precinct_csvs");
const CANONICAL_HEADER = ["county", "precinct", "office", "district", "party", "candidate", "votes"];

function normSpace(value) {
  return String(value ?? "").trim().replace(/\s+/g, " ");
}

function parseVotes(value) {
  const cleaned = String(value ?? "").replace(/,/g, "").trim();
  if (!cleaned) return "0";
  const parsed = Number(cleaned);
  if (!Number.isFinite(parsed)) return "0";
  return String(Math.round(parsed));
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ",") {
      row.push(field);
      field = "";
    } else if (ch === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
    } else if (ch !== "\r") {
      field += ch;
    }
  }
  if (field.length || row.length) {
    row.push(field);
    rows.push(row);
  }
  if (!rows.length) return [];
  const header = rows.shift().map((h) => normSpace(h).replace(/^"|"$/g, ""));
  return rows
    .filter((r) => r.some((v) => normSpace(v)))
    .map((r) => Object.fromEntries(header.map((h, idx) => [h, r[idx] ?? ""])));
}

function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\r\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

function writeCsv(filePath, rows) {
  const lines = [CANONICAL_HEADER.join(",")];
  for (const row of rows) {
    lines.push(CANONICAL_HEADER.map((key) => csvEscape(row[key])).join(","));
  }
  fs.writeFileSync(filePath, `${lines.join("\n")}\n`);
}

function isKnownPartyLabel(value) {
  const party = normSpace(value).toUpperCase();
  return (
    !party ||
    [
      "D",
      "DEM",
      "DEMOCRAT",
      "DEMOCRATIC",
      "R",
      "REP",
      "REPUBLICAN",
      "I",
      "IND",
      "INDEPENDENT",
      "OTH",
      "OTHER",
      "NA",
      "N/A",
      "WRITE-IN",
      "WRITE IN",
      "WRITEIN",
      "CONSTITUTION",
      "GREEN",
    ].includes(party)
  );
}

function normalizePartyCandidate(partyRaw, candidateRaw) {
  let party = normSpace(partyRaw);
  let candidate = normSpace(candidateRaw);
  if (/^write[- ]?in$/i.test(candidate) && party && !isKnownPartyLabel(party)) {
    candidate = party;
    party = "Write-In";
  }
  return { party, candidate };
}

function normalizeLongRows(rows) {
  return rows.map((row) => {
    const normalized = normalizePartyCandidate(row.party, row.candidate);
    return {
      county: normSpace(row.county),
      precinct: normSpace(row.precinct),
      office: normSpace(row.office),
      district: normSpace(row.district || "NA") || "NA",
      party: normalized.party || "NA",
      candidate: normalized.candidate,
      votes: parseVotes(row.votes),
    };
  });
}

function normalizeWide2024Rows(rows) {
  const out = [];
  for (const row of rows) {
    const county = normSpace(row.COUNTY);
    const precinct = normSpace(row.PRECINCT);
    const office = normSpace(row.OFFICENAME);
    const districtMatch = office.match(/[Dd]istrict\s+(\d+)/);
    const district = districtMatch ? districtMatch[1] : "NA";
    for (let i = 1; i <= 10; i += 1) {
      const candidate = normSpace(row[`RNAME${i}`]);
      if (!candidate) continue;
      out.push({
        county,
        precinct,
        office,
        district,
        party: normSpace(row[`PARTY${i}`]) || "NA",
        candidate,
        votes: parseVotes(row[`PVTALLY${i}`]),
      });
    }
  }
  return out;
}

function shouldNormalize(fileName) {
  return fileName.includes("__tn__general") && fileName.includes("precinct") && fileName.endsWith(".csv");
}

function normalizeFile(fileName) {
  const inputPath = path.join(DATA_DIR, fileName);
  const rows = parseCsv(fs.readFileSync(inputPath, "utf8"));
  if (!rows.length) return null;
  const isWide2024 = "OFFICENAME" in rows[0] && "RNAME1" in rows[0];
  const canonicalRows = isWide2024 ? normalizeWide2024Rows(rows) : normalizeLongRows(rows);
  const outputPath = path.join(OUT_DIR, fileName);
  writeCsv(outputPath, canonicalRows);
  return { fileName, rows: canonicalRows.length, isWide2024 };
}

function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const results = fs.readdirSync(DATA_DIR).filter(shouldNormalize).sort().map(normalizeFile).filter(Boolean);
  for (const result of results) {
    console.log(`${result.fileName}: ${result.rows} canonical rows${result.isWide2024 ? " (converted from wide)" : ""}`);
  }
}

main();
