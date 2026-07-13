#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT, "Data");
const CONTESTS_DIR = path.join(DATA_DIR, "contests");
const SOURCE_CSV = path.join(DATA_DIR, "canonical_precinct_csvs", "20201103__tn__general__precinct.csv");
const FRIENDLY_NAMES = path.join(DATA_DIR, "crosswalks", "tn_precinct_friendly_names_2020.json");

const TARGETS = [
  {
    file: "president_2020.json",
    office: "President",
  },
  {
    file: "us_senate_2020.json",
    office: "U.S. Senate",
  },
];

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    const next = text[i + 1];
    if (inQuotes) {
      if (ch === '"' && next === '"') {
        field += '"';
        i += 1;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        field += ch;
      }
      continue;
    }
    if (ch === '"') {
      inQuotes = true;
    } else if (ch === ",") {
      row.push(field);
      field = "";
    } else if (ch === "\n") {
      row.push(field);
      field = "";
      if (row.some((value) => value !== "")) rows.push(row);
      row = [];
    } else if (ch !== "\r") {
      field += ch;
    }
  }
  if (field || row.length) {
    row.push(field);
    if (row.some((value) => value !== "")) rows.push(row);
  }

  const [header, ...body] = rows;
  return body.map((values) => Object.fromEntries(header.map((key, idx) => [key, values[idx] ?? ""])));
}

function normSpace(value) {
  return String(value ?? "").trim().replace(/\s+/g, " ");
}

function normKey(value) {
  return normSpace(value)
    .toUpperCase()
    .replace(/^\d+\s+/, "")
    .replace(/\bNO\.\s*/g, "")
    .replace(/&/g, " AND ")
    .replace(/[^A-Z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function countyFromLabel(label) {
  return normSpace(String(label || "").split(" - ")[0] || "").toUpperCase();
}

function rowTotal(row, field) {
  return Number(row[field] || 0);
}

function sumRows(rows) {
  return rows.reduce(
    (acc, row) => {
      acc.dem += rowTotal(row, "dem_votes");
      acc.rep += rowTotal(row, "rep_votes");
      acc.other += rowTotal(row, "other_votes");
      acc.total += rowTotal(row, "total_votes");
      return acc;
    },
    { dem: 0, rep: 0, other: 0, total: 0 },
  );
}

function recomputeRow(row) {
  row.dem_votes = Math.max(0, Math.round(Number(row.dem_votes || 0)));
  row.rep_votes = Math.max(0, Math.round(Number(row.rep_votes || 0)));
  row.other_votes = Math.max(0, Math.round(Number(row.other_votes || 0)));
  row.total_votes = row.dem_votes + row.rep_votes + row.other_votes;
  row.margin = row.rep_votes - row.dem_votes;
  row.margin_pct = row.total_votes ? Number(((row.margin / row.total_votes) * 100).toFixed(4)) : 0;
  row.winner = row.margin > 0 ? "REP" : row.margin < 0 ? "DEM" : "TIE";
  row.color = row.color || "";
}

function addResidual(rows, field, targetValue) {
  const current = rows.reduce((sum, row) => sum + rowTotal(row, field), 0);
  const diff = Math.round(targetValue) - current;
  if (!diff) return;
  const recipient = rows
    .filter((row) => rowTotal(row, field) > 0 || rowTotal(row, "total_votes") > 0)
    .sort((a, b) => rowTotal(b, field) - rowTotal(a, field))[0] || rows[0];
  if (!recipient) return;
  recipient[field] = Math.max(0, rowTotal(recipient, field) + diff);
  recomputeRow(recipient);
}

function scaleRowsToOriginalTotals(rows, originalTotals) {
  const current = sumRows(rows);
  const factors = {
    dem_votes: current.dem ? originalTotals.dem / current.dem : 1,
    rep_votes: current.rep ? originalTotals.rep / current.rep : 1,
    other_votes: current.other ? originalTotals.other / current.other : 1,
  };

  for (const row of rows) {
    row.dem_votes = rowTotal(row, "dem_votes") * factors.dem_votes;
    row.rep_votes = rowTotal(row, "rep_votes") * factors.rep_votes;
    row.other_votes = rowTotal(row, "other_votes") * factors.other_votes;
    recomputeRow(row);
  }

  addResidual(rows, "dem_votes", originalTotals.dem);
  addResidual(rows, "rep_votes", originalTotals.rep);
  addResidual(rows, "other_votes", originalTotals.other);
  for (const row of rows) recomputeRow(row);
}

function partyBucket(party) {
  const key = normSpace(party).toUpperCase();
  if (key === "DEMOCRATIC") return "dem";
  if (key === "REPUBLICAN") return "rep";
  return "other";
}

function loadHamiltonNameToCode() {
  const friendly = JSON.parse(fs.readFileSync(FRIENDLY_NAMES, "utf8"));
  const hamilton = friendly.counties?.Hamilton || {};
  const byName = new Map();
  const ambiguous = new Set();

  for (const [code, name] of Object.entries(hamilton)) {
    const key = normKey(name);
    if (!key) continue;
    if (byName.has(key)) ambiguous.add(key);
    byName.set(key, code);
  }
  for (const key of ambiguous) byName.delete(key);
  return byName;
}

function loadSourceRowsByCode(office) {
  const sourceRows = parseCsv(fs.readFileSync(SOURCE_CSV, "utf8"));
  const nameToCode = loadHamiltonNameToCode();
  const byCode = new Map();
  const sourceNames = new Set();
  const matchedSourceNames = new Set();

  for (const row of sourceRows) {
    if (normSpace(row.county).toUpperCase() !== "HAMILTON") continue;
    if (normSpace(row.office) !== office) continue;
    const sourceName = normSpace(row.precinct);
    const code = nameToCode.get(normKey(sourceName));
    sourceNames.add(sourceName);
    if (!code) continue;
    matchedSourceNames.add(sourceName);
    const label = `HAMILTON - ${code}`;
    if (!byCode.has(label)) {
      byCode.set(label, {
        county: label,
        dem_votes: 0,
        rep_votes: 0,
        other_votes: 0,
        total_votes: 0,
        dem_candidate: "",
        rep_candidate: "",
        color: "",
      });
    }
    const out = byCode.get(label);
    const votes = Number(row.votes || 0);
    const bucket = partyBucket(row.party);
    out[`${bucket}_votes`] += votes;
    if (bucket === "dem" && !out.dem_candidate) out.dem_candidate = normSpace(row.candidate);
    if (bucket === "rep" && !out.rep_candidate) out.rep_candidate = normSpace(row.candidate);
  }

  for (const row of byCode.values()) recomputeRow(row);
  return {
    rowsByLabel: byCode,
    sourcePrecincts: sourceNames.size,
    matchedSourcePrecincts: matchedSourceNames.size,
    unmatchedSourcePrecincts: [...sourceNames].filter((name) => !matchedSourceNames.has(name)).sort(),
  };
}

function patchContest(target) {
  const contestPath = path.join(CONTESTS_DIR, target.file);
  const node = JSON.parse(fs.readFileSync(contestPath, "utf8"));
  const source = loadSourceRowsByCode(target.office);
  const rows = node.rows || [];
  const hamiltonRows = rows.filter((row) => countyFromLabel(row.county) === "HAMILTON");
  const originalTotals = sumRows(hamiltonRows);
  const replacedLabels = [];

  for (const row of hamiltonRows) {
    const replacement = source.rowsByLabel.get(normSpace(row.county));
    if (!replacement) continue;
    row.dem_votes = replacement.dem_votes;
    row.rep_votes = replacement.rep_votes;
    row.other_votes = replacement.other_votes;
    row.dem_candidate = replacement.dem_candidate || row.dem_candidate || "";
    row.rep_candidate = replacement.rep_candidate || row.rep_candidate || "";
    recomputeRow(row);
    replacedLabels.push(row.county);
  }

  if (!replacedLabels.length) {
    console.log(`${target.file}: no exact Hamilton VTD name replacements found`);
    return;
  }

  scaleRowsToOriginalTotals(hamiltonRows, originalTotals);
  const finalTotals = sumRows(hamiltonRows);
  node.meta = {
    ...(node.meta || {}),
    hamilton_2020_exact_vtd_name_patch: true,
    hamilton_2020_exact_vtd_name_patch_applied_at: new Date().toISOString(),
    hamilton_2020_exact_vtd_name_source: "Data/canonical_precinct_csvs/20201103__tn__general__precinct.csv",
    hamilton_2020_exact_vtd_name_lookup: "Data/crosswalks/tn_precinct_friendly_names_2020.json",
    hamilton_2020_exact_vtd_name_office: target.office,
    hamilton_2020_exact_vtd_name_replaced_rows: replacedLabels.length,
    hamilton_2020_exact_vtd_name_source_precincts: source.sourcePrecincts,
    hamilton_2020_exact_vtd_name_matched_source_precincts: source.matchedSourcePrecincts,
    hamilton_2020_exact_vtd_name_unmatched_source_precincts: source.unmatchedSourcePrecincts,
    hamilton_2020_exact_vtd_name_preserved_hamilton_totals: finalTotals.dem === originalTotals.dem
      && finalTotals.rep === originalTotals.rep
      && finalTotals.other === originalTotals.other,
    hamilton_2020_exact_vtd_name_sample_labels: replacedLabels.slice(0, 20),
  };

  fs.writeFileSync(contestPath, `${JSON.stringify(node, null, 2)}\n`);
  console.log(`${target.file}: replaced ${replacedLabels.length}/${hamiltonRows.length} Hamilton rows, matched ${source.matchedSourcePrecincts}/${source.sourcePrecincts} source precinct names`);
}

function main() {
  for (const target of TARGETS) patchContest(target);
}

main();
