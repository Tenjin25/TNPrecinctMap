#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT, "Data");
const CONTESTS_DIR = path.join(DATA_DIR, "contests");
const REPORTS_DIR = path.join(DATA_DIR, "reports");
const PRECINCT_GEOJSON = path.join(DATA_DIR, "tn_voting_precincts.geojson");

function normSpace(value) {
  return String(value ?? "").trim().replace(/\s+/g, " ");
}

function normCounty(value) {
  return normSpace(value).toUpperCase();
}

function countyFromLabel(label) {
  return normCounty(String(label || "").split(" - ")[0] || "");
}

function precinctCodeFromLabel(label) {
  const text = String(label || "");
  if (!text.includes(" - ")) return "";
  return normSpace(text.split(" - ").slice(1).join(" - ")).toUpperCase();
}

function isNonGeographicPrecinctCode(code) {
  const c = normSpace(code).toUpperCase();
  if (!c) return true;
  if (/^(ABSEN|PROVI|TRANS)(\b|\s|-|_)/.test(c)) return true;
  if (/^(ABSENTEE|PROVISIONAL|CURBSIDE|EV)$/.test(c)) return true;
  if (/^(EV|OS)(\b|-|_)/.test(c) || /^OS[A-Z0-9]/.test(c)) return true;
  if (c.includes("ABSENTEE") || c.includes("PROVISIONAL") || c.includes("TRANSFER") || c.includes("CURBSIDE")) return true;
  if (c.includes("ONE STOP") || c.includes("ONE-STOP") || c.includes("ONESTOP")) return true;
  if (c.includes("EARLY VOT") || c.includes("MAIL ABSENTEE") || c.includes("VOTE CENTER") || c.includes("VOTECENTER")) return true;
  if (c.includes("ALL_COUNTY") || c.startsWith("NG-") || c.startsWith("NG ")) return true;
  if (c.startsWith("UNM-") || c.startsWith("UNM ")) return true;
  return false;
}

function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\r\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

function loadMappedLabels() {
  const labels = new Set();
  const byCounty = new Map();
  const geo = JSON.parse(fs.readFileSync(PRECINCT_GEOJSON, "utf8"));
  for (const feature of geo.features || []) {
    const props = feature.properties || {};
    const county = normCounty(props.county_norm || props.county_nam || "");
    const precId = normSpace(props.prec_id || props.VTDST20 || props.vtdst20 || "");
    if (!county || !precId) continue;
    const label = `${county} - ${precId}`;
    labels.add(label);
    if (!byCounty.has(county)) byCounty.set(county, new Set());
    byCounty.get(county).add(label);
  }
  return { labels, byCounty };
}

function sumRows(rows) {
  return rows.reduce(
    (acc, row) => {
      acc.dem += Number(row.dem_votes || 0);
      acc.rep += Number(row.rep_votes || 0);
      acc.other += Number(row.other_votes || 0);
      acc.total += Number(row.total_votes || 0);
      return acc;
    },
    { dem: 0, rep: 0, other: 0, total: 0 },
  );
}

function contestFiles() {
  return fs.readdirSync(CONTESTS_DIR)
    .filter((name) => name.endsWith(".json") && name !== "manifest.json")
    .sort((a, b) => a.localeCompare(b, "en", { numeric: true }));
}

function analyzeContest(fileName, mapped) {
  const node = JSON.parse(fs.readFileSync(path.join(CONTESTS_DIR, fileName), "utf8"));
  const rows = node.rows || [];
  const rowsByCounty = new Map();
  const zeroMappedByCounty = new Map();
  const zeroUnmappedByCounty = new Map();
  const nonGeographicRows = [];
  const unmappedRows = [];
  const mappedRows = [];
  const zeroRows = [];

  for (const row of rows) {
    const label = normSpace(row.county);
    const county = countyFromLabel(label);
    const code = precinctCodeFromLabel(label);
    const total = Number(row.total_votes || 0);
    const isMapped = mapped.labels.has(label);
    const isNonGeo = isNonGeographicPrecinctCode(code);
    if (county) rowsByCounty.set(county, (rowsByCounty.get(county) || 0) + 1);
    if (isMapped) mappedRows.push(label);
    if (!isMapped) unmappedRows.push(label);
    if (isNonGeo) nonGeographicRows.push(label);
    if (total <= 0) {
      zeroRows.push(label);
      if (isMapped) zeroMappedByCounty.set(county, (zeroMappedByCounty.get(county) || 0) + 1);
      else zeroUnmappedByCounty.set(county, (zeroUnmappedByCounty.get(county) || 0) + 1);
    }
  }

  const totals = sumRows(rows);
  const totalIntegrityDelta = totals.total - (totals.dem + totals.rep + totals.other);
  const mappedZeroRows = zeroRows.filter((label) => mapped.labels.has(label));
  const nonGeographicZeroRows = zeroRows.filter((label) => isNonGeographicPrecinctCode(precinctCodeFromLabel(label)));
  const unmappedZeroRows = zeroRows.filter((label) => !mapped.labels.has(label) && !isNonGeographicPrecinctCode(precinctCodeFromLabel(label)));
  const inferredPriorRows = Number(node.meta?.prior_vtd20_gap_fill_prior_rows || 0);
  const inferredNeighborRows = Number(node.meta?.prior_vtd20_gap_fill_neighbor_rows || 0);
  const inferredWeightedCounties = Array.isArray(node.meta?.weighted_vtd20_replaced_counties)
    ? node.meta.weighted_vtd20_replaced_counties.length
    : 0;

  return {
    file: fileName,
    contest_type: node.contest_type || "",
    year: Number(node.year || 0),
    rows: rows.length,
    mapped_rows: mappedRows.length,
    unmapped_rows: unmappedRows.length,
    non_geographic_rows: nonGeographicRows.length,
    zero_rows: zeroRows.length,
    mapped_zero_rows: mappedZeroRows.length,
    unmapped_zero_rows: unmappedZeroRows.length,
    non_geographic_zero_rows: nonGeographicZeroRows.length,
    inferred_prior_rows: inferredPriorRows,
    inferred_neighbor_rows: inferredNeighborRows,
    inferred_total_rows: inferredPriorRows + inferredNeighborRows,
    weighted_vtd20_replaced_counties: inferredWeightedCounties,
    total_votes: totals.total,
    total_integrity_delta: totalIntegrityDelta,
    zero_mapped_by_county: Object.fromEntries([...zeroMappedByCounty.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))),
    zero_unmapped_by_county: Object.fromEntries([...zeroUnmappedByCounty.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))),
    mapped_zero_labels: mappedZeroRows,
    unmapped_zero_labels: unmappedZeroRows,
    non_geographic_zero_labels: nonGeographicZeroRows,
  };
}

function writeCsv(records, outPath) {
  const header = [
    "file",
    "contest_type",
    "year",
    "rows",
    "mapped_rows",
    "unmapped_rows",
    "non_geographic_rows",
    "zero_rows",
    "mapped_zero_rows",
    "unmapped_zero_rows",
    "non_geographic_zero_rows",
    "inferred_prior_rows",
    "inferred_neighbor_rows",
    "inferred_total_rows",
    "weighted_vtd20_replaced_counties",
    "total_votes",
    "total_integrity_delta",
    "top_mapped_zero_counties",
  ];
  const lines = [header.join(",")];
  for (const row of records) {
    const topCounties = Object.entries(row.zero_mapped_by_county || {})
      .slice(0, 10)
      .map(([county, count]) => `${county}:${count}`)
      .join(";");
    const flat = { ...row, top_mapped_zero_counties: topCounties };
    lines.push(header.map((key) => csvEscape(flat[key])).join(","));
  }
  fs.writeFileSync(outPath, `${lines.join("\n")}\n`);
}

function main() {
  fs.mkdirSync(REPORTS_DIR, { recursive: true });
  const mapped = loadMappedLabels();
  const records = contestFiles().map((fileName) => analyzeContest(fileName, mapped));
  const summary = {
    generated_at: new Date().toISOString(),
    precinct_geojson: "Data/tn_voting_precincts.geojson",
    mapped_precinct_labels: mapped.labels.size,
    contests: records,
  };
  const jsonPath = path.join(REPORTS_DIR, "tn_precinct_coverage_report.json");
  const csvPath = path.join(REPORTS_DIR, "tn_precinct_coverage_report.csv");
  fs.writeFileSync(jsonPath, `${JSON.stringify(summary, null, 2)}\n`);
  writeCsv(records, csvPath);

  console.log(`Wrote ${records.length} contest coverage records`);
  console.log(`JSON: ${path.relative(ROOT, jsonPath)}`);
  console.log(`CSV:  ${path.relative(ROOT, csvPath)}`);
  for (const row of records.filter((r) => r.mapped_zero_rows > 0).slice(0, 12)) {
    console.log(`${row.file}: mapped_zero=${row.mapped_zero_rows} top=${Object.entries(row.zero_mapped_by_county).slice(0, 5).map(([c, n]) => `${c}:${n}`).join(" ")}`);
  }
}

main();
