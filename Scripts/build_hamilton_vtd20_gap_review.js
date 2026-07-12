#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT, "Data");
const CROSSWALKS_DIR = path.join(DATA_DIR, "crosswalks");
const OUT_PATH = path.join(CROSSWALKS_DIR, "tn_hamilton_vtd20_gap_review.csv");
const GRAPH_PATH = path.join(CROSSWALKS_DIR, "dra_tn_vtd20_geojson_v07", "TN_2020_graph.json");
const FRIENDLY_PATH = path.join(CROSSWALKS_DIR, "tn_precinct_friendly_names_2020.json");

const TARGET_CONTESTS = [
  ["governor_2022", 2022],
  ["president_2024", 2024],
  ["us_senate_2024", 2024],
];

function normSpace(value) {
  return String(value ?? "").trim().replace(/\s+/g, " ");
}

function csvEscape(value) {
  const text = String(value ?? "");
  if (/[",\r\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
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
  const header = rows.shift().map(normSpace);
  return rows
    .filter((r) => r.some((v) => normSpace(v)))
    .map((r) => Object.fromEntries(header.map((h, idx) => [h, r[idx] ?? ""])));
}

function loadJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function loadHamiltonZeroCodes() {
  const codes = new Set();
  for (const [contest] of TARGET_CONTESTS) {
    const contestPath = path.join(DATA_DIR, "contests", `${contest}.json`);
    const data = loadJson(contestPath);
    for (const row of data.rows || []) {
      const label = String(row.county || "");
      if (!label.startsWith("HAMILTON - ")) continue;
      if (Number(row.total_votes || 0) > 0) continue;
      codes.add(label.split(" - ")[1]);
    }
  }
  return [...codes].sort();
}

function loadCrosswalkSourcesByDst(year) {
  const out = new Map();
  const rows = parseCsv(fs.readFileSync(path.join(CROSSWALKS_DIR, `tn_precinct_to_vtd20_blockweighted_${year}.csv`), "utf8"));
  for (const row of rows) {
    if (row.county_norm !== "HAMILTON") continue;
    const dst = normSpace(row.dst_vtd20);
    if (!dst || dst.startsWith("000")) continue;
    const weight = Number(row.weight || 0);
    if (!Number.isFinite(weight) || weight <= 0) continue;
    if (!out.has(dst)) out.set(dst, []);
    out.get(dst).push({
      source: normSpace(row.from_precinct_norm),
      weight,
      method: normSpace(row.match_method),
    });
  }
  for (const [dst, entries] of out.entries()) {
    entries.sort((a, b) => b.weight - a.weight || a.source.localeCompare(b.source));
    out.set(dst, entries);
  }
  return out;
}

function summarizeSources(entries) {
  const bySource = new Map();
  for (const entry of entries || []) {
    const prev = bySource.get(entry.source) || { weight: 0, methods: new Set() };
    prev.weight += entry.weight;
    if (entry.method) prev.methods.add(entry.method);
    bySource.set(entry.source, prev);
  }
  return [...bySource.entries()]
    .sort((a, b) => b[1].weight - a[1].weight || a[0].localeCompare(b[0]))
    .map(([source, info]) => `${source}:${info.weight.toFixed(4)}:${[...info.methods].join("+")}`)
    .join("; ");
}

function main() {
  const friendly = loadJson(FRIENDLY_PATH).counties.Hamilton || {};
  const graph = loadJson(GRAPH_PATH);
  const sourceByDst2022 = loadCrosswalkSourcesByDst(2022);
  const sourceByDst2024 = loadCrosswalkSourcesByDst(2024);
  const zeroCodes = loadHamiltonZeroCodes();
  const rows = [];

  for (const code of zeroCodes) {
    const geoid = `47065${code}`;
    const neighbors = (graph[geoid] || [])
      .map((n) => String(n).replace(/^47065/, ""))
      .filter((n) => n && n !== code)
      .sort();
    const neighborDetails = neighbors
      .map((n) => {
        const name = friendly[n] || "";
        const src2024 = summarizeSources(sourceByDst2024.get(n));
        return `${n} ${name}${src2024 ? ` [2024 ${src2024}]` : ""}`;
      })
      .join(" | ");
    const neighborSources2022 = summarizeSources(neighbors.flatMap((n) => sourceByDst2022.get(n) || []));
    const neighborSources2024 = summarizeSources(neighbors.flatMap((n) => sourceByDst2024.get(n) || []));
    rows.push({
      county_norm: "HAMILTON",
      dst_vtd20: code,
      vtd20_name: friendly[code] || "",
      zero_in_contests: TARGET_CONTESTS.map(([contest]) => contest).join(";"),
      neighbor_vtd20s: neighbors.join(";"),
      neighbor_details_2024: neighborDetails,
      candidate_source_precincts_2022: neighborSources2022,
      candidate_source_precincts_2024: neighborSources2024,
      review_status: "needs_review",
      reviewed_source_precinct: "",
      reviewed_notes: "",
    });
  }

  const header = [
    "county_norm",
    "dst_vtd20",
    "vtd20_name",
    "zero_in_contests",
    "neighbor_vtd20s",
    "neighbor_details_2024",
    "candidate_source_precincts_2022",
    "candidate_source_precincts_2024",
    "review_status",
    "reviewed_source_precinct",
    "reviewed_notes",
  ];
  const lines = [header.join(",")];
  for (const row of rows) {
    lines.push(header.map((key) => csvEscape(row[key])).join(","));
  }
  fs.writeFileSync(OUT_PATH, `${lines.join("\n")}\n`);
  console.log(`Wrote ${rows.length} Hamilton VTD20 gap review rows to ${path.relative(ROOT, OUT_PATH)}`);
}

main();
