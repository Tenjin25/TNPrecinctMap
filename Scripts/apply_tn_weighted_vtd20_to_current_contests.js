#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT, "Data");
const CONTESTS_DIR = path.join(DATA_DIR, "contests");
const CROSSWALKS_DIR = path.join(DATA_DIR, "crosswalks");
const PRECINCT_GEOJSON = path.join(DATA_DIR, "tn_voting_precincts.geojson");

const TARGETS = [
  {
    year: 2000,
    contestType: "president",
    sourceCsv: path.join(DATA_DIR, "20001107__tn__general__president__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "president_2000.json"),
    sourceShape: "long",
  },
  {
    year: 2000,
    contestType: "us_senate",
    sourceCsv: path.join(DATA_DIR, "20001107__tn__general__senate__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "us_senate_2000.json"),
    sourceShape: "long",
  },
  {
    year: 2002,
    contestType: "governor",
    sourceCsv: path.join(DATA_DIR, "20021105__tn__general__governor__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "governor_2002.json"),
    sourceShape: "long",
  },
  {
    year: 2002,
    contestType: "us_senate",
    sourceCsv: path.join(DATA_DIR, "20021105__tn__general__senate__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "us_senate_2002.json"),
    sourceShape: "long",
  },
  {
    year: 2004,
    contestType: "president",
    sourceCsv: path.join(DATA_DIR, "20041102__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "president_2004.json"),
    sourceShape: "long",
  },
  {
    year: 2006,
    contestType: "governor",
    sourceCsv: path.join(DATA_DIR, "20061107__tn__general__governor__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "governor_2006.json"),
    sourceShape: "long",
  },
  {
    year: 2006,
    contestType: "us_senate",
    sourceCsv: path.join(DATA_DIR, "20061107__tn__general__senate__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "us_senate_2006.json"),
    sourceShape: "long",
  },
  {
    year: 2008,
    contestType: "president",
    sourceCsv: path.join(DATA_DIR, "20081104__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "president_2008.json"),
    sourceShape: "long",
  },
  {
    year: 2008,
    contestType: "us_senate",
    sourceCsv: path.join(DATA_DIR, "20081104__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "us_senate_2008.json"),
    sourceShape: "long",
  },
  {
    year: 2010,
    contestType: "governor",
    sourceCsv: path.join(DATA_DIR, "20101102__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "governor_2010.json"),
    sourceShape: "long",
  },
  {
    year: 2012,
    contestType: "president",
    sourceCsv: path.join(DATA_DIR, "20121106__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "president_2012.json"),
    sourceShape: "long",
  },
  {
    year: 2012,
    contestType: "us_senate",
    sourceCsv: path.join(DATA_DIR, "20121106__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "us_senate_2012.json"),
    sourceShape: "long",
  },
  {
    year: 2014,
    contestType: "governor",
    sourceCsv: path.join(DATA_DIR, "20141104__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "governor_2014.json"),
    sourceShape: "long",
  },
  {
    year: 2014,
    contestType: "us_senate",
    sourceCsv: path.join(DATA_DIR, "20141104__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "us_senate_2014.json"),
    sourceShape: "long",
  },
  {
    year: 2016,
    contestType: "president",
    sourceCsv: path.join(DATA_DIR, "20161108__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "president_2016.json"),
    sourceShape: "long",
  },
  {
    year: 2018,
    contestType: "governor",
    sourceCsv: path.join(DATA_DIR, "20181106__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "governor_2018.json"),
    sourceShape: "long",
  },
  {
    year: 2018,
    contestType: "us_senate",
    sourceCsv: path.join(DATA_DIR, "20181106__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "us_senate_2018.json"),
    sourceShape: "long",
  },
  {
    year: 2020,
    contestType: "president",
    sourceCsv: path.join(DATA_DIR, "20201103__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "president_2020.json"),
    sourceShape: "long",
  },
  {
    year: 2020,
    contestType: "us_senate",
    sourceCsv: path.join(DATA_DIR, "20201103__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "us_senate_2020.json"),
    sourceShape: "long",
  },
  {
    year: 2022,
    contestType: "governor",
    sourceCsv: path.join(DATA_DIR, "20221108__tn__general__governor__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "governor_2022.json"),
    sourceShape: "long",
  },
  {
    year: 2024,
    contestType: "president",
    sourceCsv: path.join(DATA_DIR, "20241105__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "president_2024.json"),
    sourceShape: "wide",
  },
  {
    year: 2024,
    contestType: "us_senate",
    sourceCsv: path.join(DATA_DIR, "20241105__tn__general__precinct.csv"),
    outputJson: path.join(CONTESTS_DIR, "us_senate_2024.json"),
    sourceShape: "wide",
  },
];

function normSpace(value) {
  return String(value || "").trim().replace(/\s+/g, " ");
}

function normText(value) {
  return normSpace(value).toUpperCase().replace(/[^A-Z0-9 ]+/g, " ").replace(/\s+/g, " ").trim();
}

function normCounty(value) {
  return normText(value);
}

function normPrecinct(value) {
  return normText(value);
}

function parseVotes(value) {
  const cleaned = String(value ?? "").replace(/,/g, "").trim();
  if (!cleaned) return 0;
  const parsed = Number(cleaned);
  if (!Number.isFinite(parsed)) return 0;
  return Math.round(parsed);
}

function partyBucket(value) {
  const party = normSpace(value).toUpperCase();
  if (party === "D" || party === "DEM" || party === "DEMOCRAT" || party.startsWith("DEMOCRAT")) return "DEM";
  if (party === "R" || party === "REP" || party === "REPUBLICAN" || party.startsWith("REPUBLICAN")) return "REP";
  return "OTHER";
}

function isKnownPartyLabel(value) {
  const party = normSpace(value).toUpperCase();
  return (
    !party ||
    party === "D" ||
    party === "DEM" ||
    party === "DEMOCRAT" ||
    party === "DEMOCRATIC" ||
    party === "R" ||
    party === "REP" ||
    party === "REPUBLICAN" ||
    party === "I" ||
    party === "IND" ||
    party === "INDEPENDENT" ||
    party === "OTH" ||
    party === "OTHER" ||
    party === "NA" ||
    party === "N/A" ||
    party === "WRITE-IN" ||
    party === "WRITE IN" ||
    party === "WRITEIN" ||
    party === "CONSTITUTION" ||
    party === "GREEN"
  );
}

function normalizeLongPartyCandidate(row) {
  let party = normSpace(row.party);
  let candidate = normSpace(row.candidate);
  const candidateUpper = candidate.toUpperCase();

  // Some Tennessee exports use party=<candidate>, candidate=Write-In.
  if ((candidateUpper === "WRITE-IN" || candidateUpper === "WRITE IN" || candidateUpper === "WRITEIN") && party && !isKnownPartyLabel(party)) {
    candidate = party;
    party = "Write-In";
  }
  return { party, candidate };
}

function inferContestType(officeRaw) {
  const office = normSpace(officeRaw).toUpperCase();
  if (office.includes("PRESIDENT")) return "president";
  if (office.includes("GOVERNOR") && !office.includes("LIEUTENANT")) return "governor";
  if (office.includes("U.S. SENATE") || office.includes("UNITED STATES SENATE")) return "us_senate";
  return null;
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
      continue;
    }
    if (ch === '"') {
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
  const header = rows[0].map((h) => h.trim());
  return rows.slice(1).filter((r) => r.some((v) => String(v || "").trim())).map((r) => {
    const out = {};
    header.forEach((h, idx) => {
      out[h] = r[idx] ?? "";
    });
    return out;
  });
}

class Totals {
  constructor() {
    this.dem = 0;
    this.rep = 0;
    this.other = 0;
    this.demCandidates = new Map();
    this.repCandidates = new Map();
  }

  add(party, candidate, votes) {
    if (!Number.isFinite(votes) || votes <= 0) return;
    const cand = normSpace(candidate);
    if (party === "DEM") {
      this.dem += votes;
      if (cand) this.demCandidates.set(cand, (this.demCandidates.get(cand) || 0) + votes);
    } else if (party === "REP") {
      this.rep += votes;
      if (cand) this.repCandidates.set(cand, (this.repCandidates.get(cand) || 0) + votes);
    } else {
      this.other += votes;
    }
  }

  scale(demFactor, repFactor, otherFactor) {
    this.dem *= demFactor;
    this.rep *= repFactor;
    this.other *= otherFactor;
    for (const [candidate, votes] of this.demCandidates.entries()) {
      this.demCandidates.set(candidate, votes * demFactor);
    }
    for (const [candidate, votes] of this.repCandidates.entries()) {
      this.repCandidates.set(candidate, votes * repFactor);
    }
  }

  isNonZero() {
    return this.dem > 0 || this.rep > 0 || this.other > 0;
  }

  topCandidate(bucket) {
    let winner = "";
    let topVotes = -1;
    for (const [candidate, votes] of bucket.entries()) {
      if (votes > topVotes) {
        winner = candidate;
        topVotes = votes;
      }
    }
    return winner;
  }

  toRow(label) {
    const dem = Math.round(this.dem);
    const rep = Math.round(this.rep);
    const other = Math.round(this.other);
    const total = dem + rep + other;
    const margin = rep - dem;
    const marginPct = total ? Number(((margin / total) * 100).toFixed(4)) : 0;
    const winner = margin > 0 ? "REP" : margin < 0 ? "DEM" : "TIE";
    return {
      county: label,
      dem_votes: dem,
      rep_votes: rep,
      other_votes: other,
      total_votes: total,
      dem_candidate: this.topCandidate(this.demCandidates),
      rep_candidate: this.topCandidate(this.repCandidates),
      margin,
      margin_pct: marginPct,
      winner,
      color: "",
    };
  }
}

function totalsFromRows(rows) {
  const totals = new Totals();
  for (const row of rows) {
    totals.add("DEM", row.dem_candidate || "", Number(row.dem_votes || 0));
    totals.add("REP", row.rep_candidate || "", Number(row.rep_votes || 0));
    totals.add("OTHER", "", Number(row.other_votes || 0));
  }
  return totals;
}

function loadOverlayLabels() {
  const geo = JSON.parse(fs.readFileSync(PRECINCT_GEOJSON, "utf8"));
  const labels = new Map();
  const allLabels = new Set();
  for (const feature of geo.features || []) {
    const props = feature.properties || {};
    const county = normCounty(props.county_norm || props.county_nam || "");
    const precId = normSpace(props.prec_id || props.VTDST20 || props.vtdst20 || "");
    if (!county || !precId) continue;
    const label = `${county} - ${precId}`;
    if (!labels.has(county)) labels.set(county, new Set());
    labels.get(county).add(label);
    allLabels.add(label);
  }
  return { byCounty: labels, allLabels };
}

function loadWeightedCrosswalk(year) {
  const file = path.join(CROSSWALKS_DIR, `tn_precinct_to_vtd20_blockweighted_${year}.csv`);
  const rows = parseCsv(fs.readFileSync(file, "utf8"));
  const grouped = new Map();
  for (const row of rows) {
    const county = normCounty(row.county_norm);
    const precinct = normPrecinct(row.from_precinct_norm);
    const dst = normSpace(row.dst_vtd20);
    const weight = Number(row.weight);
    if (!county || !precinct || !dst || !Number.isFinite(weight) || weight <= 0) continue;
    const key = `${county}\t${precinct}`;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key).push({ dst, weight });
  }
  for (const [key, allocs] of grouped.entries()) {
    const sum = allocs.reduce((acc, item) => acc + item.weight, 0);
    if (sum > 0) {
      grouped.set(key, allocs.map((item) => ({ dst: item.dst, weight: item.weight / sum })));
    }
  }
  return { file, grouped };
}

function getTotals(map, label) {
  if (!map.has(label)) map.set(label, new Totals());
  return map.get(label);
}

function addAllocatedVotes(totalsByLabel, crosswalk, overlayLabelSet, county, precinct, party, candidate, votes, stats) {
  if (!votes) return;
  const key = `${county}\t${precinct}`;
  const allocs = crosswalk.get(key);
  if (!allocs || !allocs.length) {
    stats.unmatchedSourceRows += 1;
    stats.unmatchedVotes += votes;
    return;
  }
  const drawableAllocs = allocs
    .map((alloc) => ({ ...alloc, label: `${county} - ${alloc.dst}` }))
    .filter((alloc) => overlayLabelSet.has(alloc.label));
  const drawableWeight = drawableAllocs.reduce((acc, item) => acc + item.weight, 0);
  if (!drawableAllocs.length || drawableWeight <= 0) {
    stats.offMapSourceRows += 1;
    stats.offMapVotes += votes;
    return;
  }
  stats.matchedSourceRows += 1;
  stats.matchedVotes += votes;
  if (drawableAllocs.length > 1) stats.splitSourceRows += 1;
  for (const alloc of drawableAllocs) {
    getTotals(totalsByLabel, alloc.label).add(party, candidate, votes * (alloc.weight / drawableWeight));
  }
}

function collectLongVotes(target, crosswalk, overlayLabelSet) {
  const totalsByLabel = new Map();
  const stats = { matchedSourceRows: 0, unmatchedSourceRows: 0, offMapSourceRows: 0, splitSourceRows: 0, matchedVotes: 0, unmatchedVotes: 0, offMapVotes: 0 };
  const rows = parseCsv(fs.readFileSync(target.sourceCsv, "utf8"));
  for (const row of rows) {
    if (inferContestType(row.office) !== target.contestType) continue;
    const county = normCounty(row.county);
    const precinct = normPrecinct(row.precinct);
    const votes = parseVotes(row.votes);
    const normalized = normalizeLongPartyCandidate(row);
    addAllocatedVotes(totalsByLabel, crosswalk, overlayLabelSet, county, precinct, partyBucket(normalized.party), normalized.candidate, votes, stats);
  }
  return { totalsByLabel, stats };
}

function collectWideVotes(target, crosswalk, overlayLabelSet) {
  const totalsByLabel = new Map();
  const stats = { matchedSourceRows: 0, unmatchedSourceRows: 0, offMapSourceRows: 0, splitSourceRows: 0, matchedVotes: 0, unmatchedVotes: 0, offMapVotes: 0 };
  const rows = parseCsv(fs.readFileSync(target.sourceCsv, "utf8"));
  for (const row of rows) {
    if (inferContestType(row.OFFICENAME) !== target.contestType) continue;
    const county = normCounty(row.COUNTY);
    const precinct = normPrecinct(row.PRECINCT);
    for (let i = 1; i <= 10; i += 1) {
      const candidate = row[`RNAME${i}`];
      if (!normSpace(candidate)) continue;
      const votes = parseVotes(row[`PVTALLY${i}`]);
      addAllocatedVotes(totalsByLabel, crosswalk, overlayLabelSet, county, precinct, partyBucket(row[`PARTY${i}`]), candidate, votes, stats);
    }
  }
  return { totalsByLabel, stats };
}

function countyFromLabel(label) {
  return normCounty(String(label || "").split(" - ")[0] || "");
}

function preserveExistingRows(existingRows, computedLabels, replacedCounties) {
  const preserved = [];
  for (const row of existingRows) {
    const label = normSpace(row.county);
    if (!label || computedLabels.has(label)) continue;
    if (replacedCounties.has(countyFromLabel(label))) continue;
    preserved.push(row);
  }
  return preserved;
}

function sortRows(rows) {
  rows.sort((a, b) => String(a.county || "").localeCompare(String(b.county || ""), "en", { numeric: true }));
}

function recomputeRow(row) {
  row.total_votes = Number(row.dem_votes || 0) + Number(row.rep_votes || 0) + Number(row.other_votes || 0);
  row.margin = Number(row.rep_votes || 0) - Number(row.dem_votes || 0);
  row.margin_pct = row.total_votes ? Number(((row.margin / row.total_votes) * 100).toFixed(4)) : 0;
  row.winner = row.margin > 0 ? "REP" : row.margin < 0 ? "DEM" : "TIE";
}

function applyBucketResidual(rows, field, targetValue) {
  const current = rows.reduce((sum, row) => sum + Number(row[field] || 0), 0);
  let diff = Math.round(targetValue) - current;
  if (!diff) return;
  const ranked = rows
    .filter((row) => Number(row.total_votes || 0) > 0)
    .sort((a, b) => Number(b[field] || 0) - Number(a[field] || 0));
  const target = ranked[0] || rows[0];
  if (!target) return;
  target[field] = Math.max(0, Number(target[field] || 0) + diff);
  recomputeRow(target);
}

function rebalanceCountyRows(rows, county, targetTotals) {
  const countyRows = rows.filter((row) => countyFromLabel(row.county) === county);
  applyBucketResidual(countyRows, "dem_votes", targetTotals.dem);
  applyBucketResidual(countyRows, "rep_votes", targetTotals.rep);
  applyBucketResidual(countyRows, "other_votes", targetTotals.other);
}

function patchContest(target, overlayLabels) {
  const existing = JSON.parse(fs.readFileSync(target.outputJson, "utf8"));
  const { file: crosswalkFile, grouped: crosswalk } = loadWeightedCrosswalk(target.year);
  const collected = target.sourceShape === "wide"
    ? collectWideVotes(target, crosswalk, overlayLabels.allLabels)
    : collectLongVotes(target, crosswalk, overlayLabels.allLabels);
  const computedRows = [];
  const computedLabels = new Set();
  const computedByCounty = new Map();
  for (const [label, totals] of collected.totalsByLabel.entries()) {
    if (!totals.isNonZero()) continue;
    const county = countyFromLabel(label);
    if (!computedByCounty.has(county)) computedByCounty.set(county, []);
    computedByCounty.get(county).push({ label, totals });
  }

  const existingRows = existing.rows || [];
  const existingByCounty = new Map();
  for (const row of existingRows) {
    const county = countyFromLabel(row.county);
    if (!county) continue;
    if (!existingByCounty.has(county)) existingByCounty.set(county, []);
    existingByCounty.get(county).push(row);
  }

  const replacedCounties = new Set();
  const replacementStats = [];
  for (const [county, items] of computedByCounty.entries()) {
    const drawableCount = overlayLabels.byCounty.get(county)?.size || 0;
    const coverage = drawableCount ? items.length / drawableCount : 0;
    const existingCountyRows = existingByCounty.get(county) || [];
    const existingTotals = totalsFromRows(existingCountyRows);
    const computedTotals = totalsFromRows(items.map((item) => item.totals.toRow(item.label)));
    const existingTotalVotes = existingTotals.dem + existingTotals.rep + existingTotals.other;
    const computedTotalVotes = computedTotals.dem + computedTotals.rep + computedTotals.other;
    const voteCoverage = existingTotalVotes ? computedTotalVotes / existingTotalVotes : 0;
    if (coverage < 0.75 || voteCoverage < 0.5 || !existingTotalVotes || !computedTotalVotes) {
      continue;
    }
    const demFactor = computedTotals.dem ? existingTotals.dem / computedTotals.dem : 1;
    const repFactor = computedTotals.rep ? existingTotals.rep / computedTotals.rep : 1;
    const otherFactor = computedTotals.other ? existingTotals.other / computedTotals.other : 1;
    const oldZeroCount = existingCountyRows.filter((row) => Number(row.total_votes || 0) <= 0).length;
    const scaledItems = items.map((item) => {
      const scaled = item.totals;
      scaled.scale(demFactor, repFactor, otherFactor);
      return item;
    });
    const scaledZeroCount = scaledItems.filter((item) => Number(item.totals.toRow(item.label).total_votes || 0) <= 0).length;
    const newZeroCount = Math.max(0, drawableCount - items.length) + scaledZeroCount;
    if (newZeroCount >= oldZeroCount) {
      continue;
    }
    for (const item of items) {
      computedLabels.add(item.label);
      computedRows.push(item.totals.toRow(item.label));
    }
    replacedCounties.add(county);
    replacementStats.push({
      county,
      coverage: Number(coverage.toFixed(4)),
      voteCoverage: Number(voteCoverage.toFixed(4)),
      labels: items.length,
      oldZeroCount,
      newZeroCount,
    });
  }

  const rows = [...computedRows, ...preserveExistingRows(existingRows, computedLabels, replacedCounties)];
  if (!computedRows.length) {
    console.log(`${path.basename(target.outputJson)}: no coverage-improving county replacements; unchanged`);
    return;
  }
  const existingLabels = new Set(rows.map((row) => normSpace(row.county)).filter(Boolean));
  let zeroFill = 0;
  for (const labels of overlayLabels.byCounty.values()) {
    for (const label of labels) {
      if (existingLabels.has(label)) continue;
      const county = countyFromLabel(label);
      if (!replacedCounties.has(county) && existingByCounty.has(county)) continue;
      rows.push(new Totals().toRow(label));
      existingLabels.add(label);
      zeroFill += 1;
    }
  }
  for (const county of replacedCounties) {
    rebalanceCountyRows(rows, county, totalsFromRows(existingByCounty.get(county) || []));
  }
  sortRows(rows);

  const out = {
    ...existing,
    meta: {
      ...(existing.meta || {}),
      source: existing.meta?.source || "tn_precinct_csv_to_2024_precinct_ids",
      rows: rows.length,
      synthetic_zero_rows_added: zeroFill,
      weighted_vtd20_allocation: true,
      weighted_vtd20_source: path.relative(ROOT, crosswalkFile).replace(/\\/g, "/"),
      weighted_vtd20_applied_at: new Date().toISOString(),
      weighted_vtd20_matched_source_rows: collected.stats.matchedSourceRows,
      weighted_vtd20_split_source_rows: collected.stats.splitSourceRows,
      weighted_vtd20_unmatched_source_rows: collected.stats.unmatchedSourceRows,
      weighted_vtd20_off_map_source_rows: collected.stats.offMapSourceRows,
      weighted_vtd20_matched_votes: Math.round(collected.stats.matchedVotes),
      weighted_vtd20_unmatched_votes: Math.round(collected.stats.unmatchedVotes),
      weighted_vtd20_off_map_votes: Math.round(collected.stats.offMapVotes),
      weighted_vtd20_preserved_existing_rows: rows.length - computedRows.length - zeroFill,
      weighted_vtd20_replaced_counties: replacementStats,
    },
    rows,
  };
  fs.writeFileSync(target.outputJson, `${JSON.stringify(out, null, 2)}\n`);
  const zeroRows = rows.filter((row) => Number(row.total_votes || 0) <= 0).length;
  console.log(`${path.basename(target.outputJson)}: rows=${rows.length} computed=${computedRows.length} zero=${zeroRows} splitSourceRows=${collected.stats.splitSourceRows} unmatchedVotes=${Math.round(collected.stats.unmatchedVotes)} offMapVotes=${Math.round(collected.stats.offMapVotes)}`);
}

function main() {
  const overlayLabels = loadOverlayLabels();
  for (const target of TARGETS) {
    patchContest(target, overlayLabels);
  }
}

main();
