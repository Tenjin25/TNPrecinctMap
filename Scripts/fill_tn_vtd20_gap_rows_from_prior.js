#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT, "Data");
const CONTESTS_DIR = path.join(ROOT, "Data", "contests");
const PRECINCT_GEOJSON = path.join(DATA_DIR, "tn_voting_precincts.geojson");
const COUNTY_GEOJSON = path.join(DATA_DIR, "tl_2020_47_county20.geojson");
const GRAPH_JSON = path.join(DATA_DIR, "crosswalks", "dra_tn_vtd20_geojson_v07", "TN_2020_graph.json");

const TARGETS = [
  {
    target: "governor_2022.json",
    prior: "president_2020.json",
    note: "2022 governor zero VTD20 rows filled from 2020 presidential VTD20 size/share proxy",
  },
  {
    target: "president_2024.json",
    prior: "president_2020.json",
    note: "2024 presidential zero VTD20 rows filled from 2020 presidential VTD20 size/share proxy",
  },
  {
    target: "us_senate_2024.json",
    prior: "us_senate_2020.json",
    note: "2024 U.S. Senate zero VTD20 rows filled from 2020 U.S. Senate VTD20 size/share proxy",
  },
];

function normSpace(value) {
  return String(value ?? "").trim().replace(/\s+/g, " ");
}

function normCounty(value) {
  return normSpace(value).toUpperCase();
}

function countyFromLabel(label) {
  return normCounty(String(label || "").split(" - ")[0] || "");
}

function loadContest(fileName) {
  return JSON.parse(fs.readFileSync(path.join(CONTESTS_DIR, fileName), "utf8"));
}

function loadJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function rowTotal(row, field) {
  return Number(row[field] || 0);
}

function rowBuckets(row) {
  return {
    dem: rowTotal(row, "dem_votes"),
    rep: rowTotal(row, "rep_votes"),
    other: rowTotal(row, "other_votes"),
  };
}

function countyTotals(rows) {
  return rows.reduce(
    (acc, row) => {
      acc.dem += rowTotal(row, "dem_votes");
      acc.rep += rowTotal(row, "rep_votes");
      acc.other += rowTotal(row, "other_votes");
      return acc;
    },
    { dem: 0, rep: 0, other: 0 },
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
}

function topCandidate(rows, voteField, candidateField) {
  let bestCandidate = "";
  let bestVotes = -1;
  for (const row of rows) {
    const candidate = normSpace(row[candidateField]);
    const votes = rowTotal(row, voteField);
    if (candidate && votes > bestVotes) {
      bestCandidate = candidate;
      bestVotes = votes;
    }
  }
  return bestCandidate;
}

function addResidual(rows, field, targetValue) {
  const current = rows.reduce((sum, row) => sum + rowTotal(row, field), 0);
  const diff = Math.round(targetValue) - current;
  if (!diff) return;
  const recipient = rows
    .filter((row) => row.total_votes > 0 || rowTotal(row, field) > 0)
    .sort((a, b) => rowTotal(b, field) - rowTotal(a, field))[0] || rows[0];
  if (!recipient) return;
  recipient[field] = Math.max(0, rowTotal(recipient, field) + diff);
  recomputeRow(recipient);
}

function scaleCountyBackToOriginal(rows, originalTotals) {
  const afterFill = countyTotals(rows);
  const factors = {
    dem: afterFill.dem ? originalTotals.dem / afterFill.dem : 1,
    rep: afterFill.rep ? originalTotals.rep / afterFill.rep : 1,
    other: afterFill.other ? originalTotals.other / afterFill.other : 1,
  };

  for (const row of rows) {
    row.dem_votes = rowTotal(row, "dem_votes") * factors.dem;
    row.rep_votes = rowTotal(row, "rep_votes") * factors.rep;
    row.other_votes = rowTotal(row, "other_votes") * factors.other;
    recomputeRow(row);
  }

  addResidual(rows, "dem_votes", originalTotals.dem);
  addResidual(rows, "rep_votes", originalTotals.rep);
  addResidual(rows, "other_votes", originalTotals.other);
  for (const row of rows) recomputeRow(row);
}

function mapRowsByLabel(rows) {
  return new Map(rows.map((row) => [normSpace(row.county), row]));
}

function loadCountyFipsByName() {
  const out = new Map();
  const geo = loadJson(COUNTY_GEOJSON);
  for (const feature of geo.features || []) {
    const props = feature.properties || {};
    const county = normCounty(props.NAME20 || props.name || "");
    const fips = String(props.COUNTYFP20 || props.countyfp || "").padStart(3, "0");
    if (county && /^\d{3}$/.test(fips)) out.set(county, fips);
  }
  return out;
}

function loadLabelGeoidIndexes() {
  const countyFips = loadCountyFipsByName();
  const geo = loadJson(PRECINCT_GEOJSON);
  const labelToGeoid = new Map();
  const geoidToLabel = new Map();
  for (const feature of geo.features || []) {
    const props = feature.properties || {};
    const county = normCounty(props.county_norm || props.county_nam || "");
    const fips = countyFips.get(county);
    const precId = normSpace(props.prec_id || props.VTDST20 || props.vtdst20 || "");
    if (!county || !fips || !/^\d+$/.test(precId)) continue;
    const label = `${county} - ${precId}`;
    const geoid = `47${fips}${precId}`;
    labelToGeoid.set(label, geoid);
    geoidToLabel.set(geoid, label);
  }
  return { labelToGeoid, geoidToLabel };
}

function groupRowsByCounty(rows) {
  const out = new Map();
  for (const row of rows) {
    const county = countyFromLabel(row.county);
    if (!county) continue;
    if (!out.has(county)) out.set(county, []);
    out.get(county).push(row);
  }
  return out;
}

function neighborRowsFor(row, rowsByLabel, graph, labelIndexes) {
  const label = normSpace(row.county);
  const geoid = labelIndexes.labelToGeoid.get(label);
  if (!geoid) return [];
  const county = countyFromLabel(label);
  return (graph[geoid] || [])
    .map((neighborGeoid) => labelIndexes.geoidToLabel.get(String(neighborGeoid)))
    .filter((neighborLabel) => neighborLabel && countyFromLabel(neighborLabel) === county)
    .map((neighborLabel) => rowsByLabel.get(neighborLabel))
    .filter((neighborRow) => neighborRow && rowTotal(neighborRow, "total_votes") > 0);
}

function averageNeighborBuckets(neighbors) {
  const total = neighbors.reduce((sum, row) => sum + rowTotal(row, "total_votes"), 0);
  if (!total) return null;
  return {
    demShare: neighbors.reduce((sum, row) => sum + rowTotal(row, "dem_votes"), 0) / total,
    repShare: neighbors.reduce((sum, row) => sum + rowTotal(row, "rep_votes"), 0) / total,
    otherShare: neighbors.reduce((sum, row) => sum + rowTotal(row, "other_votes"), 0) / total,
    avgTotal: total / neighbors.length,
    labels: neighbors.map((row) => row.county),
  };
}

function fillTarget({ target, prior, note }) {
  const targetNode = loadContest(target);
  const priorNode = loadContest(prior);
  const priorByLabel = mapRowsByLabel(priorNode.rows || []);
  const targetByLabel = mapRowsByLabel(targetNode.rows || []);
  const targetByCounty = groupRowsByCounty(targetNode.rows || []);
  const graph = fs.existsSync(GRAPH_JSON) ? loadJson(GRAPH_JSON) : {};
  const labelIndexes = loadLabelGeoidIndexes();
  const priorFillStats = [];
  const neighborFillStats = [];
  let filledRows = 0;
  let priorFilledRows = 0;
  let neighborFilledRows = 0;

  for (const [county, rows] of targetByCounty.entries()) {
    const originalTotals = countyTotals(rows);
    if (!originalTotals.dem && !originalTotals.rep && !originalTotals.other) continue;

    const nonzeroRows = rows.filter((row) => rowTotal(row, "total_votes") > 0);
    if (!nonzeroRows.length) continue;

    const zeroRows = rows.filter((row) => {
      if (rowTotal(row, "total_votes") > 0) return false;
      const priorRow = priorByLabel.get(normSpace(row.county));
      return priorRow && rowTotal(priorRow, "total_votes") > 0;
    });
    const demCandidate = topCandidate(nonzeroRows, "dem_votes", "dem_candidate");
    const repCandidate = topCandidate(nonzeroRows, "rep_votes", "rep_candidate");

    if (zeroRows.length) {
      const priorCountyRows = rows
        .map((row) => priorByLabel.get(normSpace(row.county)))
        .filter(Boolean);
      const priorTotals = countyTotals(priorCountyRows);
      const totalFactor = priorTotals.dem + priorTotals.rep + priorTotals.other
        ? (originalTotals.dem + originalTotals.rep + originalTotals.other) / (priorTotals.dem + priorTotals.rep + priorTotals.other)
        : 1;
      const bucketFactors = {
        dem: priorTotals.dem ? originalTotals.dem / priorTotals.dem : totalFactor,
        rep: priorTotals.rep ? originalTotals.rep / priorTotals.rep : totalFactor,
        other: priorTotals.other ? originalTotals.other / priorTotals.other : totalFactor,
      };

      for (const row of zeroRows) {
        const priorRow = priorByLabel.get(normSpace(row.county));
        const priorBuckets = rowBuckets(priorRow);
        row.dem_votes = priorBuckets.dem * bucketFactors.dem;
        row.rep_votes = priorBuckets.rep * bucketFactors.rep;
        row.other_votes = priorBuckets.other * bucketFactors.other;
        row.dem_candidate = row.dem_candidate || demCandidate;
        row.rep_candidate = row.rep_candidate || repCandidate;
        recomputeRow(row);
      }

      filledRows += zeroRows.length;
      priorFilledRows += zeroRows.length;
      priorFillStats.push({
        county,
        rows_filled: zeroRows.length,
        prior_source: `Data/contests/${prior}`,
        total_votes_preserved: true,
        labels: zeroRows.map((row) => row.county),
      });
    }

    const neighborRows = rows.filter((row) => rowTotal(row, "total_votes") <= 0 && labelIndexes.labelToGeoid.has(normSpace(row.county)));
    const neighborFilled = [];
    for (const row of neighborRows) {
      const neighbors = neighborRowsFor(row, targetByLabel, graph, labelIndexes);
      const avg = averageNeighborBuckets(neighbors);
      if (!avg) continue;
      row.dem_votes = avg.avgTotal * avg.demShare;
      row.rep_votes = avg.avgTotal * avg.repShare;
      row.other_votes = avg.avgTotal * avg.otherShare;
      row.dem_candidate = row.dem_candidate || demCandidate;
      row.rep_candidate = row.rep_candidate || repCandidate;
      recomputeRow(row);
      neighborFilled.push({
        label: row.county,
        neighbor_labels: avg.labels,
      });
    }

    if (neighborFilled.length) {
      filledRows += neighborFilled.length;
      neighborFilledRows += neighborFilled.length;
      neighborFillStats.push({
        county,
        rows_filled: neighborFilled.length,
        total_votes_preserved: true,
        labels: neighborFilled.map((item) => item.label),
        neighbor_evidence: neighborFilled,
      });
    }

    if (!zeroRows.length && !neighborFilled.length) continue;
    scaleCountyBackToOriginal(rows, originalTotals);
  }

  if (!filledRows) {
    console.log(`${target}: no prior-backed zero VTD20 rows to fill`);
    return;
  }

  targetNode.meta = {
    ...(targetNode.meta || {}),
    rows: (targetNode.rows || []).length,
    prior_vtd20_gap_fill: true,
    prior_vtd20_gap_fill_applied_at: new Date().toISOString(),
    prior_vtd20_gap_fill_source: `Data/contests/${prior}`,
    prior_vtd20_gap_fill_note: note,
    prior_vtd20_gap_fill_rows: filledRows,
    prior_vtd20_gap_fill_prior_rows: priorFilledRows,
    prior_vtd20_gap_fill_neighbor_rows: neighborFilledRows,
    prior_vtd20_gap_fill_counties: priorFillStats,
    neighbor_vtd20_gap_fill: neighborFilledRows > 0,
    neighbor_vtd20_gap_fill_source: "Data/crosswalks/dra_tn_vtd20_geojson_v07/TN_2020_graph.json",
    neighbor_vtd20_gap_fill_counties: neighborFillStats,
  };
  fs.writeFileSync(path.join(CONTESTS_DIR, target), `${JSON.stringify(targetNode, null, 2)}\n`);
  console.log(`${target}: filled ${filledRows} zero VTD20 rows (${priorFilledRows} prior, ${neighborFilledRows} neighbor)`);
}

function main() {
  for (const target of TARGETS) fillTarget(target);
}

main();
