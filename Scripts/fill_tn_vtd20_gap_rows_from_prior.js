#!/usr/bin/env node
"use strict";

const fs = require("fs");
const path = require("path");

const ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT, "Data");
const CONTESTS_DIR = path.join(ROOT, "Data", "contests");
const OFFICIAL_VTD20_GEOJSON = path.join(DATA_DIR, "tn_vtd_2020.geojson");
const DRA_PRECINCT_GEOJSON = path.join(DATA_DIR, "tn_voting_precincts.geojson");
const COUNTY_GEOJSON = path.join(DATA_DIR, "tl_2020_47_county20.geojson");
const GRAPH_JSON = path.join(DATA_DIR, "crosswalks", "dra_tn_vtd20_geojson_v07", "TN_2020_graph.json");

const TARGETS = [
  {
    target: "president_2000.json",
    prior: "",
    note: "2000 presidential zero VTD20 rows filled from adjacent VTD20 neighbor / county-share proxy",
  },
  {
    target: "us_senate_2000.json",
    prior: "",
    note: "2000 U.S. Senate zero VTD20 rows filled from adjacent VTD20 neighbor / county-share proxy",
  },
  {
    target: "governor_2002.json",
    prior: "president_2000.json",
    note: "2002 governor zero VTD20 rows filled from 2000 presidential same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "us_senate_2002.json",
    prior: "president_2000.json",
    note: "2002 U.S. Senate zero VTD20 rows filled from 2000 presidential same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "president_2004.json",
    prior: "president_2000.json",
    note: "2004 presidential zero VTD20 rows filled from 2000 presidential same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "governor_2006.json",
    prior: "governor_2002.json",
    note: "2006 governor zero VTD20 rows filled from 2002 governor same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "us_senate_2006.json",
    prior: "us_senate_2000.json",
    note: "2006 U.S. Senate zero VTD20 rows filled from 2000 U.S. Senate same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "president_2008.json",
    prior: "president_2004.json",
    note: "2008 presidential zero VTD20 rows filled from 2004 presidential same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "us_senate_2008.json",
    prior: "us_senate_2002.json",
    note: "2008 U.S. Senate zero VTD20 rows filled from 2002 U.S. Senate same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "governor_2010.json",
    prior: "governor_2006.json",
    note: "2010 governor zero VTD20 rows filled from 2006 governor same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "president_2012.json",
    prior: "president_2008.json",
    note: "2012 presidential zero VTD20 rows filled from 2008 presidential same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "us_senate_2012.json",
    prior: "us_senate_2006.json",
    note: "2012 U.S. Senate zero VTD20 rows filled from 2006 U.S. Senate same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "governor_2014.json",
    prior: "governor_2010.json",
    note: "2014 governor zero VTD20 rows filled from 2010 governor same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "us_senate_2014.json",
    prior: "us_senate_2008.json",
    note: "2014 U.S. Senate zero VTD20 rows filled from 2008 U.S. Senate same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "president_2016.json",
    prior: "president_2012.json",
    note: "2016 presidential zero VTD20 rows filled from 2012 presidential same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "governor_2018.json",
    prior: "governor_2014.json",
    note: "2018 governor zero VTD20 rows filled from 2014 governor same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "us_senate_2018.json",
    prior: "us_senate_2012.json",
    note: "2018 U.S. Senate zero VTD20 rows filled from 2012 U.S. Senate same-VTD size/share proxy, then neighbor / county-share",
  },
  {
    target: "president_2020.json",
    prior: "",
    note: "2020 presidential zero VTD20 rows filled from adjacent VTD20 neighbor / county-share proxy",
  },
  {
    target: "us_senate_2020.json",
    prior: "",
    note: "2020 U.S. Senate zero VTD20 rows filled from adjacent VTD20 neighbor / county-share proxy",
  },
  {
    target: "governor_2022.json",
    prior: "president_2020.json",
    note: "2022 governor zero VTD20 rows filled from 2020 presidential VTD20 size/share proxy, then neighbor / county-share",
  },
  {
    target: "president_2024.json",
    prior: "president_2020.json",
    note: "2024 presidential zero VTD20 rows filled from 2020 presidential VTD20 size/share proxy, then neighbor / county-share",
  },
  {
    target: "us_senate_2024.json",
    prior: "us_senate_2020.json",
    note: "2024 U.S. Senate zero VTD20 rows filled from 2020 U.S. Senate VTD20 size/share proxy, then neighbor / county-share",
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
  const fipsToCounty = new Map([...countyFips.entries()].map(([name, fips]) => [fips, name]));
  const labelToGeoid = new Map();
  const geoidToLabel = new Map();

  // Prefer the lighter DRA precinct layer when present; IDs match official VTDST20.
  const preferDra = fs.existsSync(DRA_PRECINCT_GEOJSON);
  const geoPath = preferDra ? DRA_PRECINCT_GEOJSON : OFFICIAL_VTD20_GEOJSON;
  const geo = loadJson(geoPath);

  for (const feature of geo.features || []) {
    const props = feature.properties || {};
    let county = normCounty(props.county_norm || props.county_nam || "");
    let fips = countyFips.get(county) || "";
    const countyFp = String(props.COUNTYFP20 || props.countyfp || "").padStart(3, "0");
    if ((!county || !fips) && /^\d{3}$/.test(countyFp)) {
      fips = countyFp;
      county = fipsToCounty.get(fips) || county;
    }
    const precRaw = props.VTDST20 || props.vtdst20 || props.prec_id || "";
    const precId = String(precRaw).trim();
    if (!county || !fips || !/^\d+$/.test(precId)) continue;
    const code = precId.padStart(6, "0");
    const label = `${county} - ${code}`;
    const geoid = `47${fips}${code}`;
    labelToGeoid.set(label, geoid);
    geoidToLabel.set(geoid, label);
  }
  return { labelToGeoid, geoidToLabel, geoPath };
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
  if (!neighbors.length || total <= 0) return null;
  return {
    demShare: neighbors.reduce((sum, row) => sum + rowTotal(row, "dem_votes"), 0) / total,
    repShare: neighbors.reduce((sum, row) => sum + rowTotal(row, "rep_votes"), 0) / total,
    otherShare: neighbors.reduce((sum, row) => sum + rowTotal(row, "other_votes"), 0) / total,
    avgTotal: total / neighbors.length,
    labels: neighbors.map((row) => row.county),
  };
}

function applyProxyBuckets(row, avg, demCandidate, repCandidate, source) {
  row.dem_votes = avg.avgTotal * avg.demShare;
  row.rep_votes = avg.avgTotal * avg.repShare;
  row.other_votes = avg.avgTotal * avg.otherShare;
  row.dem_candidate = row.dem_candidate || demCandidate;
  row.rep_candidate = row.rep_candidate || repCandidate;
  row.inferred_gap_fill = source;
  recomputeRow(row);
}

function rescueRoundedZeros(rows, demCandidate, repCandidate) {
  const stillZero = rows.filter((row) => rowTotal(row, "total_votes") <= 0);
  if (!stillZero.length) return 0;
  let rescued = 0;
  for (const row of stillZero) {
    const donor = rows
      .filter((candidate) => rowTotal(candidate, "total_votes") > 1)
      .sort((a, b) => rowTotal(b, "total_votes") - rowTotal(a, "total_votes"))[0];
    if (!donor) break;
    if (rowTotal(donor, "rep_votes") >= rowTotal(donor, "dem_votes") && rowTotal(donor, "rep_votes") > 0) {
      donor.rep_votes = rowTotal(donor, "rep_votes") - 1;
      row.rep_votes = 1;
      row.dem_votes = 0;
      row.other_votes = 0;
    } else if (rowTotal(donor, "dem_votes") > 0) {
      donor.dem_votes = rowTotal(donor, "dem_votes") - 1;
      row.dem_votes = 1;
      row.rep_votes = 0;
      row.other_votes = 0;
    } else {
      donor.other_votes = rowTotal(donor, "other_votes") - 1;
      row.other_votes = 1;
      row.dem_votes = 0;
      row.rep_votes = 0;
    }
    row.dem_candidate = row.dem_candidate || demCandidate;
    row.rep_candidate = row.rep_candidate || repCandidate;
    row.inferred_gap_fill = row.inferred_gap_fill || "county_share_rescue";
    recomputeRow(donor);
    recomputeRow(row);
    rescued += 1;
  }
  return rescued;
}

function fillTarget({ target, prior, note }) {
  const targetNode = loadContest(target);
  const priorPath = prior ? path.join(CONTESTS_DIR, prior) : "";
  const priorNode = priorPath && fs.existsSync(priorPath) ? loadContest(prior) : null;
  const originalRowCount = (targetNode.rows || []).length;
  const priorByLabel = mapRowsByLabel(priorNode?.rows || []);
  const targetByLabel = mapRowsByLabel(targetNode.rows || []);
  const targetByCounty = groupRowsByCounty(targetNode.rows || []);
  const graph = fs.existsSync(GRAPH_JSON) ? loadJson(GRAPH_JSON) : {};
  const labelIndexes = loadLabelGeoidIndexes();
  const priorFillStats = [];
  const neighborFillStats = [];
  const countyShareFillStats = [];
  let filledRows = 0;
  let priorFilledRows = 0;
  let neighborFilledRows = 0;
  let countyShareFilledRows = 0;
  let rescuedRows = 0;

  for (const [county, rows] of targetByCounty.entries()) {
    const originalTotals = countyTotals(rows);
    if (!originalTotals.dem && !originalTotals.rep && !originalTotals.other) continue;

    const nonzeroRows = rows.filter((row) => rowTotal(row, "total_votes") > 0);
    if (!nonzeroRows.length) continue;

    const zeroRows = priorNode
      ? rows.filter((row) => {
        if (rowTotal(row, "total_votes") > 0) return false;
        const priorRow = priorByLabel.get(normSpace(row.county));
        return priorRow && rowTotal(priorRow, "total_votes") > 0;
      })
      : [];
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
        row.inferred_gap_fill = "prior_vtd20";
        recomputeRow(row);
      }

      filledRows += zeroRows.length;
      priorFilledRows += zeroRows.length;
      priorFillStats.push({
        county,
        rows_filled: zeroRows.length,
        prior_source: prior ? `Data/contests/${prior}` : "",
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
      applyProxyBuckets(row, avg, demCandidate, repCandidate, "neighbor_vtd20");
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

    const countyShareRows = rows.filter((row) => rowTotal(row, "total_votes") <= 0);
    const countyAvg = averageNeighborBuckets(nonzeroRows);
    const countyShareFilled = [];
    if (countyAvg && countyShareRows.length) {
      for (const row of countyShareRows) {
        applyProxyBuckets(row, countyAvg, demCandidate, repCandidate, "county_share");
        countyShareFilled.push(row.county);
      }
      filledRows += countyShareFilled.length;
      countyShareFilledRows += countyShareFilled.length;
      countyShareFillStats.push({
        county,
        rows_filled: countyShareFilled.length,
        total_votes_preserved: true,
        labels: countyShareFilled,
      });
    }

    if (!zeroRows.length && !neighborFilled.length && !countyShareFilled.length) continue;
    scaleCountyBackToOriginal(rows, originalTotals);
    rescuedRows += rescueRoundedZeros(rows, demCandidate, repCandidate);
  }

  const prunedNonGeographicRows = [];
  targetNode.rows = (targetNode.rows || []).filter((row) => {
    if (rowTotal(row, "total_votes") > 0) return true;
    const code = precinctCodeFromLabel(row.county);
    if (!isNonGeographicPrecinctCode(code)) return true;
    prunedNonGeographicRows.push(row.county);
    return false;
  });

  const remainingZeros = (targetNode.rows || []).filter((row) => rowTotal(row, "total_votes") <= 0).length;

  if (!filledRows && !prunedNonGeographicRows.length && !rescuedRows) {
    console.log(`${target}: no prior/neighbor/county-share zero VTD20 rows or zero-vote non-geographic rows to fill`);
    return;
  }

  targetNode.meta = {
    ...(targetNode.meta || {}),
    rows: (targetNode.rows || []).length,
    prior_vtd20_gap_fill: true,
    prior_vtd20_gap_fill_applied_at: new Date().toISOString(),
    prior_vtd20_gap_fill_source: prior ? `Data/contests/${prior}` : "",
    prior_vtd20_gap_fill_note: note,
    prior_vtd20_gap_fill_rows: filledRows,
    prior_vtd20_gap_fill_prior_rows: priorFilledRows,
    prior_vtd20_gap_fill_neighbor_rows: neighborFilledRows,
    prior_vtd20_gap_fill_county_share_rows: countyShareFilledRows,
    prior_vtd20_gap_fill_rescued_rows: rescuedRows,
    prior_vtd20_gap_fill_remaining_zeros: remainingZeros,
    prior_vtd20_gap_fill_counties: priorFillStats,
    neighbor_vtd20_gap_fill: neighborFilledRows > 0,
    neighbor_vtd20_gap_fill_source: "Data/crosswalks/dra_tn_vtd20_geojson_v07/TN_2020_graph.json",
    neighbor_vtd20_gap_fill_counties: neighborFillStats,
    county_share_vtd20_gap_fill: countyShareFilledRows > 0,
    county_share_vtd20_gap_fill_counties: countyShareFillStats,
    zero_vote_non_geographic_rows_pruned: prunedNonGeographicRows.length,
    zero_vote_non_geographic_pruned_labels: prunedNonGeographicRows,
    gap_fill_label_index_source: path.relative(ROOT, labelIndexes.geoPath).replace(/\\/g, "/"),
  };
  fs.writeFileSync(path.join(CONTESTS_DIR, target), `${JSON.stringify(targetNode, null, 2)}\n`);
  console.log(
    `${target}: filled ${filledRows} zero VTD20 rows `
    + `(${priorFilledRows} prior, ${neighborFilledRows} neighbor, ${countyShareFilledRows} county-share), `
    + `rescued ${rescuedRows}, remainingZeros=${remainingZeros}, `
    + `pruned ${prunedNonGeographicRows.length} zero non-geographic rows, `
    + `rows ${originalRowCount}->${targetNode.rows.length}`,
  );
}

function main() {
  for (const target of TARGETS) fillTarget(target);
}

main();
