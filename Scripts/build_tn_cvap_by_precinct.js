#!/usr/bin/env node
/**
 * Build TN CVAP estimates aggregated to 2020 precinct geometry (VTD_CODE) via BlockAssign.
 *
 * Inputs:
 *  - Data/tn_cvap_2024_2020_b_csv/tn_cvap_2024_2020_b.csv  (block-level CVAP + citizen totals)
 *  - Data/crosswalks/blockassign_tn_vtd.csv                (block GEOID20 -> county fips + vtd_code)
 *
 * Output:
 *  - Data/tn_cvap_by_precinct_2020.json
 */

const fs = require("fs");
const path = require("path");
const readline = require("readline");

const ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(ROOT, "Data");
const INPUT_CVAP = path.join(
  DATA_DIR,
  "tn_cvap_2024_2020_b_csv",
  "tn_cvap_2024_2020_b.csv"
);
const INPUT_BLOCKASSIGN = path.join(DATA_DIR, "crosswalks", "blockassign_tn_vtd.csv");
const OUTPUT_JSON = path.join(DATA_DIR, "tn_cvap_by_precinct_2020.json");

function parseIntLoose(raw) {
  const s = String(raw ?? "").trim();
  if (!s) return 0;
  const n = Number(s);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
}

function addInto(target, delta) {
  for (const [k, v] of Object.entries(delta)) {
    target[k] = (target[k] || 0) + (Number(v) || 0);
  }
}

async function loadBlockAssignIndex() {
  if (!fs.existsSync(INPUT_BLOCKASSIGN)) {
    throw new Error(`Missing ${INPUT_BLOCKASSIGN}`);
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(INPUT_BLOCKASSIGN, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let header = null;
  let idxGeoid = -1;
  let idxCounty = -1;
  let idxVtd = -1;

  const out = new Map(); // GEOID20 -> { countyfp, vtd }
  let rows = 0;

  for await (const line of rl) {
    if (!line) continue;
    if (!header) {
      header = line.split(",").map((s) => s.trim());
      idxGeoid = header.indexOf("block_geoid_2020");
      idxCounty = header.indexOf("county_fips");
      idxVtd = header.indexOf("vtd_code");
      if (idxGeoid < 0 || idxCounty < 0 || idxVtd < 0) {
        throw new Error(`Unexpected header in ${INPUT_BLOCKASSIGN}`);
      }
      continue;
    }

    const parts = line.split(",");
    const geoid = (parts[idxGeoid] || "").trim();
    const countyfp = (parts[idxCounty] || "").trim().padStart(3, "0");
    const vtd = (parts[idxVtd] || "").trim().padStart(6, "0");
    if (!geoid || !countyfp || !vtd) continue;
    out.set(geoid, { countyfp, vtd });
    rows += 1;
  }

  return { index: out, rows };
}

async function buildCvapAggregates(blockIndex) {
  if (!fs.existsSync(INPUT_CVAP)) {
    throw new Error(`Missing ${INPUT_CVAP}`);
  }

  const rl = readline.createInterface({
    input: fs.createReadStream(INPUT_CVAP, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let header = null;
  const idx = {};

  const want = [
    "GEOID20",
    "CVAP_TOT24",
    "CVAP_WHT24",
    "CVAP_BLA24",
    "CVAP_HSP24",
    "CVAP_ASI24",
    "CVAP_AMI24",
    "CVAP_2OM24",
    "CVAP_NHS24",
  ];

  const countyAgg = new Map(); // countyfp -> totals
  const precinctAgg = new Map(); // `${countyfp}-${vtd}` -> totals

  let blockRows = 0;
  let matchedBlocks = 0;
  let unmatchedBlocks = 0;
  let skippedZeroBlocks = 0;

  for await (const line of rl) {
    if (!line) continue;
    if (!header) {
      header = line.split(",").map((s) => s.trim());
      for (const col of want) {
        idx[col] = header.indexOf(col);
      }
      if (idx.GEOID20 < 0 || idx.CVAP_TOT24 < 0) {
        throw new Error(`Unexpected header in ${INPUT_CVAP}`);
      }
      continue;
    }

    blockRows += 1;
    const parts = line.split(",");
    const geoid = (parts[idx.GEOID20] || "").trim();
    if (!geoid) continue;

    const match = blockIndex.get(geoid);
    if (!match) {
      unmatchedBlocks += 1;
      continue;
    }
    matchedBlocks += 1;

    const totals = {
      cvap_tot: parseIntLoose(parts[idx.CVAP_TOT24]),
      cvap_wht: idx.CVAP_WHT24 >= 0 ? parseIntLoose(parts[idx.CVAP_WHT24]) : 0,
      cvap_bla: idx.CVAP_BLA24 >= 0 ? parseIntLoose(parts[idx.CVAP_BLA24]) : 0,
      cvap_hsp: idx.CVAP_HSP24 >= 0 ? parseIntLoose(parts[idx.CVAP_HSP24]) : 0,
      cvap_asi: idx.CVAP_ASI24 >= 0 ? parseIntLoose(parts[idx.CVAP_ASI24]) : 0,
      cvap_ami: idx.CVAP_AMI24 >= 0 ? parseIntLoose(parts[idx.CVAP_AMI24]) : 0,
      cvap_2om: idx.CVAP_2OM24 >= 0 ? parseIntLoose(parts[idx.CVAP_2OM24]) : 0,
      cvap_nhs: idx.CVAP_NHS24 >= 0 ? parseIntLoose(parts[idx.CVAP_NHS24]) : 0,
    };

    if (
      totals.cvap_tot === 0 &&
      totals.cvap_wht === 0 &&
      totals.cvap_bla === 0 &&
      totals.cvap_hsp === 0 &&
      totals.cvap_asi === 0 &&
      totals.cvap_ami === 0 &&
      totals.cvap_2om === 0 &&
      totals.cvap_nhs === 0
    ) {
      skippedZeroBlocks += 1;
      continue;
    }

    const countyKey = match.countyfp;
    const precinctKey = `${match.countyfp}-${match.vtd}`;

    if (!countyAgg.has(countyKey)) countyAgg.set(countyKey, {});
    if (!precinctAgg.has(precinctKey)) precinctAgg.set(precinctKey, {});

    addInto(countyAgg.get(countyKey), totals);
    addInto(precinctAgg.get(precinctKey), totals);
  }

  function mapToObject(map) {
    const obj = {};
    for (const [k, v] of map.entries()) obj[k] = v;
    return obj;
  }

  return {
    meta: {
      source: "tn_cvap_2024_2020_b.csv + blockassign_tn_vtd.csv",
      generated_at: new Date().toISOString(),
      input_blocks_rows: blockRows,
      matched_blocks: matchedBlocks,
      unmatched_blocks: unmatchedBlocks,
      skipped_zero_cvap_blocks: skippedZeroBlocks,
      counties: countyAgg.size,
      precincts: precinctAgg.size,
    },
    counties: mapToObject(countyAgg),
    precincts: mapToObject(precinctAgg),
  };
}

async function main() {
  const { index: blockIndex, rows: blockIndexRows } = await loadBlockAssignIndex();
  const payload = await buildCvapAggregates(blockIndex);
  payload.meta.blockassign_rows = blockIndexRows;

  fs.writeFileSync(OUTPUT_JSON, JSON.stringify(payload, null, 2), "utf8");
  process.stdout.write(
    `Wrote ${OUTPUT_JSON} (counties=${payload.meta.counties}, precincts=${payload.meta.precincts})\n`
  );
}

main().catch((err) => {
  console.error(err?.stack || String(err));
  process.exitCode = 1;
});

