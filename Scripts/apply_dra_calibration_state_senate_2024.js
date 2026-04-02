#!/usr/bin/env node
/* eslint-disable no-console */
/**
 * Calibrate TN State Senate 2024 presidential district results to match a DRA
 * "district-statistics" export (Dem/Rep/Oth shares).
 *
 * Updates:
 * - Data/district_contests/state_senate_president_2024.json (per-district votes/margins)
 * - Data/district_contests/manifest.json (dem_total/rep_total for that file)
 * - Data/district_contests/calibration_targets.csv (merge/replace targets for state_senate/president/2024)
 * - Data/district_contests/calibration_overrides.json (enable + merge/replace overrides for state_senate/president/2024)
 */

const fs = require("node:fs");
const path = require("node:path");

const ROOT = path.join(__dirname, "..");
const DRA_PATH = path.join(ROOT, "Data", "district-statistics 2024 state senate pres.csv");
const SLICE_PATH = path.join(ROOT, "Data", "district_contests", "state_senate_president_2024.json");
const MANIFEST_PATH = path.join(ROOT, "Data", "district_contests", "manifest.json");
const TARGETS_PATH = path.join(ROOT, "Data", "district_contests", "calibration_targets.csv");
const OVERRIDES_PATH = path.join(ROOT, "Data", "district_contests", "calibration_overrides.json");

function normSpace(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

function parseCsv(text) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  const pushField = () => {
    row.push(field);
    field = "";
  };
  const pushRow = () => {
    rows.push(row);
    row = [];
  };

  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"') {
        const next = text[i + 1];
        if (next === '"') {
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
      continue;
    }
    if (ch === ",") {
      pushField();
      continue;
    }
    if (ch === "\r") continue;
    if (ch === "\n") {
      pushField();
      pushRow();
      continue;
    }
    field += ch;
  }
  pushField();
  if (row.length > 1 || (row.length === 1 && normSpace(row[0]))) pushRow();

  if (!rows.length) return [];
  const header = rows[0].map((h) => normSpace(h));
  return rows
    .slice(1)
    .filter((r) => r.some((v) => normSpace(v)))
    .map((r) => {
      const obj = {};
      header.forEach((h, idx) => {
        obj[h] = r[idx] ?? "";
      });
      return obj;
    });
}

function parseDistrictId(raw) {
  const s = normSpace(raw).replace(/^"|"$/g, "");
  if (!s) return null;
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  if (!Number.isFinite(n) || n <= 0) return null;
  return String(n);
}

function parseFrac(raw) {
  const s = normSpace(raw).replace("%", "");
  if (!s) return null;
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return n;
}

function allocateVotes(total, fracs) {
  const keys = Object.keys(fracs);
  const floats = {};
  const floors = {};
  let floorSum = 0;
  keys.forEach((k) => {
    const v = Number(fracs[k]);
    const floatVotes = Math.max(0, total * v);
    floats[k] = floatVotes;
    const base = Math.floor(floatVotes);
    floors[k] = base;
    floorSum += base;
  });

  let remainder = total - floorSum;
  const order = keys
    .slice()
    .sort((a, b) => (floats[b] - floors[b]) - (floats[a] - floors[a]));

  let idx = 0;
  while (remainder > 0) {
    const k = order[idx % order.length];
    floors[k] += 1;
    remainder -= 1;
    idx += 1;
  }

  // If we somehow over-allocated (should not happen), remove from smallest remainders.
  if (remainder < 0) {
    const rev = order.slice().reverse();
    idx = 0;
    while (remainder < 0) {
      const k = rev[idx % rev.length];
      if (floors[k] > 0) {
        floors[k] -= 1;
        remainder += 1;
      }
      idx += 1;
    }
  }
  return floors;
}

function toPct(n) {
  return `${(Number(n) * 100).toFixed(2)}%`;
}

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(p, obj) {
  fs.writeFileSync(p, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
}

function readCsvFile(p) {
  if (!fs.existsSync(p)) return { header: null, rows: [] };
  const text = fs.readFileSync(p, "utf8");
  const lines = text.split(/\r?\n/).filter((l) => l.trim().length);
  if (!lines.length) return { header: null, rows: [] };
  const header = lines[0].split(",").map((h) => h.trim());
  const rows = lines.slice(1).map((line) => line.split(","));
  return { header, rows, rawLines: lines };
}

function writeCsv(p, header, rows) {
  const out = [header.join(",")];
  rows.forEach((r) => out.push(r.join(",")));
  fs.writeFileSync(p, `${out.join("\n")}\n`, "utf8");
}

function main() {
  if (!fs.existsSync(DRA_PATH)) {
    console.error(`Missing DRA CSV: ${DRA_PATH}`);
    process.exit(2);
  }
  if (!fs.existsSync(SLICE_PATH)) {
    console.error(`Missing district slice JSON: ${SLICE_PATH}`);
    process.exit(2);
  }

  const draText = fs.readFileSync(DRA_PATH, "utf8");
  const draRows = parseCsv(draText);
  const draByDistrict = new Map();
  draRows.forEach((r) => {
    const district = parseDistrictId(r.ID);
    if (!district) return;
    const dem = parseFrac(r.Dem);
    const rep = parseFrac(r.Rep);
    const oth = parseFrac(r.Oth);
    if (dem === null || rep === null || oth === null) return;
    draByDistrict.set(district, { dem, rep, oth });
  });

  const slice = readJson(SLICE_PATH);
  const results = slice?.general?.results || {};

  const missing = [];
  const updated = [];
  let demTotal = 0;
  let repTotal = 0;

  Object.keys(results).forEach((district) => {
    if (!/^\d+$/.test(String(district))) return;
    const dra = draByDistrict.get(String(Number(district)));
    if (!dra) {
      missing.push(String(district));
      return;
    }
    const existing = results[district] || {};
    const totalVotes = Number(existing.total_votes || 0) || 0;
    if (totalVotes <= 0) return;

    const alloc = allocateVotes(totalVotes, { dem: dra.dem, rep: dra.rep, other: dra.oth });
    const demVotes = alloc.dem;
    const repVotes = alloc.rep;
    const otherVotes = alloc.other;
    const margin = repVotes - demVotes;
    const marginPct = totalVotes ? (margin / totalVotes * 100) : 0;
    const winner = margin > 0 ? "REP" : (margin < 0 ? "DEM" : "TIE");

    results[district] = {
      ...existing,
      dem_votes: demVotes,
      rep_votes: repVotes,
      other_votes: otherVotes,
      total_votes: totalVotes,
      margin,
      margin_pct: Number(marginPct.toFixed(4)),
      winner
    };

    demTotal += demVotes;
    repTotal += repVotes;
    updated.push({
      district,
      dem: dra.dem,
      rep: dra.rep,
      oth: dra.oth,
      before: { dem: existing.dem_votes, rep: existing.rep_votes, other: existing.other_votes, total: totalVotes },
      after: { dem: demVotes, rep: repVotes, other: otherVotes, total: totalVotes }
    });
  });

  writeJson(SLICE_PATH, slice);

  if (fs.existsSync(MANIFEST_PATH)) {
    const manifest = readJson(MANIFEST_PATH);
    const files = Array.isArray(manifest?.files) ? manifest.files : [];
    const entry = files.find((f) => f?.scope === "state_senate" && Number(f?.year) === 2024 && f?.contest_type === "president");
    if (entry) {
      entry.dem_total = demTotal;
      entry.rep_total = repTotal;
      entry.districts = 33;
    }
    writeJson(MANIFEST_PATH, manifest);
  }

  // Build calibration targets + overrides from the calibrated slice results (votes-based).
  const demCand = results?.["1"]?.dem_candidate || "Kamala D. Harris";
  const repCand = results?.["1"]?.rep_candidate || "Donald J. Trump";

  // calibration_targets.csv merge/replace
  const header = [
    "scope",
    "contest_type",
    "year",
    "district",
    "dem_votes",
    "rep_votes",
    "other_votes",
    "dem_candidate",
    "rep_candidate"
  ];

  let keptRows = [];
  if (fs.existsSync(TARGETS_PATH)) {
    const text = fs.readFileSync(TARGETS_PATH, "utf8");
    const lines = text.split(/\r?\n/).filter((l) => l.trim().length);
    if (lines.length > 1) {
      keptRows = lines
        .slice(1)
        .map((l) => l.split(","))
        .filter((cols) => cols.length >= 4)
        .filter((cols) => {
          const scope = normSpace(cols[0]).toLowerCase();
          const contest = normSpace(cols[1]).toLowerCase();
          const year = normSpace(cols[2]);
          return !(scope === "state_senate" && contest === "president" && year === "2024");
        });
    }
  }

  const newRows = [];
  for (let d = 1; d <= 33; d += 1) {
    const key = String(d);
    const r = results[key];
    if (!r) continue;
    newRows.push([
      "state_senate",
      "president",
      "2024",
      key,
      String(Number(r.dem_votes || 0) || 0),
      String(Number(r.rep_votes || 0) || 0),
      String(Number(r.other_votes || 0) || 0),
      `"${demCand.replace(/\"/g, '""')}"`,
      `"${repCand.replace(/\"/g, '""')}"`
    ]);
  }
  writeCsv(TARGETS_PATH, header, [...keptRows, ...newRows]);

  // calibration_overrides.json merge/replace
  let overridesPayload = { enabled: true, generated_by: "manual calibration overrides for build_tn_contests.py", overrides: [] };
  if (fs.existsSync(OVERRIDES_PATH)) {
    try {
      overridesPayload = readJson(OVERRIDES_PATH);
    } catch (_) {
      // ignore
    }
  }
  overridesPayload.enabled = true;
  const prev = Array.isArray(overridesPayload.overrides) ? overridesPayload.overrides : [];
  const keep = prev.filter((o) => {
    const scope = normSpace(o?.scope).toLowerCase();
    const contest = normSpace(o?.contest_type).toLowerCase();
    const year = String(o?.year ?? "");
    return !(scope === "state_senate" && contest === "president" && year === "2024");
  });
  const next = [];
  for (let d = 1; d <= 33; d += 1) {
    const key = String(d);
    const r = results[key];
    if (!r) continue;
    next.push({
      scope: "state_senate",
      contest_type: "president",
      year: 2024,
      district: key,
      dem_votes: Number(r.dem_votes || 0) || 0,
      rep_votes: Number(r.rep_votes || 0) || 0,
      other_votes: Number(r.other_votes || 0) || 0,
      dem_candidate: demCand,
      rep_candidate: repCand
    });
  }
  overridesPayload.overrides = [...keep, ...next].sort((a, b) => {
    const scopeA = normSpace(a.scope);
    const scopeB = normSpace(b.scope);
    if (scopeA !== scopeB) return scopeA.localeCompare(scopeB);
    const ctA = normSpace(a.contest_type);
    const ctB = normSpace(b.contest_type);
    if (ctA !== ctB) return ctA.localeCompare(ctB);
    const yA = Number(a.year || 0);
    const yB = Number(b.year || 0);
    if (yA !== yB) return yA - yB;
    return Number(a.district || 0) - Number(b.district || 0);
  });
  writeJson(OVERRIDES_PATH, overridesPayload);

  console.log(JSON.stringify({
    updated_districts: updated.length,
    missing_dra_districts: missing,
    example_sd31: updated.find((u) => u.district === "31")
      ? {
          dra: {
            dem: toPct(updated.find((u) => u.district === "31").dem),
            rep: toPct(updated.find((u) => u.district === "31").rep),
            oth: toPct(updated.find((u) => u.district === "31").oth)
          },
          after: updated.find((u) => u.district === "31").after
        }
      : null
  }, null, 2));
}

main();

