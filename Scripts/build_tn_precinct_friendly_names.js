#!/usr/bin/env node

const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const dataDir = path.join(root, 'Data');
const crosswalkDir = path.join(dataDir, 'crosswalks');

const vtdPath = path.join(dataDir, 'tn_vtd_2020_census_statewide.geojson');
const countyPath = path.join(dataDir, 'tl_2020_47_county20.geojson');
const outPath = path.join(crosswalkDir, 'tn_precinct_friendly_names_2020.json');

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function normalizeCountyName(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function formatDisplayName(raw) {
  let s = String(raw || '').replace(/[_]+/g, ' ').replace(/\s+/g, ' ').trim();
  if (!s) return '';
  s = s.replace(/'S\b/g, "'s");
  s = s.replace(/\bSt (?=[A-Z])/g, 'St. ');
  s = s.replace(/\bMt (?=[A-Z])/g, 'Mt. ');
  return s;
}

function pickVtdLabel(props) {
  const namelsad = normalizeCountyName(props.NAMELSAD20 || '');
  const name = normalizeCountyName(props.NAME20 || '');
  return formatDisplayName(namelsad || name);
}

function main() {
  if (!fs.existsSync(vtdPath)) {
    throw new Error(`Missing VTD source: ${vtdPath}`);
  }
  if (!fs.existsSync(countyPath)) {
    throw new Error(`Missing county source: ${countyPath}`);
  }

  const vtd = readJson(vtdPath);
  const counties = readJson(countyPath);

  const countyByFp = new Map();
  for (const feature of counties.features || []) {
    const props = feature && feature.properties ? feature.properties : {};
    const countyFp = String(props.COUNTYFP20 || '').padStart(3, '0');
    const countyName = normalizeCountyName(props.NAME20 || props.NAME || '');
    if (countyFp && countyName) {
      countyByFp.set(countyFp, countyName);
    }
  }

  const out = {
    version: 1,
    generated_at: new Date().toISOString(),
    source: path.basename(vtdPath),
    label_field: 'NAMELSAD20',
    fallback_label_field: 'NAME20',
    counties: {}
  };

  let entries = 0;
  for (const feature of vtd.features || []) {
    const props = feature && feature.properties ? feature.properties : {};
    const countyFp = String(props.COUNTYFP20 || '').padStart(3, '0');
    const countyName = countyByFp.get(countyFp) || '';
    const code = String(props.VTDST20 || '').trim().toUpperCase();
    const label = pickVtdLabel(props);
    if (!countyName || !code || !label) continue;
    if (!out.counties[countyName]) out.counties[countyName] = {};
    out.counties[countyName][code] = label;
    entries += 1;
  }

  fs.mkdirSync(crosswalkDir, { recursive: true });
  fs.writeFileSync(outPath, `${JSON.stringify(out, null, 2)}\n`, 'utf8');

  const countyCount = Object.keys(out.counties).length;
  console.log(JSON.stringify({
    output: path.relative(root, outPath),
    counties: countyCount,
    entries
  }, null, 2));
}

main();
