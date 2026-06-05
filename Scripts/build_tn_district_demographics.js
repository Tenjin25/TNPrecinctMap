const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');
const dataDir = path.join(root, 'Data');
const crosswalkDir = path.join(dataDir, 'crosswalks');
const cvapDir = path.join(dataDir, 'cvap_aggregates');

function readJson(relPath) {
  return JSON.parse(fs.readFileSync(path.join(root, relPath), 'utf8'));
}

function readCsv(relPath) {
  const text = fs.readFileSync(path.join(root, relPath), 'utf8').trim();
  if (!text) return [];
  const lines = text.split(/\r?\n/);
  const headers = lines.shift().split(',').map((h) => h.trim());
  return lines
    .filter((line) => line.trim().length > 0)
    .map((line) => {
      const cols = line.split(',');
      const row = {};
      headers.forEach((header, idx) => {
        row[header] = (cols[idx] || '').trim();
      });
      return row;
    });
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function csvEscape(value) {
  const str = value == null ? '' : String(value);
  return /[",\n]/.test(str) ? `"${str.replace(/"/g, '""')}"` : str;
}

function writeCsv(filePath, rows, columns) {
  const header = columns.join(',');
  const body = rows.map((row) => columns.map((col) => csvEscape(row[col])).join(','));
  fs.writeFileSync(filePath, `${header}\n${body.join('\n')}\n`, 'utf8');
}

function normalizeCountyName(value) {
  return String(value || '').trim().toUpperCase();
}

function numeric(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function round(value, digits = 2) {
  const factor = 10 ** digits;
  return Math.round((Number(value) + Number.EPSILON) * factor) / factor;
}

function pointInRing(point, ring) {
  const [x, y] = point;
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const xi = ring[i][0], yi = ring[i][1];
    const xj = ring[j][0], yj = ring[j][1];
    const intersects = ((yi > y) !== (yj > y))
      && (x < ((xj - xi) * (y - yi)) / ((yj - yi) || Number.EPSILON) + xi);
    if (intersects) inside = !inside;
  }
  return inside;
}

function pointInPolygon(point, polygonCoords) {
  if (!Array.isArray(polygonCoords) || !polygonCoords.length) return false;
  if (!pointInRing(point, polygonCoords[0])) return false;
  for (let i = 1; i < polygonCoords.length; i += 1) {
    if (pointInRing(point, polygonCoords[i])) return false;
  }
  return true;
}

function pointInGeometry(point, geometry) {
  if (!geometry) return false;
  if (geometry.type === 'Polygon') return pointInPolygon(point, geometry.coordinates);
  if (geometry.type === 'MultiPolygon') {
    return geometry.coordinates.some((poly) => pointInPolygon(point, poly));
  }
  return false;
}

function buildCountyNameByFips() {
  const counties = readJson('Data/tl_2020_47_county20.geojson');
  const byFips = new Map();
  for (const feature of counties.features || []) {
    const props = feature.properties || {};
    const fips = String(props.COUNTYFP20 || props.COUNTYFP || props.GEOID || '')
      .replace(/\D/g, '')
      .slice(-3)
      .padStart(3, '0');
    const name = normalizeCountyName(props.NAME20 || props.NAME || props.CountyName || props.COUNTYNAME);
    if (fips && name) byFips.set(fips, name);
  }
  return byFips;
}

function buildPrecinctCvapMap() {
  const byFips = buildCountyNameByFips();
  const payload = readJson('Data/tn_cvap_by_precinct_2020.json');
  const out = new Map();
  for (const [rawKey, row] of Object.entries(payload.precincts || {})) {
    const [countyFipsRaw, precIdRaw] = String(rawKey).split('-', 2);
    const countyFips = String(countyFipsRaw || '').replace(/\D/g, '').slice(-3).padStart(3, '0');
    const county = byFips.get(countyFips) || '';
    const precId = String(precIdRaw || '').trim().toUpperCase();
    if (!county || !precId) continue;
    out.set(`${county} - ${precId}`, {
      precinct_norm: `${county} - ${precId}`,
      cvap_tot: numeric(row.cvap_tot),
      cvap_wht: numeric(row.cvap_wht),
      cvap_bla: numeric(row.cvap_bla),
      cvap_hsp: numeric(row.cvap_hsp),
      cvap_asi: numeric(row.cvap_asi),
      cvap_ami: numeric(row.cvap_ami),
      cvap_2om: numeric(row.cvap_2om),
      cvap_nhs: numeric(row.cvap_nhs),
    });
  }
  return out;
}

function buildCountyDemographics(countyPopulationRows, precinctCvapMap) {
  const countyPopByName = new Map();
  for (const row of countyPopulationRows || []) {
    const county = normalizeCountyName(row.county_name);
    if (!county) continue;
    countyPopByName.set(county, numeric(row.pop_2025));
  }

  const totalsByCounty = new Map();
  for (const [precinctKey, cvap] of precinctCvapMap.entries()) {
    const county = normalizeCountyName(String(precinctKey || '').split(' - ', 1)[0]);
    if (!county) continue;
    if (!totalsByCounty.has(county)) {
      totalsByCounty.set(county, {
        county,
        total_population: 0,
        vap_18plus: 0,
        white_vap: 0,
        black_vap: 0,
        hispanic_vap: 0,
        native_vap: 0,
        asian_vap: 0,
        pacific_vap: 0,
        multiracial_vap: 0,
      });
    }
    const row = totalsByCounty.get(county);
    row.total_population += numeric(cvap.cvap_tot);
    row.vap_18plus += numeric(cvap.cvap_tot);
    row.white_vap += numeric(cvap.cvap_wht);
    row.black_vap += numeric(cvap.cvap_bla);
    row.hispanic_vap += numeric(cvap.cvap_hsp);
    row.native_vap += numeric(cvap.cvap_ami);
    row.asian_vap += numeric(cvap.cvap_asi);
    row.multiracial_vap += numeric(cvap.cvap_2om);
    row.pacific_vap += numeric(cvap.cvap_pac);
  }

  const counties = {};
  Array.from(totalsByCounty.values())
    .sort((a, b) => a.county.localeCompare(b.county))
    .forEach((row) => {
      const popEstimate = countyPopByName.get(row.county) || row.total_population;
      const vap = numeric(row.vap_18plus);
      counties[row.county] = {
        county: row.county,
        total_population: Math.round(popEstimate),
        vap_18plus: Math.round(vap),
        white_pop_pct: vap > 0 ? round((row.white_vap / vap) * 100) : 0,
        black_pop_pct: vap > 0 ? round((row.black_vap / vap) * 100) : 0,
        hispanic_pop_pct: vap > 0 ? round((row.hispanic_vap / vap) * 100) : 0,
        native_pop_pct: vap > 0 ? round((row.native_vap / vap) * 100) : 0,
        asian_pop_pct: vap > 0 ? round((row.asian_vap / vap) * 100) : 0,
        pacific_pop_pct: vap > 0 ? round((row.pacific_vap / vap) * 100) : 0,
        multiracial_pop_pct: vap > 0 ? round((row.multiracial_vap / vap) * 100) : 0,
      };
    });

  return {
    source: 'precinct_cvap_aggregation_with_2025_county_population_estimates',
    counties,
  };
}

function buildPrecinctCentroids() {
  const centroids = readJson('Data/tn_precinct_centroids_dra_v07.geojson');
  return (centroids.features || []).map((feature) => ({
    point: feature.geometry?.coordinates || null,
    precinct_norm: String(feature.properties?.precinct_norm || '').trim().toUpperCase(),
  })).filter((row) => row.point && row.precinct_norm);
}

function getDistrictId(scope, props) {
  if (scope === 'congressional') return String(props.DISTRICT || props.CD118FP || props.CD119FP || '').replace(/^0+/, '') || '';
  if (scope === 'state_house') return String(props.SLDLST || props.district || '').replace(/^0+/, '') || '';
  if (scope === 'state_senate') return String(props.SLDUST || props.district || '').replace(/^0+/, '') || '';
  return '';
}

function aggregateScope(scope, geometryPath, precinctCentroids, precinctCvapMap) {
  const geojson = readJson(geometryPath);
  const districts = (geojson.features || []).map((feature) => {
    const district = getDistrictId(scope, feature.properties || {});
    return district ? { district, geometry: feature.geometry } : null;
  }).filter(Boolean);

  const totals = new Map();
  for (const centroid of precinctCentroids) {
    const cvap = precinctCvapMap.get(centroid.precinct_norm);
    if (!cvap) continue;
    const match = districts.find((district) => pointInGeometry(centroid.point, district.geometry));
    if (!match) continue;
    if (!totals.has(match.district)) {
      totals.set(match.district, {
        district: match.district,
        total_population: 0,
        total_vap: 0,
        white_vap: 0,
        black_vap: 0,
        hispanic_vap: 0,
        native_vap: 0,
        asian_vap: 0,
        pacific_vap: 0,
        multiracial_vap: 0,
      });
    }
    const row = totals.get(match.district);
    row.total_population += cvap.cvap_tot;
    row.total_vap += cvap.cvap_tot;
    row.white_vap += cvap.cvap_wht;
    row.black_vap += cvap.cvap_bla;
    row.hispanic_vap += cvap.cvap_hsp;
    row.native_vap += cvap.cvap_ami;
    row.asian_vap += cvap.cvap_asi;
    row.multiracial_vap += cvap.cvap_2om;
    row.pacific_vap += cvap.cvap_pac || 0;
  }

  const detailRows = Array.from(totals.values())
    .sort((a, b) => Number(a.district) - Number(b.district))
    .map((row) => {
      const total = numeric(row.total_vap);
      return {
        district: row.district,
        total_population: Math.round(row.total_population),
        total_vap: Math.round(row.total_vap),
        white_vap_pct: total > 0 ? round((row.white_vap / total) * 100) : 0,
        black_vap_pct: total > 0 ? round((row.black_vap / total) * 100) : 0,
        hispanic_vap_pct: total > 0 ? round((row.hispanic_vap / total) * 100) : 0,
        native_vap_pct: total > 0 ? round((row.native_vap / total) * 100) : 0,
        asian_vap_pct: total > 0 ? round((row.asian_vap / total) * 100) : 0,
        pacific_vap_pct: total > 0 ? round((row.pacific_vap / total) * 100) : 0,
        multiracial_vap_pct: total > 0 ? round((row.multiracial_vap / total) * 100) : 0,
      };
    });

  const cvapRows = detailRows.map((row) => ({
    district: row.district,
    CVAP_TOT24: row.total_vap,
  }));

  return { detailRows, cvapRows };
}

function main() {
  ensureDir(cvapDir);
  const precinctCvapMap = buildPrecinctCvapMap();
  const precinctCentroids = buildPrecinctCentroids();
  const countyPopulationRows = readCsv('Data/CO-EST2025-POP-47.cleaned.csv');

  const specs = [
    {
      scope: 'congressional',
      geometryPath: 'Data/tl_2022_47_cd118.geojson',
      infoOuts: [
        path.join(dataDir, 'tn_congressional_districts.csv'),
      ],
      cvapOuts: [
        path.join(cvapDir, 'cd118_2022_lines__cvap24.csv'),
        path.join(cvapDir, 'cd119_2024_lines__cvap24.csv'),
      ],
    },
    {
      scope: 'congressional_2026',
      geometryPath: 'Data/tl_2026_47_cd2026.geojson',
      infoOuts: [
        path.join(dataDir, 'tn_congressional_districts_2026.csv'),
      ],
      cvapOuts: [
        path.join(cvapDir, 'cd2026_2026_lines__cvap24.csv'),
      ],
    },
    {
      scope: 'state_house',
      geometryPath: 'Data/tl_2022_47_sldl.geojson',
      infoOuts: [path.join(dataDir, 'tn_state_house_districts.csv')],
      cvapOuts: [
        path.join(cvapDir, 'state_house_2022_lines__cvap24.csv'),
        path.join(cvapDir, 'state_house_2024_lines__cvap24.csv'),
      ],
    },
    {
      scope: 'state_senate',
      geometryPath: 'Data/tl_2022_47_sldu.geojson',
      infoOut: path.join(dataDir, 'tn_state_senate_districts.csv'),
      cvapOuts: [
        path.join(cvapDir, 'state_senate_2022_lines__cvap24.csv'),
        path.join(cvapDir, 'state_senate_2024_lines__cvap24.csv'),
      ],
    },
  ];

  for (const spec of specs) {
    const scope = spec.scope === 'congressional_2026' ? 'congressional' : spec.scope;
    const { detailRows, cvapRows } = aggregateScope(scope, spec.geometryPath, precinctCentroids, precinctCvapMap);
    for (const infoOut of spec.infoOuts || []) {
      writeCsv(infoOut, detailRows, [
      'district',
      'total_population',
      'total_vap',
      'white_vap_pct',
      'black_vap_pct',
      'hispanic_vap_pct',
      'native_vap_pct',
      'asian_vap_pct',
      'pacific_vap_pct',
      'multiracial_vap_pct',
      ]);
    }
    for (const outPath of spec.cvapOuts) {
      writeCsv(outPath, cvapRows, ['district', 'CVAP_TOT24']);
    }
  }

  const countyDemographics = buildCountyDemographics(countyPopulationRows, precinctCvapMap);
  fs.writeFileSync(
    path.join(dataDir, 'county_demographics_2020_dp1.json'),
    `${JSON.stringify(countyDemographics, null, 2)}\n`,
    'utf8'
  );

  console.log('Built Tennessee district demographic tables and CVAP aggregates.');
}

main();
