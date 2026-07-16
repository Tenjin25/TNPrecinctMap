# CVAP override comparison bundle

This folder keeps candidate (CVAP-split-override) carryover outputs separate from live app files.

## Layout

- `candidate_crosswalks/` — current carryovers with CVAP split overrides applied
- `area_weighted_crosswalks/` — geometry-only rebuild with no split overrides
- `live_district_contests_2026/` — snapshot of live 2026 congressional district contest JSONs
- `uncalibrated_district_contests_2026/` — same builder with `calibration_overrides.json` disabled (CD 5/8/9 raw aggregation; CD 1/2 still use 2022-line transfer)
- `congressional_2026_calibrated_vs_uncalibrated.csv` — rows that change when calibration is removed
- `congressional_2026_county_contributions_president_2024.csv` — uncalibrated county vote contributions within each 2026 CD
- `congressional_2026_county_contributions_cd589_summary.csv` — top counties in CD 5/8/9 plus cal vs uncal district gaps
- `congressional_2026_weight_diff.csv` — precinct/district rows where candidate weights differ from area
- `congressional_2026_president_2024_totals_diff.csv` — district totals from candidate crosswalk vs live contest JSON

## Notes

- Live TNPrecinctMap currently loads `Data/district_contests_2026/` directly; it does not ship the precinct carryover CSVs.
- Only three 2026 congressional precincts currently have CVAP overrides (Davidson 001898, Montgomery 006457, Rutherford 007691).
- Calibration replaces whole CD 5/8/9 totals, so calibrated results have no county breakdown; county shares below are from the uncalibrated area-weighted allocation.
