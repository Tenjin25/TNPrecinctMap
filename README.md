# Volunteer State Election Atlas (Tennessee Election Atlas)

Interactive Tennessee election map focused on county, district, and precinct analysis with a newsroom-style county storytelling panel.

This project runs as a single-page app from `index.html` and reads local data assets from `Data/`.

## Recent Development Timeline

This project has been moving on two tracks at once: frontend/panel polish for the atlas UI, and deeper historical/data-pipeline work for Tennessee precinct and district results. The dated notes below pull together the recent repo history, including work that was previously undocumented in this README.

### 2026-03-04

- Established the initial Tennessee atlas data/build foundation:
  - precinct map data and build scripts
  - statewide contest manifests
  - district contest slices
  - centroid-ready precinct contest mapping
- Added county-fallback district allocation for statewide contest layers.
- Added the first statewide district slices and precinct-to-district mapping pipeline.
- Updated the Mapbox token configuration in `index.html`.

### 2026-03-20 to 2026-03-23

- Adopted the NC-style UI shell for the Tennessee atlas and applied Tennessee branding.
- Simplified district sourcing by removing older district-line toggles.
- Switched district allocation to shapefile-overlay joins and rebuilt district contests.
- Improved historical precinct code resolution and crosswalk split matching for legislative overlays.
- Reduced urban precinct smearing and improved unresolved-precinct recovery in district aggregation.
- Added creator credit/watermark metadata.
- Added the first shift-mode work to make trend movement easier to interpret.
- Simplified lead labels by removing explicit D/R plus-prefix styling in the index UI.

### 2026-03-28 to 2026-04-03

- Fixed 2024 state-senate allocations in Shelby County.
- Added Tennessee county trajectory and census-analysis sections.
- Brought the county analysis/editorial blocks closer to the NCMap structure and layout.
- Guarded trend rendering to avoid map-init failures.
- Added Tri-Cities CSA context to the county census insight flow.
- Elevated the county editorial panel and documented the broader panel update.
- Corrected Hamblen County archetype classification.

### 2026-04-08 to 2026-04-10

- Matched hover tier/flip chips and county-hover behavior more closely to NCMap.
- Added 2024 state-house results and increased district opacity for better readability.
- Added the Tennessee governor 2022 precinct CSV and results pipeline.
- Removed the old `VTD 2000` map view and added CVAP estimates.
- Added county vote-delta comparisons versus the previous cycle.
- Added the NCMap-style county population delta block.
- Fixed HD-41 `2022` (Windle vs Butler) handling and corrected district flip baselines.
- Refined county hover layout, click behavior, wording, and no-raw-vote presentation.
- Renamed the atlas and shifted UI accents to the Tennessee / UT palette.
- Updated README coverage for the NCMap-style county-hover/local-run workflow.

### 2026-04-13 to 2026-04-24

- Fixed Senate flip comparison-cycle logic.
- Improved precinct-to-`VTD20` matching and rebuilt slices.
- Reduced historical district leakage and hid older pre-2022 state-house contests where needed.
- Fixed `2024 PRCTSEQ -> VTD20` mapping and the precinct overlay join path.
- Preferred `2024 PRCTSEQ` mapping to reduce precinct collisions.
- Filled missing precinct rows across contests and improved the precinct no-row UI messaging.
- Fixed popup-title wrapping and a CSS parse error caused by a stray brace.
- Ported and completed a more polished NC-style mobile sheet/layout experience.
- Styled district boundaries more like DRA / SC map treatments.
- Added several district calibration adjustments, including HD-37, HD-50, HD-59, HD-74, HD-75, and HD-90.
- Improved shift-mode hover tooltips and added percent-sign formatting in shift hover copy.

### 2026-05-05 to 2026-05-18

- Improved precinct-matching accuracy and wired the Tennessee precinct-alias crosswalk.
- Fixed U.S. Senate flip-chip comparison-year chains and applied seat-year chains to trend timelines.
- Improved uncontested-race display so they can appear without misleading margins.
- Adjusted the HD-41 `2022` read so the Windle independent result lands in the intended lean-R bucket.
- Added and wired older historical source imports:
  - `1998` governor county data
  - `2000`, `2002`, and `2006` election source CSVs
  - derived contest JSONs and manifest updates
- Extended the Senate timeline years and standardized the `1998` governor contest naming.
- Fixed Van Buren County key/value issues in the `1998` governor import.
- Documented the historical import / manifest / timeline changes.
- Refined VoteHub / hover formatting:
  - raw-vote styling
  - winner-line tone
  - shift-block formatting
  - uncontested labeling
  - flip/shift placement
- Aligned mobile tooltip behavior with the NC map.
- Matched town/county label halos and focus-opacity behavior more closely to NC styling.
- Fixed margin-bucket rounding drift near the `1%` threshold.

### 2026-05-23 to 2026-05-31

- Added confidence-tiered precinct crosswalk outputs and the manual-fix workflow.
- Switched precinct-resolution work over to DRA-derived Tennessee VTD/precinct geometry inputs.
- Updated the map sources to the DRA `v07` precinct geometry filenames.
- Refined the title badge, mobile layers sheet density, and tooltip button styling to better match other state-map treatments.
- Restored fuller mobile county-tooltip detail, including vote and population deltas.

### 2026-06-02 to 2026-06-04

- Added the population-change mode to the Tennessee precinct map and polished its hover colors/visuals.
- Promoted the population-growth hover line and removed redundant pop-change tooltip header copy.
- Added `1998` governor to the manifest where needed.
- Refined the broader Tennessee atlas theme and controls.
- Wired Tennessee data manifests, population loaders, and CVAP loaders into the atlas.
- Hid modeled contests from the selector and restored swing-o-meter controls.
- Added the first `2026` congressional aggregation path.
- Restored `2026` congressional district metadata.
- Updated `MAPBOX_TOKEN` handling with fallback/default behavior in `index.html`.

### 2026-06-05

- Polished the desktop population-change tooltip and aligned it with the NC map interaction style.
- Surfaced a clearer population-change summary in the desktop county experience.
- Fixed county demographics wording and simplified the dominance-label hierarchy.
- Made majority vs plurality distinctions explicit in county demographics labels and badges.

### 2026-06-06 to 2026-06-19

- Added logic to transfer unchanged districts to the 2026 district lines where appropriate.
- Updated search placeholders and example buttons.
- Corrected miscalculations to match Secretary of State PDF references.
- Refactored year-comparison logic for candidate trends.
- Refactored district title/number formatting helpers.

### 2026-06-21

- Fixed Tennessee legislative winner labels.
- Refined contest filtering behavior for legislative views:
  - limited the contest filter to the intended state-house context
  - restored Tennessee senate filtering behavior
  - tightened the legislative contest filter logic

### 2026-06-22

- Fixed 2026 congressional demographics asset selection.
- Extended TN-08 2026 calibration beyond the presidential line to other statewide contests.
- Updated focus values for district, state house, and state senate views.
- Fixed the precinct alias loader and added district CSV assets needed by the expanded district pipeline.
- Added Shelby/TN-08 review tooling:
  - `Scripts/export_tn08_shelby_review.py`
  - `Scripts/export_tn08_shelby_geometry_review.py`
  - `Scripts/export_shelby_cd589_transition_review.py`
  - supporting Shelby congressional transition reports
- Fixed Shelby `PRCTSEQ` mapping for the 2026 congressional line builder.
- Refined 2026 congressional fallback mapping and line aggregation.

### 2026-06-24 to 2026-06-25

- Extended 2026 congressional calibration across the statewide contest set:
  - TN-05 calibration
  - TN-08 calibration and follow-up revisions
  - TN-09 calibration across the 2026 set
- Improved district demographics hover-card presentation.

### 2026-06-27

- Revisited 2026 congressional calibration assumptions:
  - restored TN-08 benchmark calibration
  - restored TN-05 strength in the 2026 set
  - reverted an older over-expansion of TN-08 calibration

### 2026-07-01

- Moved Sequatchie County from the Middle Tennessee bucket to East Tennessee in the county archetype/regional logic.
- Fixed mojibake cleanup issues and refreshed cache-buster values.

### 2026-07-02

- Refreshed app build/cache IDs again for a new deploy cycle.
- Added uploaded data/assets that were later wired into the atlas build pipeline.

### 2026-07-04

- Cleaned up tooltip logic and bumped the frontend cache-buster/build IDs so UI changes load more reliably after deploys.

### 2026-07-10

- Added an official Census rebuild path for Tennessee `vtd00` and `vtd10` precinct geography:
  - `Scripts/fetch_tn_census_2000_vtds.py`
  - `Scripts/fetch_tn_census_2010_vtds.py`
  - rebuilt `tn_vtd00_to_vtd10_overlap.csv`, `tn_vtd10_to_vtd20_overlap.csv`, and `tn_vtd00_to_vtd20_overlap.csv`
- Expanded the precinct crosswalk override workflow:
  - source-side overrides via `override_src_vtdst`
  - direct destination fallbacks via `override_dst_vtd20` for reviewed `matched_no_transfer` cases
- Added source-tagged historical precinct crosswalk output support so multiple same-year source files can coexist.
- Improved `2020` precinct matching coverage with reviewed overrides for counties including Hamilton, Shelby, Weakley, Maury, Washington, Dyer, Blount, Loudon, and Montgomery.
- Updated confidence reporting so the report prefers the strongest available match for each `(year, county, precinct)` key when both generic and source-tagged crosswalk files exist.

### 2026-07-12

- Rebuilt the tracked `2014`, `2016`, and `2018` precinct-to-`VTD20` crosswalks with a larger reviewed override set covering urban and repeated low-confidence counties including Hamilton, Shelby, Washington, Hardin, Lauderdale, Loudon, Carroll, Blount, Monroe, Maury, Weakley, and Montgomery.
- Reduced tracked decade crosswalk low-confidence rows to:
  - `2014`: `8`
  - `2016`: `6`
  - `2018`: `2`
- Eliminated unmatched rows in those tracked decade crosswalk files while leaving non-geographic `PAPER BALLOTS` buckets as the remaining low-confidence cases instead of forcing fake geography.
- Added `Scripts/build_tn_precinct_friendly_names.js` and the generated `Data/crosswalks/tn_precinct_friendly_names_2020.json` lookup so the frontend can map official `VTD20` codes to cleaner display names.
- Updated the precinct hover/selected-panel labeling logic so precinct mode now prefers the friendly `NAME20` labels statewide, including Davidson and Shelby, instead of falling back to old code-style labels in those counties.
- Refreshed the frontend cache-buster/build IDs again so the new precinct-name behavior and crosswalk-backed map updates load immediately after deploy.

### Historical Contest / Manifest Work

Recent historical-data expansion also added or rebuilt the following:

- New historical source CSVs:
  - `Data/19981103__tn__general__governor__county.csv`
  - `Data/20001107__tn__general__president__county.csv`
  - `Data/20001107__tn__general__president__precinct.csv`
  - `Data/20001107__tn__general__senate__precinct.csv`
  - `Data/20021105__tn__general__governor__precinct.csv`
  - `Data/20021105__tn__general__senate__precinct.csv`
  - `Data/20061107__tn__general__governor__precinct.csv`
- New or rebuilt contest JSON outputs:
  - `Data/contests/governor_1998.json`
  - `Data/contests/president_2000.json`
  - `Data/contests/us_senate_2000.json`
  - `Data/contests/us_senate_2002.json`
  - `Data/contests/governor_2002.json`
  - `Data/contests/governor_2006.json`
- District contest slices for the newly added years in:
  - `Data/district_contests/congressional_*`
  - `Data/district_contests/state_house_*`
  - `Data/district_contests/state_senate_*`
- Manifest and labeling fixes:
  - contest dropdown population now honors `major_party_contested`
  - noncompetitive council-of-state style contests can be filtered from selection lists
  - `Data/contests/manifest.json` and `Data/district_contests/manifest.json` were updated for the added historical files
  - `governor_1998_county.json` was standardized to `governor_1998.json`
- US Senate timeline expansion:
  - Seat A chain: `2000, 2006, 2012, 2018, 2024`
  - Seat B chain: `2002, 2008, 2014, 2020`
- 1998 Van Buren cleanup:
  - fixed the OCR county-key split
  - corrected county values
  - re-reconciled statewide totals to the official `STATE TOTAL` line

## County Hover (NCMap-style)

County hover tooltips are intentionally modeled after `NCMap.html`:

- **Hover (desktop):** shows a compact “quickline” + delta block; click the hover card itself to expand/pin the full card.
- **Click county (desktop):** selects/flies to the county but does **not** pin the hover card (matches NCMap).
- **Tap county (touch):** shows the county card and pins it (tap Close to dismiss).
- **No raw vote totals in the card body:** the result card is percent/margin-focused (raw vote *deltas* still appear in the delta block).

### Shift mode hover tooltips

When `Viz Mode = Shift`, hover tooltips prioritize the actual shift value (current signed margin minus prior-cycle signed margin):

- Headline: `Shift: R+X.XX% since YYYY` / `Shift: D+X.XX% since YYYY`
- Lines: current-year result + prior-year result + short interpretation (moved left/right / nearly unchanged), with `%` shown on margins
- If prior-cycle data is missing for that geography, the tooltip shows `No comparable prior result` instead of a misleading shift.

### Delta block

When available, the county delta block includes:

- **Population change (2020→2025)** plus optional **2020→2024** and **2024→2025** lines (U.S. Census County Population Estimates / CO-EST).
- **Vote deltas vs the previous available cycle → current** for the active contest (R delta, D delta, total vote gain).

### Tooltip + Demographics Polish

- The desktop county population-change hover now uses a more card-like, NCMap-style treatment so the headline change and the 2020→2025 context read more cleanly at a glance.
- Demographics labels now distinguish between **majority** and **plurality** based on the top share, with a separate mixed / near-tie label for close splits. The legend and hover badges now use different color accents so the distinction is visible at a glance.
- County demographics hover cards now surface the dominant group label directly, and the county share copy now correctly describes the data as county VAP race shares rather than total population.

## 2026 Congressional Notes

- 2026 congressional statewide slices live in `Data/district_contests_2026/`.
- Districts `1` and `2` intentionally keep their transferred unaffected legacy U.S. House line data, while other 2026 congressional statewide contests are rebuilt against `Data/tl_2026_47_cd2026.geojson`.
- The 2026 congressional builder is `Scripts/build_tn_congressional_2026_district_contests.py`.
- A Shelby-specific `PRCTSEQ` wiring fix now ensures 2024 rows resolve against the real 2026 congressional precinct overlay instead of falling into county fallback buckets.
- For targeted review work on the remaining TN-08 gap, export a Shelby-only audit CSV with:

```powershell
py Scripts/export_tn08_shelby_review.py
```

That writes:

- `Data/reports/tn08_shelby_review_2024_president.csv`

For a geometry-focused follow-up that tags each Shelby precinct as `core_tn08`, `boundary_split`, or `sliver_only`, run:

```powershell
py Scripts/export_tn08_shelby_geometry_review.py
```

That writes:

- `Data/reports/tn08_shelby_geometry_review_2024_president.csv`

## What Was Upgraded

The county-detail experience was upgraded to a story-first, election-desk standard with stronger hierarchy, clearer context, and better scanability.

### 1) Stronger Top Summary (Dominant Takeaway)
- Introduced a primary "At a Glance" summary block for selected counties.
- Prioritized:
  - who won
  - by how much
  - the county's main political identity
- Tuned spacing and visual weight so supporting details do not compete with the headline takeaway.

### 2) "Why It Votes This Way" Editorial Block
- Added a dedicated explanatory section directly under the top summary.
- Uses concise analyst-style language (headline + short supporting lines).
- Connects:
  - election trend
  - Tennessee regional geography
  - growth/demographic signals when available

### 3) Tennessee County Archetype System
- Added dynamic county archetype classification logic in the trend panel pipeline.
- Archetypes are Tennessee-specific and human-readable, including patterns such as:
  - Nashville suburban battleground
  - Fast-growing Middle Tennessee exurb
  - Memphis-area Democratic base
  - Appalachian Republican stronghold
  - College-influenced county
  - Small metro swing county
  - Long-term Republican trend county
- Archetype output is used as narrative context, not just a badge.

### 4) Confidence Meter
- Added a "Confidence" meter with Low / Medium / High output.
- Confidence is derived from:
  - margin size
  - consistency across cycles
  - volatility
- Display is intentionally subtle and integrated with the county summary card.

### 5) "Compared with Tennessee" Context Line
- Added a statewide benchmark comparison so county results are instantly interpretable in state context.
- Example style:
  - "This county is X points more Republican than Tennessee overall."
  - "This county voted X points to the left of the statewide result."

### 6) Reduced Cognitive Load with Progressive Disclosure
- Moved deeper supporting detail behind a collapsible section:
  - trajectory snapshot
  - census checks/insights
  - cycle-by-cycle history
- Keeps first-read county interpretation fast while preserving depth for power users.

### 7) Hover Tooltip Chips (Winner / Rating / Flip)
- Hover tooltips now include a competitiveness “rating” (tier) chip alongside the winner chip.
- Flip badges now appear after the rating chip (matching the ordering/placement used in `NCMap.html`).
- Applied across hover tooltips for counties, districts, and precincts in `index.html`.

## Hierarchy and Story Flow

County panel now reads in this order:
1. County name + result
2. Dominant summary ("At a Glance")
3. Why it votes this way
4. Confidence + statewide comparison + key supporting context
5. Deeper detail (collapsible)

This structure is designed so a user can understand the county in a few seconds.

## Truncation and Overflow Hardening

Additional CSS refinements were added to prevent clipped text and over-compressed labels across desktop and mobile:

- Trend header and caption now wrap safely rather than ellipsizing critical context.
- County headline, archetype, support values, and watch text now allow robust wrapping.
- Trend chips/badges no longer force hard one-line clipping in constrained widths.
- Legacy county-story panel styles were also hardened to keep both render paths safe.

These updates improve readability for long county narratives, long contest labels, and narrow viewport edge cases.

## Mobile UI Port (NCMap-style)

The Tennessee atlas now includes a substantially more complete phone/tablet interaction model ported from the NC map:

- **Bottom dock + sheets:** mobile uses docked `Search`, `Layers`, and `Legend` buttons that open draggable bottom sheets instead of stacking floating panels over the map.
- **Smarter overlay choreography:** opening a mobile sheet tucks away hover UI, dims or collapses competing overlays, and restores the vote counter when the sheet closes.
- **Small-screen layout polish:** the contest toolbar, search hint chips, vote breakdown panel, and trend/census cards now reflow more cleanly on narrow screens.
- **Height-aware positioning:** mobile overlay offsets now track dock height and vote-counter height so stacked panels avoid collisions more reliably.
- **Selection persistence across contest changes:** when a county, precinct, district, or region is pinned, changing contests now refreshes that same selected geography automatically instead of leaving the panel on stale results.

This work lives primarily in `index.html` and keeps the Tennessee mobile experience aligned with the NC map interaction model.

## Key Implementation Areas (index.html)

- Editorial county render path and narrative block composition:
  - `renderFocusTrendSeries(...)`
- New county narrative helpers:
  - `tnCountyConfidenceFromAnnotated(...)`
  - `tnCountyArchetypeProfile(...)`
  - `getStatewideSignedMarginForContest(...)`
  - `tnCountyVsStateLine(...)`
  - `renderCountyConfidenceMeterHTML(...)`
- New/updated style groups:
  - `.focus-county-*`
  - `.focus-trend-*` wrapping safeguards
  - `.county-story-*` overflow safeguards
  - `.mobile-dock`, `.mobile-sheet-*`, and mobile-only focus card refinements

## Validation Summary

Live browser smoke validation (Playwright harness) confirmed:
- county selection path still works
- editorial county panel sections render:
  - At a Glance
  - Why It Votes This Way
  - Compared with Tennessee
  - Confidence
  - Deeper Detail
- precinct toggle interaction remains intact

## Known Runtime Notes

- A valid Mapbox token is required for full map-style load and map-click interaction in some environments.
- If Mapbox style fails (403), direct county rendering can still be validated via app functions/data path.
- Local 404s indicate missing optional assets in the current runtime environment, not a structural break in the county-panel upgrade itself.

## Project Structure

- `index.html`: single-file app UI, logic, and styling
- `Data/`: contests, geometry, demographics, and support datasets
- `Scripts/`: local data-prep utilities

## District Calibration Notes

Some district-level statewide results are intentionally calibrated after the base allocation pass.

- Manual district overrides live in `Data/district_contests/calibration_overrides.json`.
- Rebuild district outputs with `Scripts/build_tn_contests.py` after changing allocation logic or adding overrides.
- Generated district slices are written to `Data/district_contests/`, with the file list tracked in `Data/district_contests/manifest.json`.
- External district-stat reference files can be used as calibration targets when generated district margins are directionally or numerically off.

### HD-30 guardrail

There is a lightweight validation script for the current Tennessee House District 30 assumption set:

```powershell
.\.venv\Scripts\python.exe Scripts\validate_hd30_safe_r.py
```

Current rule:
- district `30` should be Republican for `state_house` statewide contests in `president`, `governor`, and `us_senate`
- except `us_senate 2006`, which is allowed to remain non-Republican

## Improving VTD (Precinct) Matches

Statewide precinct-level results are keyed to the 2020 precinct geometry IDs (6-digit `VTD`/`VTDST20` codes).

- Rebuild contest slices with `Scripts/build_tn_contests.py` after changing crosswalk logic or overrides.
- If a specific precinct label won’t resolve cleanly, add a one-off mapping in `LEGACY_PRECINCT_VTD20_OVERRIDES` in `Scripts/build_tn_contests.py`.
- If a specific **2024 PRCTSEQ** value won’t map to a valid `VTDST20` (which can break older-year precinct joins too), add a manual mapping row in `Data/crosswalks/tn_prctseq_to_vtd20_overrides.csv` and rebuild.
- If the issue is a recurring name variant across years, update the crosswalk inputs in `Data/crosswalks/` (notably `tn_precinct_to_2024.csv` and `tn_precinct_aliases.csv`) and rebuild.

### Official Census VTD rebuild path

The repo now supports rebuilding Tennessee's historical VTD overlap chain from official Census county-level VTD files.

- `Scripts/fetch_tn_census_2000_vtds.py` downloads and merges Tennessee county `vtd00` ZIPs into a statewide working GeoJSON.
- `Scripts/fetch_tn_census_2010_vtds.py` downloads and merges Tennessee county `vtd10` ZIPs into a statewide working GeoJSON.
- `Scripts/build_tn_vtd_overlap_crosswalks.py` rebuilds:
  - `Data/crosswalks/tn_vtd00_to_vtd10_overlap.csv`
  - `Data/crosswalks/tn_vtd10_to_vtd20_overlap.csv`
  - `Data/crosswalks/tn_vtd00_to_vtd20_overlap.csv`

The merged statewide GeoJSONs are treated as local rebuild artifacts; the tracked overlap CSV outputs are the durable checked-in products.

### Source-tagged historical precinct crosswalks

Historical Tennessee precinct source files can now coexist for the same election year without clobbering each other.

- `Scripts/build_dra_style_block_crosswalks.py` supports `--source-tag`.
- `Scripts/batch_build_dra_style_crosswalks.py` derives that tag from the source filename.
- Output files can therefore coexist for years like `2000` and `2002`, for example:
  - `tn_precinct_to_vtd20_blockweighted_2000__20001107_tn_general_president_precinct.csv`
  - `tn_precinct_to_vtd20_blockweighted_2000__20001107_tn_general_senate_precinct.csv`

### Manual override and confidence-report flow

Reviewed hard cases now live in `Data/crosswalks/tn_crosswalk_manual_overrides.csv`.

- `override_src_vtdst` is used when a precinct label should resolve to a specific source-side VTD.
- `override_dst_vtd20` can now be used as a direct fallback for reviewed `matched_no_transfer` cases where the source-side match is known but the transfer row is missing.
- `Scripts/export_crosswalk_confidence_reports.py` now prefers the strongest available match per `(year, county, precinct)` key when both generic and source-tagged outputs exist.

This is the workflow behind the current `2020` improvement pass, including the Hamilton, Shelby, Weakley, Maury, Washington, and Dyer cleanup work.

## Local Run

From the project root:

```powershell
node .\.tmp_local_server.js
```

Then open:

`http://127.0.0.1:8000/index.html`

If you update and don’t see changes, hard refresh (`Ctrl+F5`) to bypass cached `index.html` / CSV loads.

The app also includes a manual static-data cache buster in `index.html`:

- `DATA_CACHE_BUSTER`
- `APP_BUILD_ID`

Bump those when you want phones or stubborn browsers to fetch fresh JSON / CSV / GeoJSON files immediately after a push.
