# Volunteer State Election Atlas (Tennessee Election Atlas)

Interactive Tennessee election map focused on county, district, and precinct analysis with a newsroom-style county storytelling panel.

This project runs as a single-page app from `index.html` and reads local data assets from `Data/`.

## County Hover (NCMap-style)

County hover tooltips are intentionally modeled after `NCMap.html`:

- **Hover (desktop):** shows a compact “quickline” + delta block; click the hover card itself to expand/pin the full card.
- **Click county (desktop):** selects/flies to the county but does **not** pin the hover card (matches NCMap).
- **Tap county (touch):** shows the county card and pins it (tap Close to dismiss).
- **No raw vote totals in the card body:** the result card is percent/margin-focused (raw vote *deltas* still appear in the delta block).

### Delta block

When available, the county delta block includes:

- **Population change (2020→2025)** plus optional **2020→2024** and **2024→2025** lines (U.S. Census County Population Estimates / CO-EST).
- **Vote deltas vs the previous available cycle → current** for the active contest (R delta, D delta, total vote gain).

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

## Improving VTD (Precinct) Matches

Statewide precinct-level results are keyed to the 2020 precinct geometry IDs (6-digit `VTD`/`VTDST20` codes).

- Rebuild contest slices with `Scripts/build_tn_contests.py` after changing crosswalk logic or overrides.
- If a specific precinct label won’t resolve cleanly, add a one-off mapping in `LEGACY_PRECINCT_VTD20_OVERRIDES` in `Scripts/build_tn_contests.py`.
- If a specific **2024 PRCTSEQ** value won’t map to a valid `VTDST20` (which can break older-year precinct joins too), add a manual mapping row in `Data/crosswalks/tn_prctseq_to_vtd20_overrides.csv` and rebuild.
- If the issue is a recurring name variant across years, update the crosswalk inputs in `Data/crosswalks/` (notably `tn_precinct_to_2024.csv` and `tn_precinct_aliases.csv`) and rebuild.

## Local Run

From the project root:

```powershell
node .\.tmp_local_server.js
```

Then open:

`http://127.0.0.1:8000/index.html`

If you update and don’t see changes, hard refresh (`Ctrl+F5`) to bypass cached `index.html` / CSV loads.
