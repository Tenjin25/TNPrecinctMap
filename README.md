# Tri-Star Volunteer Atlas (Tennessee Election Atlas)

Interactive Tennessee election map focused on county, district, and precinct analysis with a newsroom-style county storytelling panel.

This project runs as a single-page app from `index.html` and reads local data assets from `Data/`.

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

## Local Run

From the project root:

```powershell
.\.venv\Scripts\python.exe -m http.server 4173
```

Then open:

`http://127.0.0.1:4173/index.html`
