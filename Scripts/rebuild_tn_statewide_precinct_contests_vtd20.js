#!/usr/bin/env node
"use strict";

// Precinct-only rebuild entrypoint. This intentionally delegates to the
// VTD20 allocation script and never touches Data/district_contests* outputs.
require("./apply_tn_weighted_vtd20_to_current_contests.js");
