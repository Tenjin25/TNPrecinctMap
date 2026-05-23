Hamilton 2004 -> 2024 crosswalk review

Applied direct one-to-one fixes in `tn_precinct_to_2024.csv` where the 2024 Hamilton inventory already has matching sibling targets:

- `COLLEGEDALE 1 -> COLLEGEDALE A (0012)`
- `COLLEGEDALE 2 -> COLLEGEDALE B (0013)`
- `EAST BRAINERD 1 -> EAST BRAINERD A (0021)`
- `EAST BRAINERD 2 -> EAST BRAINERD B (0022)`
- `FALLING WATER 1 -> FALLING WATER A (0032)`
- `FALLING WATER 2 -> FALLING WATER B (0033)`
- `NORTH CHATTANOOGA 1 -> NORTH CHATTANOOGA A (0057)`
- `NORTH CHATTANOOGA 2 -> NORTH CHATTANOOGA B (0058)`
- `PLEASANT GROVE 1 -> PLEASANT GROVE A (0065)`
- `PLEASANT GROVE 2 -> PLEASANT GROVE B (0066)`

Still ambiguous and not auto-edited:

- `SODDY DAISY 4/5` still need judgment. Overlap supports `1 -> A`, `2 -> B`, `3 -> C`, but `4` remains split mostly `A/B` and `5` still spills into `MOWBRAY`.
- `LOOKOUT VALLEY 1..4` currently collapse to `LOOKOUT VALLEY (0046)` with no numbered modern siblings.
- `MOUNTAIN CREEK 1/2/4` currently collapse to `MOUNTAIN CREEK A (0051)`, but 2024 has `A/B/C`.
- `LOOKOUT MOUNTAIN 1/2` currently collapse to unsuffixed `LOOKOUT MOUNTAIN (0045)`.
- `STUART HEIGHTS 1/2` currently collapse to unsuffixed `STUART HEIGHTS (0084)`.

Recommended next step:

- Rebuild contest slices with `Scripts/build_tn_contests.py` so the corrected 2004 Hamilton mappings propagate into `Data/contests/`.
