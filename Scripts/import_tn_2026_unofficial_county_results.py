#!/usr/bin/env python3
"""Build 2026 Tennessee county contest JSON from the official unofficial returns."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "Data" / "contests"
CAPTURED_DATE = "2026-11-04"

GOVERNOR_DATA = """\
ANDERSON|9989|11223|2755|28 / 28
BEDFORD|4114|7347|1737|11 / 11
BENTON|1137|2897|728|6 / 6
BLEDSOE|878|2468|530|13 / 13
BLOUNT|14856|24830|5811|27 / 27
BRADLEY|8327|17061|3996|18 / 18
CAMPBELL|1978|5394|1236|21 / 21
CANNON|1047|2494|802|9 / 9
CARROLL|1739|4179|1213|15 / 15
CARTER|3596|7892|4319|22 / 22
CHEATHAM|5087|7205|1782|8 / 8
CHESTER|961|2683|984|7 / 7
CLAIBORNE|1924|5753|1312|16 / 16
CLAY|507|1264|377|7 / 7
COCKE|2120|5966|1619|18 / 18
COFFEE|4685|8723|2216|17 / 17
CROCKETT|1022|1848|831|12 / 12
CUMBERLAND|5576|15880|2776|23 / 23
DAVIDSON|160749|48546|15094|178 / 178
DECATUR|701|2242|445|9 / 9
DEKALB|1412|3122|917|12 / 12
DICKSON|5093|8597|2232|13 / 13
DYER|2266|5367|1308|15 / 15
FAYETTE|5576|10196|1690|18 / 18
FENTRESS|1013|3952|854|13 / 13
FRANKLIN|3659|7342|1656|18 / 18
GIBSON|4107|6974|2120|22 / 22
GILES|2383|5278|1040|14 / 14
GRAINGER|1325|4174|1121|10 / 10
GREENE|4365|11738|3625|17 / 17
GRUNDY|878|2292|471|10 / 10
HAMBLEN|4767|8921|2154|16 / 16
HAMILTON|63364|50673|11229|92 / 92
HANCOCK|312|938|332|7 / 7
HARDEMAN|2999|3098|727|12 / 12
HARDIN|1331|5307|886|14 / 14
HAWKINS|3127|9547|2820|19 / 19
HAYWOOD|2718|1643|461|11 / 11
HENDERSON|1469|4362|1264|12 / 12
HENRY|2708|5600|1448|8 / 8
HICKMAN|1617|3893|1030|14 / 14
HOUSTON|650|1445|288|7 / 7
HUMPHREYS|1535|2924|844|8 / 8
JACKSON|873|1970|614|13 / 13
JEFFERSON|3883|9626|2416|11 / 11
JOHNSON|943|3264|1014|10 / 10
KNOX|76116|66600|14291|99 / 99
LAKE|356|681|111|3 / 3
LAUDERDALE|2060|2695|851|10 / 10
LAWRENCE|2682|7350|2264|24 / 24
LEWIS|891|2251|641|9 / 9
LINCOLN|2362|6924|784|20 / 20
LOUDON|6674|14678|2713|15 / 15
MACON|1126|3374|1088|10 / 10
MADISON|12560|11652|3125|29 / 29
MARION|2713|4945|949|17 / 17
MARSHALL|3046|5810|1598|9 / 9
MAURY|12162|19783|4636|22 / 22
MCMINN|3667|8480|2057|13 / 13
MCNAIRY|1265|4406|1686|16 / 16
MEIGS|870|2321|586|6 / 6
MONROE|3142|8374|2118|13 / 13
MONTGOMERY|27759|24018|5367|39 / 39
MOORE|508|1490|322|5 / 5
MORGAN|1020|3139|840|12 / 12
OBION|1934|5305|833|12 / 12
OVERTON|1657|3932|1146|9 / 9
PERRY|497|1380|358|10 / 10
PICKETT|431|1102|423|9 / 9
POLK|1213|3256|699|7 / 7
PUTNAM|8333|12276|3019|18 / 18
RHEA|2060|5371|1123|12 / 12
ROANE|5364|9954|2333|20 / 20
ROBERTSON|7674|12582|3115|18 / 18
RUTHERFORD|50530|41699|9812|30 / 30
SCOTT|879|2902|745|10 / 10
SEQUATCHIE|1080|3165|700|9 / 9
SEVIER|7163|16018|3138|25 / 25
SHELBY|164812|66529|22766|142 / 142
SMITH|1321|3321|1129|8 / 8
STEWART|1029|2908|522|7 / 7
SULLIVAN|12890|25660|7727|25 / 25
SUMNER|24433|33424|7681|29 / 29
TIPTON|4986|9813|1891|9 / 9
TROUSDALE|787|1496|468|10 / 10
UNICOI|1279|3123|1184|8 / 8
UNION|1044|3145|746|9 / 9
VAN BUREN|523|1198|462|7 / 7
WARREN|3094|5039|2270|20 / 20
WASHINGTON|15092|19430|8365|23 / 23
WAYNE|668|2812|491|14 / 14
WEAKLEY|2389|5310|1073|7 / 7
WHITE|1837|4756|1472|11 / 11
WILLIAMSON|40224|54843|9047|27 / 27
WILSON|21385|28278|6496|18 / 18
"""

SENATE_DATA = """\
ANDERSON|9521|13663|504|28 / 28
BEDFORD|3726|8745|268|11 / 11
BENTON|1065|3509|101|6 / 6
BLEDSOE|806|2940|70|13 / 13
BLOUNT|14475|29702|910|27 / 27
BRADLEY|7926|20127|623|18 / 18
CAMPBELL|1813|6351|205|21 / 21
CANNON|993|3098|107|9 / 9
CARROLL|1662|5236|151|15 / 15
CARTER|3611|10933|444|22 / 22
CHEATHAM|4929|8724|296|8 / 8
CHESTER|1014|3433|94|7 / 7
CLAIBORNE|1646|6916|215|16 / 16
CLAY|444|1544|51|7 / 7
COCKE|1940|7111|228|18 / 18
COFFEE|4569|10544|374|17 / 17
CROCKETT|991|2484|77|12 / 12
CUMBERLAND|5314|17907|418|23 / 23
DAVIDSON|158305|59248|3674|178 / 178
DECATUR|645|2612|59|9 / 9
DEKALB|1286|3859|102|12 / 12
DICKSON|4960|10410|374|13 / 13
DYER|2012|6397|192|15 / 15
FAYETTE|5529|11122|290|18 / 18
FENTRESS|916|4646|107|13 / 13
FRANKLIN|3550|8485|282|18 / 18
GIBSON|3754|8618|285|22 / 22
GILES|2226|5987|188|14 / 14
GRAINGER|1228|5172|125|10 / 10
GREENE|4275|14597|494|17 / 17
GRUNDY|780|2610|102|10 / 10
HAMBLEN|4478|10563|289|16 / 16
HAMILTON|62609|59388|2255|92 / 92
HANCOCK|260|1226|37|7 / 7
HARDEMAN|2965|3566|134|12 / 12
HARDIN|1299|5968|159|14 / 14
HAWKINS|3069|11428|361|19 / 19
HAYWOOD|2741|1924|90|11 / 11
HENDERSON|1416|5455|132|12 / 12
HENRY|2428|6689|226|8 / 8
HICKMAN|1599|4718|168|14 / 14
HOUSTON|610|1667|62|7 / 7
HUMPHREYS|1467|3625|120|8 / 8
JACKSON|792|2457|82|13 / 13
JEFFERSON|3657|11475|365|11 / 11
JOHNSON|961|3934|103|10 / 10
KNOX|75557|78503|2210|99 / 99
LAKE|319|757|39|3 / 3
LAUDERDALE|1968|3216|109|10 / 10
LAWRENCE|2371|9041|396|24 / 24
LEWIS|866|2814|77|9 / 9
LINCOLN|2273|7511|175|20 / 20
LOUDON|6459|17014|370|15 / 15
MACON|1023|4262|118|10 / 10
MADISON|12708|13954|533|29 / 29
MARION|2453|5801|211|17 / 17
MARSHALL|2796|7071|231|9 / 9
MAURY|12059|23036|630|22 / 22
MCMINN|3384|10286|365|13 / 13
MCNAIRY|1361|5608|209|16 / 16
MEIGS|789|2794|68|6 / 6
MONROE|2945|10225|332|13 / 13
MONTGOMERY|27373|27311|1270|39 / 39
MOORE|475|1791|39|5 / 5
MORGAN|923|3920|113|12 / 12
OBION|1796|6093|126|12 / 12
OVERTON|1565|4758|139|9 / 9
PERRY|434|1656|47|10 / 10
PICKETT|401|1443|39|9 / 9
POLK|1091|3943|100|7 / 7
PUTNAM|7721|14961|432|18 / 18
RHEA|1922|6412|183|12 / 12
ROANE|5018|12085|372|20 / 20
ROBERTSON|7210|15005|493|18 / 18
RUTHERFORD|49225|48977|2122|30 / 30
SCOTT|735|3466|102|10 / 10
SEQUATCHIE|1015|3688|106|9 / 9
SEVIER|6924|18601|501|25 / 25
SHELBY|167623|75272|5591|142 / 142
SMITH|1213|4254|136|8 / 8
STEWART|968|3323|83|7 / 7
SULLIVAN|13111|31817|994|25 / 25
SUMNER|23655|39673|1227|29 / 29
TIPTON|4770|11069|373|9 / 9
TROUSDALE|723|1872|53|10 / 10
UNICOI|1282|3902|145|8 / 8
UNION|950|3592|87|9 / 9
VAN BUREN|465|1565|75|7 / 7
WARREN|3004|6646|282|20 / 20
WASHINGTON|15329|25201|1151|23 / 23
WAYNE|578|3214|65|14 / 14
WEAKLEY|2141|6431|171|7 / 7
WHITE|1734|6084|177|11 / 11
WILLIAMSON|38783|63349|1527|27 / 27
WILSON|21224|34356|1072|18 / 18
"""


def parse_rows(raw: str, dem_candidate: str, rep_candidate: str) -> list[dict]:
    rows = []
    for line in raw.strip().splitlines():
        county, dem_raw, rep_raw, other_raw, precincts = line.split("|")
        dem_votes, rep_votes, other_votes = map(int, (dem_raw, rep_raw, other_raw))
        total_votes = dem_votes + rep_votes + other_votes
        margin = rep_votes - dem_votes
        rows.append(
            {
                "county": county,
                "dem_votes": dem_votes,
                "rep_votes": rep_votes,
                "other_votes": other_votes,
                "total_votes": total_votes,
                "dem_candidate": dem_candidate,
                "rep_candidate": rep_candidate,
                "margin": margin,
                "margin_pct": round((margin / total_votes) * 100, 4) if total_votes else 0,
                "winner": "REP" if margin > 0 else ("DEM" if margin < 0 else "TIE"),
                "color": "",
                "precincts_reporting": precincts,
            }
        )
    return rows


def build(contest_type: str, raw: str, dem_candidate: str, rep_candidate: str, source_url: str) -> dict:
    rows = parse_rows(raw, dem_candidate, rep_candidate)
    assert len(rows) == 95
    assert all(row["precincts_reporting"].split("/")[0].strip() == row["precincts_reporting"].split("/")[1].strip() for row in rows)
    return {
        "contest_type": contest_type,
        "year": 2026,
        "meta": {
            "source": "Tennessee Secretary of State Election Night Reporting Dashboard",
            "source_url": source_url,
            "election_date": "2026-11-03",
            "captured_date": CAPTURED_DATE,
            "status": "Final Unofficial Results",
            "unofficial": True,
            "counties_reporting": 95,
            "counties_total": 95,
            "geography": "county",
        },
        "rows": rows,
    }


def main() -> None:
    contests = {
        "governor_2026.json": build(
            "governor",
            GOVERNOR_DATA,
            "Jerri Green",
            "Marsha Blackburn",
            "https://www.elections.tn.gov/county-breakdown/Governor",
        ),
        "us_senate_2026.json": build(
            "us_senate",
            SENATE_DATA,
            "Marquita Bradshaw",
            "Bill Hagerty",
            "https://www.elections.tn.gov/county-breakdown/United%20States%20Senate",
        ),
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for filename, payload in contests.items():
        (OUT_DIR / filename).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        totals = {
            field: sum(int(row[field]) for row in payload["rows"])
            for field in ("dem_votes", "rep_votes", "other_votes", "total_votes")
        }
        print(filename, totals)


if __name__ == "__main__":
    main()
