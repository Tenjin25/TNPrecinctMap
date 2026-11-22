"""Build 2026 Tennessee legislative district results from official unofficial returns.

The statewide dashboard supplies every General Assembly contest except candidate-level
write-in totals for House District 93.  That contest is completed from Shelby County's
official write-in report.
"""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "Data" / "district_contests"
STATE_SOURCE = "https://www.elections.tn.gov/categories/general-assembly/offices"
SHELBY_WRITE_IN_SOURCE = (
    "https://www.electionsshelbytn.gov/wp-content/uploads/2026/11/"
    "Write-In-Names-Report.pdf"
)

# scope|district|dem_candidate|dem_votes|rep_candidate|rep_votes|other_votes
RAW_RESULTS = """
state_senate|1||0|J. Adam Lowe|40401|0
state_senate|3||0|Rusty Crowe|39006|0
state_senate|5|David Miller|26347|Jimmy Matlock|41483|0
state_senate|7|Bryan Langan|30044|Richard M. Briggs|33498|0
state_senate|9||0|Steve Southerland|41139|0
state_senate|11|Tim Roberts|36789|Bo Watson|41395|0
state_senate|13|Angela Evans Wynn|30981|Dawn White|33424|0
state_senate|15||0|Paul Bailey|47612|0
state_senate|17|Lindsey Patrick-Wright|34517|Mark Pody|38466|0
state_senate|19|Charlane Oliver|46773||0|0
state_senate|21|Jeff Yarbro|46432||0|0
state_senate|23||0|Kerry Roberts|43196|18947
state_senate|25||0|Ed Jackson|35953|0
state_senate|27|David A. Hardy|35465|Jack Johnson|55816|0
state_senate|29|Raumesh Akbari|41625||0|0
state_senate|31|David Weatherspoon|35234|Brent Taylor|38004|0
state_senate|33|London Lamar|39468||0|0
state_house|1||0|Jessica Means|13227|0
state_house|2||0|Gary Stidham|14166|0
state_house|3|Lori Love|4563|Timothy Hill|15151|0
state_house|4||0|Renea Jones|14874|0
state_house|5|Catherine Bacon|4467|David B. Hawk|14813|0
state_house|6|Brad Batt|7947|Tim Hicks|13879|675
state_house|7|Sarah Jayne Della Vecchia|7154|Rebecca Keefauver Alexander|11585|0
state_house|8|Hilary Trudell|7150|Jordan Henderson|15800|0
state_house|9||0|Gary W. Hicks Jr|14301|0
state_house|10||0|Rick Eldridge|13437|0
state_house|11|Sheila Grooms McMahan|4712|Kenny Cody|13948|392
state_house|12||0|Fred A. Atchley|12608|0
state_house|13|Brett Windrow|9901|Robert Stevens|9345|0
state_house|14|Lindsay Young Honaker|13543|Jason Zachary|17097|0
state_house|15|Sam McKenzie|10925||0|0
state_house|16|Eliza Boles|8800|Michele Carringer|12947|0
state_house|17||0|Andrew Ellis Farmer|13700|0
state_house|18|Bryan Goldberg|14020|Elaine Davis|11452|0
state_house|19|Jenny Fowler|7414|Dave Wright|14086|0
state_house|20|Karen Gertz|7121|Tom Stinnett|13802|759
state_house|21||0|Lowell Russell|20778|0
state_house|22||0|Dan Howell|16532|0
state_house|23||0|Mark Cochran|13248|3852
state_house|24|Daniel F. Jones|5368|Kevin Raper|11485|0
state_house|25|Monica Mowdy|6139|Cameron Sexton|20492|0
state_house|26||0|Greg Martin|18164|0
state_house|27|Paige Quirin|12999|Michele Reneau|15088|0
state_house|28|Yusuf A. Hakeem|13008||0|4967
state_house|29|Ryan V. Scofield|9606|Greg Vital|15585|0
state_house|30|Art Rymer|11071|Esther Helton Haynes|11611|0
state_house|31|Michael Woodlee|4696|Andy (Andrew) J. Swafford|13722|613
state_house|32|Ali Simpson|6514|Jack L. Stockton|16385|0
state_house|33|Anne Backus|8631|Rick Scarbrough|11571|0
state_house|34|Owen Farnsworth|10751|Tim Rudd|9938|0
state_house|35|Phil Barnes|7489|William Slater|12503|0
state_house|36||0|Noah Smith|13056|0
state_house|37|Mary Ann Young|8536|Charlie Baum|9596|0
state_house|38|Seth McMillan|2943|Kelly T. Keisling|12888|0
state_house|39||0|Iris Rudder|14457|0
state_house|40|Daniel T. Hawthorne|4323|Michael Hale|14592|0
state_house|41|R. Jason Goodman|4268|Ed Butler|13744|0
state_house|42|Trenton Tyler Strode|7319|Ryan D. Williams|13093|0
state_house|43|Samantha Petit|5508|Paul Sherrell|11695|0
state_house|44|Latoya Holcomb|7568|William Lamberth|14924|0
state_house|45|Mandy Cook|9177|Brian Stewart|12570|0
state_house|46|Dominic Howard|8951|Clark Boyd|17187|0
state_house|47|Mike Stein|5503|Rush Bricken|12775|0
state_house|48|Matt Ferry|9694|Bryan Terry|11849|0
state_house|49|Connie Casha|10417|Mike Sparks|8498|0
state_house|50|Bo Mitchell|15478||0|0
state_house|51|Aftyn Behn|22630||0|0
state_house|52|Justin Jones|11922|Michele Vetter|2763|247
state_house|53|Jason L. Powell|13501||0|0
state_house|54|Vincent Dixie|19690||0|0
state_house|55|John Ray Clemmons|10856|Jayla Thomas|1918|0
state_house|56|Bob Freeman|20492||0|0
state_house|57|Matt Burchfield|12189|Susan M. Lynn|16323|0
state_house|58|Harold M. Love, Jr.|15710||0|0
state_house|59|Mark Proctor|18372|Bill Hancock|13571|0
state_house|60|Shaundelle Brooks|15080|Eva Angelina Romero|8517|0
state_house|61|Becca Ripley|12508|Gino Bulso|16707|0
state_house|62|Brieanna L. Akers|5243|Pat Marsh|13527|0
state_house|63|Laura Andreson|11991|Jake McCalmon|18760|0
state_house|64|Craig D'Apolito|9305|Scott Cepicky|15364|0
state_house|65|Julian Herbert Pierre-Griffin|11067|Lee Reeves|18552|0
state_house|66|Tamara Bavendam|7626|Sabi "Doc" Kumar|14892|0
state_house|67|Ronnie L. Glynn|9754|Chris Lanier|5739|0
state_house|68|Garfield Scott|10318|Aron Maberry|14152|0
state_house|69|Rachael Murray|5053|Jody Barrett|13094|0
state_house|70|Rayanne Moos|4244|Clay Doggett|14749|0
state_house|71|Matt Day|5429|Kip Capley|15392|0
state_house|72||0|Kirk Haston|14343|0
state_house|73||0|Chris Todd|13700|0
state_house|74||0|Jay D. Reedy|14630|0
state_house|75|Allie Phillips|7822|Michael Lankford|6396|0
state_house|76||0|Tandy Darby|13927|0
state_house|77||0|Rusty Grills|11442|3356
state_house|78|Hillary Capes-Gwinn|7530|Perry Keenan|13460|0
state_house|79||0|Brock Martin|11537|3801
state_house|80|Andrea Bond Johnson|9180|Julian A. McTizic|5404|0
state_house|81||0|Brett A. Giannini|12980|0
state_house|82||0|Chris Hurt|12157|0
state_house|83|Margaret Price|12903|Mark White|13238|0
state_house|84|Matthew Szalaj|10900||0|0
state_house|85|Jesse Chism|20557||0|0
state_house|86|Justin J. Pearson|13515||0|0
state_house|87|Karen Camper|11516|Reggie William Hall|1265|0
state_house|88|Larry J. Miller|13129||0|0
state_house|89|Randy DeBord|11088|Justin Lafferty|13663|0
state_house|90|Gloria Johnson|12928||0|0
state_house|91|Torrey C. Harris|14186||0|0
state_house|92|Teri Mai|8085|Todd Warner|13512|0
state_house|93|John Bradley|2052||0|1294
state_house|94|Francine Johnson|6745|Ron M. Gant|16924|0
state_house|95||0|Kevin Vaughan|18483|0
state_house|96|Telisa Franklin|14941||0|0
state_house|97|Jesse Huseth|13895|John Gillespie|10502|0
state_house|98|Antonio Parkinson|13175||0|0
state_house|99||0|Tom Leatherwood|15003|8655
""".strip()


def parse_results() -> dict[str, dict[str, dict[str, object]]]:
    grouped: dict[str, dict[str, dict[str, object]]] = {
        "state_house": {},
        "state_senate": {},
    }
    for line in RAW_RESULTS.splitlines():
        scope, district, dem_name, dem_raw, rep_name, rep_raw, other_raw = line.split("|")
        dem_votes, rep_votes, other_votes = map(int, (dem_raw, rep_raw, other_raw))
        total_votes = dem_votes + rep_votes + other_votes
        margin = rep_votes - dem_votes
        winner = "REP" if margin > 0 else "DEM" if margin < 0 else "TIE"
        grouped[scope][district] = {
            "dem_votes": dem_votes,
            "rep_votes": rep_votes,
            "other_votes": other_votes,
            "total_votes": total_votes,
            "dem_candidate": dem_name,
            "rep_candidate": rep_name,
            "margin": margin,
            "margin_pct": round(margin / total_votes * 100, 4) if total_votes else 0.0,
            "winner": winner,
        }
    return grouped


def write_scope(scope: str, results: dict[str, dict[str, object]]) -> Path:
    expected = 99 if scope == "state_house" else 17
    expected_districts = (
        {str(number) for number in range(1, 100)}
        if scope == "state_house"
        else {str(number) for number in range(1, 34, 2)}
    )
    if set(results) != expected_districts:
        missing = sorted(expected_districts - set(results), key=int)
        extra = sorted(set(results) - expected_districts, key=int)
        raise ValueError(f"{scope}: missing={missing}, extra={extra}")

    meta: dict[str, object] = {
        "source": "Tennessee Secretary of State Election Night Reporting Dashboard",
        "source_url": STATE_SOURCE,
        "election_date": "2026-11-03",
        "captured_date": "2026-11-09",
        "status": "Final Unofficial Results",
        "unofficial": True,
        "geography": "district",
        "districts": expected,
    }
    if scope == "state_house":
        meta.update(
            {
                "house_district_93_source_url": SHELBY_WRITE_IN_SOURCE,
                "house_district_93_note": (
                    "The state dashboard reports zero candidate votes for this write-in-only "
                    "contest. Shelby County's official write-in report records 3,346 votes: "
                    "John Bradley 2,052; all remaining write-ins 1,294. Bradley is normalized "
                    "to the Democratic column for atlas display; the remainder is aggregated "
                    "as other votes."
                ),
            }
        )

    payload = {
        "scope": scope,
        "contest_type": scope,
        "year": 2026,
        "meta": meta,
        "general": {"results": dict(sorted(results.items(), key=lambda item: int(item[0])))},
    }
    output = OUTPUT_DIR / f"{scope}_{scope}_2026.json"
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return output


def main() -> None:
    grouped = parse_results()
    for scope, results in grouped.items():
        output = write_scope(scope, results)
        dem_total = sum(int(row["dem_votes"]) for row in results.values())
        rep_total = sum(int(row["rep_votes"]) for row in results.values())
        other_total = sum(int(row["other_votes"]) for row in results.values())
        print(
            f"{output.relative_to(ROOT)}: {len(results)} districts; "
            f"DEM={dem_total:,} REP={rep_total:,} OTHER={other_total:,}"
        )


if __name__ == "__main__":
    main()
