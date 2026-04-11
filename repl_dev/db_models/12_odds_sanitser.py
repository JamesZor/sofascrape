import logging
import re
from typing import Any, Dict

logger = logging.getLogger(__name__)


class OddsDataSanitizer:
    """
    Single Responsibility: Mutates and cleans raw odds JSON payloads.
    Prioritizes structural integrity and flags corrupted markets for downstream grading.
    """

    @staticmethod
    def sanitize(raw_data: Dict[str, Any], match_id: int) -> None:
        if not raw_data or "markets" not in raw_data:
            return

        for market in raw_data.get("markets", []):
            market_name = market.get("marketName", "")
            market_group = market.get("marketGroup", "")
            choices = market.get("choices", [])
            market_id_val = market.get("marketId")

            # NEW: Step 0: Detect corruption before we alter the names
            is_corrupted = OddsDataSanitizer._has_duplicates(choices)

            # Step 1: Force Strict Positional Names (Ignore API typos completely)
            OddsDataSanitizer._enforce_structural_names(
                market_name, market_group, choices
            )

            # Step 2: Fallback for unmapped markets (Generic Duplicates)
            OddsDataSanitizer._resolve_generic_duplicates(market)

            # Step 3: Safe Boolean Coercion (Aware of corruption)
            OddsDataSanitizer._safely_resolve_booleans(
                choices, is_corrupted, market_id_val
            )

    @staticmethod
    def _has_duplicates(choices: list) -> bool:
        """Checks if the API sent duplicate name strings before we clean them."""
        seen = set()
        for c in choices:
            name = str(c.get("name", ""))
            if name in seen:
                return True
            seen.add(name)
        return False

    @staticmethod
    def _enforce_structural_names(
        market_name: str, market_group: str, choices: list
    ) -> None:
        """Forcefully overrides choice names based on expected market structure."""
        if market_group == "1X2" and len(choices) == 3:
            choices[0]["name"] = "1"
            choices[1]["name"] = "X"
            choices[2]["name"] = "2"

        elif market_name == "First team to score" and len(choices) == 3:
            choices[0]["name"] = "Home"
            choices[1]["name"] = "No goal"
            choices[2]["name"] = "Away"

        elif market_name.lower() == "asian handicap" and len(choices) == 2:
            roles = ["Home", "Away"]
            for i in range(2):
                original_name = choices[i].get("name", "")
                match = re.match(r"(\([+-]?\d+\.?\d*\)\s*)(.*)", original_name)
                if match:
                    choices[i]["name"] = f"{match.group(1)}{roles[i]}"
                else:
                    choices[i]["name"] = roles[i]

    @staticmethod
    def _resolve_generic_duplicates(market: Dict[str, Any]) -> None:
        """Catches any remaining duplicates in markets we didn't strictly map above."""
        seen_names = set()
        for choice in market.get("choices", []):
            original_name = str(choice.get("name", ""))
            if original_name in seen_names:
                counter = 1
                new_name = f"{original_name}_dup_{counter}"
                while new_name in seen_names:
                    counter += 1
                    new_name = f"{original_name}_dup_{counter}"
                choice["name"] = new_name
                seen_names.add(new_name)
            else:
                seen_names.add(original_name)

    @staticmethod
    def _safely_resolve_booleans(
        choices: list, is_corrupted: bool, market_id: int
    ) -> None:
        """
        Applies type safety. If corrupted, flags all choices as None for downstream grading.
        """
        if is_corrupted:
            logger.warning(
                f"⚠️ Market {market_id} corrupted by duplicates. Flagging 'winning' as None for grading."
            )
            for c in choices:
                c["winning"] = None
            return

        # Normal safe resolution for uncorrupted markets
        found_winner = False
        for c in choices:
            if "winning" not in c or c["winning"] is None:
                c["winning"] = False
            elif isinstance(c["winning"], str):
                c["winning"] = str(c["winning"]).strip().lower() in [
                    "true",
                    "1",
                    "t",
                    "y",
                    "yes",
                ]

            if c["winning"] is True:
                if not found_winner:
                    found_winner = True
                else:
                    c["winning"] = False
                    logger.warning(
                        f"🛡️ Safety: Stripped secondary 'winning: True' from '{c.get('name')}'."
                    )


# ==========================================
# TEST EXECUTION BLOCK (REPL)
# ==========================================
if __name__ == "__main__":
    dirty_json_string = """
    {"markets":[{"sourceId":179852440,"structureType":1,"marketId":1,"marketName":"Full time","isLive":false,"fid":179852440,"suspended":false,"id":111089360,"marketGroup":"1X2","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"17/25","fractionalValue":"17/25","sourceId":1432003727,"name":"1","winning":false,"change":0},{"initialFractionalValue":"3/1","fractionalValue":"31/10","sourceId":1432003730,"name":"X","winning":false,"change":1},{"initialFractionalValue":"11/4","fractionalValue":"16/5","sourceId":1432003731,"name":"2","winning":true,"change":1}]},{"sourceId":179852440,"structureType":1,"marketId":2,"marketName":"Double chance","isLive":false,"fid":179852440,"suspended":false,"id":111578565,"marketGroup":"Double chance","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"1/5","fractionalValue":"2/9","sourceId":1478559838,"name":"1X","winning":false,"change":1},{"initialFractionalValue":"11/10","fractionalValue":"1/1","sourceId":1478559834,"name":"X2","winning":true,"change":-1},{"initialFractionalValue":"2/9","fractionalValue":"2/9","sourceId":1478559837,"name":"12","winning":true,"change":0}]},{"sourceId":179852440,"structureType":1,"marketId":3,"marketName":"1st half","isLive":false,"fid":179852440,"suspended":false,"id":111578566,"marketGroup":"1X2","marketPeriod":"1st half","choices":[{"initialFractionalValue":"6/5","fractionalValue":"13/10","sourceId":1478559455,"name":"1","winning":false,"change":1},{"initialFractionalValue":"6/4","fractionalValue":"7/5","sourceId":1478559462,"name":"X","winning":false,"change":-1},{"initialFractionalValue":"7/2","fractionalValue":"10/3","sourceId":1478559463,"name":"1","winning":false,"change":-1}]},{"sourceId":179852440,"structureType":1,"marketId":4,"marketName":"Draw no bet","isLive":false,"fid":179852440,"suspended":false,"id":111578567,"marketGroup":"Draw no bet","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"3/10","fractionalValue":"4/11","sourceId":1478559438,"name":"1","winning":false,"change":1},{"initialFractionalValue":"12/5","fractionalValue":"2/1","sourceId":1478559434,"name":"2","winning":true,"change":-1}]},{"sourceId":179852440,"structureType":1,"marketId":17,"marketName":"Asian handicap","isLive":false,"fid":179852441,"suspended":false,"id":111578570,"marketGroup":"Asian Handicap","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"37/40","fractionalValue":"41/40","sourceId":1432003734,"name":"(-0.75) Rangers","change":1},{"initialFractionalValue":"37/40","fractionalValue":"33/40","sourceId":1432003735,"name":"(0.75) Heart of Midlothian","change":-1}]},{"sourceId":179852440,"structureType":1,"marketId":6,"marketName":"First team to score","isLive":false,"fid":179852440,"suspended":false,"id":111578569,"marketGroup":"First team to score","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"4/7","fractionalValue":"4/7","sourceId":1478559442,"name":"Rangers","winning":false,"change":0},{"initialFractionalValue":"16/1","fractionalValue":"16/1","sourceId":1478559444,"name":"No goal","winning":false,"change":0},{"initialFractionalValue":"7/5","fractionalValue":"11/8","sourceId":1478559446,"name":"Rangers","winning":false,"change":-1}]}], "eventId": 14035148}
    """
    # dirty_json_string = """
    # {"markets":[{"sourceId":181404987,"structureType":1,"marketId":1,"marketName":"Full time","isLive":false,"fid":181404987,"suspended":false,"id":113120699,"marketGroup":"1X2","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"81\/100","fractionalValue":"19\/25","sourceId":1641145596,"name":"1","winning":true,"change":-1},{"initialFractionalValue":"27\/10","fractionalValue":"27\/10","sourceId":1641145597,"name":"X","winning":false,"change":0},{"initialFractionalValue":"29\/10","fractionalValue":"31\/10","sourceId":1641145598,"name":"2","winning":false,"change":1}]},{"sourceId":181404987,"structureType":1,"marketId":2,"marketName":"Double chance","isLive":false,"fid":181404987,"suspended":false,"id":113454283,"marketGroup":"Double chance","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"1\/4","fractionalValue":"2\/9","sourceId":1655424249,"name":"1X","winning":true,"change":-1},{"initialFractionalValue":"10\/11","fractionalValue":"19\/20","sourceId":1655424247,"name":"X2","winning":false,"change":1},{"initialFractionalValue":"2\/7","fractionalValue":"2\/7","sourceId":1655424248,"name":"12","winning":true,"change":0}]},{"sourceId":181404987,"structureType":1,"marketId":3,"marketName":"1st half","isLive":false,"fid":181404987,"suspended":false,"id":113454284,"marketGroup":"1X2","marketPeriod":"1st half","choices":[{"initialFractionalValue":"7\/5","fractionalValue":"11\/8","sourceId":1655423994,"name":"X","winning":true,"change":-1},{"initialFractionalValue":"13\/10","fractionalValue":"13\/10","sourceId":1655423995,"name":"X","winning":true,"change":0},{"initialFractionalValue":"10\/3","fractionalValue":"7\/2","sourceId":1655423996,"name":"2","winning":false,"change":1}]},{"sourceId":181404987,"structureType":1,"marketId":4,"marketName":"Draw no bet","isLive":false,"fid":181404987,"suspended":false,"id":113454285,"marketGroup":"Draw no bet","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"2\/5","fractionalValue":"4\/11","sourceId":1655423985,"name":"1","winning":true,"change":-1},{"initialFractionalValue":"7\/4","fractionalValue":"2\/1","sourceId":1655423987,"name":"2","winning":false,"change":1}]},{"sourceId":181404987,"structureType":1,"marketId":5,"marketName":"Both teams to score","isLive":false,"fid":181404987,"suspended":false,"id":113454286,"marketGroup":"Both teams to score","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"7\/10","fractionalValue":"3\/4","sourceId":1655432439,"name":"Yes","winning":false,"change":1},{"initialFractionalValue":"21\/20","fractionalValue":"1\/1","sourceId":1655432440,"name":"No","winning":true,"change":-1}]},{"sourceId":181404987,"structureType":2,"marketId":9,"marketName":"Match goals","choiceGroup":"0.5","isLive":false,"fid":181404987,"suspended":false,"id":113454296,"marketGroup":"Match goals","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"1\/25","fractionalValue":"1\/25","sourceId":1655432293,"name":"Over","winning":true,"change":0},{"initialFractionalValue":"11\/1","fractionalValue":"11\/1","sourceId":1655432298,"name":"Under","winning":false,"change":0}]},{"sourceId":181404987,"structureType":2,"marketId":9,"marketName":"Match goals","choiceGroup":"1.5","isLive":false,"fid":181404987,"suspended":false,"id":113454295,"marketGroup":"Match goals","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"1\/4","fractionalValue":"1\/4","sourceId":1655432303,"name":"Over","winning":false,"change":0},{"initialFractionalValue":"3\/1","fractionalValue":"3\/1","sourceId":1655432307,"name":"Under","winning":true,"change":0}]},{"sourceId":181404987,"structureType":2,"marketId":9,"marketName":"Match goals","choiceGroup":"2.5","isLive":false,"fid":181404987,"suspended":false,"id":113454289,"marketGroup":"Match goals","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"4\/5","fractionalValue":"4\/5","sourceId":1641145599,"name":"Over","winning":false,"change":0},{"initialFractionalValue":"1\/1","fractionalValue":"1\/1","sourceId":1641145600,"name":"Under","winning":true,"change":0}]},{"sourceId":181404987,"structureType":2,"marketId":9,"marketName":"Match goals","choiceGroup":"3.5","isLive":false,"fid":181404987,"suspended":false,"id":113454294,"marketGroup":"Match goals","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"2\/1","fractionalValue":"2\/1","sourceId":1655432304,"name":"Over","winning":false,"change":0},{"initialFractionalValue":"2\/5","fractionalValue":"2\/5","sourceId":1655432294,"name":"Under","winning":true,"change":0}]},{"sourceId":181404987,"structureType":2,"marketId":9,"marketName":"Match goals","choiceGroup":"4.5","isLive":false,"fid":181404987,"suspended":false,"id":113454293,"marketGroup":"Match goals","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"9\/2","fractionalValue":"9\/2","sourceId":1655432308,"name":"Over","winning":false,"change":0},{"initialFractionalValue":"1\/7","fractionalValue":"1\/7","sourceId":1655432305,"name":"Under","winning":true,"change":0}]},{"sourceId":181404987,"structureType":2,"marketId":9,"marketName":"Match goals","choiceGroup":"5.5","isLive":false,"fid":181404987,"suspended":false,"id":113454292,"marketGroup":"Match goals","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"10\/1","fractionalValue":"10\/1","sourceId":1655432295,"name":"Over","winning":false,"change":0},{"initialFractionalValue":"1\/20","fractionalValue":"1\/20","sourceId":1655432296,"name":"Under","winning":true,"change":0}]},{"sourceId":181404987,"structureType":2,"marketId":9,"marketName":"Match goals","choiceGroup":"6.5","isLive":false,"fid":181404987,"suspended":false,"id":113454291,"marketGroup":"Match goals","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"22\/1","fractionalValue":"22\/1","sourceId":1655432297,"name":"Over","winning":false,"change":0},{"initialFractionalValue":"1\/80","fractionalValue":"1\/80","sourceId":1655432306,"name":"Under","winning":true,"change":0}]},{"sourceId":181404987,"structureType":2,"marketId":9,"marketName":"Match goals","choiceGroup":"7.5","isLive":false,"fid":181404987,"suspended":false,"id":113454290,"marketGroup":"Match goals","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"50\/1","fractionalValue":"50\/1","sourceId":1655432301,"name":"Over","winning":false,"change":0},{"initialFractionalValue":"1\/500","fractionalValue":"1\/500","sourceId":1655432302,"name":"Under","winning":true,"change":0}]},{"sourceId":181404987,"structureType":1,"marketId":17,"marketName":"Asian handicap","isLive":false,"fid":181404988,"suspended":false,"id":113454288,"marketGroup":"Asian Handicap","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"7\/8","fractionalValue":"33\/40","sourceId":1641145601,"name":"(-0.5) Heart of Midlothian","change":-1},{"initialFractionalValue":"39\/40","fractionalValue":"41\/40","sourceId":1641145602,"name":"(0.5) Hibernian","change":1}]},{"sourceId":181404987,"structureType":2,"marketId":20,"marketName":"Cards in match","choiceGroup":"4.5","isLive":false,"fid":181404987,"suspended":false,"id":113842128,"marketGroup":"Total Cards","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"8\/13","fractionalValue":"4\/5","sourceId":1764708645,"name":"Over","change":1},{"initialFractionalValue":"6\/5","fractionalValue":"10\/11","sourceId":1764708642,"name":"Under","change":-1}]},{"sourceId":181404987,"structureType":2,"marketId":21,"marketName":"Corners 2-Way","choiceGroup":"10.5","isLive":false,"fid":181404987,"suspended":false,"id":113465068,"marketGroup":"Corners 2-Way","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"8\/11","fractionalValue":"8\/11","sourceId":1735660696,"name":"Over","winning":false,"change":0},{"initialFractionalValue":"1\/1","fractionalValue":"1\/1","sourceId":1735660697,"name":"Under","winning":true,"change":0}]},{"sourceId":181404987,"structureType":1,"marketId":6,"marketName":"First team to score","isLive":false,"fid":181404987,"suspended":false,"id":113454287,"marketGroup":"First team to score","marketPeriod":"Full-time","choices":[{"initialFractionalValue":"8\/13","fractionalValue":"8\/13","sourceId":1655423989,"name":"No goal","winning":false,"change":0},{"initialFractionalValue":"11\/1","fractionalValue":"11\/1","sourceId":1655423991,"name":"No goal","winning":false,"change":0},{"initialFractionalValue":"11\/8","fractionalValue":"7\/5","sourceId":1655423993,"name":"Hibernian","winning":false,"change":1}]}],"eventId":14035158}
    # """
    raw_data = json.loads(dirty_json_string)
    match_id = raw_data.get("eventId", 0)

    print("\n--- 🧹 RUNNING DATA SANITIZER ---\n")
    OddsDataSanitizer.sanitize(raw_data, match_id)
    print("\n--- ✅ SANITIZATION COMPLETE ---\n")

    for market in raw_data["markets"]:
        if market["marketId"] in [3, 6, 17]:
            print(f"🏟️ MARKET {market['marketId']} ({market['marketName']}):")
            for choice in market["choices"]:
                print(f"   Name: '{choice['name']}' | Winning: {choice.get('winning')}")
            print("-" * 40)
