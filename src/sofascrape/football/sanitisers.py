# src/sofascrape/football/sanitisers.py

import logging
import re
from typing import Any, Dict

logger = logging.getLogger(__name__)


class OddsDataSanitiser:
    """
    Single Responsibility: Mutates and cleans raw odds JSON payloads.
    Prioritizes structural integrity and flags corrupted markets for downstream grading.
    """

    @staticmethod
    def sanitise(raw_data: Dict[str, Any], match_id: int) -> None:
        if not raw_data or "markets" not in raw_data:
            return

        for market in raw_data.get("markets", []):
            market_name = market.get("marketName", "")
            market_group = market.get("marketGroup", "")
            choices = market.get("choices", [])
            market_id_val = market.get("marketId")

            # NEW: Step 0: Detect corruption before we alter the names
            is_corrupted = OddsDataSanitiser._has_duplicates(choices)

            # Step 1: Force Strict Positional Names (Ignore API typos completely)
            OddsDataSanitiser._enforce_structural_names(
                market_name, market_group, choices
            )

            # Step 2: Fallback for unmapped markets (Generic Duplicates)
            OddsDataSanitiser._resolve_generic_duplicates(market)

            # Step 3: Safe Boolean Coercion (Aware of corruption)
            OddsDataSanitiser._safely_resolve_booleans(
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
            logger.info(
                f"Market {market_id} corrupted by duplicates. Flagging 'winning' as None for grading."
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
                    logger.info(
                        f"Safety: Stripped secondary 'winning: True' from '{c.get('name')}'."
                    )
