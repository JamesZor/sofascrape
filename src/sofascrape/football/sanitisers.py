# src/sofascrape/football/sanitisers.py

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class OddsDataSanitizer:
    """
    Single Responsibility: Mutates and cleans raw odds JSON payloads
    from third-party APIs to prevent database constraint violations
    and normalize missing fields before formal schema validation.
    """

    @staticmethod
    def sanitize(raw_data: Dict[str, Any], match_id: int) -> None:
        """Sanitizes the odds data in-place."""
        if not raw_data or "markets" not in raw_data:
            return

        for market in raw_data.get("markets", []):
            seen_names = set()
            market_id = market.get("marketId")

            for choice in market.get("choices", []):
                original_name = str(choice.get("name", ""))

                # 1. THE WINNING BOOL FIX
                if "winning" not in choice or choice["winning"] is None:
                    choice["winning"] = False
                elif not isinstance(choice["winning"], bool):
                    choice["winning"] = str(choice["winning"]).strip().lower() in [
                        "true",
                        "1",
                        "t",
                        "y",
                        "yes",
                    ]

                # 2. THE DUPLICATE NAME FIX
                if original_name in seen_names:
                    # Specific Heuristic: missing '2'
                    if original_name == "1" and "2" not in seen_names:
                        new_name = "2"
                    else:
                        # Generic Heuristic: Suffix for duplicates
                        counter = 1
                        new_name = f"{original_name}_dup_{counter}"
                        while new_name in seen_names:
                            counter += 1
                            new_name = f"{original_name}_dup_{counter}"

                    logger.warning(
                        f"Sanitizer: Renamed duplicate choice '{original_name}' "
                        f"to '{new_name}' in Market {market_id} for Match {match_id}."
                    )

                    choice["name"] = new_name
                    seen_names.add(new_name)

                    # 3. PREVENT DUPLICATE WINNERS
                    if choice["winning"] is True:
                        choice["winning"] = False
                        logger.warning(
                            f"Sanitizer: Stripped 'winning: true' from duplicate choice '{new_name}'."
                        )

                else:
                    seen_names.add(original_name)
