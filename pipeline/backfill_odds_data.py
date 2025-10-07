# pipeline/backfill_odds_data.py

import argparse
import logging
from typing import Dict, Set

from sofascrape.loader import FootballLoader
from sofascrape.quality.manager import SeasonQualityManager

# --- Basic logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logging.getLogger("sofascrape").setLevel(logging.WARNING)
logging.getLogger("webdriver").setLevel(logging.WARNING)


def get_match_ids_for_season(tournament_id: int, season_id: int) -> Set[int]:
    """
    Finds all existing match IDs for a specific tournament and season.
    """
    print(f"Discovering matches for Tournament {tournament_id}, Season {season_id}...")
    loader = FootballLoader()

    # Load data for just the specified season
    data = loader.load_tournament_seasons({tournament_id: {season_id}})

    if not data or tournament_id not in data or season_id not in data[tournament_id]:
        print("  No existing golden data found for this season.")
        return set()

    match_ids = set(data[tournament_id][season_id].keys())
    print(f"  Found {len(match_ids)} existing matches.")
    return match_ids


def main():
    """
    Main script to backfill odds data for a specific tournament and season.
    """
    parser = argparse.ArgumentParser(
        description="""
    Backfill odds data for a specific tournament and season.
    This script finds all existing matches for the given season and scrapes only the 'odds' component for them.
    """
    )
    parser.add_argument(
        "tournament_id", type=int, help="The ID of the tournament to process."
    )
    parser.add_argument("season_id", type=int, help="The ID of the season to process.")
    args = parser.parse_args()

    print(
        f"--- Starting Odds Backfill for T:{args.tournament_id}, S:{args.season_id} ---"
    )

    # 1. Find all match IDs for the specified season
    match_ids = get_match_ids_for_season(args.tournament_id, args.season_id)

    if not match_ids:
        print("No matches to process. Exiting.")
        return

    # 2. Build the specific retry dictionary for this season's matches
    odds_retry_dict = {match_id: ["odds"] for match_id in match_ids}
    print(f"  Prepared to scrape 'odds' for {len(odds_retry_dict)} matches.")

    # 3. Use the SeasonQualityManager to execute this targeted scrape
    try:
        quality_manager = SeasonQualityManager(
            tournament_id=args.tournament_id, season_id=args.season_id
        )
        quality_manager.execute_scraping_retry(odds_retry_dict)
        print(f"  ✅ Successfully completed odds scraping for Season {args.season_id}.")
    except Exception as e:
        print(f"  ❌ An error occurred while processing Season {args.season_id}: {e}")

    print("\n--- Odds Backfill Process Finished ---")
    print("Next steps: Run 'build-golden' and then 'export' to verify the results.")


if __name__ == "__main__":
    main()
