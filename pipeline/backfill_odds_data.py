# pipeline/backfill_odds_data.py

import argparse
import logging
import traceback
from typing import Dict, Set

from sofascrape.loader import FootballLoader
from sofascrape.quality.manager import SeasonQualityManager

# --- Detailed Logging Setup ---
# This will create a 'backfill_odds.log' file in the same directory
log_file = "backfill_odds.log"
file_handler = logging.FileHandler(
    log_file, mode="w"
)  # 'w' to overwrite the log each time
stream_handler = logging.StreamHandler()

logging.basicConfig(
    level=logging.DEBUG,  # Set the root logger to DEBUG to capture everything
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[file_handler, stream_handler],
)

# Keep the library logs quiet to reduce noise
logging.getLogger("sofascrape").setLevel(logging.WARNING)
logging.getLogger("webdriver").setLevel(logging.WARNING)

# Get a logger specific to this script
logger = logging.getLogger("odds_backfill")


def get_match_ids_for_season(tournament_id: int, season_id: int) -> Set[int]:
    """Finds all existing match IDs for a specific tournament and season."""
    logger.debug(
        f"Attempting to discover matches for T:{tournament_id}, S:{season_id}."
    )
    loader = FootballLoader()

    data = loader.load_tournament_seasons({tournament_id: {season_id}})

    if not data or tournament_id not in data or season_id not in data[tournament_id]:
        logger.warning("No existing golden data found for this season.")
        return set()

    match_ids = set(data[tournament_id][season_id].keys())
    logger.info(f"Discovered {len(match_ids)} existing match IDs.")
    return match_ids


def main():
    """Main script to backfill odds data with detailed logging."""
    parser = argparse.ArgumentParser(
        description="Backfill odds data for a specific season."
    )
    parser.add_argument("tournament_id", type=int, help="The ID of the tournament.")
    parser.add_argument("season_id", type=int, help="The ID of the season.")
    args = parser.parse_args()

    logger.info(
        f"--- Starting Odds Backfill for T:{args.tournament_id}, S:{args.season_id} ---"
    )

    match_ids = get_match_ids_for_season(args.tournament_id, args.season_id)

    if not match_ids:
        logger.warning("No matches to process. Exiting.")
        return

    odds_retry_dict = {match_id: ["odds"] for match_id in match_ids}
    logger.debug(f"Built retry dictionary for {len(odds_retry_dict)} matches.")

    try:
        quality_manager = SeasonQualityManager(
            tournament_id=args.tournament_id, season_id=args.season_id
        )

        # --- EXECUTE ---
        logger.info("Executing scraping retry for 'odds' component...")
        quality_manager.execute_scraping_retry(odds_retry_dict)
        logger.info("Scraping retry process finished.")

        # --- VERIFY ---
        logger.info("\n--- Verifying Saved Data ---")

        # **CRITICAL FIX:** Load the most recent RUN file, not a consensus file.
        run_files = quality_manager.storage._list_run_files()
        if not run_files:
            logger.error(
                "VERIFICATION FAILED: No run files found in the storage directory."
            )
            return

        latest_run_number = quality_manager.storage._next_run_number() - 1
        logger.debug(
            f"Attempting to load latest run file (run number: {latest_run_number})."
        )

        latest_run_data = quality_manager.storage.load_run(run_number=latest_run_number)

        if not latest_run_data or not latest_run_data.matches:
            logger.error(
                "VERIFICATION FAILED: The newly saved run file is empty or could not be loaded."
            )
            return

        logger.info(
            f"Successfully loaded run file for verification. It contains {len(latest_run_data.matches)} matches."
        )

        odds_found_count = 0
        for match_event in latest_run_data.matches:
            if (
                match_event.data
                and hasattr(match_event.data, "odds")
                and match_event.data.odds
            ):
                odds_found_count += 1

        logger.info(
            f"Verification complete. Found odds data for {odds_found_count} out of {len(latest_run_data.matches)} matches in the new file."
        )

        if odds_found_count == 0:
            logger.error(
                "VERIFICATION FAILED: No odds data was actually saved. Check scraper logs."
            )
        else:
            logger.info(
                f"✅ Successfully completed and verified odds scraping for Season {args.season_id}."
            )

    except Exception as e:
        logger.critical(f"A critical error occurred during the process: {e}")
        logger.critical(traceback.format_exc())

    logger.info("\n--- Odds Backfill Process Finished ---")


if __name__ == "__main__":
    main()
