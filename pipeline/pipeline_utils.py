import logging
import time
from typing import Dict, Set

from sofascrape.loader.footballDataManager import FootballDataManager

# Assuming these classes are in these locations based on your files
from sofascrape.quality.manager import SeasonQualityManager

logger = logging.getLogger(__name__)


def run_initial_scrapes(tournament_id: int, season_id: int, num_runs: int = 2):
    """
    Executes a set number of scraping runs for a given tournament and season.
    """
    print(
        f"  Executing {num_runs} initial scrapes for t:{tournament_id} s:{season_id}..."
    )
    manager = SeasonQualityManager(tournament_id=tournament_id, season_id=season_id)

    for i in range(num_runs):
        print(f"    - Starting scrape run {i + 1}/{num_runs}...")
        try:
            manager.execute_scraping_run()
            print(f"    - Scrape run {i + 1} completed.")
            if i < num_runs - 1:
                time.sleep(60)  # Wait between scrapes as in original script
        except Exception as e:
            logger.error(
                f"Scraping run {i + 1} failed for t:{tournament_id} s:{season_id} with error: {e}"
            )


def run_repair_cycle(tournament_id: int, season_id: int):
    """
    Builds a consensus, identifies failures, and runs a retry scrape to repair the data.
    """
    print(f"  Starting repair cycle for t:{tournament_id} s:{season_id}...")
    manager = SeasonQualityManager(tournament_id=tournament_id, season_id=season_id)

    try:
        # 1. Build initial consensus
        print("    - Building initial consensus report...")
        consensus = manager.build_consensus_analysis()
        print(f"    - Initial Consensus: {consensus.season_summary}")

        # 2. Get failed matches and execute retry
        retry_dict = consensus.get_retry_dict()
        if not retry_dict:
            print("    - No failed matches found to retry. Repair cycle complete.")
            return

        print(
            f"    - Found {len(retry_dict)} matches to repair. Executing retry scrape..."
        )
        manager.execute_scraping_retry(retry_dict)

        # 3. Build final consensus
        print("    - Building final consensus report after repair...")
        final_consensus = manager.build_consensus_analysis()
        print(f"    - Final Consensus: {final_consensus.season_summary}")

    except ValueError as e:
        logger.error(
            f"Could not run repair for t:{tournament_id} s:{season_id}. Maybe not enough runs? Error: {e}"
        )
    except Exception as e:
        logger.error(
            f"Repair cycle failed for t:{tournament_id} s:{season_id} with error: {e}"
        )


def build_golden_dataset(tournament_id: int, season_id: int):
    """
    Builds and saves the golden dataset for a tournament and season.
    """
    print(f"  Building golden dataset for t:{tournament_id} s:{season_id}...")
    manager = SeasonQualityManager(tournament_id=tournament_id, season_id=season_id)
    try:
        consensus = manager.storage.load_most_current_consensus()
        if not consensus:
            print(
                f"    - No consensus found for t:{tournament_id} s:{season_id}. Skipping."
            )
            return

        golden_dict = consensus.get_golden_dataset_dict()
        if not golden_dict:
            print(
                f"    - Consensus exists, but no matches to form a golden dataset. Skipping."
            )
            return

        golden_data_set = manager.build_golden_seaosn_dataset(golden_dict)
        manager.storage.save_golden_dataset(golden_data=golden_data_set)
        print(f"    - Golden dataset saved with {len(golden_data_set)} matches.")
    except FileNotFoundError:
        logger.warning(
            f"Consensus file not found for t:{tournament_id} s:{season_id}. Cannot build golden set."
        )
    except Exception as e:
        logger.error(
            f"Failed to build golden dataset for t:{tournament_id} s:{season_id}. Error: {e}"
        )


def process_all_to_df(data_dict: Dict[int, Set[int]], output_dir: str):
    """
    Uses FootballDataManager to process all golden data into DataFrames/CSVs.
    """
    print("  Processing all golden data into DataFrames...")
    manager = FootballDataManager()
    manager.process_tournament_seasons(
        tournament_seasons=data_dict, output_dir=output_dir
    )
    print(f"  DataFrame processing complete. Files saved to '{output_dir}'.")


def check_consensus_status(tournament_id: int, season_id: int):
    """
    Loads the most recent consensus for a season and prints its summary.
    """
    try:
        manager = SeasonQualityManager(tournament_id=tournament_id, season_id=season_id)
        consensus = manager.storage.load_most_current_consensus()
        if consensus:
            # This is the line that prints the output you wanted
            print(f"  t {tournament_id} | s {season_id} : {consensus.season_summary}")
        else:
            print(
                f"  t {tournament_id} | s {season_id} : Consensus file is empty or invalid."
            )
    except FileNotFoundError:
        print(f"  t {tournament_id} | s {season_id} : No consensus file found.")
    except Exception as e:
        logger.error(
            f"Could not check consensus for t:{tournament_id} s:{season_id}. Error: {e}"
        )
