import argparse
import logging
from typing import Any, Dict

# Import the reusable logic from our new utils file
import pipeline_utils as utils
import yaml

# --- Basic logging setup ---
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)
# Quieten noisy loggers from the library
logging.getLogger("sofascrape").setLevel(logging.WARNING)
logging.getLogger("webdriver").setLevel(logging.WARNING)


def load_config(
    path: str = "/home/james/bet_project/sofascrape/pipeline/config.yaml",
) -> Dict[str, Any]:
    """Loads the YAML configuration file."""
    with open(path, "r") as f:
        return yaml.safe_load(f)  # type: ignore[no-any-return]


def get_all_seasons_from_scope(data_scope: Dict) -> Dict[int, list[int]]:
    """Helper to get all tournament/season pairs from the config."""
    all_seasons: Dict = {}
    for country, tournaments in data_scope.items():
        for tour_id, season_list in tournaments.items():
            tour_id = int(tour_id)  # Ensure key is integer
            if tour_id not in all_seasons:
                all_seasons[tour_id] = []
            all_seasons[tour_id].extend(season_list)
    return all_seasons


def main():
    parser = argparse.ArgumentParser(
        description="""
    UK Football Data Pipeline Manager.
    Orchestrates scraping, data quality checks, and final processing based on the config.yaml file.
    """,
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "command",
        choices=[
            "update",
            "build-golden",
            "export",
            "run-all",
            "check-consensus",
        ],  # Add "check-consensus" here
        help="""The pipeline stage to execute:
  - update:          Scrape and repair only the 'live_seasons' defined in config.
  - build-golden:    Build the final golden dataset for ALL seasons in config.
  - export:          Process ALL golden datasets into final CSV files.
  - run-all:         Execute the entire pipeline: update -> build-golden -> export.
  - check-consensus: Print the summary of the latest consensus for ALL seasons.
""",
    )

    args = parser.parse_args()
    config = load_config()
    data_scope = config.get("data_scope", {})
    process_settings = config.get("process_settings", {})

    # --- Execute the chosen command ---

    if args.command in ["update", "run-all"]:
        print("\n Starting Stage 1: Update Live Seasons...")
        live_season_ids = set(process_settings.get("live_seasons", []))
        if not live_season_ids:
            print("No live seasons defined in config.yaml. Skipping update.")
        else:
            all_seasons = get_all_seasons_from_scope(data_scope)
            for tour_id, season_list in all_seasons.items():
                for season_id in season_list:
                    if season_id in live_season_ids:
                        print(
                            f"\nProcessing LIVE season: Tournament={tour_id}, Season={season_id}"
                        )
                        utils.run_initial_scrapes(tour_id, season_id)
                        utils.run_repair_cycle(tour_id, season_id)
        print("✅ Stage 1 Complete.")

    if args.command in ["build-golden", "run-all"]:
        print("\n Starting Stage 2: Build Golden Datasets...")
        all_seasons = get_all_seasons_from_scope(data_scope)
        for tour_id, season_list in all_seasons.items():
            for season_id in season_list:
                utils.build_golden_dataset(tour_id, season_id)
        print("✅ Stage 2 Complete.")

    if args.command in ["export", "run-all"]:
        print("\n Starting Stage 3: Export to CSV...")
        all_seasons = get_all_seasons_from_scope(data_scope)
        output_dir = process_settings.get("output_directory", "output")
        # Convert list of seasons to set for the data manager
        export_dict = {
            tour_id: set(seasons) for tour_id, seasons in all_seasons.items()
        }
        utils.process_all_to_df(export_dict, output_dir)
        print("✅ Stage 3 Complete.")

    elif args.command == "check-consensus":
        print("\n📊 Starting Stage: Check Consensus Status...")
        all_seasons = get_all_seasons_from_scope(data_scope)
        for tour_id, season_list in all_seasons.items():
            for season_id in season_list:
                utils.check_consensus_status(tour_id, season_id)
        print("✅ Stage Complete.")

    print("\n Pipeline execution finished.")


if __name__ == "__main__":
    main()
