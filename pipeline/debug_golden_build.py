# pipeline/debug_golden_build.py

import argparse
import logging
import pickle
from typing import Any, Dict

from sofascrape.quality.manager import SeasonQualityManager
from sofascrape.schemas import general as sofaschemas

# --- Basic logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logging.getLogger("sofascrape").setLevel(logging.WARNING)
logging.getLogger("webdriver").setLevel(logging.WARNING)


def inspect_runs(tournament_id: int, season_id: int, match_id_to_inspect: int):
    """
    Loads all run data for a season and inspects the contents for a specific match.
    """
    print(f"--- Starting Debug Inspection for T:{tournament_id}, S:{season_id} ---")
    print(f"--- Focusing on Match ID: {match_id_to_inspect} ---")

    manager = SeasonQualityManager(tournament_id=tournament_id, season_id=season_id)

    # This method loads the raw run objects
    runs = manager.storage.load_avaiable_runs()

    if not runs:
        print("❌ No run files found for this season. Cannot debug.")
        return

    print(f"\nFound {len(runs)} run files: {list(runs.keys())}")
    print("-" * 50)

    # This will hold the match data from each run
    match_data_from_runs: Dict[str, sofaschemas.FootballMatchResultDetailed] = {}

    # 1. Extract the specific match from each run file
    for run_id, run_data in runs.items():
        found_match = False
        for match_event in run_data.matches:
            if match_event.match_id == match_id_to_inspect:
                if match_event.data:
                    match_data_from_runs[run_id] = match_event.data
                    found_match = True
                break
        if not found_match:
            print(f"INFO: Match {match_id_to_inspect} not found in Run '{run_id}'.")

    if not match_data_from_runs:
        print(
            f"\n❌ ERROR: Match ID {match_id_to_inspect} was not found in ANY run file."
        )
        return

    # 2. Inspect and report on what components are present in each run
    print(f"\n--- Component Inspection for Match {match_id_to_inspect} ---")
    for run_id, match_data in match_data_from_runs.items():
        print(f"\n[Run '{run_id}']")
        components_present = []
        if match_data.base:
            components_present.append("base")
        if match_data.stats:
            components_present.append("stats")
        if match_data.lineup:
            components_present.append("lineup")
        if match_data.incidents:
            components_present.append("incidents")
        if match_data.graph:
            components_present.append("graph")
        if hasattr(match_data, "odds") and match_data.odds:  # Check for odds
            components_present.append("odds")

        if components_present:
            print(f"  ✅ Contains data for components: {', '.join(components_present)}")
        else:
            print("  ⚠️ This run contains no data for this match.")

    print("-" * 50)

    # 3. Simulate the merge logic from your manager
    print("\n--- Simulating Golden Data Merge ---")

    final_merged_data: Dict[str, Any] = {"match_id": match_id_to_inspect}
    active_components = manager.config.quality.active_components
    print(f"Manager is configured to look for these components: {active_components}")

    for component_name in active_components:
        component_found = False
        for run_id, match_data in match_data_from_runs.items():
            component_data = getattr(match_data, component_name, None)
            if component_data is not None:
                final_merged_data[component_name] = component_data
                print(
                    f"  ✅ Found '{component_name}' data in Run '{run_id}'. Using this one."
                )
                component_found = True
                break  # Stop after finding the first one
        if not component_found:
            print(f"  ❌ Did not find '{component_name}' data in any run.")
            final_merged_data[component_name] = None

    # 4. Show the final merged object
    print("\n--- Final Merged Object (before saving) ---")
    final_object = sofaschemas.FootballMatchResultDetailed(**final_merged_data)

    print("Components that will be in the golden file:")
    if final_object.base:
        print("  - base")
    if final_object.stats:
        print("  - stats")
    if final_object.lineup:
        print("  - lineup")
    if final_object.incidents:
        print("  - incidents")
    if final_object.graph:
        print("  - graph")
    if final_object.odds:
        print("  - odds")  # Check for odds here

    print("\n--- Debugging Finished ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Debug the golden data build process for a single match."
    )
    parser.add_argument("tournament_id", type=int, help="The ID of the tournament.")
    parser.add_argument("season_id", type=int, help="The ID of the season.")
    parser.add_argument(
        "match_id", type=int, help="A specific Match ID from that season to inspect."
    )
    args = parser.parse_args()

    inspect_runs(args.tournament_id, args.season_id, args.match_id)

# 14035505
# 14035506
