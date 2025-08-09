import logging
from pathlib import Path
from typing import Dict, Set

from .footballloader import FootballLoader
from .footballTransformer import FootballDataTransformer

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FootballDataManager:
    """High-level manager class that orchestrates loading and transformation"""

    def __init__(self):
        self.loader = FootballLoader()
        self.transformer = FootballDataTransformer()

    def process_tournaments(self, tournament_ids: Set[int], output_dir: str = "output"):
        """Load specific tournaments and transform to CSV"""

        logger.info(f"Starting processing for tournaments: {tournament_ids}")

        # Load data
        logger.info("Loading tournament data...")
        data = self.loader.load_tournaments(tournament_ids)

        if self.loader.get_load_errors():
            logger.warning(f"Load errors: {self.loader.get_load_errors()}")

        if not data:
            logger.error("No data loaded successfully")
            return None

        # Transform data
        logger.info("Transforming data to DataFrames...")
        dataframes = self.transformer.transform(data)

        if self.transformer.get_transformation_errors():
            logger.warning(
                f"Transformation errors: {self.transformer.get_transformation_errors()}"
            )

        # Save to CSV
        logger.info(f"Saving DataFrames to {output_dir}...")
        saved_files = self.transformer.save_to_csv(output_dir)

        # Print summary
        self._print_summary(dataframes, saved_files)

        return dataframes

    def process_tournament_seasons(
        self, tournament_seasons: Dict[int, Set[int]], output_dir: str = "output"
    ):
        """Load specific tournament/season combinations and transform to CSV"""

        logger.info(f"Starting processing for tournament/seasons: {tournament_seasons}")

        # Load data
        logger.info("Loading tournament/season data...")
        data = self.loader.load_tournament_seasons(tournament_seasons)

        if self.loader.get_load_errors():
            logger.warning(f"Load errors: {self.loader.get_load_errors()}")

        if not data:
            logger.error("No data loaded successfully")
            return None

        # Transform data
        logger.info("Transforming data to DataFrames...")
        dataframes = self.transformer.transform(data)

        if self.transformer.get_transformation_errors():
            logger.warning(
                f"Transformation errors: {self.transformer.get_transformation_errors()}"
            )

        # Save to CSV
        logger.info(f"Saving DataFrames to {output_dir}...")
        saved_files = self.transformer.save_to_csv(output_dir)

        # Print summary
        self._print_summary(dataframes, saved_files)

        return dataframes

    def discover_and_process_all(self, output_dir: str = "output"):
        """Discover all available data and process everything"""

        logger.info("Discovering available tournament/season data...")
        available_data = self.loader.discover_available_data()

        if not available_data:
            logger.error("No tournament data found")
            return None

        logger.info(f"Found data for tournaments: {list(available_data.keys())}")
        for tournament_id, season_ids in available_data.items():
            logger.info(f"  Tournament {tournament_id}: {len(season_ids)} seasons")

        # Load all available data
        logger.info("Loading all available data...")
        data = self.loader.load_all_tournaments()

        if self.loader.get_load_errors():
            logger.warning(f"Load errors: {self.loader.get_load_errors()}")

        # Transform data
        logger.info("Transforming data to DataFrames...")
        dataframes = self.transformer.transform(data)

        if self.transformer.get_transformation_errors():
            logger.warning(
                f"Transformation errors: {self.transformer.get_transformation_errors()}"
            )

        # Save to CSV
        logger.info(f"Saving DataFrames to {output_dir}...")
        saved_files = self.transformer.save_to_csv(output_dir)

        # Print summary
        self._print_summary(dataframes, saved_files)

        return dataframes

    def _print_summary(self, dataframes: Dict, saved_files: Dict):
        """Print processing summary"""
        print("\n" + "=" * 60)
        print("FOOTBALL DATA PROCESSING SUMMARY")
        print("=" * 60)

        print("\nDataFrames created:")
        for name, df in dataframes.items():
            print(f"  {name}: {len(df):,} rows, {len(df.columns)} columns")

        print(f"\nFiles saved:")
        for df_name, filepath in saved_files.items():
            print(f"  {df_name}: {filepath}")

        # Show sample data
        if "matches" in dataframes and not dataframes["matches"].empty:
            print(f"\nSample matches data:")
            sample_matches = dataframes["matches"].head(3)
            for _, row in sample_matches.iterrows():
                print(
                    f"  {row.get('home_team', 'N/A')} vs {row.get('away_team', 'N/A')} "
                    f"({row.get('home_score', 'N/A')}-{row.get('away_score', 'N/A')})"
                )

        print("=" * 60)


def example_usage():
    """Example usage patterns"""

    # Initialize manager
    manager = FootballDataManager()

    # Example 1: Process specific tournaments (all their seasons)
    print("Example 1: Processing specific tournaments...")
    dataframes1 = manager.process_tournaments(
        tournament_ids={54, 23}, output_dir="output/tournaments"
    )

    # Example 2: Process specific tournament/season combinations
    print("\nExample 2: Processing specific tournament/seasons...")
    dataframes2 = manager.process_tournament_seasons(
        tournament_seasons={
            54: {62408, 62409},  # Tournament 54, seasons 62408 & 62409
            23: {55123},  # Tournament 23, season 55123
        },
        output_dir="output/custom_selection",
    )

    # Example 3: Discover and process all available data
    print("\nExample 3: Processing all available data...")
    dataframes3 = manager.discover_and_process_all(output_dir="output/complete")

    # Example 4: Custom configuration
    print("\nExample 4: Using custom transformation config...")
    custom_config_path = Path("custom_transformation_config.yaml")
    custom_manager = FootballDataManager(custom_config_path)

    dataframes4 = custom_manager.process_tournaments(
        tournament_ids={55}, output_dir="output/custom_config"
    )


"""
def test_basic_functionality():
    Test the basic loading and transformation workflow

    print("Testing basic functionality...")

    # Test data loading
    loader = FootballLoader()

    # Discover what's available
    available = loader.discover_available_data()
    print(f"Available data: {available}")

    if not available:
        print("No data found. Check your data directory structure.")
        return

    # Load a small sample
    first_tournament = list(available.keys())[0]
    first_season = list(available[first_tournament])[0]

    sample_data = loader.load_tournament_seasons({first_tournament: {first_season}})

    print(f"Loaded sample data: {len(sample_data)} tournaments")

    if sample_data:
        # Transform the sample
        transformer = FootballDataTransformer()
        dataframes = transformer.transform(sample_data)

        print(f"Created {len(dataframes)} DataFrames")
        for name, df in dataframes.items():
            print(f"  {name}: {len(df)} rows")

        # Save sample output
        saved_files = transformer.save_to_csv("output/test")
        print(f"Saved {len(saved_files)} files")


if __name__ == "__main__":
"""
# Run the test first to verify everything works
# test_basic_functionality()

# Then run the full examples
# example_usage()
