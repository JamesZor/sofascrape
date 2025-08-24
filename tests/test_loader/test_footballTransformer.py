import json
import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pytest
from omegaconf import DictConfig, OmegaConf

import sofascrape.schemas.general as sofaschema
from sofascrape.loader.footballloader import FootballLoader
from sofascrape.loader.footballTransformer import FootballDataTransformer

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    force=True,
)


# pd.set_option("display.max_columns", None)
# pd.set_option("display.max_rows", None)  # Show all rows if you want
pd.set_option("display.width", 80)
# pd.set_option("expand_frame_repr", False)


# @pytest.mark.skip()
def test_basic_functionality():
    """Test the basic loading and transformation workflow"""

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
        print(f"Names df {dataframes.keys()}.")

        dataframes["incidents"]

        for name, df in dataframes.items():
            print(f"  {name}: {len(df)} rows")

            print(df.sample(20))
            print("- -" * 50)

        # # Save sample output
        # saved_files = transformer.save_to_csv("output/test")
        # print(f"Saved {len(saved_files)} files")


@pytest.mark.skip()
def test_basic_functionality_large():
    """Test the basic loading and transformation workflow"""

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

    sample_data = loader.load_tournament_seasons(available)

    print(f"Loaded sample data: {len(sample_data)} tournaments")

    if sample_data:
        # Transform the sample
        transformer = FootballDataTransformer()
        dataframes = transformer.transform(sample_data)

        print(f"Created {len(dataframes)} DataFrames")
        print(f"Names df {dataframes.keys()}.")

        for name, df in dataframes.items():
            print(f"  {name}: {len(df)} rows")

            print(df.sample(20))
            print("- -" * 50)

        # Save sample output
        saved_files = transformer.save_to_csv("scot_champ_23_24")
        print(f"Saved {len(saved_files)} files")
