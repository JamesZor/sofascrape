from typing import List

import pandas as pd

from sofascrape.transforms import FootballDataTransformer
from sofascrape.utils import FootballLeague, ProcessingUtils


def process_league_data(pickle_file: str):
    """Process pickled league data into ML-ready format"""
    # Load the pickle
    pu = ProcessingUtils(type=FootballLeague.SCOTLAND, web_on=False)
    league_data = pu.load_pickle(file_name=pickle_file)

    # Transform to DataFrames
    transformer = FootballDataTransformer(league_data)
    dataframes = transformer.transform_all()

    # Save to CSV
    transformer.save_to_csv("/home/james/bet_project/sofascrape/data/football_data_out")

    # # Create ML-ready dataset
    # ml_dataset = transformer.create_ml_ready_dataset()
    # ml_dataset.to_csv("./football_data_output/ml_ready_dataset.csv", index=False)

    # Print summary
    print("\nData Transformation Summary:")
    for name, df in dataframes.items():
        print(f"{name}: {len(df)} rows, {len(df.columns)} columns")

    return transformer


# Alternative version with cleaner column names
def pivot_match_stats_clean(
    stats_df: pd.DataFrame, simplify_names: bool = True, periods_to_include: list = None
) -> pd.DataFrame:
    """
    Pivot match statistics with options for cleaner output.

    Args:
        stats_df: DataFrame with match statistics in long format
        simplify_names: If True, use only stat_key for column names
        periods_to_include: List of periods to include (e.g., ['ALL', '1ST', '2ND'])

    Returns:
        DataFrame with stats in wide format
    """
    df = stats_df.copy()

    # Filter periods if specified
    if periods_to_include:
        df = df[df["period"].isin(periods_to_include)]

    # Create stat identifier
    if simplify_names:
        df["stat_identifier"] = df["stat_key"]
    else:
        # Clean up names for better column headers
        df["stat_identifier"] = (
            df["group_name"].str.replace(" ", "_").str.lower() + "_" + df["stat_key"]
        )

    # Single pivot with both home and away values
    pivoted = df.pivot_table(
        index=["match_id", "season_year", "period"],
        columns="stat_identifier",
        values=["home_value", "away_value"],
        aggfunc="first",
    )

    # Flatten multi-level columns
    pivoted.columns = ["_".join(col).strip() for col in pivoted.columns.values]

    # Reset index
    result = pivoted.reset_index()

    # Optional: Reorganize columns to pair home/away stats
    if simplify_names:
        result = reorganize_columns(result)

    return result


def reorganize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reorganize columns to group home/away pairs together.
    """
    id_cols = ["match_id", "season_year", "period"]
    stat_cols = [col for col in df.columns if col not in id_cols]

    # Extract unique stat names
    home_stats = [col for col in stat_cols if col.startswith("home_value_")]
    away_stats = [col for col in stat_cols if col.startswith("away_value_")]

    # Pair them up
    paired_cols = []
    for home_col in sorted(home_stats):
        stat_name = home_col.replace("home_value_", "")
        away_col = f"away_value_{stat_name}"
        if away_col in away_stats:
            paired_cols.extend([home_col, away_col])

    return df[id_cols + paired_cols]


if __name__ == "__main__":
    t = process_league_data("scot_pl_example")
    print(t.dataframes["matches"].head(5))
    print("-" * 100)
    print(t.dataframes["match_stats"].sample(frac=0.2))
    print("-" * 100)
    print(t.dataframes["player_stats"].head(10))
    print("-" * 100)
    print(t.dataframes["incidents"].sample(frac=0.2))
    print("-" * 100)
    all_period_stats = pivot_match_stats_clean(
        t.dataframes["match_stats"], simplify_names=True, periods_to_include=["ALL"]
    )
    print(all_period_stats.columns)
    print(
        all_period_stats[
            [
                "season_year",
                "match_id",
                "home_value_ballPossession",
                "away_value_ballPossession",
                "home_value_bigChanceCreated",
                "away_value_bigChanceCreated",
                "home_value_totalShotsOnGoal",
                "away_value_totalShotsOnGoal",
            ]
        ].head(20)
    )
