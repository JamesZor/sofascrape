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


if __name__ == "__main__":
    t = process_league_data("scot_pl_20_24")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)  # Show all rows if you want
    pd.set_option("display.width", None)
    pd.set_option("expand_frame_repr", False)
    print(t.dataframes["matches"].head(30))
    # print("-" * 100)
    # print(t.dataframes["match_stats"].sample(frac=0.2))
    # print("-" * 100)
    # print(t.dataframes["player_stats"].head(10))
    # print("-" * 100)
    # print(t.dataframes["incidents"].sample(frac=0.2))
    # print("-" * 100)
    # all_period_stats = pivot_match_stats_clean(
    #     t.dataframes["match_stats"], simplify_names=True, periods_to_include=["ALL"]
    # )
    all_period_stats = t.dataframes["match_stats"]
    # print(all_period_stats.columns)
    # print(
    #     all_period_stats[all_period_stats["match_id"] == 9573751][
    #         [
    #             "season_year",
    #             "match_id",
    #             "period",
    #             "home_value_ballPossession",
    #             "away_value_ballPossession",
    #             "home_value_bigChanceCreated",
    #             "away_value_bigChanceCreated",
    #             "home_value_totalShotsOnGoal",
    #             "away_value_totalShotsOnGoal",
    #         ]
    #     ].head(30)
    # )
    # Now print your dataframe
    # print(all_period_stats[all_period_stats["match_id"] == 9573751])

    ####
    # base: pd.DataFrame = t.dataframes["matches"]
    # all_stats = all_period_stats[all_period_stats["period"] == "ALL"]
    #
    # hmm = base.merge(all_stats, how="left", on=["match_id", "season_year"])
    #
    # print(
    #     hmm[
    #         [
    #             "season_year",
    #             "match_id",
    #             "home_team_slug",
    #             "away_team_slug",
    #             "home_value_ballPossession",
    #             "away_value_ballPossession",
    #         ]
    #     ]
    # )

    print(t.dataframes["incidents"][t.dataframes["incidents"]["match_id"] == 12477058])
