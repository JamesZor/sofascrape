import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

import sofascrape.schemas.general as sofaschema

logger = logging.getLogger(__name__)

##################################################
# Incident type extractors
##################################################


def incident_type_goals_extractor(
    current_dict: Dict, incident: sofaschema.GoalIncidentSchema
) -> None:
    """In place update for the goal type incident"""
    current_dict.update(
        {
            "player_name": incident.player.name if incident.player else None,
            "assist1_name": incident.assist1.name if incident.assist1 else None,
            "assist2_name": incident.assist2.name if incident.assist2 else None,
            "home_score": incident.homeScore,
            "away_score": incident.awayScore,
            "incident_class": incident.incidentClass,
        }
    )


def incident_type_card_extractor(
    current_dict: Dict, incident: sofaschema.CardIncidentSchema
) -> None:
    """In place update for the card type incident"""
    current_dict.update(
        {
            "player_name": incident.player.name if incident.player else None,
            "card_type": incident.incidentClass,
            "reason": incident.reason,
            "rescinded": incident.rescinded,
        }
    )


def incident_type_substitution_extractor(
    current_dict: Dict, incident: sofaschema.SubstitutionIncidentSchema
) -> None:
    """In place update for the substitution type incident"""
    current_dict.update(
        {
            "player_in_name": incident.playerIn.name if incident.playerIn else None,
            "player_out_name": incident.playerOut.name if incident.playerOut else None,
            "is_injury": incident.injury,
            "incident_class": incident.incidentClass,
        }
    )


def incident_type_period_extractor(
    current_dict: Dict, incident: sofaschema.PeriodIncidentSchema
) -> None:
    """In place update for the period type incident (HT, FT)"""
    current_dict.update(
        {
            "period_text": incident.text,
            "home_score": incident.homeScore,
            "away_score": incident.awayScore,
            "is_live": incident.isLive,
            "added_time": incident.addedTime,
            "time_seconds": incident.timeSeconds,
            "reversed_period_time": incident.reversedPeriodTime,
            "reversed_period_time_seconds": incident.reversedPeriodTimeSeconds,
            "period_time_seconds": incident.periodTimeSeconds,
        }
    )


def incident_type_injury_time_extractor(
    current_dict: Dict, incident: sofaschema.InjuryTimeIncidentSchema
) -> None:
    """In place update for the injury time type incident"""
    current_dict.update(
        {
            "injury_time_length": incident.length,
            "added_time": incident.addedTime,
            "reversed_period_time": incident.reversedPeriodTime,
        }
    )


def incident_type_var_decision_extractor(
    current_dict: Dict, incident: sofaschema.VarDecisionIncidentSchema
) -> None:
    """In place update for the VAR decision type incident"""
    current_dict.update(
        {
            "var_confirmed": incident.confirmed,
            "player_name": incident.player.name if incident.player else None,
            "var_decision": incident.decision,
            "var_reason": incident.reason,
            "incident_class": incident.incidentClass,
            "reversed_period_time": incident.reversedPeriodTime,
        }
    )


def incident_type_in_game_penalty_extractor(
    current_dict: Dict, incident: sofaschema.InGamePenaltySchema
) -> None:
    """In place update for the in-game penalty type incident"""
    current_dict.update(
        {
            "player_name": incident.player.name if incident.player else None,
            "penalty_reason": incident.reason,
            "rescinded": incident.rescinded,
            "incident_class": incident.incidentClass,
            "reversed_period_time": incident.reversedPeriodTime,
        }
    )


# Strategy mapping - this is the key to your pattern
INCIDENT_EXTRACTORS: Dict[str, Callable[[Dict, Any], None]] = {
    "goal": incident_type_goals_extractor,
    "card": incident_type_card_extractor,
    "substitution": incident_type_substitution_extractor,
    "period": incident_type_period_extractor,
    "injuryTime": incident_type_injury_time_extractor,
    "varDecision": incident_type_var_decision_extractor,
    "inGamePenalty": incident_type_in_game_penalty_extractor,
}


class FootballDataTransformer:
    """Transform nested Pydantic football data into pandas DataFrames"""

    def __init__(self, league_data: Dict):
        """
        Args:
            league_data: Dictionary with season_year as key and SeasonFootballScraper as value
        """
        self.league_data = league_data
        self.dataframes: dict = {}

    def transform_all(self) -> Dict[str, pd.DataFrame]:
        """Transform all data into multiple DataFrames"""
        self.dataframes["matches"] = self._create_matches_df()
        self.dataframes["match_stats"] = self._create_match_stats_df()
        self.dataframes["player_stats"] = self._create_player_stats_df()
        self.dataframes["incidents"] = self._create_incidents_df()
        self.dataframes["momentum"] = self._create_momentum_df()
        return self.dataframes

    def _create_matches_df(self) -> pd.DataFrame:
        """Create main matches DataFrame with basic match info"""
        matches_data = []

        for season_year, season_scraper in self.league_data.items():
            if not season_scraper.data:
                continue

            for match in season_scraper.data.matches:
                if match.success and match.data and match.data.base:
                    event: sofaschema.FootballEventSchema = match.data.base.event

                    match_dict = {
                        # Identifiers
                        "season_year": season_year,
                        "season_id": match.season_id,
                        "match_id": match.match_id,
                        "tournament_id": match.tournament_id,
                        "tournament_slug": event.tournament.slug,
                        # Teams
                        "home_team": event.homeTeam.name,
                        "home_team_slug": event.homeTeam.slug,
                        "away_team": event.awayTeam.name,
                        "away_team_slug": event.awayTeam.slug,
                        # Score
                        "home_score": (
                            event.homeScore.current if event.homeScore else None
                        ),
                        "away_score": (
                            event.awayScore.current if event.awayScore else None
                        ),
                        "home_score_ht": (
                            event.homeScore.period1 if event.homeScore else None
                        ),
                        "away_score_ht": (
                            event.awayScore.period1 if event.awayScore else None
                        ),
                        # Match info
                        "match_date": datetime.fromtimestamp(event.startTimestamp),
                        "round": event.roundInfo.round,
                        "status": event.status.description,
                        "winner_code": event.winnerCode,
                        # Venue
                        "venue": event.venue.name if event.venue else None,
                        "attendance": (
                            event.attendance if hasattr(event, "attendance") else None
                        ),
                        # Features
                        "has_xg": event.hasXg,
                        "has_stats": event.hasEventPlayerStatistics,
                        # Metadata
                        "scraped_at": match.scraped_at,
                        "scraping_success_rate": match.data.success_rate,
                    }

                    matches_data.append(match_dict)

        return pd.DataFrame(matches_data)

    def _pivot_match_stats_clean(
        self,
        stats_df: pd.DataFrame,
        simplify_names: bool = True,
        periods_to_include: list = ["ALL", "1ST", "2ND"],
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
                df["group_name"].str.replace(" ", "_").str.lower()
                + "_"
                + df["stat_key"]
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
            result = self._reorganize_columns(result)

        return result

    def _reorganize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def _create_match_stats_df(self) -> pd.DataFrame:
        """Create DataFrame with aggregated match statistics"""
        stats_data = []

        for season_year, season_scraper in self.league_data.items():
            if not season_scraper.data:
                continue

            for match in season_scraper.data.matches:
                if match.success and match.data and match.data.stats:
                    # Process each period
                    for period in match.data.stats.statistics:
                        # Process each statistic group
                        for group in period.groups:
                            for stat in group.statisticsItems:
                                stat_dict = {
                                    "match_id": match.match_id,
                                    "season_year": season_year,
                                    "period": period.period,
                                    "group_name": group.groupName,
                                    "stat_key": stat.key,
                                    "stat_name": stat.name,
                                    "home_value": stat.homeValue,
                                    "away_value": stat.awayValue,
                                    "stat_type": stat.statisticsType,
                                    "value_type": stat.valueType,
                                }
                                stats_data.append(stat_dict)

        return self._pivot_match_stats_clean(pd.DataFrame(stats_data))

    def _create_player_stats_df(self) -> pd.DataFrame:
        """Create DataFrame with player-level statistics"""
        player_data = []

        for season_year, season_scraper in self.league_data.items():
            if not season_scraper.data:
                continue

            for match in season_scraper.data.matches:
                if match.success and match.data and match.data.lineup:
                    # Process home team players
                    for player_entry in match.data.lineup.home.players:
                        player_dict = self._extract_player_data(
                            player_entry, match.match_id, season_year, "home"
                        )
                        if player_dict:
                            player_data.append(player_dict)

                    # Process away team players
                    for player_entry in match.data.lineup.away.players:
                        player_dict = self._extract_player_data(
                            player_entry, match.match_id, season_year, "away"
                        )
                        if player_dict:
                            player_data.append(player_dict)

        return pd.DataFrame(player_data)

    def _extract_player_data(self, player_entry, match_id, season_year, team_side):
        """Extract player data from lineup entry"""
        player = player_entry.player
        stats = player_entry.statistics

        player_dict = {
            "match_id": match_id,
            "season_year": season_year,
            "team_side": team_side,
            "player_id": player.id,
            "player_name": player.name,
            "position": player_entry.position,
            "shirt_number": player_entry.shirtNumber,
            "is_substitute": player_entry.substitute,
            "is_captain": player_entry.captain or False,
        }

        # Add statistics if available
        if stats:
            player_dict.update(
                {
                    "minutes_played": stats.minutesPlayed,
                    "rating": stats.rating,
                    "goals": stats.goals or 0,
                    "assists": stats.goalAssist or 0,
                    "total_passes": stats.totalPass,
                    "accurate_passes": stats.accuratePass,
                    "touches": stats.touches,
                    "expected_goals": stats.expectedGoals,
                    "expected_assists": stats.expectedAssists,
                    "duels_won": stats.duelWon,
                    "duels_lost": stats.duelLost,
                    "aerials_won": stats.aerialWon,
                    "aerials_lost": stats.aerialLost,
                }
            )

        return player_dict

    def _create_incidents_df(self) -> pd.DataFrame:
        """Create DataFrame with match incidents (goals, cards, subs)"""
        incidents_data = []

        for season_year, season_scraper in self.league_data.items():
            if not season_scraper.data:
                continue

            for match in season_scraper.data.matches:
                if match.success and match.data and match.data.incidents:
                    for incident in match.data.incidents.incidents:
                        # Base incident dictionary with common fields
                        incident_dict = {
                            "match_id": match.match_id,
                            "season_year": season_year,
                            "incident_type": incident.incidentType,
                            "time": getattr(incident, "time", None),
                            "is_home": getattr(incident, "isHome", None),
                        }

                        # Apply type-specific extraction using strategy pattern
                        extractor = INCIDENT_EXTRACTORS.get(incident.incidentType)
                        if extractor:
                            extractor(incident_dict, incident)
                        else:
                            # Log unknown incident types for future implementation
                            logger.warning(
                                f"Warning: Unknown incident type '{incident.incidentType}' encountered"
                            )

                        incidents_data.append(incident_dict)
        return pd.DataFrame(incidents_data)

    # def _create_incidents_df(self) -> pd.DataFrame:
    #     """Create DataFrame with match incidents (goals, cards, subs)"""
    #     incidents_data = []
    #
    #     for season_year, season_scraper in self.league_data.items():
    #         if not season_scraper.data:
    #             continue
    #
    #         for match in season_scraper.data.matches:
    #             if match.success and match.data and match.data.incidents:
    #                 for incident in match.data.incidents.incidents:
    #                     incident_dict = {
    #                         "match_id": match.match_id,
    #                         "season_year": season_year,
    #                         "incident_type": incident.incidentType,
    #                         "time": getattr(incident, "time", None),
    #                         "is_home": getattr(incident, "isHome", None),
    #                     }
    #
    #                     # Add type-specific fields
    #                     if incident.incidentType == "goal":
    #                         incident_dict.update(
    #                             {
    #                                 "player_name": (
    #                                     incident.player.name
    #                                     if incident.player
    #                                     else None
    #                                 ),
    #                                 "assist1_name": (
    #                                     incident.assist1.name
    #                                     if incident.assist1
    #                                     else None
    #                                 ),
    #                                 "home_score": incident.homeScore,
    #                                 "away_score": incident.awayScore,
    #                             }
    #                         )
    #                     elif incident.incidentType == "card":
    #                         incident_dict.update(
    #                             {
    #                                 "player_name": (
    #                                     incident.player.name
    #                                     if incident.player
    #                                     else None
    #                                 ),
    #                                 "card_type": incident.incidentClass,
    #                             }
    #                         )
    #                     elif incident.incidentType == "substitution":
    #                         incident_dict.update(
    #                             {
    #                                 "player_in": incident.playerIn.name,
    #                                 "player_out": incident.playerOut.name,
    #                                 "is_injury": incident.injury,
    #                             }
    #                         )
    #
    #                     incidents_data.append(incident_dict)
    #
    #     return pd.DataFrame(incidents_data)
    #
    def _create_momentum_df(self) -> pd.DataFrame:
        """Create DataFrame with match momentum data"""
        momentum_data = []

        for season_year, season_scraper in self.league_data.items():
            if not season_scraper.data:
                continue

            for match in season_scraper.data.matches:
                if match.success and match.data and match.data.graph:
                    for point in match.data.graph.graphPoints:
                        momentum_dict = {
                            "match_id": match.match_id,
                            "season_year": season_year,
                            "minute": point.minute,
                            "momentum_value": point.value,
                        }
                        momentum_data.append(momentum_dict)

        return pd.DataFrame(momentum_data)

    def save_to_csv(self, output_dir: str = "./output"):
        """Save all DataFrames to CSV files"""
        import os

        os.makedirs(output_dir, exist_ok=True)

        for name, df in self.dataframes.items():
            filepath = os.path.join(output_dir, f"{name}.csv")
            df.to_csv(filepath, index=False)
            logger.info(f"Saved {name} DataFrame to {filepath} ({len(df)} rows)")

    def create_ml_ready_dataset(self) -> pd.DataFrame:
        """Create a single DataFrame ready for ML with engineered features"""
        if "matches" not in self.dataframes:
            self.transform_all()

        # Start with matches as base
        ml_df = self.dataframes["matches"].copy()

        # Add aggregated statistics
        if "match_stats" in self.dataframes:
            # Pivot statistics to wide format
            stats_pivot = self.dataframes["match_stats"][
                self.dataframes["match_stats"]["period"] == "ALL"
            ].pivot_table(
                index="match_id",
                columns="stat_key",
                values=["home_value", "away_value"],
                aggfunc="first",
            )

            # Flatten column names
            stats_pivot.columns = [
                "_".join(col).strip() for col in stats_pivot.columns.values
            ]

            # Merge with main DataFrame
            ml_df = ml_df.merge(
                stats_pivot, left_on="match_id", right_index=True, how="left"
            )

        # Add derived features
        ml_df["goal_difference"] = ml_df["home_score"] - ml_df["away_score"]
        ml_df["total_goals"] = ml_df["home_score"] + ml_df["away_score"]
        ml_df["home_win"] = (ml_df["winner_code"] == 1).astype(int)
        ml_df["draw"] = (ml_df["winner_code"] == 3).astype(int)
        ml_df["away_win"] = (ml_df["winner_code"] == 2).astype(int)

        # Add time-based features
        ml_df["match_hour"] = pd.to_datetime(ml_df["match_date"]).dt.hour
        ml_df["match_dayofweek"] = pd.to_datetime(ml_df["match_date"]).dt.dayofweek
        ml_df["match_month"] = pd.to_datetime(ml_df["match_date"]).dt.month

        return ml_df


# Usage example
def process_league_data(pickle_file: str):
    """Process pickled league data into ML-ready format"""
    # Load the pickle
    pu = ProcessingUtils(type=FootballLeague.SCOTLAND, web_on=False)
    league_data = pu.load_pickle(file_name=pickle_file)

    # Transform to DataFrames
    transformer = FootballDataTransformer(league_data)
    dataframes = transformer.transform_all()

    # Save to CSV
    transformer.save_to_csv("./football_data_output")

    # Create ML-ready dataset
    ml_dataset = transformer.create_ml_ready_dataset()
    ml_dataset.to_csv("./football_data_output/ml_ready_dataset.csv", index=False)

    # Print summary
    print("\nData Transformation Summary:")
    for name, df in dataframes.items():
        print(f"{name}: {len(df)} rows, {len(df.columns)} columns")

    return transformer


if __name__ == "__main__":
    # Process your scraped data
    transformer = process_league_data("scot_pl_example")

    # Example: Get specific DataFrames for analysis
    matches_df = transformer.dataframes["matches"]
    stats_df = transformer.dataframes["match_stats"]

    # Example: Filter for specific analysis
    recent_matches = matches_df[matches_df["season_year"] == "23/24"]
    print(f"\nRecent season matches: {len(recent_matches)}")
