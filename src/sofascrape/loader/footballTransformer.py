import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from omegaconf import DictConfig, OmegaConf

import sofascrape.schemas.general as sofaschema

logger = logging.getLogger(__name__)


class FootballDataTransformer:
    """Transform nested football data into pandas DataFrames with configurable processing"""

    def __init__(self, config_path: Optional[Path] = None):
        """Initialize transformer with configuration

        Args:
            config_path: Path to transformation config YAML, defaults to transformationConfig.yaml
        """
        self.config = self._load_config(config_path)
        self.dataframes: Dict[str, pd.DataFrame] = {}
        self.transformation_errors: List[str] = []

    def _load_config(self, config_path: Optional[Path]) -> DictConfig:
        """Load transformation configuration"""
        if config_path is None:
            config_path = Path(__file__).parent / "transformationConfig.yaml"

        if not config_path.exists():
            logger.warning(f"Config file {config_path} not found, using defaults")
            raise FileExistsError("No config found.")

        return OmegaConf.load(config_path)  # type: ignore[return-value]

    def clear_errors(self) -> None:
        """Clear transformation error log"""
        self.transformation_errors.clear()

    def get_transformation_errors(self) -> List[str]:
        """Get list of transformation errors"""
        return self.transformation_errors.copy()

    def transform(
        self,
        loaded_data: Dict[
            int, Dict[int, Dict[int, sofaschema.FootballMatchResultDetailed]]
        ],
    ) -> Dict[str, pd.DataFrame]:
        """Main transformation method
        Args:
            loaded_data: Data from FootballLoader in format Dict[tournament_id, Dict[season_id, Dict[match_id, match_data]]]
        Returns:
            Dictionary of DataFrames by data type
        """
        self.clear_errors()
        self.dataframes.clear()

        logger.info("Starting football data transformation")

        # Log input data size
        total_tournaments = len(loaded_data)
        total_seasons = sum(
            len(tournament_data) for tournament_data in loaded_data.values()
        )
        total_matches = sum(
            len(season_data)
            for tournament_data in loaded_data.values()
            for season_data in tournament_data.values()
        )

        logger.info(
            f"Input data size: {total_tournaments} tournaments, {total_seasons} seasons, {total_matches} matches"
        )

        # Log detailed breakdown per tournament
        for tournament_id, tournament_data in loaded_data.items():
            tournament_seasons = len(tournament_data)
            tournament_matches = sum(
                len(season_data) for season_data in tournament_data.values()
            )
            logger.debug(
                f"Tournament {tournament_id}: {tournament_seasons} seasons, {tournament_matches} matches"
            )

            # Optional: Log per season if you want even more detail
            if logger.isEnabledFor(logging.DEBUG):
                for season_id, season_data in tournament_data.items():
                    season_matches = len(season_data)
                    logger.debug(f"  Season {season_id}: {season_matches} matches")

        # Get configured data types to process
        data_types = self.config.data_extraction.include_data_types
        logger.info(f"Will create DataFrames for: {data_types}")

        # Get configured data types to process
        data_types = self.config.data_extraction.include_data_types

        if "matches" in data_types:
            self.dataframes["matches"] = self._create_matches_df(loaded_data)

        if "match_stats" in data_types:
            self.dataframes["match_stats"] = self._create_match_stats_df(loaded_data)

        if "player_stats" in data_types:
            self.dataframes["player_stats"] = self._create_player_stats_df(loaded_data)

        if "incidents" in data_types:
            self.dataframes["incidents"] = self._create_incidents_df(loaded_data)

        if "momentum" in data_types:
            self.dataframes["momentum"] = self._create_momentum_df(loaded_data)

        # Validate output
        self._validate_output()

        logger.info(
            f"Transformation complete. Created {len(self.dataframes)} DataFrames"
        )
        if self.transformation_errors:
            logger.warning(
                f"Encountered {len(self.transformation_errors)} errors during transformation"
            )

        return self.dataframes

    def _apply_column_mapping(self, column_name: str) -> str:
        """Apply configured column name transformations"""
        # Apply direct field mappings first
        mappings = self.config.column_mapping.field_mappings
        if column_name in mappings:
            column_name = mappings[column_name]

        # Apply naming rules
        rules = self.config.column_mapping.naming_rules

        if rules.use_snake_case:
            # Convert camelCase to snake_case
            column_name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", column_name).lower()

        # Remove configured prefixes
        for prefix in rules.remove_prefixes:
            if column_name.startswith(prefix):
                column_name = column_name[len(prefix) :]

        # Truncate if too long
        if len(column_name) > rules.max_column_length:
            column_name = column_name[: rules.max_column_length]

        return column_name

    def _format_datetime(self, timestamp: int) -> Any:
        """Format timestamp according to config"""
        dt_config = self.config.data_formatting.datetime_processing

        try:
            dt = datetime.fromtimestamp(timestamp)

            if dt_config.output_format == "datetime":
                return dt
            else:
                return dt.strftime(dt_config.get("custom_format", "%Y-%m-%d %H:%M:%S"))
        except (ValueError, TypeError) as e:
            error_msg = f"Invalid timestamp {timestamp}: {str(e)}"
            logger.error(error_msg)
            self.transformation_errors.append(error_msg)
            return None

    def _process_stat_value(self, value: Any) -> Any:
        """Process statistic values according to config"""
        if value is None:
            null_handling = (
                self.config.stats_processing.value_processing.null_stat_handling
            )
            if null_handling == "zero":
                return 0
            elif null_handling == "null":
                return None
            else:
                return value

        # Handle percentage conversion
        if (
            isinstance(value, str)
            and self.config.stats_processing.value_processing.convert_percentages
        ):
            if value.endswith("%"):
                try:
                    numeric_value = float(value.replace("%", ""))
                    if (
                        self.config.stats_processing.value_processing.percentage_to_decimal
                    ):
                        return numeric_value / 100.0
                    else:
                        return numeric_value
                except ValueError:
                    logger.warning(f"Could not convert percentage value: {value}")
                    return value

        # Try to convert strings to numbers if configured
        if (
            isinstance(value, str)
            and self.config.data_formatting.numeric_formatting.convert_strings
        ):
            try:
                # Try int first, then float
                if "." not in value:
                    return int(value)
                else:
                    return float(value)
            except ValueError:
                pass  # Keep as string

        return value

    def _create_matches_df(self, loaded_data: Dict) -> pd.DataFrame:
        """Create matches DataFrame with basic match information"""
        matches_data = []

        for tournament_id, tournament_data in loaded_data.items():
            for season_id, season_data in tournament_data.items():
                for match_id, match_data in season_data.items():
                    try:
                        if not match_data.base or not match_data.base.event:
                            continue

                        event = match_data.base.event

                        match_dict = {
                            # Identifiers
                            "tournament_id": tournament_id,
                            "season_id": season_id,
                            "match_id": match_id,
                            "tournament_slug": (
                                event.tournament.slug if event.tournament else None
                            ),
                            # Teams
                            "home_team": (
                                event.homeTeam.name if event.homeTeam else None
                            ),
                            "home_team_slug": (
                                event.homeTeam.slug if event.homeTeam else None
                            ),
                            "away_team": (
                                event.awayTeam.name if event.awayTeam else None
                            ),
                            "away_team_slug": (
                                event.awayTeam.slug if event.awayTeam else None
                            ),
                            # Scores
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
                            "match_date": self._format_datetime(event.startTimestamp),
                            "round": event.roundInfo.round if event.roundInfo else None,
                            "status": (
                                event.status.description if event.status else None
                            ),
                            "winner_code": (
                                event.winnerCode
                                if hasattr(event, "winnerCode")
                                else None
                            ),
                            # Venue
                            "venue": event.venue.name if event.venue else None,
                            "attendance": getattr(event, "attendance", None),
                            # Features
                            "has_xg": getattr(event, "hasXg", False),
                            "has_stats": getattr(
                                event, "hasEventPlayerStatistics", False
                            ),
                        }

                        # Add time-based features if configured
                        if (
                            self.config.data_formatting.datetime_processing.add_derived_time
                            and match_dict["match_date"] is not None
                        ):
                            dt = match_dict["match_date"]
                            if isinstance(dt, datetime):
                                match_dict["match_hour"] = dt.hour
                                match_dict["match_dayofweek"] = dt.weekday()
                                match_dict["match_month"] = dt.month

                        # Apply column name mapping
                        match_dict = {
                            self._apply_column_mapping(k): v
                            for k, v in match_dict.items()
                        }
                        matches_data.append(match_dict)

                    except Exception as e:
                        error_msg = f"Error processing match {match_id}: {str(e)}"
                        logger.error(error_msg)
                        self.transformation_errors.append(error_msg)
                        if (
                            not self.config.data_quality.error_handling.continue_on_error
                        ):
                            raise

        return pd.DataFrame(matches_data)

    def _create_match_stats_df(self, loaded_data: Dict) -> pd.DataFrame:
        """Create match statistics DataFrame"""
        stats_data = []

        configured_periods = self.config.data_extraction.stats_periods
        exclude_groups = self.config.stats_processing.stat_groups.exclude_groups
        include_groups = self.config.stats_processing.stat_groups.include_groups

        for tournament_id, tournament_data in loaded_data.items():
            for season_id, season_data in tournament_data.items():
                for match_id, match_data in season_data.items():
                    try:
                        if not match_data.stats or not match_data.stats.statistics:
                            continue

                        for period in match_data.stats.statistics:
                            # Filter periods based on config
                            if (
                                configured_periods
                                and period.period not in configured_periods
                            ):
                                continue

                            for group in period.groups:
                                # Apply group filtering
                                if exclude_groups and group.groupName in exclude_groups:
                                    continue
                                if (
                                    include_groups
                                    and group.groupName not in include_groups
                                ):
                                    continue

                                for stat in group.statisticsItems:
                                    stat_dict = {
                                        "tournament_id": tournament_id,
                                        "season_id": season_id,
                                        "match_id": match_id,
                                        "period": period.period,
                                        "group_name": group.groupName,
                                        "stat_key": stat.key,
                                        "stat_name": stat.name,
                                        "home_value": self._process_stat_value(
                                            stat.homeValue
                                        ),
                                        "away_value": self._process_stat_value(
                                            stat.awayValue
                                        ),
                                        "stat_type": getattr(
                                            stat, "statisticsType", None
                                        ),
                                        "value_type": getattr(stat, "valueType", None),
                                    }

                                    # Apply column name mapping
                                    stat_dict = {
                                        self._apply_column_mapping(k): v
                                        for k, v in stat_dict.items()
                                    }
                                    stats_data.append(stat_dict)

                    except Exception as e:
                        error_msg = (
                            f"Error processing stats for match {match_id}: {str(e)}"
                        )
                        logger.error(error_msg)
                        self.transformation_errors.append(error_msg)
                        if (
                            not self.config.data_quality.error_handling.continue_on_error
                        ):
                            raise

        # Create initial DataFrame
        stats_df = pd.DataFrame(stats_data)

        if stats_df.empty:
            return stats_df

        # Apply pivot transformation
        return self._pivot_match_stats(stats_df)

    def _pivot_match_stats(self, stats_df: pd.DataFrame) -> pd.DataFrame:
        """Pivot match statistics from long to wide format based on config"""
        if stats_df.empty:
            return stats_df

        pivot_config = self.config.stats_processing.pivot_settings

        # Create stat identifier based on config
        if pivot_config.simplify_names:
            stats_df["stat_identifier"] = stats_df["stat_key"]
        else:
            stats_df["stat_identifier"] = (
                stats_df["group_name"].str.replace(" ", "_").str.lower()
                + "_"
                + stats_df["stat_key"]
            )

        # Pivot the data
        try:
            pivoted = stats_df.pivot_table(
                index=["match_id", "tournament_id", "season_id", "period"],
                columns="stat_identifier",
                values=["home_value", "away_value"],
                aggfunc="first",
            )

            # Flatten multi-level columns
            pivoted.columns = ["_".join(col).strip() for col in pivoted.columns.values]

            # Reset index
            result = pivoted.reset_index()

            # Reorganize columns if configured
            if pivot_config.reorganize_pairs:
                result = self._reorganize_stat_columns(result)

            return result

        except Exception as e:
            error_msg = f"Error pivoting match stats: {str(e)}"
            logger.error(error_msg)
            self.transformation_errors.append(error_msg)
            return stats_df

    def _reorganize_stat_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorganize columns to group home/away pairs together"""
        id_cols = ["match_id", "tournament_id", "season_id", "period"]
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

    def _create_player_stats_df(self, loaded_data: Dict) -> pd.DataFrame:
        """Create player statistics DataFrame"""
        player_data = []

        player_config = self.config.data_extraction.player_filters

        for tournament_id, tournament_data in loaded_data.items():
            for season_id, season_data in tournament_data.items():
                for match_id, match_data in season_data.items():
                    try:
                        if not match_data.lineup:
                            continue

                        # Process home team players
                        if match_data.lineup.home and match_data.lineup.home.players:
                            for player_entry in match_data.lineup.home.players:
                                player_dict = self._extract_player_data(
                                    player_entry,
                                    tournament_id,
                                    season_id,
                                    match_id,
                                    "home",
                                    player_config,
                                )
                                if player_dict:
                                    player_data.append(player_dict)

                        # Process away team players
                        if match_data.lineup.away and match_data.lineup.away.players:
                            for player_entry in match_data.lineup.away.players:
                                player_dict = self._extract_player_data(
                                    player_entry,
                                    tournament_id,
                                    season_id,
                                    match_id,
                                    "away",
                                    player_config,
                                )
                                if player_dict:
                                    player_data.append(player_dict)

                    except Exception as e:
                        error_msg = f"Error processing player stats for match {match_id}: {str(e)}"
                        logger.error(error_msg)
                        self.transformation_errors.append(error_msg)
                        if (
                            not self.config.data_quality.error_handling.continue_on_error
                        ):
                            raise

        return pd.DataFrame(player_data)

    def _extract_player_data(
        self, player_entry, tournament_id, season_id, match_id, team_side, player_config
    ):
        """Extract individual player data with config-based filtering"""
        if not player_entry.player:
            return None

        # Apply player filters
        if not player_config.include_substitutes and player_entry.substitute:
            return None

        stats = player_entry.statistics
        minutes_played = stats.minutesPlayed if stats else 0

        if minutes_played < player_config.min_minutes_played:
            return None

        if not player_config.include_dnp and minutes_played == 0:
            return None

        player = player_entry.player

        player_dict = {
            "tournament_id": tournament_id,
            "season_id": season_id,
            "match_id": match_id,
            "team_side": team_side,
            "player_id": player.id,
            "player_name": player.name,
            "position": player_entry.position,
            "shirt_number": player_entry.shirtNumber,
            "is_substitute": player_entry.substitute,
            "is_captain": getattr(player_entry, "captain", False),
        }

        # Add statistics if available
        if stats:
            player_dict.update(
                {
                    "minutes_played": stats.minutesPlayed,
                    "rating": stats.rating,
                    "goals": stats.goals or 0,
                    "assists": getattr(stats, "goalAssist", 0),
                    "total_passes": getattr(stats, "totalPass", None),
                    "accurate_passes": getattr(stats, "accuratePass", None),
                    "touches": getattr(stats, "touches", None),
                    "expected_goals": getattr(stats, "expectedGoals", None),
                    "expected_assists": getattr(stats, "expectedAssists", None),
                    "duels_won": getattr(stats, "duelWon", None),
                    "duels_lost": getattr(stats, "duelLost", None),
                    "aerials_won": getattr(stats, "aerialWon", None),
                    "aerials_lost": getattr(stats, "aerialLost", None),
                }
            )

        # Apply column name mapping
        player_dict = {self._apply_column_mapping(k): v for k, v in player_dict.items()}
        return player_dict

    def _create_incidents_df(self, loaded_data: Dict) -> pd.DataFrame:
        """Create match incidents DataFrame (goals, cards, substitutions)"""
        incidents_data = []

        for tournament_id, tournament_data in loaded_data.items():
            for season_id, season_data in tournament_data.items():
                for match_id, match_data in season_data.items():
                    try:
                        if (
                            not match_data.incidents
                            or not match_data.incidents.incidents
                        ):
                            continue

                        for incident in match_data.incidents.incidents:
                            incident_dict = {
                                "tournament_id": tournament_id,
                                "season_id": season_id,
                                "match_id": match_id,
                                "incident_type": incident.incidentType,
                                "time": getattr(incident, "time", None),
                                "is_home": getattr(incident, "isHome", None),
                            }

                            # Add type-specific fields
                            if incident.incidentType == "goal":
                                incident_dict.update(
                                    {
                                        "player_name": (
                                            incident.player.name
                                            if incident.player
                                            else None
                                        ),
                                        "assist1_name": (
                                            incident.assist1.name
                                            if getattr(incident, "assist1", None)
                                            else None
                                        ),
                                        "home_score": getattr(
                                            incident, "homeScore", None
                                        ),
                                        "away_score": getattr(
                                            incident, "awayScore", None
                                        ),
                                    }
                                )
                            elif incident.incidentType == "card":
                                incident_dict.update(
                                    {
                                        "player_name": (
                                            incident.player.name
                                            if incident.player
                                            else None
                                        ),
                                        "card_type": getattr(
                                            incident, "incidentClass", None
                                        ),
                                    }
                                )
                            elif incident.incidentType == "substitution":
                                incident_dict.update(
                                    {
                                        "player_in": (
                                            incident.playerIn.name
                                            if getattr(incident, "playerIn", None)
                                            else None
                                        ),
                                        "player_out": (
                                            incident.playerOut.name
                                            if getattr(incident, "playerOut", None)
                                            else None
                                        ),
                                        "is_injury": getattr(incident, "injury", False),
                                    }
                                )

                            # Apply column name mapping
                            incident_dict = {
                                self._apply_column_mapping(k): v
                                for k, v in incident_dict.items()
                            }
                            incidents_data.append(incident_dict)

                    except Exception as e:
                        error_msg = (
                            f"Error processing incidents for match {match_id}: {str(e)}"
                        )
                        logger.error(error_msg)
                        self.transformation_errors.append(error_msg)
                        if (
                            not self.config.data_quality.error_handling.continue_on_error
                        ):
                            raise

        return pd.DataFrame(incidents_data)

    def _create_momentum_df(self, loaded_data: Dict) -> pd.DataFrame:
        """Create match momentum DataFrame"""
        momentum_data = []

        for tournament_id, tournament_data in loaded_data.items():
            for season_id, season_data in tournament_data.items():
                for match_id, match_data in season_data.items():
                    try:
                        if not match_data.graph or not match_data.graph.graphPoints:
                            continue

                        for point in match_data.graph.graphPoints:
                            momentum_dict = {
                                "tournament_id": tournament_id,
                                "season_id": season_id,
                                "match_id": match_id,
                                "minute": point.minute,
                                "momentum_value": point.value,
                            }

                            # Apply column name mapping
                            momentum_dict = {
                                self._apply_column_mapping(k): v
                                for k, v in momentum_dict.items()
                            }
                            momentum_data.append(momentum_dict)

                    except Exception as e:
                        error_msg = (
                            f"Error processing momentum for match {match_id}: {str(e)}"
                        )
                        logger.error(error_msg)
                        self.transformation_errors.append(error_msg)
                        if (
                            not self.config.data_quality.error_handling.continue_on_error
                        ):
                            raise

        return pd.DataFrame(momentum_data)

    def _validate_output(self) -> None:
        """Validate generated DataFrames against configuration rules"""
        validation_config = self.config.data_quality.output_validation

        for df_name, df in self.dataframes.items():
            # Check minimum rows
            if len(df) < validation_config.min_rows_per_dataframe:
                error_msg = f"DataFrame {df_name} has only {len(df)} rows, minimum is {validation_config.min_rows_per_dataframe}"
                logger.warning(error_msg)
                self.transformation_errors.append(error_msg)

            # Check null percentage
            if not df.empty:
                null_pct = (df.isnull().sum() / len(df) * 100).max()
                if null_pct > validation_config.max_null_percentage:
                    error_msg = f"DataFrame {df_name} has {null_pct:.1f}% nulls, maximum allowed is {validation_config.max_null_percentage}%"
                    logger.warning(error_msg)
                    self.transformation_errors.append(error_msg)

            # Check required columns
            required_columns = validation_config.required_columns.get(df_name, [])
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                error_msg = (
                    f"DataFrame {df_name} missing required columns: {missing_columns}"
                )
                logger.error(error_msg)
                self.transformation_errors.append(error_msg)

    def save_to_csv(self, output_dir: Optional[str] = None) -> Dict[str, str]:
        """Save all DataFrames to CSV files

        Returns:
            Dictionary mapping dataframe names to file paths
        """
        if not self.dataframes:
            logger.warning("No DataFrames to save. Run transform() first.")
            return {}

        # Use configured output directory if not provided
        if output_dir is None:
            output_dir = self.config.output.directory_structure.base_dir

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        saved_files = {}

        for df_name, df in self.dataframes.items():
            try:
                # Generate filename based on config
                filename = self._generate_filename(df_name)
                filepath = output_dir / filename

                # Save with configured CSV options
                csv_config = self.config.output.csv_options
                df.to_csv(
                    filepath,
                    sep=csv_config.separator,
                    encoding=csv_config.encoding,
                    index=csv_config.index,
                    date_format=csv_config.date_format,
                    na_rep=csv_config.na_rep,
                )

                saved_files[df_name] = str(filepath)
                logger.info(f"Saved {df_name} DataFrame to {filepath} ({len(df)} rows)")

            except Exception as e:
                error_msg = f"Failed to save {df_name}: {str(e)}"
                logger.error(error_msg)
                self.transformation_errors.append(error_msg)

        return saved_files

    def _generate_filename(self, dataframe_name: str) -> str:
        """Generate filename based on configuration"""
        file_config = self.config.output.file_naming

        # Start with base pattern
        filename = file_config.pattern.format(
            prefix=file_config.prefix,
            datatype=dataframe_name,
            tournament="mixed",  # Default for multi-tournament data
        )

        # Add timestamp if configured
        if file_config.include_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            name_parts = filename.rsplit(".", 1)
            if len(name_parts) == 2:
                filename = f"{name_parts[0]}_{timestamp}.{name_parts[1]}"
            else:
                filename = f"{filename}_{timestamp}"

        return filename
