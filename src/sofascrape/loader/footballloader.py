"""
Example uses:

loader = FootballLoader()

# Load specific tournaments (all their seasons)
data = loader.load_tournaments({54, 23})

# Load specific tournament/season combinations
data = loader.load_tournament_seasons({
    54: {62408, 62409},  # Tournament 54, seasons 62408 & 62409
    23: {55123}          # Tournament 23, season 55123
})

# Check what's available first
available = loader.discover_available_data()
# Returns: {54: {62408, 62409, 62410}, 23: {55123, 55124}}

# Handle errors
if loader.get_load_errors():
    print("Some files failed to load:", loader.get_load_errors())


"""

import logging
import pickle
from pathlib import Path
from typing import Dict, List, Optional, Set

from omegaconf import DictConfig, OmegaConf

from sofascrape.schemas import general as sofaschema

logger = logging.getLogger(__name__)


class FootballLoader:
    """Handles the loading of Golden data sets from the data dir,
    data/
    ├── tournament_54/
    │   ├── season_62408/
    │   │   ├── golden/
    │   │   │   └── golden_data.pkl
    """

    def __init__(self) -> None:
        self.config: DictConfig = self._get_config()
        self._set_base_dir()
        self.load_errors: List[str] = []  # Track errors during loading

    def _set_base_dir(self) -> None:
        """Set the data dir location"""
        try:
            self.base_dir: Path = Path(self.config.storage.base_dir)
            if not self.base_dir.exists():
                logger.warning(f"Base directory {self.base_dir} does not exist")
        except Exception as e:
            logger.error(f"Error setting base directory: {str(e)}")
            raise

    def _get_config(self) -> DictConfig:
        config_path: Path = (Path(__file__).parent) / "footballDataConfig.yaml"
        return OmegaConf.load(config_path)  # type: ignore[return-value]

    def _load_golden_data(
        self, file_path: Path
    ) -> Optional[Dict[int, sofaschema.FootballMatchResultDetailed]]:
        """Load golden data from pickle file. Returns None if loading fails."""
        try:
            with open(file_path, "rb") as f:
                data = pickle.load(f)

            if not isinstance(data, Dict):
                raise TypeError(f"Loaded data is not a dict, got {type(data)}")
            return data

        except (FileNotFoundError, pickle.UnpicklingError, TypeError) as e:
            error_msg = f"Failed to load {file_path}: {str(e)}"
            logger.error(error_msg)
            self.load_errors.append(error_msg)
            return None
        except Exception as e:
            error_msg = f"Unexpected error loading {file_path}: {str(e)}"
            logger.error(error_msg)
            self.load_errors.append(error_msg)
            return None

    def _get_tournament_paths(
        self, tournament_ids: Optional[Set[int]] = None
    ) -> List[Path]:
        """Get tournament paths, optionally filtered by tournament IDs"""
        if not self.base_dir.exists():
            return []

        all_tournament_paths = [
            path
            for path in self.base_dir.iterdir()
            if path.is_dir()
            and path.name.startswith(self.config.file_formats.tournament)
        ]

        if tournament_ids is None:
            return all_tournament_paths

        return [
            tournament_path
            for tournament_path in all_tournament_paths
            if self._extract_tournament_id(tournament_path) in tournament_ids
        ]

    def _get_season_paths(
        self, tournament_path: Path, season_ids: Optional[Set[int]] = None
    ) -> List[Path]:
        """Get season paths within a tournament, optionally filtered by season IDs"""
        if not tournament_path.exists():
            return []

        all_season_paths = [
            season_path
            for season_path in tournament_path.iterdir()
            if season_path.is_dir()
            and season_path.name.startswith(self.config.file_formats.season)
        ]

        if season_ids is None:
            return all_season_paths

        return [
            season_path
            for season_path in all_season_paths
            if self._extract_season_id(season_path) in season_ids
        ]

    def _extract_tournament_id(self, tournament_path: Path) -> int:
        """Extract tournament ID from path name (tournament_123 -> 123)"""
        return int(tournament_path.name.split("_")[-1])

    def _extract_season_id(self, season_path: Path) -> int:
        """Extract season ID from path name (season_456 -> 456)"""
        return int(season_path.name.split("_")[-1])

    def _get_golden_data_path(self, season_path: Path) -> Path:
        """Get the path to golden data file for a season"""
        return (
            season_path
            / self.config.storage.golden_subdir
            / self.config.file_formats.golden
        )

    def _has_golden_data(self, season_path: Path) -> bool:
        """Check if season has golden data file"""
        golden_path = self._get_golden_data_path(season_path)
        return golden_path.exists() and golden_path.is_file()

    def clear_errors(self) -> None:
        """Clear the error log"""
        self.load_errors.clear()

    def get_load_errors(self) -> List[str]:
        """Get list of errors that occurred during loading"""
        return self.load_errors.copy()

    def discover_available_data(self) -> Dict[int, Set[int]]:
        """Discover all available tournament/season combinations with golden data"""
        available_data = {}

        for tournament_path in self._get_tournament_paths():
            tournament_id = self._extract_tournament_id(tournament_path)

            season_ids = set()
            for season_path in self._get_season_paths(tournament_path):
                if self._has_golden_data(season_path):
                    season_id = self._extract_season_id(season_path)
                    season_ids.add(season_id)

            if season_ids:  # Only include tournaments that have valid seasons
                available_data[tournament_id] = season_ids

        return available_data

    def load_all_tournaments(
        self,
    ) -> Dict[int, Dict[int, Dict[int, sofaschema.FootballMatchResultDetailed]]]:
        """Load all available tournaments and their seasons"""
        self.clear_errors()
        result = {}

        for tournament_path in self._get_tournament_paths():
            tournament_id = self._extract_tournament_id(tournament_path)
            tournament_data = {}

            for season_path in self._get_season_paths(tournament_path):
                if self._has_golden_data(season_path):
                    season_id = self._extract_season_id(season_path)
                    golden_path = self._get_golden_data_path(season_path)

                    data = self._load_golden_data(golden_path)
                    if data is not None:
                        tournament_data[season_id] = data

            if tournament_data:  # Only include if we loaded some data
                result[tournament_id] = tournament_data

        logger.info(
            f"Loaded {len(result)} tournaments with {sum(len(seasons) for seasons in result.values())} seasons total"
        )
        if self.load_errors:
            logger.warning(f"Encountered {len(self.load_errors)} errors during loading")

        return result

    def load_tournaments(
        self, tournament_ids: Set[int]
    ) -> Dict[int, Dict[int, Dict[int, sofaschema.FootballMatchResultDetailed]]]:
        """Load specific tournaments and all their available seasons"""
        self.clear_errors()
        result = {}

        for tournament_path in self._get_tournament_paths(tournament_ids):
            tournament_id = self._extract_tournament_id(tournament_path)
            tournament_data = {}

            for season_path in self._get_season_paths(tournament_path):
                if self._has_golden_data(season_path):
                    season_id = self._extract_season_id(season_path)
                    golden_path = self._get_golden_data_path(season_path)

                    data = self._load_golden_data(golden_path)
                    if data is not None:
                        tournament_data[season_id] = data

            if tournament_data:
                result[tournament_id] = tournament_data

        logger.info(
            f"Loaded {len(result)} tournaments with {sum(len(seasons) for seasons in result.values())} seasons total"
        )
        if self.load_errors:
            logger.warning(f"Encountered {len(self.load_errors)} errors during loading")

        return result

    def load_tournament_seasons(
        self, tournament_seasons: Dict[int, Set[int]]
    ) -> Dict[int, Dict[int, Dict[int, sofaschema.FootballMatchResultDetailed]]]:
        """Load specific tournament/season combinations

        Args:
            tournament_seasons: Dict mapping tournament_id -> set of season_ids

        Returns:
            Dict[tournament_id, Dict[season_id, match_data]]
        """
        self.clear_errors()
        result = {}

        for tournament_id, season_ids in tournament_seasons.items():
            tournament_paths = self._get_tournament_paths({tournament_id})

            if not tournament_paths:
                error_msg = f"Tournament {tournament_id} not found"
                logger.error(error_msg)
                self.load_errors.append(error_msg)
                continue

            tournament_path = tournament_paths[0]
            tournament_data = {}

            for season_path in self._get_season_paths(tournament_path, season_ids):
                if self._has_golden_data(season_path):
                    season_id = self._extract_season_id(season_path)
                    golden_path = self._get_golden_data_path(season_path)

                    data = self._load_golden_data(golden_path)
                    if data is not None:
                        tournament_data[season_id] = data
                else:
                    season_id = self._extract_season_id(season_path)
                    error_msg = f"No golden data found for tournament {tournament_id}, season {season_id}"
                    logger.error(error_msg)
                    self.load_errors.append(error_msg)

            # Check for requested seasons that weren't found
            loaded_season_ids = set(tournament_data.keys())
            missing_seasons = season_ids - loaded_season_ids
            for missing_season in missing_seasons:
                error_msg = (
                    f"Season {missing_season} not found in tournament {tournament_id}"
                )
                logger.error(error_msg)
                self.load_errors.append(error_msg)

            if tournament_data:
                result[tournament_id] = tournament_data

        logger.info(
            f"Loaded {len(result)} tournaments with {sum(len(seasons) for seasons in result.values())} seasons total"
        )
        if self.load_errors:
            logger.warning(f"Encountered {len(self.load_errors)} errors during loading")

        return result
