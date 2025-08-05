import logging
import pickle
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from omegaconf import DictConfig, OmegaConf

logger = logging.getLogger(__name__)


class StorageHandler:
    """Handles saving and loading of the scraping runs and analysis.
    Example data structure:
        data/
        ├── tournament_54/
        │   ├── season_62408/
        │   │   ├── runs/
        │   │   │   ├── run_week1_20250805.pkl
        │   │   │   ├── run_week2_20250812.pkl
        │   │   │   └── retry_stats_20250815.pkl
        │   │   ├── analysis/
        │   │   │   ├── consensus_20250815.pkl
        │   │   │   └── review_report_20250815.html
        │   │   ├── golden/
        │   │   │   └── golden_dataset_20250815.pkl
        │   │   └── logs/
        │   │       ├── quality_analysis_20250815.log
        │   │       └── retry_20250815.log

    """

    def __init__(
        self, tournament_id: int, season_id: int, config: Optional[DictConfig] = None
    ) -> None:
        logger.debug(f"Init StorageHandler for {tournament_id = }, {season_id =}")

        if config is not None:
            self.config: DictConfig = config
        else:
            self.config: DictConfig = self._get_config()

        self.tournament_id: int = tournament_id
        self.season_id: int = season_id
        self._setup_directories()

        #

    def _get_config(self) -> DictConfig:
        config_path = (Path(__file__).parent.parent) / "config" / "quality_config.yaml"
        return OmegaConf.load(config_path)

    def _set_base_dir(self, home: Path) -> None:
        """Set the data dir location"""
        try:
            self.base_dir: Path = home / self.config.storage.base_dir
            self.base_dir.mkdir(parents=False, exist_ok=True)
            logger.info(f"Base directory {self.base_dir} created or already exists.")
        except PermissionError:
            logger.error(f"Permission denied creating {self.base_dir}.")
        except Exception as e:
            logger.error(f"Error with directory {self.base_dir}: {str(e)}.")

    def _set_tournament_dir(self) -> None:
        """Set the data dir location"""
        try:
            path_str = self.config.storage.file_formats.tournament.format(
                tournament_id=self.tournament_id
            )
            self.dir_tournament: Path = self.base_dir / path_str
            if not self.dir_tournament.exists():
                logger.info(
                    f"No tournament directory {self.tournament_id} creating one @ {self.dir_tournament}"
                )
                self.dir_tournament.mkdir(parents=False, exist_ok=True)

        except PermissionError:
            logger.error(f"Permission denied creating {self.dir_tournament}.")
        except Exception as e:
            logger.error(f"Error with directory {self.tournament_id}: {str(e)}.")

    def _set_season_dir(self) -> None:
        """Set the data dir location"""
        try:
            path_str = self.config.storage.file_formats.season.format(
                season_id=self.season_id
            )
            self.dir_season: Path = self.dir_tournament / path_str
            logger.debug(self.dir_season)
            if not self.dir_season.exists():
                logger.info(
                    f"No season directory {self.season_id} creating one @ {self.dir_season}"
                )
                self.dir_season.mkdir(parents=False, exist_ok=True)

        except PermissionError:
            logger.error(f"Permission denied creating {self.dir_season}.")
        except Exception as e:
            logger.error(f"Error with directory {self.season_id}: {str(e)}.")

    def _set_sub_dir(self) -> None:
        try:
            self.dir_run: Path = self.dir_season / self.config.storage.runs_subdir
            self.dir_analysis: Path = (
                self.dir_season / self.config.storage.analysis_subdir
            )
            self.dir_golden: Path = self.dir_season / self.config.storage.golden_subdir
            self.dir_logs: Path = self.dir_season / self.config.storage.logs_subdir

            self.dir_run.mkdir(parents=False, exist_ok=True)
            self.dir_analysis.mkdir(parents=False, exist_ok=True)
            self.dir_golden.mkdir(parents=False, exist_ok=True)
            self.dir_logs.mkdir(parents=False, exist_ok=True)

        except PermissionError:
            logger.error("Permission denied creating subdirs")
        except Exception as e:
            logger.error(f"Error with sub directory {self.season_id}: {str(e)}.")

    def _setup_directories(self) -> None:
        """
        Create the diretory structure for the given tournament_id and season_id.
        Checks for the existances, else will create in data.
        """

        self._set_base_dir(home=Path(__file__).parent.parent.parent.parent.parent)
        self._set_tournament_dir()
        self._set_season_dir()
        self._set_sub_dir()

    ###

    def _get_runs(self) -> List[Path]:
        """
        Collect all the names of the runs.
        """
        return list(self.dir_run.iterdir())

    def _next_run_number(self) -> int:
        """
        Get the next run number - checks what there is and adds.
        """
        runs = [int(str(run_path.name)[0]) for run_path in self._get_runs()]
        if runs:
            return sorted(runs)[-1] + 1
        return 1

    def save_scraping_run(self, run_data: Any) -> None:
        """
        Save a scrape with a name iterated.
        """
