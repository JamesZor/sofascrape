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

        logger.debug("Init completed, dir set up.")

    def _get_config(self) -> DictConfig:
        config_path = (Path(__file__).parent.parent) / "config" / "quality_config.yaml"
        return OmegaConf.load(config_path)

    def _set_base_dir(self, home: Path) -> None:
        """Set the data dir location"""
        try:
            self.base_dir: Path = home / self.config.storage.base_dir
            self.base_dir.mkdir(parents=False, exist_ok=True)
            logger.debug(f"Base directory {self.base_dir} created or already exists.")
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

    def _list_run_files(self) -> List[str]:
        """
        Collect all the names of the runs.
        """
        return [path.name for path in list(self.dir_run.iterdir())]

    def _list_available_runs_numbers(self) -> List[int]:
        """Get the list of runs saved in ../run"""
        return [int(str(run_path).split("_")[0]) for run_path in self._list_run_files()]

    def _next_run_number(self) -> int:
        """
        Get the next run number - checks what there is and adds.
        """
        runs = self._list_available_runs_numbers()
        if runs:
            return sorted(runs)[-1] + 1
        return 1

    def save_scraping_run(self, run_data: Any) -> None:
        """
        Save a scrape with a name iterated.
        """
        try:
            run_number: int = self._next_run_number()
            date_time_str: str = datetime.now().strftime("%d%m%y_%H%m")
            run_name: str = f"{run_number}_{date_time_str}.pkl"
            file: Path = self.dir_run / run_name
            logger.debug(f"Saving {run_number =} @ {file =} ...")
            with open(file, "wb") as f:
                pickle.dump(obj=run_data, file=f)
            logger.info(f"File {run_number =} saved @ {file = }.")
        except Exception as e:
            logger.error(f"Failed to save run {run_number}, @{file=} : {str(e)}.")

    def get_run_file(self, run_number: int) -> str:
        """Get the file for the run number, checks if in"""
        # Check run number is there
        if run_number not in set(self._list_available_runs_numbers()):
            logger.error(
                f"Run number {run_number = } is not in data {self._list_available_runs_numbers() =}."
            )
            raise ValueError("run_number must be in the available runs")

        # get file name
        files: List[str] = [
            file
            for file in self._list_run_files()
            if ((file.startswith(str(run_number)) and file.endswith(".pkl")))
        ]
        # check empty
        if not files:
            logger.error(f"Could not find any files in {self.dir_run}.")

        if len(files) > 1:
            logger.warning(f"More than one file found, returning the first\n{files =}")
        else:
            logger.info(f"File found for {run_number = } in {self.dir_run}.")

        return files[0]

    def load_run(self, run_number: int) -> Any:  # TODO change any to run type.
        """Loads the run, given the run number"""

        try:
            file_name: str = self.get_run_file(run_number=run_number)
            file: Path = self.dir_run / file_name
            with open(file, "rb") as f:
                run_data = pickle.load(f)

            return run_data
        except ValueError as ve:
            logger.error(f"Error with run number: {str(ve)}.")

        except Exception as e:
            logger.error(f"An error occured trying to load {run_number =}: {str(e)}.")

    def load_avaiable_runs(self) -> Dict[str, Any]:
        """Load all available runs for this season"""

        runs_results: Dict[str, Any] = {}

        for run_number in self._list_available_runs_numbers():
            try:
                runs_results[str(run_number)] = self.load_run(run_number=run_number)
            except Exception as e:
                logger.warning(f"Failed to load run {run_number}. {str(e)}.")

        logger.info(f"Loaded {len(runs_results.keys())} runs: {runs_results.keys()}.")

        return runs_results
