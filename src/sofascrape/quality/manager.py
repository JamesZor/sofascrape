import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from omegaconf import DictConfig, OmegaConf

import sofascrape.schemas.general as sofaschema
from sofascrape.football import SeasonFootballScraper

from .core.comparator import Comparator
from .core.dataclasses import SeasonConsensusResult
from .storage.run_storage import StorageHandler

logger = logging.getLogger(__name__)


class SeasonQualityManager:
    """Main orchestrator for the quality assurance system"""

    def __init__(
        self, tournament_id: int, season_id: int, config: Optional[DictConfig] = None
    ):
        self.tournament_id = tournament_id
        self.season_id = season_id
        if config is None:
            logger.debug("No config given, getting default.")
            self.config = self._get_config()
        else:
            self.config = config

        self.storage = StorageHandler(
            tournament_id=tournament_id, season_id=season_id, config=self.config
        )

        self.comparator = Comparator(config=self.config)

    def _get_config(self) -> DictConfig:
        config_path = (Path(__file__).parent) / "config" / "quality_config.yaml"
        return OmegaConf.load(config_path)  # type: ignore[return-value]

    # ========================================================================
    # Scraping operations
    # ========================================================================
    def execute_scraping_run(self) -> None:
        """Execute a single scraping run and save results"""
        season_scraper = SeasonFootballScraper(
            tournamentid=self.tournament_id, seasonid=self.season_id
        )
        # TODO: Change to full once working and tested.
        season_scraper._scrape_debug(use_threading=True, max_workers=3)
        # season_scraper.scrape(use_threading=True, max_workers=10)

        self.storage.save_scraping_run(season_scraper.data)

        if isinstance(season_scraper.events_scraper.data, sofaschema.EventsListSchema):  # type: ignore[union-attr]
            if self.storage.season_event_details is None:
                self.storage.save_season_event_details(
                    season_scraper.events_scraper.data  # type: ignore[union-attr]
                )

    def execute_scraping_retry(self, retry: Dict[str, List[str]]) -> None:
        """Executes a retry scrape for the matches/componets given for the retry.
        Saves the results as a partial run in the runs dir.
        Using SeasonConsensusResult method get_retry_dict.
        """
        season_scraper = SeasonFootballScraper(
            tournamentid=self.tournament_id, seasonid=self.season_id
        )
        season_scraper.scrape_retry(retry=retry)
        self.storage.save_scraping_run_retry(run_data=season_scraper.data)

    # ========================================================================
    # Comparator operations
    # ========================================================================

    def load_available_runs(
        self,
    ) -> Dict[str, List[sofaschema.FootballMatchResultDetailed]]:
        """Load all available runs for analysis"""

        # TEST: and remove if works.
        # available_runs: Dict = {}
        # for run_id, item in self.storage.load_avaiable_runs().items():
        #     available_runs[run_id] = [x.data for x in item.matches]

        return {
            run_id: [x.data for x in item.matches]  # type: ignore[misc]
            for run_id, item in self.storage.load_avaiable_runs().items()
        }

    def build_consensus_analysis(self) -> SeasonConsensusResult:
        """Build consensus analysis across specified runs"""
        runs_data_dicts = self.load_available_runs()

        if len(runs_data_dicts.keys()) < 2:
            logger.error(
                f"Need more than two run to form consensus, currently have {len(runs_data_dicts.keys())}. Run some season scrapes."
            )
            raise ValueError(
                f"Need more run data for tournamentid:{self.tournament_id},seasonid: {self.season_id}."
            )

        season_consensus = self.comparator.build_season_consensus(
            run_data=runs_data_dicts,
            tournament_id=self.tournament_id,
            season_id=self.season_id,
        )

        self.storage.save_consensus(consensus_data=season_consensus)
        return season_consensus

    # ========================================================================
    # golden operations
    # ========================================================================

    def load_available_runs_dict(
        self,
    ) -> Dict[str, Dict[int, sofaschema.FootballMatchResultDetailed]]:

        runs = self.load_available_runs()

        runs_dict: Dict[str, Dict[int, sofaschema.FootballMatchResultDetailed]] = {}

        for run_id, list_match_results in runs.items():
            runs_dict[run_id] = {match.match_id: match for match in list_match_results}

        return runs_dict

    def build_golden_seaosn_dataset(self, golden_dict: Dict
