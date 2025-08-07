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
        # TODO Change to full once working and tested.
        season_scraper._scrape_debug(use_threading=True, max_workers=3)
        # season_scraper.scrape(use_threading=True, max_workers=10)

        self.storage.save_scraping_run(season_scraper.data)

        if isinstance(season_scraper.events_scraper.data, sofaschema.EventsListSchema):  # type: ignore[union-attr]
            if self.storage.season_event_details is None:
                self.storage.save_season_event_details(
                    season_scraper.events_scraper.data  # type: ignore[union-attr]
                )

    # ========================================================================
    # Comparator operations
    # ========================================================================

    def load_available_runs(self) -> Dict[str, List[sofaschema.SeasonScrapingResult]]:
        """Load all available runs for analysis"""

        # available_runs: Dict = {}
        # for run_id, item in self.storage.load_avaiable_runs().items():
        #     available_runs[run_id] = [x.data for x in item.matches]

        return {
            run_id: [x.data for x in item.matches]  # type: ignore[misc]
            for run_id, item in self.storage.load_avaiable_runs().items()
        }

    def build_consensus_analysis(self, run_ids: List[str] = None) -> "SeasonConsensus":
        """Build consensus analysis across specified runs"""
        # TODO:
        # 1. Load specified runs (or all if none specified)
        # 2. Extract quality assessments for each run
        # 3. Use consensus_builder to analyze
        # 4. Save analysis results
        # 5. Return SeasonConsensus
        pass

    # ========================================================================
    # Comparator operations
    # ========================================================================
