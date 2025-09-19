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
    def execute_incremental_scrape(self) -> bool:
        """
        Executes an efficient, incremental scrape.
        1. Fetches all match IDs for the season.
        2. Compares against the existing golden dataset.
        3. Scrapes only the new, unprocessed matches.
        Returns:
            True if new matches were found and scraped, False otherwise.
        """
        logger.info(
            f"Starting incremental scrape for t:{self.tournament_id} s:{self.season_id}..."
        )

        # 1. Get matches we have already processed from the golden data
        try:
            golden_data = self.storage.load_golden_dataset()
            processed_ids = set(golden_data.keys()) if golden_data else set()
            logger.info(
                f"Found {len(processed_ids)} matches in existing golden dataset."
            )
        except FileNotFoundError:
            processed_ids = set()
            logger.info("No existing golden dataset found. Will perform a full scrape.")

        # 2. Get all match IDs from the source using the scraper
        # We temporarily instantiate a scraper just to get the event list
        scraper_for_events = SeasonFootballScraper(
            tournamentid=self.tournament_id, seasonid=self.season_id
        )
        scraper_for_events._get_events()  # This populates scraper_for_events.valid_matchids
        all_source_ids = set(scraper_for_events.valid_matchids)

        if not all_source_ids:
            logger.warning(
                "Could not retrieve any completed matches from source. Aborting."
            )
            return False

        # 3. Find the difference
        new_match_ids = all_source_ids - processed_ids

        if not new_match_ids:
            logger.info("No new matches to scrape. Season is up-to-date.")
            return False

        logger.info(f"Found {len(new_match_ids)} new matches to scrape.")

        # 4. Execute a targeted scrape
        season_scraper = SeasonFootballScraper(
            tournamentid=self.tournament_id, seasonid=self.season_id
        )
        # Instruct the scraper to ONLY run on the new match IDs
        season_scraper.scrape(use_threading=True, match_ids_to_scrape=new_match_ids)

        # 5. Save the results as a new (partial) run
        # We'll save it as a retry/partial run to distinguish it
        self.storage.save_scraping_run_retry(run_data=season_scraper.data)

        return True

    def execute_scraping_run(self) -> None:
        """Execute a single scraping run and save results"""
        season_scraper = SeasonFootballScraper(
            tournamentid=self.tournament_id, seasonid=self.season_id
        )
        # TODO: Change to full once working and tested.
        # season_scraper._scrape_debug(use_threading=True, max_workers=3)
        # HACK: max works as config
        season_scraper.scrape(use_threading=True)

        self.storage.save_scraping_run(season_scraper.data)

        if isinstance(season_scraper.events_scraper.data, sofaschema.EventsListSchema):  # type: ignore[union-attr]
            if self.storage.season_event_details is None:
                self.storage.save_season_event_details(
                    season_scraper.events_scraper.data  # type: ignore[union-attr]
                )

    # TEST:
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

    # TEST:
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

    # TEST:
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

    # TEST:
    def load_available_runs_dict(
        self,
    ) -> Dict[int, Dict[int, sofaschema.FootballMatchResultDetailed]]:

        runs = self.load_available_runs()

        runs_dict: Dict[int, Dict[int, sofaschema.FootballMatchResultDetailed]] = {}

        for run_id, list_match_results in runs.items():
            runs_dict[int(run_id)] = {
                match.match_id: match
                for match in list_match_results
                if match is not None
            }

        return runs_dict

    # TEST:
    def build_golden_seaosn_dataset(
        self, golden_dict: Dict[int, Dict[str, int]]
    ) -> Dict[int, sofaschema.FootballMatchResultDetailed]:
        """Complie the good data together from all the runs and retry datasets.
        Example of golden_dict:
        {12476980: {'graph': 2, 'stats': 2, 'lineup': 2, 'base': 2, 'incidents': 2}, 12476946: {'graph': 1, 'stats': 1, 'lineup': 2, 'base': 1, 'incidents': 1}}

        """
        runs = self.load_available_runs_dict()

        results: Dict[int, sofaschema.FootballMatchResultDetailed] = {}

        for match_id, golden_componets in golden_dict.items():
            match_data: Dict = {"match_id": match_id}
            for compoent_name, runID in golden_componets.items():
                match_data[compoent_name] = getattr(
                    runs[runID][match_id], compoent_name
                )

            results[match_id] = sofaschema.FootballMatchResultDetailed(**match_data)

        return results
