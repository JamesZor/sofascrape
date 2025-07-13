import logging
from typing import Dict, Optional

from hydra import compose, initialize
from omegaconf import DictConfig
from pydantic import BaseModel, ValidationError
from webdriver import MyWebDriver

from sofascrape.abstract.base import BaseComponentScraper
from sofascrape.schemas.general import TournamentData

logger = logging.getLogger(__name__)


class TournamentComponentScraper(BaseComponentScraper):

    def __init__(self, tournamentid: int, webdriver: MyWebDriver) -> None:
        """Initialize the tournament scraper.

        Args:
            tournamentid: Positive integer ID of the tournament
            webdriver: WebDriver instance for scraping

        Raises:
            ValueError: If tournamentid is invalid or webdriver is wrong type
        """
        self._validate_inputs(tournamentid, webdriver)

        self.tournamentid: int = tournamentid
        self.webdriver: MyWebDriver = webdriver
        self.cfg: DictConfig = self._get_cfg()
        self.page_url: str = self.cfg.links.tournament_empty.format(
            tournamentID=tournamentid
        )
        # Initialize data attributes
        self.raw_data: Optional[Dict] = None
        self.data: Optional[schemas.general.TournamentData] = None

    def _get_cfg(self) -> DictConfig:
        with initialize(config_path="../conf/", version_base="1.3"):
            cfg = compose(config_name="general")
        return cfg

    def _validate_inputs(self, tournamentid: int, webdriver: MyWebDriver) -> None:
        """Validate constructor inputs."""
        if not isinstance(webdriver, MyWebDriver):
            raise ValueError("Correct webdriver needs to be passed.")

        if not isinstance(tournamentid, int) or tournamentid < 0:
            raise ValueError(
                f"tournamentid must be a positive integer. "
                f"Got: {tournamentid=}, {type(tournamentid)=}"
            )

    def get_data(self) -> None:
        """Fetch raw tournament data from the web page.

        Raises:
            ValueError: If page data is not a dictionary
            Exception: If web scraping fails
        """
        try:
            tournament_page_data = self.webdriver.get_page(self.page_url)

            if not isinstance(tournament_page_data, dict):
                error_msg = (
                    f"Tournament page data is incorrect. "
                    f"Expected dict, got {type(tournament_page_data)}. "
                    f"Data: {tournament_page_data} "
                    f"Proxy: {self.webdriver.set_proxy}, "
                    f"Tournament ID: {self.tournamentid}"
                )
                logger.warning(error_msg)
                raise ValueError(error_msg)

            self.raw_data = tournament_page_data
            logger.info(f"Successfully fetched data for tournament {self.tournamentid}")

        except Exception as e:
            logger.error(
                f"Failed to get data for tournament {self.tournamentid}: {str(e)}"
            )
            raise

    def parse_data(self) -> None:
        """Parse raw data into structured format using Pydantic.

        Raises:
            ValueError: If raw_data is None
            ValidationError: If data doesn't match expected schema
        """
        if self.raw_data is None:
            raise ValueError("No raw data available. Call get_data() first.")

        try:
            # Pass dict directly to Pydantic (not JSON string)
            self.data = TournamentData.model_validate(self.raw_data)
            logger.info(f"Successfully parsed data for tournament {self.tournamentid}")

        except ValidationError as e:
            logger.warning(
                f"Validation failed for tournament {self.tournamentid}: {str(e)}"
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error parsing tournament {self.tournamentid}: {str(e)}"
            )
            raise

    def process(self) -> TournamentData:
        """Complete processing pipeline: fetch and parse data.
        Returns:
            TournamentData: Parsed and validated tournament data
        Raises:
            Exception: If any step in the pipeline fails
        """
        try:
            logger.info(f"Starting processing for tournament {self.tournamentid}")
            self.get_data()
            self.parse_data()
            logger.info(f"Successfully processed tournament {self.tournamentid}")
            return self.data

        except Exception as e:
            logger.error(
                f"Processing failed for tournament {self.tournamentid}: {str(e)}"
            )
            raise
