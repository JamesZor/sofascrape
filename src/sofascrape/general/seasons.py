import logging
from typing import Dict, Optional

from hydra import compose, initialize
from omegaconf import DictConfig
from pydantic import BaseModel, ValidationError
from webdriver import MyWebDriver

from sofascrape.abstract.base import BaseComponentScraper
from sofascrape.schemas.general import SeasonsListSchema

logger = logging.getLogger(__name__)


class SeasonsComponentScraper(BaseComponentScraper):

    def __init__(self, tournamentid: int, webdriver: MyWebDriver) -> None:
        """Initialize the tournament scraper.

        Args:
            tournamentid: Positive integer ID of the tournament
            webdriver: WebDriver instance for scraping

        Raises:
            ValueError: If tournamentid is invalid or webdriver is wrong type
        """
        self.tournamentid: int = tournamentid
        self.webdriver: MyWebDriver = webdriver
        self.cfg: DictConfig = self._get_cfg()
        self.page_url: str = self.cfg.links.seasons_empty.format(
            tournamentID=tournamentid
        )
        # Initialize data attributes
        self.raw_data: Optional[Dict] = None
        self.data: Optional[SeasonsListSchema] = None

    def get_data(self):
        """Fetch the raw seasons lsit data from api.

        Raise:
            ValueError: If data is not in correct format.
            Exception: If getting data fails.
        """
        try:
            seaons_page_data = self.webdriver.get_page(self.page_url)

            if not isinstance(seaons_page_data, dict):
                raise ValueError("Data is not a dict")

            self.raw_data = seaons_page_data

        except Exception as e:
            logger.error(
                f"failed to get data for seasons, for tournament id {self.tournamentid}."
            )
