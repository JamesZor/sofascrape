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
        super().__init__(webdriver=webdriver)
        self.tournamentid: int = tournamentid
        self.webdriver: MyWebDriver = webdriver

        self.page_url: str = self.cfg.links.seasons_empty.format(
            tournamentID=tournamentid
        )

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
                f"failed to get data for seasons, for tournament id {self.tournamentid}. {str(e)}."
            )

    def parse_data(self):
        """Parse raw data into structured format using Pydantic.

        Raises:
            ValueError: If raw_data is None
            ValidationError: If data doesn't match expected schema
        """
        try:
            self.data = SeasonsListSchema.model_validate(self.raw_data)
            logger.info(
                f"Successfully parsed data for seasons for tournament {self.tournamentid}."
            )
        except ValidationError as e:
            logger.warning(
                f"validation failed for seasons {self.tournamentid =}: {str(e)}."
            )
            raise

        except Exception as e:
            logger.error(
                f"Unexpected error parsing seasons, {self.tournamentid=}: {str(e)}."
            )
            raise
