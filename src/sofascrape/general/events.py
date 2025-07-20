import logging
from typing import Any, Dict, Optional

from omegaconf import DictConfig
from pydantic import ValidationError
from webdriver import MyWebDriver

import sofascrape.schemas.general as schemas
from sofascrape.abstract.base import BaseComponentScraper

logger = logging.getLogger(__name__)


class EventsComponentScraper(BaseComponentScraper):
    """football events, might need to be changed for other sports/ events"""

    def __init__(
        self,
        webdriver: MyWebDriver,
        tournamentid: int,
        seasonid: int,
        cfg: Optional[DictConfig] = None,
    ) -> None:
        """Initialize the tournament scraper.
        Args:
            webdriver: WebDriver instance for scraping
            tournamentid: Positive integer ID of the tournament
            seasonid: Positive integer ID of the season

        Raises:
            ValueError: If tournamentid is invalid or webdriver is wrong type
        """
        super().__init__(webdriver=webdriver, cfg=cfg)

        self.tournamentid: int = tournamentid
        self.seasonid: int = seasonid
        self.page_url: str = self.cfg.links.events_season_empty.format(
            tournamentID=tournamentid, seasonID=seasonid
        )

    def get_data(self):
        """Fetch the raw seasons events lsit data from api.
        Raise:
            ValueError: If data is not in correct format.
            Exception: If getting data fails.
        """
        try:
            seasons_events_list_page_data = self.webdriver.get_page(self.page_url)

            if not isinstance(seasons_events_list_page_data, Dict):
                raise ValueError(
                    f"Data is not a dict, {type(seasons_events_list_page_data)=}."
                )
            self.raw_data = seasons_events_list_page_data

        except Exception as e:
            logger.error(
                f"Failed to get data, {self.tournamentid =}, {self.seasonid = }: with error {str(e)}."
            )

    def parse_data(self) -> None:
        """Parse raw data into structured format using Pydantic.

        Raises:
            ValueError: If raw_data is None
            ValidationError: If data doesn't match expected schema
        """
        try:
            # Pass dict directly to Pydantic (not JSON string)
            self.data = schemas.EventsListSchema.model_validate(self.raw_data)
            logger.info(
                f"Successfully parsed data for tournament {self.tournamentid}, seasonid {self.seasonid}"
            )

        except ValidationError as e:
            logger.warning(
                f"Validation failed for tournament {self.tournamentid}, seasonif {self.seasonid}: {str(e)}"
            )
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error parsing tournament {self.tournamentid}, seasonid {self.seasonid}: {str(e)}"
            )
            raise
