import logging
from typing import Dict, Optional

from omegaconf import DictConfig
from pydantic import ValidationError
from webdriver import MyWebDriver

import sofascrape.schemas.general as schemas
from sofascrape.abstract.base import BaseComponentScraper

logger = logging.getLogger(__name__)


class EventFootallComponentScraper(BaseComponentScraper):
    """For football details of an event."""

    def __init__(
        self, webdriver: MyWebDriver, matchid=int, cfg: Optional[DictConfig] = None
    ) -> None:
        super().__init__(webdriver=webdriver, cfg=cfg)
        self.matchid: int = matchid
        self.page_url: str = self.cfg.links.football_base_match.format(match_id=matchid)

    def get_data(self):
        try:
            match_base_data = self.webdriver.get_page(self.page_url)

            if not isinstance(match_base_data, Dict):
                raise ValueError(f"Data is not a dict, {type(match_base_data)=}.")
            self.raw_data = match_base_data

        except Exception as e:
            logger.error(f"Failed to get data, {self.matchid = }: with error {str(e)}.")

    def parse_data(self) -> None:
        """Parse raw data into structured format using Pydantic.

        Raises:
            ValueError: If raw_data is None
            ValidationError: If data doesn't match expected schema
        """
        try:
            # Pass dict directly to Pydantic (not JSON string)
            self.data = schemas.FootballDetailsSchema.model_validate(self.raw_data)
            logger.info(f"Successfully parsed data for event {self.matchid =}.")

        except ValidationError as e:
            logger.warning(f"Validation failed for event, {self.matchid =}, {str(e)}.")
            raise
        except Exception as e:
            logger.error(
                f"Unexpected error parsing match details event {self.matchid=}, {str(e)}."
            )
            raise
