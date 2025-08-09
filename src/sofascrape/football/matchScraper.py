import datetime
import logging
from enum import Enum
from typing import List, Optional, Union

from omegaconf import DictConfig
from webdriver import MyWebDriver

import sofascrape.schemas.general as schemas
from sofascrape.abstract.base import BaseMatchScraper

from .eventComponent import EventFootallComponentScraper
from .graphComponent import FootballGraphComponentScraper
from .incidentsComponent import FootballIncidentsComponentScraper
from .lineupComonent import FootballLineupComponentScraper
from .statsComponent import FootballStatsComponentScraper

logger = logging.getLogger(__name__)


class MatchComponentType(Enum):
    """Available component types for football matches"""

    BASE = EventFootallComponentScraper
    STATS = FootballStatsComponentScraper
    LINEUP = FootballLineupComponentScraper
    INCIDENTS = FootballIncidentsComponentScraper
    GRAPH = FootballGraphComponentScraper


# Union type for all possible data types
ComponentDataType = Union[
    schemas.FootballDetailsSchema,
    schemas.FootballStatsSchema,
    schemas.FootballLineupSchema,
    schemas.FootballIncidentsSchema,
    schemas.FootballGraphSchema,
]


class FootballMatchScraper(BaseMatchScraper):

    def __init__(
        self, webdriver: MyWebDriver, matchid: int, cfg: Optional[DictConfig] = None
    ) -> None:
        super().__init__(webdriver=webdriver, matchid=matchid, cfg=cfg)
        # TODO: Use the config setting.
        self.available_components: List[MatchComponentType] = [
            MatchComponentType.BASE,
            MatchComponentType.STATS,
            MatchComponentType.LINEUP,
            MatchComponentType.INCIDENTS,
            MatchComponentType.GRAPH,
        ]

    def _create_component_error(
        self,
        component_name: str,
        status: schemas.ComponentStatus = schemas.ComponentStatus.NOT_ATTEMPTED,
        error_message: Optional[str] = None,
    ) -> schemas.ComponentError:
        """Create a component error object"""
        return schemas.ComponentError(
            component=component_name,
            status=status,
            error_message=error_message,
            attempted_at=(
                str(datetime.datetime.now())
                if status != schemas.ComponentStatus.NOT_ATTEMPTED
                else None
            ),
        )

    def _create_empty_match_errors(self) -> schemas.MatchScrapingErrors:
        """Create empty error structure for a match"""
        return schemas.MatchScrapingErrors(
            base=self._create_component_error("base"),
            stats=self._create_component_error("stats"),
            lineup=self._create_component_error("lineup"),
            incidents=self._create_component_error("incidents"),
            graph=self._create_component_error("graph"),
        )

    def _scrape_single_component(self, component_type: MatchComponentType) -> tuple:
        """
        Scrape a single component and return (success, data, error_message)
        """
        try:
            scraper_class = component_type.value
            scraper = scraper_class(self.webdriver, self.matchid, self.cfg)
            data = scraper.process()

            if data is not None:
                logger.info(
                    f"Successfully scraped {component_type.name} for match {self.matchid}"
                )
                return True, data, None
            else:
                error_msg = f"No data returned for {component_type.name}"
                logger.warning(f"{error_msg} for match {self.matchid}")
                return False, None, error_msg

        except Exception as e:
            error_msg = f"Failed to scrape {component_type.name}: {str(e)}"
            logger.error(f"{error_msg} for match {self.matchid}")
            return False, None, error_msg

    def scrape(
        self,
        components: Optional[List[MatchComponentType]] = None,
    ) -> schemas.FootballMatchResultDetailed:
        """
        Main scrape method returning clean Pydantic result

        Args:
            components: List of components to scrape (default: all)
            detailed_errors: If True, return FootballMatchResultDetailed

        Returns:
            FootballMatchResult with all data and errors
        """
        if components is None:
            components = self.available_components

        # Initialize result containers
        component_data = {}
        error_messages = []
        successful_components = []
        failed_components = []

        errors = self._create_empty_match_errors()

        # Scrape each component
        for component_type in components:
            success, data, error_msg = self._scrape_single_component(component_type)

            component_name = component_type.name.lower()

            if success:
                component_data[component_name] = data
                successful_components.append(component_name)

                setattr(
                    errors,
                    component_name,
                    self._create_component_error(
                        component_name, schemas.ComponentStatus.SUCCESS
                    ),
                )
            else:
                failed_components.append(component_name)
                if error_msg:
                    error_messages.append(f"{component_name}: {error_msg}")

                setattr(
                    errors,
                    component_name,
                    self._create_component_error(
                        component_name, schemas.ComponentStatus.FAILED, error_msg
                    ),
                )

        # Create result
        return schemas.FootballMatchResultDetailed(
            match_id=self.matchid,
            **component_data,  # base=data, stats=data, etc.
            errors=errors,
        )
