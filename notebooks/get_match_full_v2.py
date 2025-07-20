import json
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, TypeVar, Union

from pydantic import BaseModel, Field
from webdriver import ManagerWebdriver, MyWebDriver

import sofascrape.schemas.general as schemas
from sofascrape.abstract.base import BaseMatchScraper
from sofascrape.football import (
    EventFootallComponentScraper,
    FootballGraphComponentScraper,
    FootballIncidentsComponentScraper,
    FootballLineupComponentScraper,
    FootballStatsComponentScraper,
)

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


#####
class ComponentStatus(str, Enum):
    """Status of individual component scraping"""

    SUCCESS = "success"
    FAILED = "failed"
    NOT_ATTEMPTED = "not_attempted"


class ComponentError(BaseModel):
    """Individual component error details"""

    component: str
    status: ComponentStatus
    error_message: Optional[str] = None
    attempted_at: Optional[str] = None  # timestamp


class MatchScrapingErrors(BaseModel):
    """Detailed error tracking for all components"""

    base: ComponentError
    stats: ComponentError
    lineup: ComponentError
    incidents: ComponentError
    graph: ComponentError

    @property
    def has_errors(self) -> bool:
        """Check if any component failed"""
        return any(
            error.status == ComponentStatus.FAILED
            for error in [
                self.base,
                self.stats,
                self.lineup,
                self.incidents,
                self.graph,
            ]
        )

    @property
    def successful_components(self) -> List[str]:
        """Get list of successful component names"""
        return [
            error.component
            for error in [
                self.base,
                self.stats,
                self.lineup,
                self.incidents,
                self.graph,
            ]
            if error.status == ComponentStatus.SUCCESS
        ]

    @property
    def failed_components(self) -> List[str]:
        """Get list of failed component names"""
        return [
            error.component
            for error in [
                self.base,
                self.stats,
                self.lineup,
                self.incidents,
                self.graph,
            ]
            if error.status == ComponentStatus.FAILED
        ]


class FootballMatchResultDetailed(BaseModel):
    """Complete football match data with detailed error tracking"""

    # Match metadata
    match_id: int
    scraped_at: str = Field(default_factory=lambda: str(datetime.now()))

    # Component data (None if failed/not attempted)
    base: Optional[schemas.FootballDetailsSchema] = None
    stats: Optional[schemas.FootballStatsSchema] = None
    lineup: Optional[schemas.FootballLineupSchema] = None
    incidents: Optional[schemas.FootballIncidentsSchema] = None
    graph: Optional[schemas.FootballGraphSchema] = None

    # Detailed error tracking
    errors: MatchScrapingErrors

    @property
    def success_rate(self) -> str:
        """Get success rate as string"""
        successful = len(self.errors.successful_components)
        total = successful + len(self.errors.failed_components)
        return f"{successful}/{total}"

    @property
    def has_base_data(self) -> bool:
        """Check if base data is available"""
        return (
            self.base is not None and self.errors.base.status == ComponentStatus.SUCCESS
        )

    def get_match_info(self) -> Optional[Dict]:
        """Get basic match info from base data"""
        if not self.base:
            return None

        event = self.base.event
        return {
            "home_team": event.homeTeam.name,
            "away_team": event.awayTeam.name,
            "score": f"{event.homeScore.current if event.homeScore else 0}-{event.awayScore.current if event.awayScore else 0}",
            "status": event.status.description,
            "venue": event.venue.name if event.venue else None,
        }

    def get_error_summary(self) -> str:
        """Get a summary of all errors"""
        if not self.errors.has_errors:
            return "No errors"

        failed = self.errors.failed_components
        return f"Failed components: {', '.join(failed)}"


class FootballMatchScraper(BaseMatchScraper):

    def __init__(self, webdriver: MyWebDriver, matchid: int) -> None:
        super().__init__(webdriver=webdriver, matchid=matchid)
        self.available_components: List[MatchComponentType] = [
            MatchComponentType.BASE,
            MatchComponentType.STATS,
            MatchComponentType.LINEUP,
            MatchComponentType.INCIDENTS,
            MatchComponentType.GRAPH,
        ]

        self.results: Dict[MatchComponentType, MatchComponentResult] = {}

    def _create_component_error(
        self,
        component_name: str,
        status: ComponentStatus = ComponentStatus.NOT_ATTEMPTED,
        error_message: Optional[str] = None,
    ) -> ComponentError:
        """Create a component error object"""
        return ComponentError(
            component=component_name,
            status=status,
            error_message=error_message,
            attempted_at=(
                str(datetime.now()) if status != ComponentStatus.NOT_ATTEMPTED else None
            ),
        )

    def _create_empty_match_errors(self) -> MatchScrapingErrors:
        """Create empty error structure for a match"""
        return MatchScrapingErrors(
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
            scraper = scraper_class(self.webdriver, self.matchid)
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
    ) -> FootballMatchResultDetailed:
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
                        component_name, ComponentStatus.SUCCESS
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
                        component_name, ComponentStatus.FAILED, error_msg
                    ),
                )

        # Create result
        return FootballMatchResultDetailed(
            match_id=self.matchid,
            **component_data,  # base=data, stats=data, etc.
            errors=errors,
        )

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of scraping results"""
        successful = [
            str(ct.name) for ct, result in self.results.items() if result.success
        ]
        failed = [ct.value for ct, result in self.results.items() if not result.success]

        return {
            "match_id": self.matchid,
            "total_components": len(self.results),
            "successful_components": successful,
            "failed_components": failed,
        }


if __name__ == "__main__":
    wm = ManagerWebdriver()
    driver = wm.spawn_webdriver()
    matchid = 12436870
    try:
        # Simple usage
        match_scraper = FootballMatchScraper(webdriver=driver, matchid=matchid)
        result = match_scraper.scrape()

        print(f"Match ID: {result.match_id}")
        print(f"Success rate: {result.success_rate}")

        # Check if we have base data
        if result.has_base_data:
            match_info = result.get_match_info()
            print(f"Match: {match_info['home_team']} vs {match_info['away_team']}")
            print(f"Score: {match_info['score']}")

        # Check individual components
        if result.base:
            print("✓ Base data available")
        if result.stats:
            print("✓ Stats data available")
        if result.lineup:
            print("✓ Lineup data available")
        if result.incidents:
            print("✓ Incidents data available")
        if result.graph:
            print("✓ Graph data available")

        # Show errors if any
        if result.errors:
            print(f"Errors: {result.errors}")

        # Detailed errors example
        print("\n=== Detailed Errors Example ===")
        print(f"Error summary: {result.get_error_summary()}")
        print(f"Successful: {result.errors.successful_components}")
        print(f"Failed: {result.errors.failed_components}")

        print("\n=== Detailed Example ===")
        print(result.model_dump_json(indent=8))

    finally:
        driver.close()
