import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ValidationError
from webdriver import ManagerWebdriver, MyWebDriver

import sofascrape.schemas.general as schemas
from sofascrape.football import (
    EventFootallComponentScraper,
    FootballGraphComponentScraper,
    FootballIncidentsComponentScraper,
    FootballLineupComponentScraper,
    FootballStatsComponentScraper,
)

logger = logging.getLogger(__name__)


class ComponentType(Enum):
    """Available component types for football matches"""

    BASE = "football_base_match"
    STATS = "football_stats"
    LINEUP = "football_lineup"
    INCIDENTS = "football_incidents"
    GRAPH = "football_graph"


@dataclass
class ComponentResult:
    """Result from a component scraper"""

    component_type: ComponentType
    success: bool
    data: Optional[Any] = None
    error: Optional[str] = None


class ConsolidatedMatchSchema(BaseModel):
    """Consolidated match data with reduced overlap"""

    # Core match info (from base)
    match_id: int
    slug: str
    start_timestamp: int
    status: schemas.StatusSchema
    winner_code: Optional[int] = None

    # Teams and scores (consolidated)
    home_team: schemas.FootballTeamSchema
    away_team: schemas.FootballTeamSchema
    home_score: Optional[schemas.ScoreFootballSchema] = None
    away_score: Optional[schemas.ScoreFootballSchema] = None

    # Match details (from base)
    tournament: schemas.FootballTournamentSchema
    season: schemas.SeasonSchema
    round_info: schemas.RoundInfoSchema

    # Enhanced match info (from base if available)
    attendance: Optional[int] = None
    venue: Optional[schemas.VenueSchema] = None
    referee: Optional[schemas.RefereeSchema] = None

    # Component data (optional - only if successfully scraped)
    statistics: Optional[schemas.FootballStatsSchema] = None
    lineup: Optional[schemas.FootballLineupSchema] = None
    incidents: Optional[schemas.FootballIncidentsSchema] = None
    graph: Optional[schemas.FootballGraphSchema] = None

    # Metadata
    components_scraped: List[str] = []
    components_failed: List[str] = []


class FootballMatchScraper:
    """Coordinates all component scrapers for a single football match"""

    def __init__(self, webdriver: MyWebDriver, match_id: int):
        self.webdriver = webdriver
        self.match_id = match_id
        self.results: Dict[ComponentType, ComponentResult] = {}

        # Component scrapers mapping
        self.scrapers = {
            ComponentType.BASE: EventFootallComponentScraper,
            ComponentType.STATS: FootballStatsComponentScraper,
            ComponentType.LINEUP: FootballLineupComponentScraper,
            ComponentType.INCIDENTS: FootballIncidentsComponentScraper,
            ComponentType.GRAPH: FootballGraphComponentScraper,
        }

    def scrape_component(self, component_type: ComponentType) -> ComponentResult:
        """Scrape a single component"""
        try:
            scraper_class = self.scrapers[component_type]
            scraper = scraper_class(self.webdriver, self.match_id)

            # Process the component
            data = scraper.process()

            if data is not None:
                logger.info(
                    f"Successfully scraped {component_type.value} for match {self.match_id}"
                )
                return ComponentResult(
                    component_type=component_type, success=True, data=data
                )
            else:
                error_msg = f"No data returned for {component_type.value}"
                logger.warning(f"{error_msg} for match {self.match_id}")
                return ComponentResult(
                    component_type=component_type, success=False, error=error_msg
                )

        except Exception as e:
            error_msg = f"Failed to scrape {component_type.value}: {str(e)}"
            logger.error(f"{error_msg} for match {self.match_id}")
            return ComponentResult(
                component_type=component_type, success=False, error=error_msg
            )

    def scrape_all(
        self,
        required_components: Optional[List[ComponentType]] = None,
        optional_components: Optional[List[ComponentType]] = None,
    ) -> Dict[ComponentType, ComponentResult]:
        """
        Scrape all specified components

        Args:
            required_components: Components that must succeed
            optional_components: Components that can fail without stopping
        """
        if required_components is None:
            required_components = [ComponentType.BASE]  # Base is always required

        if optional_components is None:
            optional_components = [
                ComponentType.STATS,
                ComponentType.LINEUP,
                ComponentType.INCIDENTS,
                ComponentType.GRAPH,
            ]

        # Scrape required components first
        for component_type in required_components:
            result = self.scrape_component(component_type)
            self.results[component_type] = result

            if not result.success:
                logger.error(
                    f"Required component {component_type.value} failed for match {self.match_id}"
                )
                # Could raise exception here if you want to fail fast

        # Scrape optional components
        for component_type in optional_components:
            result = self.scrape_component(component_type)
            self.results[component_type] = result

            if not result.success:
                logger.warning(
                    f"Optional component {component_type.value} failed for match {self.match_id}"
                )

        return self.results

    def consolidate_data(self) -> Optional[ConsolidatedMatchSchema]:
        """Consolidate all scraped data into a single schema"""
        # Base data is required
        base_result = self.results.get(ComponentType.BASE)
        if not base_result or not base_result.success:
            logger.error(
                f"Cannot consolidate without base data for match {self.match_id}"
            )
            return None

        base_data = base_result.data.event  # Extract event from FootballDetailsSchema

        try:
            # Start with base match data
            consolidated = ConsolidatedMatchSchema(
                match_id=base_data.id,
                slug=base_data.slug,
                start_timestamp=base_data.startTimestamp,
                status=base_data.status,
                winner_code=base_data.winnerCode,
                home_team=base_data.homeTeam,
                away_team=base_data.awayTeam,
                home_score=base_data.homeScore,
                away_score=base_data.awayScore,
                tournament=base_data.tournament,
                season=base_data.season,
                round_info=base_data.roundInfo,
                attendance=base_data.attendance,
                venue=base_data.venue,
                referee=base_data.referee,
            )

            # Add optional component data
            successful_components = []
            failed_components = []

            for component_type, result in self.results.items():
                if result.success and result.data:
                    successful_components.append(component_type.value)

                    # Add component-specific data
                    if component_type == ComponentType.STATS:
                        consolidated.statistics = result.data
                    elif component_type == ComponentType.LINEUP:
                        consolidated.lineup = result.data
                    elif component_type == ComponentType.INCIDENTS:
                        consolidated.incidents = result.data
                    elif component_type == ComponentType.GRAPH:
                        consolidated.graph = result.data
                else:
                    failed_components.append(component_type.value)

            consolidated.components_scraped = successful_components
            consolidated.components_failed = failed_components

            logger.info(f"Successfully consolidated data for match {self.match_id}")
            logger.info(f"Scraped: {successful_components}")
            if failed_components:
                logger.warning(f"Failed: {failed_components}")

            return consolidated

        except Exception as e:
            logger.error(
                f"Failed to consolidate data for match {self.match_id}: {str(e)}"
            )
            return None

    def scrape_match(
        self,
        required_components: Optional[List[ComponentType]] = None,
        optional_components: Optional[List[ComponentType]] = None,
    ) -> Optional[ConsolidatedMatchSchema]:
        """
        Main method: scrape all components and return consolidated data

        Args:
            required_components: Components that must succeed
            optional_components: Components that can fail without stopping

        Returns:
            ConsolidatedMatchSchema with all available data
        """
        logger.info(f"Starting match scraping for match {self.match_id}")

        # Scrape all components
        self.scrape_all(required_components, optional_components)

        # Consolidate and return
        return self.consolidate_data()


# Convenience functions for common use cases
def scrape_full_match(
    webdriver: MyWebDriver, match_id: int
) -> Optional[ConsolidatedMatchSchema]:
    """Scrape all available components for a match"""
    scraper = FootballMatchScraper(webdriver, match_id)
    return scraper.scrape_match()


def scrape_basic_match(
    webdriver: MyWebDriver, match_id: int
) -> Optional[ConsolidatedMatchSchema]:
    """Scrape only base match data"""
    scraper = FootballMatchScraper(webdriver, match_id)
    return scraper.scrape_match(
        required_components=[ComponentType.BASE], optional_components=[]
    )


def scrape_match_with_stats(
    webdriver: MyWebDriver, match_id: int
) -> Optional[ConsolidatedMatchSchema]:
    """Scrape base + stats (common combination)"""
    scraper = FootballMatchScraper(webdriver, match_id)
    return scraper.scrape_match(
        required_components=[ComponentType.BASE],
        optional_components=[ComponentType.STATS],
    )


if __name__ == "__main__":
    wm = ManagerWebdriver()
    d1 = wm.spawn_webdriver()
    data = scrape_full_match(webdriver=d1, match_id=12436870)

    print(data.model_dump_json(indent=6))
