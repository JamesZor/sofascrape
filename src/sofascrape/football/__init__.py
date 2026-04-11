# src/sofascrape/football/__init__.py

from .eventComponent import EventFootallComponentScraper
from .graphComponent import FootballGraphComponentScraper
from .incidentsComponent import FootballIncidentsComponentScraper
from .lineupComonent import FootballLineupComponentScraper
from .oddsComponent import FootballOddsComponentScraper
from .statsComponent import FootballStatsComponentScraper

__all__ = [
    "EventFootallComponentScraper",
    "FootballStatsComponentScraper",
    "FootballLineupComponentScraper",
    "FootballIncidentsComponentScraper",
    "FootballGraphComponentScraper",
    "FootballOddsComponentScraper",
]
