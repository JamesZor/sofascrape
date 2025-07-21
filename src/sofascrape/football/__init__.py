from .eventComponent import EventFootallComponentScraper
from .graphComponent import FootballGraphComponentScraper
from .incidentsComponent import FootballIncidentsComponentScraper
from .league import LeagueFootballScraper
from .lineupComonent import FootballLineupComponentScraper
from .matchScraper import FootballMatchScraper
from .season import SeasonFootballScraper
from .statsComponent import FootballStatsComponentScraper

__all__ = [
    "EventFootallComponentScraper",
    "FootballStatsComponentScraper",
    "FootballLineupComponentScraper",
    "FootballIncidentsComponentScraper",
    "FootballGraphComponentScraper",
    "FootballMatchScraper",
    "SeasonFootballScraper",
    "LeagueFootballScraper",
]
