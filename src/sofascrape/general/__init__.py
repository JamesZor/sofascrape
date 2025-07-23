# general

from .events import EventsComponentScraper
from .process_tournament import TournamentProcessScraper
from .seasons import SeasonsComponentScraper
from .tournament import TournamentComponentScraper

__all__ = [
    "TournamentComponentScraper",
    "SeasonsComponentScraper",
    "EventsComponentScraper",
    "TournamentProcessScraper",
]
