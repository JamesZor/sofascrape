# general

from .events import EventsComponentScraper
from .seasons import SeasonsComponentScraper
from .tournament import TournamentComponentScraper

__all__ = [
    "TournamentComponentScraper",
    "SeasonsComponentScraper",
    "EventsComponentScraper",
]
