from abc import ABC, abstractmethod
from typing import Dict


class BaseComponentScraper(ABC):
    """
    Lower level componet. Gets the data and parse it.
    """

    @abstractmethod
    def get_data(self) -> Dict:
        """Fetch raw data from a source (e.g., API, web page)."""
        pass

    @abstractmethod
    def parse_data(self) -> Dict:
        """Parse the fetched data into structured form."""
        pass

    def process(self):
        """High-level workflow: fetch, parse, and return data."""
        pass
