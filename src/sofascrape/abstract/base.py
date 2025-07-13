from abc import ABC, abstractmethod
from typing import Dict, Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


class BaseComponentScraper(ABC, Generic[T]):
    """
    Lower level componet. Gets the data and parse it.
    """

    @abstractmethod
    def get_data(self) -> None:
        """Fetch raw data from a source (e.g., API, web page)."""
        pass

    @abstractmethod
    def parse_data(self) -> None:
        """Parse the fetched data into structured form."""
        pass

    def process(self) -> T:
        """High-level workflow: fetch, parse, and return data."""
        pass
