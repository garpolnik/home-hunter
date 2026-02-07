import logging
import time
from abc import ABC, abstractmethod

import requests

from src.config import AppConfig
from src.models import Listing

logger = logging.getLogger(__name__)


class BaseFetcher(ABC):
    """Base class for all listing data fetchers."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        })

    @property
    @abstractmethod
    def source_name(self) -> str:
        pass

    @property
    @abstractmethod
    def request_delay(self) -> float:
        pass

    @abstractmethod
    def fetch_for_location(self, location: dict) -> list[Listing]:
        """Fetch listings for a single configured location."""
        pass

    def fetch_all(self) -> list[Listing]:
        """Fetch listings for all configured locations."""
        results = []
        for i, loc in enumerate(self.config.search.locations):
            try:
                logger.info(f"[{self.source_name}] Fetching location {i+1}/{len(self.config.search.locations)}: {loc.value}")
                listings = self.fetch_for_location(loc)
                logger.info(f"[{self.source_name}] Got {len(listings)} listings for {loc.value}")
                results.extend(listings)
            except Exception:
                logger.exception(f"[{self.source_name}] Error fetching {loc.value}")

            if i < len(self.config.search.locations) - 1:
                time.sleep(self.request_delay)

        logger.info(f"[{self.source_name}] Total: {len(results)} listings across all locations")
        return results
