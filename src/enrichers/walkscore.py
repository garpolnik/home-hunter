import logging
import os
import time

import requests

from src.config import AppConfig
from src.models import Listing

logger = logging.getLogger(__name__)


class WalkScoreEnricher:
    API_URL = "https://api.walkscore.com/score"

    def __init__(self, config: AppConfig):
        self.config = config
        self.api_key = os.environ.get("WALKSCORE_API_KEY")
        if not self.api_key:
            logger.warning("WALKSCORE_API_KEY not set. WalkScore enrichment disabled.")

    def enrich(self, listing: Listing):
        """Add walk_score, transit_score, and bike_score to listing."""
        if not self.api_key:
            return
        if listing.walk_score is not None:
            return  # Already enriched
        if listing.latitude is None or listing.longitude is None:
            return

        params = {
            "format": "json",
            "lat": listing.latitude,
            "lon": listing.longitude,
            "address": f"{listing.address}, {listing.city}, {listing.state} {listing.zip_code}",
            "transit": 1,
            "bike": 1,
            "wsapikey": self.api_key,
        }

        try:
            resp = requests.get(self.API_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") == 1:
                listing.walk_score = data.get("walkscore")
                if "transit" in data:
                    listing.transit_score = data["transit"].get("score")
                if "bike" in data:
                    listing.bike_score = data["bike"].get("score")
                logger.debug(f"WalkScore for {listing.address}: {listing.walk_score}")
            else:
                logger.debug(f"WalkScore unavailable for {listing.address}: status={data.get('status')}")

        except requests.RequestException:
            logger.exception(f"WalkScore API failed for {listing.address}")

        time.sleep(0.5)  # Rate limit
