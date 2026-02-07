import logging
import os
import time

import requests

from src.config import AppConfig
from src.models import Listing

logger = logging.getLogger(__name__)


class CommuteEnricher:
    """Calculate commute times to configured destinations using Google Maps Distance Matrix API."""

    GOOGLE_API_URL = "https://maps.googleapis.com/maps/api/distancematrix/json"

    def __init__(self, config: AppConfig):
        self.config = config
        self.api_key = os.environ.get("GOOGLE_MAPS_API_KEY")
        self.targets = config.enrichment.commute.targets
        if not self.api_key:
            logger.warning("GOOGLE_MAPS_API_KEY not set. Commute enrichment disabled.")

    def enrich(self, listing: Listing):
        """Add commute_minutes dict to listing."""
        if not self.api_key or not self.targets:
            return
        if listing.commute_minutes is not None:
            return  # Already enriched

        origin = f"{listing.address}, {listing.city}, {listing.state} {listing.zip_code}"
        commute_times = {}

        for target in self.targets:
            try:
                params = {
                    "origins": origin,
                    "destinations": target.address,
                    "mode": "driving",
                    "departure_time": "now",
                    "key": self.api_key,
                }
                resp = requests.get(self.GOOGLE_API_URL, params=params, timeout=10)
                resp.raise_for_status()
                data = resp.json()

                if data.get("status") == "OK":
                    rows = data.get("rows", [])
                    if rows:
                        elements = rows[0].get("elements", [])
                        if elements and elements[0].get("status") == "OK":
                            duration_seconds = elements[0]["duration"]["value"]
                            commute_times[target.name] = round(duration_seconds / 60)
                            logger.debug(
                                f"Commute {listing.address} -> {target.name}: "
                                f"{commute_times[target.name]} min"
                            )

            except requests.RequestException:
                logger.exception(f"Google Maps API failed for {listing.address} -> {target.name}")

            time.sleep(0.2)

        if commute_times:
            listing.commute_minutes = commute_times
