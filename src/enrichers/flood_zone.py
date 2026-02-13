import logging
import time

import requests

from src.config import AppConfig
from src.models import Listing

logger = logging.getLogger(__name__)

# FEMA flood zone to risk mapping
# Zones A, AE, AH, AO, AR, A99 = high risk (Special Flood Hazard Areas)
# Zone B, X (shaded) = moderate risk
# Zone C, X (unshaded) = minimal risk
# Zone V, VE = coastal high risk
HIGH_RISK_ZONES = {"A", "AE", "AH", "AO", "AR", "A99", "V", "VE"}
MODERATE_RISK_ZONES = {"B", "X-SHADED", "X SHADED"}
MINIMAL_RISK_ZONES = {"C", "X", "X-UNSHADED", "X UNSHADED"}


class FloodZoneEnricher:
    # FEMA National Flood Hazard Layer (NFHL) ArcGIS REST API
    API_URL = "https://hazards.fema.gov/arcgis/rest/services/public/NFHL/MapServer/28/query"

    def __init__(self, config: AppConfig):
        self.config = config

    def enrich(self, listing: Listing):
        """Add flood_zone and flood_risk_rating to listing."""
        if listing.flood_risk_rating is not None:
            return  # Already enriched
        if listing.latitude is None or listing.longitude is None:
            return

        params = {
            "geometry": f"{listing.longitude},{listing.latitude}",
            "geometryType": "esriGeometryPoint",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "FLD_ZONE,ZONE_SUBTY",
            "returnGeometry": "false",
            "f": "json",
        }

        try:
            resp = requests.get(self.API_URL, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            features = data.get("features", [])
            if features:
                attrs = features[0].get("attributes", {})
                zone = str(attrs.get("FLD_ZONE", "")).strip().upper()
                listing.flood_zone = zone

                if zone in HIGH_RISK_ZONES:
                    listing.flood_risk_rating = "high"
                elif zone in MODERATE_RISK_ZONES:
                    listing.flood_risk_rating = "moderate"
                else:
                    listing.flood_risk_rating = "minimal"

                logger.debug(f"Flood zone for {listing.address}: {zone} ({listing.flood_risk_rating})")
            else:
                listing.flood_risk_rating = "minimal"
                logger.debug(f"No flood data for {listing.address}, assuming minimal")

        except requests.RequestException:
            logger.exception(f"FEMA flood API failed for {listing.address}")

        time.sleep(0.3)
