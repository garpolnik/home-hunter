import logging
import os
import time
from datetime import date

import requests

from src.config import AppConfig, LocationConfig
from src.fetchers.base import BaseFetcher
from src.models import Listing, ListingSource, PriceHistoryEntry, PropertyType
from src.security import sanitize_string, sanitize_url, sanitize_numeric

logger = logging.getLogger(__name__)

PROPERTY_TYPE_MAP = {
    "single_family": PropertyType.SINGLE_FAMILY,
    "condo": PropertyType.CONDO,
    "townhome": PropertyType.TOWNHOUSE,
    "townhouse": PropertyType.TOWNHOUSE,
    "multi_family": PropertyType.MULTI_FAMILY,
    "land": PropertyType.LAND,
}

RAPIDAPI_HOST = "realty-in-us.p.rapidapi.com"


class RealtorFetcher(BaseFetcher):
    def __init__(self, config: AppConfig):
        super().__init__(config)
        self._source_config = config.sources.realtor
        self.api_key = os.environ.get("RAPIDAPI_KEY")
        if not self.api_key:
            raise ValueError("RAPIDAPI_KEY environment variable not set")
        self.session.headers.update({
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": RAPIDAPI_HOST,
        })

    @property
    def source_name(self) -> str:
        return "realtor"

    @property
    def request_delay(self) -> float:
        return self._source_config.request_delay_seconds

    def _build_location_query(self, location: LocationConfig) -> str:
        """Build location string for the API."""
        if location.type == "zip":
            return location.value
        elif location.type == "city":
            return f"{location.value}, {location.state}"
        elif location.type == "county":
            return f"{location.value} County, {location.state}"
        return location.value

    def fetch_for_location(self, location: LocationConfig) -> list[Listing]:
        filters = self.config.search.filters
        query = self._build_location_query(location)

        url = f"https://{RAPIDAPI_HOST}/properties/v3/list"

        # Build property type filter
        prop_types = []
        for pt in filters.property_types:
            if pt == "single_family":
                prop_types.append("single_family")
            elif pt == "condo":
                prop_types.append("condos")
            elif pt == "townhouse":
                prop_types.append("townhomes")
            elif pt == "multi_family":
                prop_types.append("multi_family")

        payload = {
            "limit": self._source_config.max_results_per_location,
            "offset": 0,
            "status": ["for_sale"],
            "sort": {"direction": "desc", "field": "list_date"},
        }

        # Add location - use postal_code or city
        if location.type == "zip":
            payload["postal_code"] = location.value
        elif location.type == "city":
            payload["city"] = location.value
            payload["state_code"] = location.state
        elif location.type == "county":
            payload["county"] = location.value
            payload["state_code"] = location.state

        # Add filters
        if filters.min_price > 0 or filters.max_price < 999999999:
            payload["list_price"] = {}
            if filters.min_price > 0:
                payload["list_price"]["min"] = filters.min_price
            if filters.max_price < 999999999:
                payload["list_price"]["max"] = filters.max_price

        if filters.min_beds > 0:
            payload["beds_min"] = filters.min_beds
        if filters.min_baths > 0:
            payload["baths_min"] = int(filters.min_baths)
        if filters.min_sqft > 0:
            payload["sqft_min"] = filters.min_sqft
        if prop_types:
            payload["type"] = prop_types

        try:
            resp = self.session.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException:
            logger.exception(f"Realtor API request failed for {query}")
            return []

        results = data.get("data", {}).get("home_search", {}).get("results", [])
        if not results:
            logger.info(f"No results from Realtor.com for {query}")
            return []

        listings = []
        for result in results:
            try:
                listing = self._result_to_listing(result)
                if listing:
                    listings.append(listing)
            except Exception:
                logger.exception("Failed to parse Realtor.com result")

        return listings

    def _result_to_listing(self, result: dict) -> Listing | None:
        """Map a Realtor.com API result to our Listing model, with input sanitization."""
        location = result.get("location", {})
        address_data = location.get("address", {})
        description = result.get("description", {})

        address = sanitize_string(str(address_data.get("line", "")), "address")
        if not address:
            return None

        price = sanitize_numeric(result.get("list_price"), "price", 0, 100_000_000)
        if not price:
            return None

        # Property type
        prop_type_str = str(description.get("type", ""))
        property_type = PROPERTY_TYPE_MAP.get(prop_type_str)

        # Features
        has_garage = None
        garage_spaces = None
        has_pool = None
        if description.get("garage"):
            has_garage = True
            garage_spaces = sanitize_numeric(description.get("garage"), "garage_spaces", 0, 20)
            garage_spaces = int(garage_spaces) if garage_spaces else None
        if result.get("flags", {}).get("is_pool_property"):
            has_pool = True

        # List date
        list_date = None
        raw_date = result.get("list_date")
        if raw_date and isinstance(raw_date, str):
            try:
                list_date = date.fromisoformat(raw_date[:10])
            except ValueError:
                pass

        # Photo - allow any domain since these are CDN URLs
        photo_url = None
        photos = result.get("photos", [])
        if photos and isinstance(photos, list):
            raw_photo = photos[0].get("href", "") if isinstance(photos[0], dict) else ""
            photo_url = sanitize_url(str(raw_photo), "photo_url", allow_any_domain=True)

        # Coordinates
        coord = location.get("coordinate", {}) or {}

        # Source URL
        permalink = sanitize_string(str(result.get("permalink", "")), "source_id")
        raw_url = f"https://www.realtor.com/realestateandhomes-detail/{permalink}" if permalink else ""
        source_url = sanitize_url(raw_url, "source_url")

        listing = Listing(
            source=ListingSource.REALTOR,
            source_id=sanitize_string(str(result.get("property_id", "")), "source_id"),
            source_url=source_url,
            address=address,
            city=sanitize_string(str(address_data.get("city", "")), "city"),
            state=sanitize_string(str(address_data.get("state_code", "")), "state"),
            zip_code=sanitize_string(str(address_data.get("postal_code", "")), "zip_code"),
            county=sanitize_string(str(address_data.get("county", "")), "county"),
            latitude=sanitize_numeric(coord.get("lat"), "latitude", -90, 90),
            longitude=sanitize_numeric(coord.get("lon"), "longitude", -180, 180),
            price=int(price),
            property_type=property_type,
            bedrooms=sanitize_numeric(description.get("beds"), "bedrooms", 0, 50),
            bathrooms=sanitize_numeric(description.get("baths"), "bathrooms", 0, 50),
            sqft=sanitize_numeric(description.get("sqft"), "sqft", 0, 500_000),
            lot_sqft=sanitize_numeric(description.get("lot_sqft"), "lot_sqft", 0, 50_000_000),
            year_built=sanitize_numeric(description.get("year_built"), "year_built", 1600, 2030),
            stories=sanitize_numeric(description.get("stories"), "stories", 0, 200),
            has_garage=has_garage,
            garage_spaces=garage_spaces,
            has_pool=has_pool,
            hoa_monthly=sanitize_numeric(
                result.get("hoa", {}).get("fee") if result.get("hoa") else None,
                "hoa_monthly", 0, 50_000
            ),
            list_date=list_date,
            days_on_market=sanitize_numeric(
                result.get("list_date_min_days_on_market"), "days_on_market", 0, 10_000
            ),
            photo_url=photo_url,
            status="active",
            source_urls={"realtor": source_url},
        )

        return listing
