import logging
import os
import time
from datetime import date

import requests

from src.config import AppConfig, LocationConfig
from src.fetchers.base import BaseFetcher
from src.models import Listing, ListingSource, PriceHistoryEntry, PropertyType

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
        """Map a Realtor.com API result to our Listing model."""
        location = result.get("location", {})
        address_data = location.get("address", {})
        description = result.get("description", {})

        address = address_data.get("line", "")
        if not address:
            return None

        price = result.get("list_price")
        if not price:
            return None

        # Property type
        prop_type_str = description.get("type", "")
        property_type = PROPERTY_TYPE_MAP.get(prop_type_str)

        # Features
        has_garage = None
        garage_spaces = None
        has_pool = None
        if description.get("garage"):
            has_garage = True
            garage_spaces = description.get("garage")
        if result.get("flags", {}).get("is_pool_property"):
            has_pool = True

        # List date
        list_date = None
        raw_date = result.get("list_date")
        if raw_date:
            try:
                list_date = date.fromisoformat(raw_date[:10])
            except ValueError:
                pass

        # Photo
        photo_url = None
        photos = result.get("photos", [])
        if photos:
            photo_url = photos[0].get("href")

        # Coordinates
        coord = location.get("coordinate", {}) or {}

        # Source URL
        permalink = result.get("permalink", "")
        source_url = f"https://www.realtor.com/realestateandhomes-detail/{permalink}" if permalink else ""

        listing = Listing(
            source=ListingSource.REALTOR,
            source_id=str(result.get("property_id", "")),
            source_url=source_url,
            address=address,
            city=address_data.get("city", ""),
            state=address_data.get("state_code", ""),
            zip_code=str(address_data.get("postal_code", "")),
            county=address_data.get("county", ""),
            latitude=coord.get("lat"),
            longitude=coord.get("lon"),
            price=int(price),
            property_type=property_type,
            bedrooms=description.get("beds"),
            bathrooms=description.get("baths"),
            sqft=description.get("sqft"),
            lot_sqft=description.get("lot_sqft"),
            year_built=description.get("year_built"),
            stories=description.get("stories"),
            has_garage=has_garage,
            garage_spaces=garage_spaces,
            has_pool=has_pool,
            hoa_monthly=result.get("hoa", {}).get("fee") if result.get("hoa") else None,
            list_date=list_date,
            days_on_market=result.get("list_date_min_days_on_market"),
            photo_url=photo_url,
            status="active",
            source_urls={"realtor": source_url},
        )

        return listing
