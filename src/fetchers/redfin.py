import io
import logging
import time
from datetime import date

import pandas as pd

from src.config import AppConfig, LocationConfig
from src.fetchers.base import BaseFetcher
from src.models import Listing, ListingSource, PropertyType

logger = logging.getLogger(__name__)

# Map Redfin property type strings to our enum
PROPERTY_TYPE_MAP = {
    "Single Family Residential": PropertyType.SINGLE_FAMILY,
    "Condo/Co-op": PropertyType.CONDO,
    "Townhouse": PropertyType.TOWNHOUSE,
    "Multi-Family (2-4 Unit)": PropertyType.MULTI_FAMILY,
    "Multi-Family (5+ Unit)": PropertyType.MULTI_FAMILY,
    "Vacant Land": PropertyType.LAND,
}

# Map our filter property types to Redfin's uipt codes
UIPT_MAP = {
    "single_family": "1",
    "condo": "2",
    "townhouse": "3",
    "multi_family": "4",
}

# Map location type to Redfin region_type
REGION_TYPE_MAP = {
    "zip": 2,
    "county": 5,
    "city": 6,
}


class RedfinFetcher(BaseFetcher):
    BASE_URL = "https://www.redfin.com/stingray"

    def __init__(self, config: AppConfig):
        super().__init__(config)
        self._source_config = config.sources.redfin

    @property
    def source_name(self) -> str:
        return "redfin"

    @property
    def request_delay(self) -> float:
        return self._source_config.request_delay_seconds

    def _resolve_region(self, location: LocationConfig) -> tuple[int, int]:
        """Use Redfin autocomplete to get region_id and region_type."""
        if location.type == "zip":
            query = location.value
        elif location.type == "city":
            query = f"{location.value}, {location.state}"
        elif location.type == "county":
            query = f"{location.value} County, {location.state}"
        else:
            query = location.value

        url = f"{self.BASE_URL}/do/location-autocomplete"
        params = {"location": query, "v": "2"}
        resp = self.session.get(url, params=params)
        resp.raise_for_status()

        # Response format: "{}&&{JSON}" - strip the leading {}&&
        body = resp.text
        if body.startswith("{}&&"):
            body = body[4:]

        import json
        data = json.loads(body)

        # Parse the autocomplete results
        sections = data.get("payload", {}).get("sections", [])
        for section in sections:
            for row in section.get("rows", []):
                region_id = row.get("id")
                region_type = row.get("type")
                if region_id and region_type is not None:
                    # Match expected type
                    expected_type = REGION_TYPE_MAP.get(location.type)
                    if expected_type is None or region_type == expected_type:
                        return int(region_id.split("_")[-1]) if "_" in str(region_id) else int(region_id), region_type

        raise ValueError(f"Could not resolve region for: {query}")

    def fetch_for_location(self, location: LocationConfig) -> list[Listing]:
        region_id, region_type = self._resolve_region(location)
        time.sleep(1)  # Brief pause between autocomplete and search

        filters = self.config.search.filters

        # Build uipt parameter from configured property types
        uipt_codes = []
        for pt in filters.property_types:
            if pt in UIPT_MAP:
                uipt_codes.append(UIPT_MAP[pt])
        uipt = ",".join(uipt_codes) if uipt_codes else "1,2,3"

        params = {
            "al": 1,
            "region_id": region_id,
            "region_type": region_type,
            "status": 1,  # Active listings
            "num_homes": self._source_config.max_results_per_location,
            "uipt": uipt,
        }

        if filters.min_price > 0:
            params["min_price"] = filters.min_price
        if filters.max_price < 999999999:
            params["max_price"] = filters.max_price
        if filters.min_beds > 0:
            params["num_beds"] = filters.min_beds
        if filters.min_baths > 0:
            params["num_baths"] = int(filters.min_baths)
        if filters.min_sqft > 0:
            params["min_listing_approx_size"] = filters.min_sqft
        if filters.max_sqft < 999999:
            params["max_listing_approx_size"] = filters.max_sqft

        url = f"{self.BASE_URL}/api/gis-csv"
        resp = self.session.get(url, params=params)
        resp.raise_for_status()

        if not resp.text.strip() or "No results" in resp.text:
            logger.info(f"No results from Redfin for region {region_id}")
            return []

        try:
            df = pd.read_csv(io.StringIO(resp.text))
        except Exception:
            logger.exception("Failed to parse Redfin CSV response")
            return []

        listings = []
        for _, row in df.iterrows():
            try:
                listing = self._row_to_listing(row)
                if listing:
                    listings.append(listing)
            except Exception:
                logger.exception(f"Failed to parse Redfin row: {row.get('ADDRESS', 'unknown')}")

        return listings

    def _row_to_listing(self, row: pd.Series) -> Listing | None:
        """Map a Redfin CSV row to our Listing model."""
        address = str(row.get("ADDRESS", "")).strip()
        price = row.get("PRICE")

        if not address or pd.isna(price):
            return None

        # Parse property type
        prop_type_str = str(row.get("PROPERTY TYPE", ""))
        property_type = PROPERTY_TYPE_MAP.get(prop_type_str)

        # Parse optional numeric fields safely
        def safe_int(val) -> int | None:
            if pd.isna(val):
                return None
            try:
                return int(float(val))
            except (ValueError, TypeError):
                return None

        def safe_float(val) -> float | None:
            if pd.isna(val):
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        # Build source URL
        url_path = row.get("URL (SEE https://www.redfin.com/buy-a-home/comparative-market-analysis FOR INFO ON PRICING)", "")
        if pd.isna(url_path):
            url_path = ""
        source_url = f"https://www.redfin.com{url_path}" if url_path else ""

        # Parse list date
        list_date = None
        raw_date = row.get("LIST DATE") or row.get("SOLD DATE")
        if raw_date and not pd.isna(raw_date):
            try:
                list_date = date.fromisoformat(str(raw_date))
            except ValueError:
                pass

        listing = Listing(
            source=ListingSource.REDFIN,
            source_id=str(row.get("MLS#", row.get("REDFIN ESTIMATE", ""))),
            source_url=source_url,
            address=address,
            city=str(row.get("CITY", "")).strip(),
            state=str(row.get("STATE OR PROVINCE", "")).strip(),
            zip_code=str(row.get("ZIP OR POSTAL CODE", "")).strip(),
            price=int(float(price)),
            property_type=property_type,
            bedrooms=safe_int(row.get("BEDS")),
            bathrooms=safe_float(row.get("BATHS")),
            sqft=safe_int(row.get("SQUARE FEET")),
            lot_sqft=safe_int(row.get("LOT SIZE")),
            year_built=safe_int(row.get("YEAR BUILT")),
            hoa_monthly=safe_float(row.get("HOA/MONTH")),
            list_date=list_date,
            days_on_market=safe_int(row.get("DAYS ON MARKET")),
            latitude=safe_float(row.get("LATITUDE")),
            longitude=safe_float(row.get("LONGITUDE")),
            redfin_estimate=safe_int(row.get("REDFIN ESTIMATE")),
            status="active",
            source_urls={"redfin": source_url},
        )

        return listing
