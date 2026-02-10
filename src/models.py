from pydantic import BaseModel, Field
from typing import Any, Optional
from datetime import date, datetime
from enum import Enum


class PropertyType(str, Enum):
    SINGLE_FAMILY = "single_family"
    CONDO = "condo"
    TOWNHOUSE = "townhouse"
    MULTI_FAMILY = "multi_family"
    LAND = "land"


class ListingSource(str, Enum):
    REDFIN = "redfin"
    REALTOR = "realtor"
    ZILLOW = "zillow"


class PriceHistoryEntry(BaseModel):
    date: date
    price: int
    event: str  # "Listed", "Price Change", "Sold", "Pending"


class Listing(BaseModel):
    # Identity
    id: Optional[str] = None
    source: ListingSource
    source_id: str
    source_url: str = ""

    # Location
    address: str
    city: str
    state: str
    zip_code: str
    county: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Property basics
    price: int
    property_type: Optional[PropertyType] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    sqft: Optional[int] = None
    lot_sqft: Optional[int] = None
    year_built: Optional[int] = None
    stories: Optional[int] = None

    # Features
    has_garage: Optional[bool] = None
    garage_spaces: Optional[int] = None
    has_basement: Optional[bool] = None
    has_pool: Optional[bool] = None
    has_fireplace: Optional[bool] = None
    hoa_monthly: Optional[float] = None

    # Listing metadata
    list_date: Optional[date] = None
    days_on_market: Optional[int] = None
    status: str = "active"
    price_history: list[PriceHistoryEntry] = []
    photo_url: Optional[str] = None

    # Estimates
    redfin_estimate: Optional[int] = None
    zestimate: Optional[int] = None

    # Tax info
    annual_tax: Optional[float] = None
    tax_rate: Optional[float] = None

    # Enrichment data (populated post-fetch)
    walk_score: Optional[int] = None
    transit_score: Optional[int] = None
    bike_score: Optional[int] = None
    flood_zone: Optional[str] = None
    flood_risk_rating: Optional[str] = None
    school_rating: Optional[float] = None
    commute_minutes: Optional[dict] = None

    # Scoring (populated by scoring engine)
    deal_score: Optional[float] = None
    score_breakdown: Optional[dict] = None

    # Dedup
    normalized_address: Optional[str] = None
    first_seen: Optional[datetime] = None
    last_seen: Optional[datetime] = None
    is_new: bool = True

    # Multi-source tracking
    source_urls: dict[str, str] = {}  # {source_name: url}

    # Display attributes (populated by newsletter generator)
    highlights: Optional[list[Any]] = None
    top_scores: Optional[list[Any]] = None


class AreaStats(BaseModel):
    area_key: str
    median_price: Optional[int] = None
    median_price_per_sqft: Optional[float] = None
    median_lot_size: Optional[int] = None
    median_dom: Optional[int] = None
    sample_size: int = 0
    computed_at: Optional[datetime] = None
