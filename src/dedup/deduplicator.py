import hashlib
import logging
import re

from src.models import Listing

logger = logging.getLogger(__name__)

STREET_ABBREVS = {
    "street": "st", "avenue": "ave", "boulevard": "blvd",
    "drive": "dr", "lane": "ln", "court": "ct", "circle": "cir",
    "place": "pl", "road": "rd", "terrace": "ter", "way": "wy",
    "trail": "trl", "parkway": "pkwy", "highway": "hwy",
    "north": "n", "south": "s", "east": "e", "west": "w",
    "northeast": "ne", "northwest": "nw", "southeast": "se", "southwest": "sw",
    "apartment": "apt", "unit": "unit", "suite": "ste", "#": "apt",
}


def normalize_address(address: str, city: str, state: str, zip_code: str) -> str:
    """Produce a canonical address string for dedup matching."""
    addr = address.lower().strip()
    # Remove punctuation except #
    addr = re.sub(r"[.,']", "", addr)
    # Standardize abbreviations
    tokens = addr.split()
    normalized_tokens = []
    for token in tokens:
        normalized_tokens.append(STREET_ABBREVS.get(token, token))
    addr = " ".join(normalized_tokens)
    # Combine with city/state/zip
    canonical = f"{addr}|{city.lower().strip()}|{state.upper().strip()}|{zip_code.strip()[:5]}"
    return canonical


def address_fingerprint(normalized_address: str) -> str:
    """SHA-256 hash of normalized address for use as dedup key."""
    return hashlib.sha256(normalized_address.encode()).hexdigest()[:16]


def _is_geo_match(a: Listing, b: Listing, threshold_deg: float = 0.0001) -> bool:
    """Check if two listings are at roughly the same location (~11 meters)."""
    if a.latitude is None or a.longitude is None or b.latitude is None or b.longitude is None:
        return False
    return (
        abs(a.latitude - b.latitude) < threshold_deg
        and abs(a.longitude - b.longitude) < threshold_deg
    )


def _is_price_similar(a: Listing, b: Listing, tolerance: float = 0.05) -> bool:
    """Check if prices are within tolerance of each other."""
    if a.price == 0 or b.price == 0:
        return False
    ratio = min(a.price, b.price) / max(a.price, b.price)
    return ratio >= (1 - tolerance)


def _beds_baths_match(a: Listing, b: Listing) -> bool:
    """Check if bed/bath counts match."""
    if a.bedrooms is not None and b.bedrooms is not None and a.bedrooms != b.bedrooms:
        return False
    if a.bathrooms is not None and b.bathrooms is not None and a.bathrooms != b.bathrooms:
        return False
    return True


def _merge_listings(listings: list[Listing]) -> Listing:
    """Merge duplicate listings from different sources, keeping richest data."""
    if len(listings) == 1:
        return listings[0]

    # Use the first listing as base
    merged = listings[0].model_copy()

    # Merge source URLs
    all_source_urls = {}
    for listing in listings:
        all_source_urls.update(listing.source_urls)
        if listing.source_url:
            all_source_urls[listing.source.value] = listing.source_url
    merged.source_urls = all_source_urls

    # Take the best available data from each source
    for listing in listings[1:]:
        # Prefer non-None values
        if merged.sqft is None and listing.sqft is not None:
            merged.sqft = listing.sqft
        if merged.lot_sqft is None and listing.lot_sqft is not None:
            merged.lot_sqft = listing.lot_sqft
        if merged.year_built is None and listing.year_built is not None:
            merged.year_built = listing.year_built
        if merged.hoa_monthly is None and listing.hoa_monthly is not None:
            merged.hoa_monthly = listing.hoa_monthly
        if merged.annual_tax is None and listing.annual_tax is not None:
            merged.annual_tax = listing.annual_tax
        if merged.has_garage is None and listing.has_garage is not None:
            merged.has_garage = listing.has_garage
        if merged.has_basement is None and listing.has_basement is not None:
            merged.has_basement = listing.has_basement
        if merged.has_pool is None and listing.has_pool is not None:
            merged.has_pool = listing.has_pool
        if merged.has_fireplace is None and listing.has_fireplace is not None:
            merged.has_fireplace = listing.has_fireplace
        if merged.school_rating is None and listing.school_rating is not None:
            merged.school_rating = listing.school_rating
        if merged.photo_url is None and listing.photo_url is not None:
            merged.photo_url = listing.photo_url

        # Source-specific estimates
        if listing.source.value == "redfin" and listing.redfin_estimate:
            merged.redfin_estimate = listing.redfin_estimate
        if listing.source.value == "zillow" and listing.zestimate:
            merged.zestimate = listing.zestimate

        # Use longer price history
        if len(listing.price_history) > len(merged.price_history):
            merged.price_history = listing.price_history

        # Use more accurate coordinates (prefer ones that exist)
        if merged.latitude is None and listing.latitude is not None:
            merged.latitude = listing.latitude
            merged.longitude = listing.longitude

    return merged


class Deduplicator:
    """Deduplicates listings across multiple sources."""

    def process(self, listings: list[Listing]) -> list[Listing]:
        """Deduplicate a list of listings, returning unique listings with merged data."""
        # Phase 1: Normalize addresses
        for listing in listings:
            listing.normalized_address = normalize_address(
                listing.address, listing.city, listing.state, listing.zip_code
            )

        # Phase 2: Group by normalized address fingerprint
        groups: dict[str, list[Listing]] = {}
        for listing in listings:
            fp = address_fingerprint(listing.normalized_address)
            if fp not in groups:
                groups[fp] = []
            groups[fp].append(listing)

        # Phase 3: For ungrouped listings, try geo-matching
        singletons = [group[0] for fp, group in groups.items() if len(group) == 1]
        multi_groups = {fp: group for fp, group in groups.items() if len(group) > 1}

        # Check singletons against each other for geo-matches
        geo_merged = set()
        for i, a in enumerate(singletons):
            if i in geo_merged:
                continue
            for j in range(i + 1, len(singletons)):
                if j in geo_merged:
                    continue
                if (
                    _is_geo_match(a, singletons[j])
                    and _is_price_similar(a, singletons[j])
                    and _beds_baths_match(a, singletons[j])
                ):
                    fp_a = address_fingerprint(a.normalized_address)
                    if fp_a not in multi_groups:
                        multi_groups[fp_a] = [a]
                    multi_groups[fp_a].append(singletons[j])
                    geo_merged.add(i)
                    geo_merged.add(j)
                    logger.info(f"Geo-matched: '{a.address}' and '{singletons[j].address}'")

        # Phase 4: Merge each group
        result = []
        seen_fps = set()

        for fp, group in multi_groups.items():
            merged = _merge_listings(group)
            result.append(merged)
            seen_fps.add(fp)
            if len(group) > 1:
                sources = [l.source.value for l in group]
                logger.info(f"Merged {len(group)} listings for '{merged.address}' from {sources}")

        # Add remaining singletons that weren't geo-matched
        for i, listing in enumerate(singletons):
            if i not in geo_merged:
                fp = address_fingerprint(listing.normalized_address)
                if fp not in seen_fps:
                    result.append(listing)

        logger.info(f"Dedup: {len(listings)} -> {len(result)} unique listings")
        return result
