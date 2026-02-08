"""
Security utilities for input sanitization and defense against
malicious data from external APIs.

External data sources (Redfin, Realtor.com, RapidAPI, FEMA, WalkScore, Google Maps)
return untrusted data that could contain:
- Injection payloads in address/text fields (XSS, template injection)
- Unexpected data types or out-of-range values
- Malformed URLs that could redirect or phish

All external data must pass through these sanitizers before use.
"""

import html
import re
import logging
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Allowed URL schemes
SAFE_SCHEMES = {"http", "https"}

# Allowed domains for listing source URLs
TRUSTED_DOMAINS = {
    "www.redfin.com",
    "redfin.com",
    "www.realtor.com",
    "realtor.com",
    "www.zillow.com",
    "zillow.com",
}

# Max lengths for string fields to prevent memory abuse
MAX_FIELD_LENGTHS = {
    "address": 200,
    "city": 100,
    "state": 10,
    "zip_code": 10,
    "county": 100,
    "source_id": 100,
    "source_url": 500,
    "status": 20,
    "flood_zone": 20,
    "flood_risk_rating": 20,
    "photo_url": 500,
}

# Pattern for detecting potential template injection (Jinja2)
TEMPLATE_INJECTION_PATTERN = re.compile(r"\{\{.*?\}\}|\{%.*?%\}|\{#.*?#\}")

# Pattern for detecting script tags and event handlers
XSS_PATTERNS = [
    re.compile(r"<\s*script", re.IGNORECASE),
    re.compile(r"javascript\s*:", re.IGNORECASE),
    re.compile(r"on\w+\s*=", re.IGNORECASE),
    re.compile(r"<\s*iframe", re.IGNORECASE),
    re.compile(r"<\s*object", re.IGNORECASE),
    re.compile(r"<\s*embed", re.IGNORECASE),
]


def sanitize_string(value: str, field_name: str = "unknown", max_length: int | None = None) -> str:
    """
    Sanitize a string field from external API data.
    - HTML-escapes dangerous characters
    - Truncates to max length
    - Strips template injection patterns
    - Removes null bytes
    """
    if not isinstance(value, str):
        value = str(value)

    # Remove null bytes
    value = value.replace("\x00", "")

    # Strip leading/trailing whitespace
    value = value.strip()

    # Check for template injection attempts
    if TEMPLATE_INJECTION_PATTERN.search(value):
        logger.warning(f"Potential template injection in {field_name}: {value[:100]}")
        value = TEMPLATE_INJECTION_PATTERN.sub("", value)

    # Check for XSS patterns
    for pattern in XSS_PATTERNS:
        if pattern.search(value):
            logger.warning(f"Potential XSS in {field_name}: {value[:100]}")
            value = html.escape(value)
            break

    # Enforce max length
    limit = max_length or MAX_FIELD_LENGTHS.get(field_name, 500)
    if len(value) > limit:
        value = value[:limit]

    return value


def sanitize_url(url: str, field_name: str = "url", allow_any_domain: bool = False) -> str:
    """
    Validate and sanitize a URL.
    - Only allows http/https schemes
    - Optionally restricts to trusted domains
    - Returns empty string if invalid
    """
    if not url or not isinstance(url, str):
        return ""

    url = url.strip()

    try:
        parsed = urlparse(url)
    except Exception:
        logger.warning(f"Malformed URL in {field_name}: {url[:100]}")
        return ""

    if parsed.scheme not in SAFE_SCHEMES:
        logger.warning(f"Unsafe URL scheme in {field_name}: {parsed.scheme}")
        return ""

    if not allow_any_domain and parsed.hostname and parsed.hostname not in TRUSTED_DOMAINS:
        # For photo URLs and API URLs, we allow any domain
        if field_name not in ("photo_url", "api_url"):
            logger.warning(f"Untrusted domain in {field_name}: {parsed.hostname}")
            return ""

    # Enforce length
    limit = MAX_FIELD_LENGTHS.get(field_name, 500)
    if len(url) > limit:
        return ""

    return url


def sanitize_numeric(value, field_name: str = "unknown", min_val: float | None = None, max_val: float | None = None) -> float | int | None:
    """
    Validate a numeric value is within expected range.
    Returns None if invalid.
    """
    if value is None:
        return None

    try:
        num = float(value)
    except (ValueError, TypeError):
        logger.warning(f"Non-numeric value in {field_name}: {value}")
        return None

    # Check for NaN/Inf
    if num != num or num == float("inf") or num == float("-inf"):
        return None

    if min_val is not None and num < min_val:
        logger.warning(f"Value below minimum in {field_name}: {num} < {min_val}")
        return None
    if max_val is not None and num > max_val:
        logger.warning(f"Value above maximum in {field_name}: {num} > {max_val}")
        return None

    return num


def sanitize_listing_data(data: dict) -> dict:
    """
    Sanitize all fields in a raw listing data dictionary before
    constructing a Listing model.
    """
    sanitized = {}

    # String fields
    for field in ("address", "city", "state", "zip_code", "county", "source_id", "status"):
        if field in data and data[field] is not None:
            sanitized[field] = sanitize_string(str(data[field]), field)

    # URL fields
    for field in ("source_url",):
        if field in data and data[field]:
            sanitized[field] = sanitize_url(str(data[field]), field)

    if "photo_url" in data and data["photo_url"]:
        sanitized["photo_url"] = sanitize_url(str(data["photo_url"]), "photo_url", allow_any_domain=True)

    # Numeric fields with ranges
    numeric_ranges = {
        "price": (0, 100_000_000),
        "bedrooms": (0, 50),
        "bathrooms": (0, 50),
        "sqft": (0, 500_000),
        "lot_sqft": (0, 50_000_000),
        "year_built": (1600, 2030),
        "stories": (0, 200),
        "garage_spaces": (0, 20),
        "hoa_monthly": (0, 50_000),
        "days_on_market": (0, 10_000),
        "latitude": (-90, 90),
        "longitude": (-180, 180),
        "redfin_estimate": (0, 100_000_000),
        "zestimate": (0, 100_000_000),
        "annual_tax": (0, 1_000_000),
        "walk_score": (0, 100),
        "transit_score": (0, 100),
        "bike_score": (0, 100),
        "school_rating": (0, 10),
    }

    for field, (min_v, max_v) in numeric_ranges.items():
        if field in data and data[field] is not None:
            sanitized[field] = sanitize_numeric(data[field], field, min_v, max_v)

    # Pass through other fields unchanged
    for key, value in data.items():
        if key not in sanitized:
            sanitized[key] = value

    return sanitized
