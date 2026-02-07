from datetime import date

from src.models import AreaStats, Listing


def score_price_vs_estimate(listing: Listing, area_stats: AreaStats) -> float:
    """
    Compare listing price to Redfin/Zestimate.
    Score 1.0 if price is 15%+ below estimate.
    Score 0.5 if price equals estimate.
    Score 0.0 if price is 15%+ above estimate.
    """
    estimate = listing.redfin_estimate or listing.zestimate
    if not estimate or estimate == 0:
        return 0.5
    ratio = listing.price / estimate
    return max(0.0, min(1.0, (1.15 - ratio) / 0.30))


def score_price_per_sqft(listing: Listing, area_stats: AreaStats) -> float:
    """
    Compare price/sqft to area median.
    Score 1.0 if 30%+ below median. Score 0.5 at median. Score 0.0 if 30%+ above.
    """
    if not listing.sqft or not area_stats.median_price_per_sqft:
        return 0.5
    ppsf = listing.price / listing.sqft
    ratio = ppsf / area_stats.median_price_per_sqft
    return max(0.0, min(1.0, (1.30 - ratio) / 0.60))


def score_days_on_market(listing: Listing, area_stats: AreaStats) -> float:
    """
    Longer DOM = more negotiating power.
    Score 0.0 at 0 days. Score 1.0 at 3x area median or 120+ days.
    """
    if listing.days_on_market is None:
        return 0.5
    median_dom = area_stats.median_dom or 30
    threshold = max(median_dom * 3, 120)
    return min(1.0, listing.days_on_market / threshold)


def score_price_reductions(listing: Listing, area_stats: AreaStats) -> float:
    """
    Score based on cumulative price reductions.
    Score 1.0 if cumulative reduction >= 15%. Score 0.0 if no reductions.
    """
    if not listing.price_history or len(listing.price_history) < 2:
        return 0.0
    original_price = listing.price_history[0].price
    if original_price == 0:
        return 0.0
    total_reduction = (original_price - listing.price) / original_price
    if total_reduction <= 0:
        return 0.0
    return min(1.0, total_reduction / 0.15)


def score_lot_size(listing: Listing, area_stats: AreaStats) -> float:
    """Score based on lot size vs area median. 2x median = perfect score."""
    if not listing.lot_sqft or not area_stats.median_lot_size:
        return 0.5
    ratio = listing.lot_sqft / area_stats.median_lot_size
    return max(0.0, min(1.0, ratio / 2.0))


def score_hoa(listing: Listing, area_stats: AreaStats) -> float:
    """No HOA = 1.0, escalating penalty for higher HOA fees."""
    if listing.hoa_monthly is None or listing.hoa_monthly == 0:
        return 1.0
    if listing.hoa_monthly <= 100:
        return 0.8
    if listing.hoa_monthly <= 200:
        return 0.7
    if listing.hoa_monthly <= 350:
        return 0.5
    if listing.hoa_monthly <= 500:
        return 0.3
    return 0.1


def score_tax_rate(listing: Listing, area_stats: AreaStats) -> float:
    """
    Lower effective tax rate = higher score.
    Score 1.0 at 0.5%, Score 0.5 at 2.0%, Score 0.0 at 4.0%.
    """
    if not listing.annual_tax or not listing.price:
        return 0.5
    effective_rate = listing.annual_tax / listing.price
    return max(0.0, min(1.0, (0.04 - effective_rate) / 0.035))


def score_school_rating(listing: Listing, area_stats: AreaStats) -> float:
    """GreatSchools 1-10 scale mapped linearly to 0-1."""
    if listing.school_rating is None:
        return 0.5
    return min(1.0, max(0.0, listing.school_rating / 10.0))


def score_walk_score(listing: Listing, area_stats: AreaStats) -> float:
    """Walk Score 0-100 mapped to 0-1."""
    if listing.walk_score is None:
        return 0.5
    return listing.walk_score / 100.0


def score_flood_risk(listing: Listing, area_stats: AreaStats) -> float:
    """Minimal risk = 1.0, Moderate = 0.5, High = 0.1."""
    mapping = {"minimal": 1.0, "moderate": 0.5, "high": 0.1}
    return mapping.get(listing.flood_risk_rating, 0.5)


def score_commute(listing: Listing, area_stats: AreaStats) -> float:
    """
    Average commute to all configured targets.
    Under 20 min = 1.0, 60+ min = 0.0.
    """
    if not listing.commute_minutes:
        return 0.5
    times = list(listing.commute_minutes.values())
    if not times:
        return 0.5
    avg = sum(times) / len(times)
    return max(0.0, min(1.0, (60 - avg) / 40))


def score_property_age(listing: Listing, area_stats: AreaStats) -> float:
    """Newer = higher score, reflecting lower maintenance risk."""
    if not listing.year_built:
        return 0.5
    age = date.today().year - listing.year_built
    if age <= 5:
        return 1.0
    if age <= 20:
        return 0.8
    if age <= 40:
        return 0.6
    if age <= 60:
        return 0.4
    return 0.2


def score_bed_bath_value(listing: Listing, area_stats: AreaStats) -> float:
    """More beds+baths per dollar = better value."""
    if not listing.bedrooms or not listing.price:
        return 0.5
    baths = listing.bathrooms or 0
    rooms = listing.bedrooms + baths
    value_per_100k = rooms / (listing.price / 100_000)
    return min(1.0, value_per_100k / 3.0)


def score_features(listing: Listing, area_stats: AreaStats) -> float:
    """Bonus for desirable features: garage, basement, pool, fireplace."""
    bonus = 0.0
    if listing.has_garage:
        bonus += 0.3
    if listing.has_basement:
        bonus += 0.3
    if listing.has_pool:
        bonus += 0.2
    if listing.has_fireplace:
        bonus += 0.2
    return min(1.0, bonus)
