"""
Market analyzer that studies current listing data to classify market conditions
and recommend DOM filter settings.

Analyzes:
- Median and percentile days-on-market across all listings
- Price reduction frequency and magnitude (seller desperation signal)
- Inventory turnover rate (new listings vs total tracked)
- Listing absorption rate (how fast homes go pending/sold)

Produces a MarketReport with a recommended max_dom setting tailored to current
market velocity.
"""

import logging
import statistics
from dataclasses import dataclass, field
from datetime import datetime

from src.models import AreaStats, Listing

logger = logging.getLogger(__name__)


class MarketCondition:
    """Market condition classifications."""
    VERY_HOT = "very_hot"      # Homes selling in days
    HOT = "hot"                # Strong seller's market
    NORMAL = "normal"          # Balanced market
    SLOW = "slow"              # Buyer's market
    VERY_SLOW = "very_slow"    # Stagnant, lots of sitting inventory


# Thresholds for median DOM classification (in days)
# Based on NAR (National Association of Realtors) benchmarks:
#   < 14 days median = very hot
#   14-29 days = hot
#   30-59 days = normal/balanced
#   60-89 days = slow
#   90+ days = very slow
DOM_THRESHOLDS = {
    MarketCondition.VERY_HOT: (0, 14),
    MarketCondition.HOT: (14, 30),
    MarketCondition.NORMAL: (30, 60),
    MarketCondition.SLOW: (60, 90),
    MarketCondition.VERY_SLOW: (90, float("inf")),
}

# Recommended settings per market condition
RECOMMENDED_SETTINGS = {
    MarketCondition.VERY_HOT: {
        "max_dom_multiplier": 3.0,
        "max_dom_absolute": 60,
        "description": (
            "Homes are selling extremely fast (median under 14 days). "
            "Any listing sitting longer than a few weeks likely has issues. "
            "Recommended: filter out listings older than 60 days."
        ),
    },
    MarketCondition.HOT: {
        "max_dom_multiplier": 3.5,
        "max_dom_absolute": 120,
        "description": (
            "Strong seller's market (median 14-30 days). "
            "Most desirable homes go under contract within a month. "
            "Recommended: filter out listings older than 120 days."
        ),
    },
    MarketCondition.NORMAL: {
        "max_dom_multiplier": 4.0,
        "max_dom_absolute": 180,
        "description": (
            "Balanced market (median 30-60 days). "
            "Homes are selling at a healthy pace with room for negotiation. "
            "Recommended: filter out listings older than 180 days."
        ),
    },
    MarketCondition.SLOW: {
        "max_dom_multiplier": 5.0,
        "max_dom_absolute": 270,
        "description": (
            "Buyer's market (median 60-90 days). "
            "Inventory is building up and sellers may be flexible on price. "
            "Recommended: filter out listings older than 270 days."
        ),
    },
    MarketCondition.VERY_SLOW: {
        "max_dom_multiplier": 6.0,
        "max_dom_absolute": 365,
        "description": (
            "Stagnant market (median 90+ days). "
            "Significant inventory sitting on the market. "
            "Many sellers are likely motivated and open to offers well below asking. "
            "Recommended: filter out listings older than 365 days."
        ),
    },
}


@dataclass
class MarketReport:
    """Complete market analysis report."""

    # Overall classification
    condition: str = MarketCondition.NORMAL
    condition_label: str = ""

    # DOM statistics
    median_dom: float = 0
    mean_dom: float = 0
    dom_25th_percentile: float = 0
    dom_75th_percentile: float = 0
    dom_90th_percentile: float = 0

    # Price reduction analysis
    pct_with_reductions: float = 0       # % of listings that had at least one price drop
    avg_reduction_pct: float = 0         # Average reduction as % of original price
    total_listings_analyzed: int = 0

    # Inventory analysis
    new_listing_pct: float = 0           # % of listings that are new this run
    total_active: int = 0

    # Recommended settings
    recommended_max_dom_multiplier: float = 4.0
    recommended_max_dom_absolute: int = 180
    recommendation_description: str = ""

    # Per-ZIP breakdown
    zip_conditions: dict = field(default_factory=dict)  # {zip: {condition, median_dom, ...}}

    # Timestamp
    analyzed_at: str = ""


def _percentile(sorted_data: list[float], pct: float) -> float:
    """Calculate percentile from sorted data."""
    if not sorted_data:
        return 0
    k = (len(sorted_data) - 1) * (pct / 100)
    f = int(k)
    c = f + 1
    if c >= len(sorted_data):
        return sorted_data[-1]
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


def _classify_market(median_dom: float) -> str:
    """Classify market condition based on median DOM."""
    for condition, (low, high) in DOM_THRESHOLDS.items():
        if low <= median_dom < high:
            return condition
    return MarketCondition.VERY_SLOW


CONDITION_LABELS = {
    MarketCondition.VERY_HOT: "Very Hot (Strong Seller's Market)",
    MarketCondition.HOT: "Hot (Seller's Market)",
    MarketCondition.NORMAL: "Normal (Balanced Market)",
    MarketCondition.SLOW: "Slow (Buyer's Market)",
    MarketCondition.VERY_SLOW: "Very Slow (Stagnant Market)",
}


def analyze_market(
    all_listings: list[Listing],
    new_listings: list[Listing],
    area_stats: dict[str, AreaStats],
) -> MarketReport:
    """
    Analyze current market conditions from listing data and produce
    a MarketReport with recommended DOM filter settings.
    """
    report = MarketReport()
    report.analyzed_at = datetime.now().isoformat()
    report.total_active = len(all_listings)
    report.total_listings_analyzed = len(all_listings)

    # Collect DOM values
    dom_values = [
        l.days_on_market for l in all_listings
        if l.days_on_market is not None
    ]

    if not dom_values:
        logger.warning("No DOM data available for market analysis. Using defaults.")
        report.condition = MarketCondition.NORMAL
        report.condition_label = CONDITION_LABELS[MarketCondition.NORMAL]
        settings = RECOMMENDED_SETTINGS[MarketCondition.NORMAL]
        report.recommended_max_dom_multiplier = settings["max_dom_multiplier"]
        report.recommended_max_dom_absolute = settings["max_dom_absolute"]
        report.recommendation_description = settings["description"]
        return report

    dom_sorted = sorted(dom_values)

    # DOM statistics
    report.median_dom = statistics.median(dom_sorted)
    report.mean_dom = statistics.mean(dom_sorted)
    report.dom_25th_percentile = _percentile(dom_sorted, 25)
    report.dom_75th_percentile = _percentile(dom_sorted, 75)
    report.dom_90th_percentile = _percentile(dom_sorted, 90)

    # Classify overall market
    report.condition = _classify_market(report.median_dom)
    report.condition_label = CONDITION_LABELS[report.condition]

    # Price reduction analysis
    listings_with_reductions = 0
    reduction_pcts = []
    for listing in all_listings:
        if listing.price_history and len(listing.price_history) >= 2:
            original = listing.price_history[0].price
            if original > 0 and listing.price < original:
                listings_with_reductions += 1
                reduction_pcts.append((original - listing.price) / original * 100)

    if all_listings:
        report.pct_with_reductions = listings_with_reductions / len(all_listings) * 100
    if reduction_pcts:
        report.avg_reduction_pct = statistics.mean(reduction_pcts)

    # New listing percentage (inventory freshness)
    if all_listings:
        report.new_listing_pct = len(new_listings) / len(all_listings) * 100

    # Factor price reductions into the recommendation:
    # High reduction rate (>30%) suggests market is slower than DOM alone indicates
    # Low reduction rate (<10%) suggests market is hotter than DOM alone indicates
    adjusted_condition = report.condition
    if report.pct_with_reductions > 30 and report.condition in (
        MarketCondition.VERY_HOT, MarketCondition.HOT
    ):
        # Many price reductions despite fast DOM - market may be cooling
        adjusted_condition = MarketCondition.NORMAL
        logger.info(
            f"Market adjusted from {report.condition} to {adjusted_condition} "
            f"due to high price reduction rate ({report.pct_with_reductions:.0f}%)"
        )
    elif report.pct_with_reductions < 10 and report.condition in (
        MarketCondition.SLOW, MarketCondition.VERY_SLOW
    ):
        # Very few reductions despite high DOM - sellers are firm, market may be firmer
        adjusted_condition = MarketCondition.NORMAL
        logger.info(
            f"Market adjusted from {report.condition} to {adjusted_condition} "
            f"due to low price reduction rate ({report.pct_with_reductions:.0f}%)"
        )

    # Get recommended settings
    settings = RECOMMENDED_SETTINGS[adjusted_condition]
    report.recommended_max_dom_multiplier = settings["max_dom_multiplier"]
    report.recommended_max_dom_absolute = settings["max_dom_absolute"]
    report.recommendation_description = settings["description"]

    # Per-ZIP breakdown
    for zip_code, stats in area_stats.items():
        if stats.median_dom is not None:
            zip_condition = _classify_market(stats.median_dom)
            zip_settings = RECOMMENDED_SETTINGS[zip_condition]
            report.zip_conditions[zip_code] = {
                "condition": zip_condition,
                "condition_label": CONDITION_LABELS[zip_condition],
                "median_dom": stats.median_dom,
                "sample_size": stats.sample_size,
                "recommended_max_dom": zip_settings["max_dom_absolute"],
            }

    logger.info(
        f"Market Analysis: {report.condition_label} | "
        f"Median DOM: {report.median_dom:.0f} days | "
        f"Price reductions: {report.pct_with_reductions:.0f}% of listings | "
        f"Recommended max DOM: {report.recommended_max_dom_absolute} days"
    )

    return report


def format_market_report(report: MarketReport) -> str:
    """Format the market report as a readable text summary for logging."""
    lines = [
        "=" * 60,
        "MARKET ANALYSIS REPORT",
        "=" * 60,
        f"Overall Condition:  {report.condition_label}",
        "",
        "Days on Market Statistics:",
        f"  Median:           {report.median_dom:.0f} days",
        f"  Mean:             {report.mean_dom:.0f} days",
        f"  25th percentile:  {report.dom_25th_percentile:.0f} days",
        f"  75th percentile:  {report.dom_75th_percentile:.0f} days",
        f"  90th percentile:  {report.dom_90th_percentile:.0f} days",
        "",
        "Inventory:",
        f"  Total active:     {report.total_active}",
        f"  New this run:     {report.new_listing_pct:.1f}%",
        "",
        "Price Reductions:",
        f"  Listings with reductions: {report.pct_with_reductions:.1f}%",
        f"  Average reduction:        {report.avg_reduction_pct:.1f}%",
        "",
        "Recommended Settings:",
        f"  max_dom_multiplier: {report.recommended_max_dom_multiplier}",
        f"  max_dom_absolute:   {report.recommended_max_dom_absolute} days",
        "",
        report.recommendation_description,
    ]

    if report.zip_conditions:
        lines.append("")
        lines.append("Per-ZIP Breakdown:")
        for zip_code, data in sorted(report.zip_conditions.items()):
            lines.append(
                f"  {zip_code}: {data['condition_label']} "
                f"(median {data['median_dom']} days, "
                f"{data['sample_size']} listings, "
                f"recommended max {data['recommended_max_dom']} days)"
            )

    lines.append("=" * 60)
    return "\n".join(lines)
