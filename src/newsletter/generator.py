import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import jinja2

from src.config import AppConfig
from src.models import AreaStats, Listing

logger = logging.getLogger(__name__)

# Pretty names for score criteria
CRITERIA_LABELS = {
    "price_vs_estimate": "Price vs Estimate",
    "price_per_sqft": "Price/SqFt",
    "days_on_market": "Days on Market",
    "price_reductions": "Price Drops",
    "lot_size_value": "Lot Size",
    "hoa_cost": "HOA Cost",
    "tax_rate": "Tax Rate",
    "school_rating": "Schools",
    "walk_score": "Walkability",
    "flood_risk": "Flood Risk",
    "commute_time": "Commute",
    "property_age": "Property Age",
    "bed_bath_value": "Bed/Bath Value",
    "features_bonus": "Features",
}


@dataclass
class Highlight:
    text: str
    css_class: str  # "highlight-good", "highlight-warn", or ""


def _build_highlights(listing: Listing) -> list[Highlight]:
    """Generate human-readable highlight tags for a listing."""
    highlights = []

    # Price vs estimate
    estimate = listing.redfin_estimate or listing.zestimate
    if estimate and estimate > 0:
        diff_pct = (estimate - listing.price) / estimate * 100
        if diff_pct > 5:
            highlights.append(Highlight(f"{diff_pct:.0f}% below estimate", "highlight-good"))
        elif diff_pct < -5:
            highlights.append(Highlight(f"{abs(diff_pct):.0f}% above estimate", "highlight-warn"))

    # Days on market
    if listing.days_on_market is not None:
        if listing.days_on_market > 90:
            highlights.append(Highlight(f"{listing.days_on_market} days on market", "highlight-good"))
        elif listing.days_on_market <= 7:
            highlights.append(Highlight("Just listed", ""))

    # Price reductions
    if listing.price_history and len(listing.price_history) >= 2:
        original = listing.price_history[0].price
        if original > listing.price:
            drops = sum(1 for e in listing.price_history if e.event == "Price Change")
            reduction_pct = (original - listing.price) / original * 100
            highlights.append(Highlight(f"{drops} price drop(s), -{reduction_pct:.0f}%", "highlight-good"))

    # HOA
    if listing.hoa_monthly and listing.hoa_monthly > 300:
        highlights.append(Highlight(f"${listing.hoa_monthly:.0f}/mo HOA", "highlight-warn"))
    elif listing.hoa_monthly is not None and listing.hoa_monthly == 0:
        highlights.append(Highlight("No HOA", "highlight-good"))

    # Walk score
    if listing.walk_score is not None:
        if listing.walk_score >= 70:
            highlights.append(Highlight(f"Walk Score: {listing.walk_score}", "highlight-good"))

    # Flood risk
    if listing.flood_risk_rating == "high":
        highlights.append(Highlight("High flood risk", "highlight-warn"))

    return highlights


def _get_top_scores(listing: Listing, limit: int = 5) -> list[tuple[str, float]]:
    """Get the top N scoring criteria for display."""
    if not listing.score_breakdown:
        return []
    sorted_scores = sorted(listing.score_breakdown.items(), key=lambda x: x[1], reverse=True)
    return [(CRITERIA_LABELS.get(name, name), value) for name, value in sorted_scores[:limit]]


@dataclass
class NewsletterSection:
    name: str
    listings: list


class NewsletterGenerator:
    def __init__(self, config: AppConfig):
        self.config = config
        template_dir = Path(__file__).parent / "templates"
        self.env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(str(template_dir)),
            autoescape=True,
        )

    def _prepare_listing(self, listing: Listing) -> Listing:
        """Add display-ready attributes to a listing."""
        listing.highlights = _build_highlights(listing)
        listing.top_scores = _get_top_scores(listing)
        return listing

    def _build_sections(
        self, new_listings: list[Listing], all_listings: list[Listing]
    ) -> list[NewsletterSection]:
        """Build newsletter sections from listings."""
        sections = []

        # Top Deals - highest scoring listings
        threshold = self.config.scoring.top_deal_threshold
        top_deals = sorted(
            [l for l in all_listings if l.deal_score is not None and l.deal_score >= threshold],
            key=lambda l: l.deal_score,
            reverse=True,
        )[:10]
        sections.append(NewsletterSection(
            name="Top Deals",
            listings=[self._prepare_listing(l) for l in top_deals],
        ))

        # New This Month
        new_sorted = sorted(
            [l for l in new_listings if l.deal_score is not None],
            key=lambda l: l.deal_score,
            reverse=True,
        )[:15]
        sections.append(NewsletterSection(
            name="New This Month",
            listings=[self._prepare_listing(l) for l in new_sorted],
        ))

        # Price Drops
        price_drops = []
        for l in all_listings:
            if l.price_history and len(l.price_history) >= 2:
                original = l.price_history[0].price
                if original > l.price:
                    l._reduction_pct = (original - l.price) / original
                    price_drops.append(l)
        price_drops.sort(key=lambda l: l._reduction_pct, reverse=True)
        sections.append(NewsletterSection(
            name="Price Drops",
            listings=[self._prepare_listing(l) for l in price_drops[:10]],
        ))

        return sections

    def render(
        self,
        new_listings: list[Listing],
        all_listings: list[Listing],
        area_stats: dict[str, AreaStats],
    ) -> str:
        """Render the newsletter HTML."""
        sections = self._build_sections(new_listings, all_listings)

        scored = [l for l in all_listings if l.deal_score is not None]
        avg_score = round(sum(l.deal_score for l in scored) / len(scored), 1) if scored else None
        top_deal_count = sum(
            1 for l in all_listings
            if l.deal_score is not None and l.deal_score >= self.config.scoring.top_deal_threshold
        )

        template = self.env.get_template("newsletter.html")
        return template.render(
            sections=sections,
            area_stats=area_stats,
            run_date=datetime.now().strftime("%B %Y"),
            total_new=len(new_listings),
            total_tracked=len(all_listings),
            top_deal_count=top_deal_count,
            avg_score=avg_score,
        )
