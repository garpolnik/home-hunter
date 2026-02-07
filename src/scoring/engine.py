import logging

from src.config import AppConfig
from src.models import AreaStats, Listing
from src.scoring import criteria

logger = logging.getLogger(__name__)

# Map criterion names to scoring functions
CRITERIA_FUNCS = {
    "price_vs_estimate": criteria.score_price_vs_estimate,
    "price_per_sqft": criteria.score_price_per_sqft,
    "days_on_market": criteria.score_days_on_market,
    "price_reductions": criteria.score_price_reductions,
    "lot_size_value": criteria.score_lot_size,
    "hoa_cost": criteria.score_hoa,
    "tax_rate": criteria.score_tax_rate,
    "school_rating": criteria.score_school_rating,
    "walk_score": criteria.score_walk_score,
    "flood_risk": criteria.score_flood_risk,
    "commute_time": criteria.score_commute,
    "property_age": criteria.score_property_age,
    "bed_bath_value": criteria.score_bed_bath_value,
    "features_bonus": criteria.score_features,
}


class ScoringEngine:
    def __init__(self, weights: dict[str, float], config: AppConfig):
        self.weights = weights
        self.config = config

    def score(self, listing: Listing, area_stats: AreaStats) -> tuple[float, dict]:
        """
        Compute composite deal score (0-100) and per-criterion breakdown.
        Returns (composite_score, breakdown_dict).
        """
        breakdown = {}
        weighted_sum = 0.0
        total_weight = 0.0

        for criterion_name, func in CRITERIA_FUNCS.items():
            weight = self.weights.get(criterion_name, 0.0)
            if weight == 0.0:
                continue

            try:
                sub_score = func(listing, area_stats)
            except Exception:
                logger.exception(f"Error scoring {criterion_name} for {listing.address}")
                sub_score = 0.5

            breakdown[criterion_name] = round(sub_score, 3)
            weighted_sum += sub_score * weight
            total_weight += weight

        composite = (weighted_sum / total_weight * 100) if total_weight > 0 else 50.0
        return round(composite, 1), breakdown
