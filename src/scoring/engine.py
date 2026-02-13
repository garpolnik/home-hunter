import json
import logging
import os

import anthropic

from src.config import AppConfig
from src.models import AreaStats, Listing

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """\
You are a real estate deal analyst. You will be given a property listing and \
local area statistics. Your job is to evaluate how good of a "deal" this \
listing is on a scale from 0 to 100, where:

- 0-20: Terrible deal — significantly overpriced or major red flags
- 21-40: Below average — overpriced relative to the market or notable downsides
- 41-60: Average — fairly priced, nothing remarkable
- 61-80: Good deal — priced below market with solid fundamentals
- 81-100: Exceptional deal — significantly underpriced with strong positives

Consider ALL of the following factors holistically:
- Price relative to Redfin estimate / Zestimate (below estimate = better deal)
- Price per sqft relative to area median
- Days on market (longer DOM = more negotiating power)
- Price reduction history (larger/more reductions = motivated seller)
- Property features (garage, basement, pool, fireplace)
- HOA costs (lower = better)
- Tax rate (lower = better)
- School ratings (higher = better)
- Walk/transit/bike scores (higher = better)
- Flood risk (lower = better)
- Commute times (shorter = better)
- Property age and condition implications
- Lot size relative to area median
- Overall value (beds + baths for the price)

Use your judgment to weigh these factors contextually. For example, a property \
priced 10% below estimate with price reductions and high DOM is a stronger \
deal signal than any single factor alone.

IMPORTANT: Be concise. Keep the total JSON response under 500 characters.
Rationale: 1-2 short sentences. Strengths/weaknesses: 2-3 items each, max 8 words per item.

You MUST respond with ONLY valid JSON in this exact format:
{"score": <integer 0-100>, "rationale": "<1-2 sentences>", "strengths": ["<short phrase>", ...], "weaknesses": ["<short phrase>", ...]}

Do not include any text outside the JSON object.\
"""


def _build_listing_prompt(listing: Listing, area_stats: AreaStats) -> str:
    """Build a structured text description of the listing for Claude."""
    estimate = listing.redfin_estimate or listing.zestimate
    estimate_label = "Redfin estimate" if listing.redfin_estimate else "Zestimate"
    estimate_str = f"${estimate:,}" if estimate else "N/A"
    price_vs_estimate = ""
    if estimate and estimate > 0:
        diff_pct = ((listing.price - estimate) / estimate) * 100
        direction = "above" if diff_pct > 0 else "below"
        price_vs_estimate = f" ({abs(diff_pct):.1f}% {direction} {estimate_label})"

    ppsf = f"${listing.price / listing.sqft:,.0f}" if listing.sqft else "N/A"
    area_ppsf = f"${area_stats.median_price_per_sqft:,.0f}" if area_stats.median_price_per_sqft else "N/A"

    price_history_str = "None"
    if listing.price_history and len(listing.price_history) >= 2:
        entries = [f"  {e.date}: ${e.price:,} ({e.event})" for e in listing.price_history[:10]]
        price_history_str = "\n".join(entries)
        original = listing.price_history[0].price
        if original > 0:
            total_reduction_pct = ((original - listing.price) / original) * 100
            if total_reduction_pct > 0:
                price_history_str += f"\n  Total reduction: {total_reduction_pct:.1f}%"

    features = []
    if listing.has_garage:
        features.append(f"Garage ({listing.garage_spaces or '?'} spaces)")
    if listing.has_basement:
        features.append("Basement")
    if listing.has_pool:
        features.append("Pool")
    if listing.has_fireplace:
        features.append("Fireplace")
    features_str = ", ".join(features) if features else "None noted"

    commute_str = "N/A"
    if listing.commute_minutes:
        parts = [f"  {dest}: {mins:.0f} min" for dest, mins in listing.commute_minutes.items()]
        commute_str = "\n".join(parts)

    tax_str = "N/A"
    if listing.annual_tax:
        tax_str = f"${listing.annual_tax:,.0f}/year"
        if listing.price:
            eff_rate = (listing.annual_tax / listing.price) * 100
            tax_str += f" ({eff_rate:.2f}% effective rate)"

    return f"""\
=== PROPERTY LISTING ===
Address: {listing.address}, {listing.city}, {listing.state} {listing.zip_code}
Price: ${listing.price:,}{price_vs_estimate}
{estimate_label}: {estimate_str}
Price/sqft: {ppsf}

Property type: {listing.property_type.value if listing.property_type else 'Unknown'}
Bedrooms: {listing.bedrooms or 'N/A'}
Bathrooms: {listing.bathrooms or 'N/A'}
Sqft: {f'{listing.sqft:,}' if listing.sqft else 'N/A'}
Lot size: {f'{listing.lot_sqft:,} sqft' if listing.lot_sqft else 'N/A'}
Year built: {listing.year_built or 'N/A'}
Stories: {listing.stories or 'N/A'}

Features: {features_str}
HOA: {f'${listing.hoa_monthly:,.0f}/month' if listing.hoa_monthly else 'None'}
Taxes: {tax_str}

Days on market: {listing.days_on_market if listing.days_on_market is not None else 'N/A'}
Status: {listing.status}

Price history:
{price_history_str}

Walk score: {listing.walk_score if listing.walk_score is not None else 'N/A'}/100
Transit score: {listing.transit_score if listing.transit_score is not None else 'N/A'}/100
Bike score: {listing.bike_score if listing.bike_score is not None else 'N/A'}/100
Flood risk: {listing.flood_risk_rating or 'Unknown'}
School rating: {f'{listing.school_rating}/10' if listing.school_rating is not None else 'N/A'}

Commute times:
{commute_str}

=== AREA STATISTICS ({area_stats.area_key}) ===
Median price: {f'${area_stats.median_price:,}' if area_stats.median_price else 'N/A'}
Median price/sqft: {area_ppsf}
Median days on market: {area_stats.median_dom if area_stats.median_dom else 'N/A'}
Median lot size: {f'{area_stats.median_lot_size:,} sqft' if area_stats.median_lot_size else 'N/A'}
Sample size: {area_stats.sample_size}

Evaluate this listing and provide a deal score.\
"""


class ScoringEngine:
    def __init__(self, weights: dict[str, float], config: AppConfig):
        self.config = config
        self.model = getattr(config.scoring, "claude_model", "claude-haiku-4-5-20251001")
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            logger.warning(
                "ANTHROPIC_API_KEY not set. Scoring will fall back to default scores."
            )
        self.client = anthropic.Anthropic(api_key=api_key) if api_key else None

    def score(self, listing: Listing, area_stats: AreaStats) -> tuple[float, dict]:
        """
        Send listing data to Claude for deal scoring.
        Returns (composite_score, breakdown_dict).
        """
        if not self.client:
            return 50.0, {"rationale": "No API key — default score", "strengths": [], "weaknesses": []}

        prompt = _build_listing_prompt(listing, area_stats)

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=512,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )

            text = response.content[0].text.strip()
            # Strip markdown code fences if present
            if text.startswith("```"):
                text = text.split("\n", 1)[1] if "\n" in text else text[3:]
                text = text.rsplit("```", 1)[0].strip()
            result = json.loads(text)

            score = max(0, min(100, int(result["score"])))
            breakdown = {
                "rationale": result.get("rationale", ""),
                "strengths": result.get("strengths", []),
                "weaknesses": result.get("weaknesses", []),
            }

            logger.debug(f"Claude scored {listing.address}: {score} — {breakdown['rationale']}")
            return float(score), breakdown

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Claude response for {listing.address}: {e}\nRaw response: {text[:500]}")
            return 50.0, {"rationale": "Scoring error — could not parse response", "strengths": [], "weaknesses": []}
        except anthropic.APIError as e:
            logger.error(f"Claude API error scoring {listing.address}: {e}")
            return 50.0, {"rationale": "Scoring error — API failure", "strengths": [], "weaknesses": []}
        except Exception:
            logger.exception(f"Unexpected error scoring {listing.address}")
            return 50.0, {"rationale": "Scoring error — unexpected failure", "strengths": [], "weaknesses": []}
