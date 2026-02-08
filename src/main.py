import logging
import sys
from pathlib import Path

from src.config import AppConfig, FilterConfig, load_config
from src.db import Database
from src.dedup.deduplicator import Deduplicator
from src.fetchers.base import BaseFetcher
from src.fetchers.redfin import RedfinFetcher
from src.models import AreaStats, Listing
from src.newsletter.generator import NewsletterGenerator
from src.newsletter.sender import EmailSender
from src.map_generator import generate_map
from src.scoring.engine import ScoringEngine

logger = logging.getLogger(__name__)


def _passes_age_filter(
    listing: Listing,
    area_stats: dict[str, AreaStats],
    filters: FilterConfig,
) -> bool:
    """
    Determine if a listing should be included based on dynamic market-aware age limits.
    Uses area median DOM * multiplier as the threshold, capped by an absolute max.
    In a hot market (low median DOM), stale listings are filtered aggressively.
    In a slow market (high median DOM), the threshold is more generous.
    """
    if listing.days_on_market is None:
        return True  # Can't filter without DOM data

    # Get area-specific median DOM
    zip_stats = area_stats.get(listing.zip_code)
    median_dom = zip_stats.median_dom if zip_stats and zip_stats.median_dom else 30

    # Dynamic threshold = median * multiplier, capped by absolute max
    dynamic_max = int(median_dom * filters.max_dom_multiplier)
    effective_max = min(dynamic_max, filters.max_dom_absolute)

    # Always have a floor of 30 days so we don't filter too aggressively
    effective_max = max(effective_max, 30)

    if listing.days_on_market > effective_max:
        logger.debug(
            f"Filtered stale listing: {listing.address} "
            f"(DOM={listing.days_on_market}, max={effective_max}, "
            f"median={median_dom})"
        )
        return False
    return True


def get_enabled_fetchers(config: AppConfig) -> list[BaseFetcher]:
    """Return instantiated fetchers for all enabled sources."""
    fetchers = []
    if config.sources.redfin.enabled:
        fetchers.append(RedfinFetcher(config))
    if config.sources.realtor.enabled:
        try:
            from src.fetchers.realtor import RealtorFetcher
            fetchers.append(RealtorFetcher(config))
        except ImportError:
            logger.warning("Realtor fetcher not available")
    if config.sources.zillow.enabled:
        try:
            from src.fetchers.zillow import ZillowFetcher
            fetchers.append(ZillowFetcher(config))
        except ImportError:
            logger.warning("Zillow fetcher not available")
    return fetchers


def get_enabled_enrichers(config: AppConfig) -> list:
    """Return instantiated enrichers for all enabled enrichment sources."""
    enrichers = []
    if config.enrichment.walkscore.enabled:
        try:
            from src.enrichers.walkscore import WalkScoreEnricher
            enrichers.append(WalkScoreEnricher(config))
        except ImportError:
            logger.warning("WalkScore enricher not available")
    if config.enrichment.flood_zone.enabled:
        try:
            from src.enrichers.flood_zone import FloodZoneEnricher
            enrichers.append(FloodZoneEnricher(config))
        except ImportError:
            logger.warning("FloodZone enricher not available")
    if config.enrichment.commute.enabled:
        try:
            from src.enrichers.commute import CommuteEnricher
            enrichers.append(CommuteEnricher(config))
        except ImportError:
            logger.warning("Commute enricher not available")
    return enrichers


def run(config_path: str = "config/config.yaml"):
    """Main pipeline: fetch -> dedupe -> enrich -> score -> email."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("=== Home Deal Finder - Starting Run ===")

    # Load config
    config = load_config(config_path)
    logger.info(f"Loaded config: {len(config.search.locations)} locations, sources: "
                f"redfin={config.sources.redfin.enabled}, "
                f"realtor={config.sources.realtor.enabled}, "
                f"zillow={config.sources.zillow.enabled}")

    # Initialize database
    db = Database(config.database.path)

    errors = []

    # Phase 1: Fetch listings from all enabled sources
    logger.info("--- Phase 1: Fetching Listings ---")
    raw_listings = []
    for fetcher in get_enabled_fetchers(config):
        try:
            listings = fetcher.fetch_all()
            raw_listings.extend(listings)
        except Exception as e:
            logger.exception(f"Fetcher {fetcher.source_name} failed")
            errors.append(f"{fetcher.source_name}: {e}")

    logger.info(f"Fetched {len(raw_listings)} total raw listings")

    if not raw_listings:
        logger.warning("No listings fetched. Exiting.")
        db.log_run(0, 0, 0, "; ".join(errors))
        db.close()
        return

    # Phase 2: Deduplicate
    logger.info("--- Phase 2: Deduplication ---")
    deduplicator = Deduplicator()
    unique_listings = deduplicator.process(raw_listings)
    logger.info(f"After dedup: {len(unique_listings)} unique listings")

    # Phase 3: Reconcile with database (identify new vs existing)
    logger.info("--- Phase 3: Database Reconciliation ---")
    new_listings, updated_listings = db.reconcile(unique_listings)
    logger.info(f"New: {len(new_listings)}, Updated: {len(updated_listings)}")

    # Phase 4: Enrich new listings
    logger.info("--- Phase 4: Enrichment ---")
    enrichers = get_enabled_enrichers(config)
    for listing in new_listings:
        for enricher in enrichers:
            try:
                enricher.enrich(listing)
            except Exception:
                logger.exception(f"Enricher failed for {listing.address}")

    # Phase 4.5: Filter stale listings based on dynamic market-aware threshold
    logger.info("--- Phase 4.5: Dynamic Listing Age Filter ---")
    all_listings_unfiltered = new_listings + updated_listings

    # First pass: persist and compute area stats so we have median DOM
    db.upsert_listings(all_listings_unfiltered)
    area_stats = db.compute_area_stats()

    filters = config.search.filters
    if filters.max_dom_multiplier is not None:
        before_count = len(all_listings_unfiltered)
        filtered_new = []
        filtered_updated = []

        for listing in new_listings:
            if _passes_age_filter(listing, area_stats, filters):
                filtered_new.append(listing)
        for listing in updated_listings:
            if _passes_age_filter(listing, area_stats, filters):
                filtered_updated.append(listing)

        new_listings = filtered_new
        updated_listings = filtered_updated
        removed = before_count - len(new_listings) - len(updated_listings)
        logger.info(f"Listing age filter: removed {removed} stale listings "
                    f"(kept {len(new_listings)} new, {len(updated_listings)} updated)")
    else:
        logger.info("Dynamic listing age filter disabled")

    # Phase 5: Score all remaining listings
    logger.info("--- Phase 5: Scoring ---")
    all_listings = new_listings + updated_listings

    scorer = ScoringEngine(config.scoring.weights, config)
    for listing in all_listings:
        zip_stats = area_stats.get(listing.zip_code, AreaStats(area_key=listing.zip_code))
        listing.deal_score, listing.score_breakdown = scorer.score(listing, zip_stats)

    # Update scores in DB
    db.upsert_listings(all_listings)

    scored = [l for l in all_listings if l.deal_score is not None]
    if scored:
        avg = sum(l.deal_score for l in scored) / len(scored)
        top = max(scored, key=lambda l: l.deal_score)
        logger.info(f"Avg deal score: {avg:.1f}, Best: {top.deal_score} ({top.address})")

    # Phase 6: Generate interactive map
    logger.info("--- Phase 6: Map Generation ---")
    generate_map(all_listings, "data/map.html")

    # Phase 7: Generate and send newsletter
    logger.info("--- Phase 7: Newsletter ---")
    generator = NewsletterGenerator(config)
    html = generator.render(new_listings, all_listings, area_stats)

    sender = EmailSender(config)
    emails_sent = sender.send(html, new_count=len(new_listings))

    # Log run
    db.log_run(
        listings_fetched=len(raw_listings),
        new_listings=len(new_listings),
        emails_sent=emails_sent,
        errors="; ".join(errors),
    )
    db.close()

    logger.info(f"=== Run Complete: {len(new_listings)} new, {emails_sent} emails sent ===")


if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config/config.yaml"
    run(config_path)
