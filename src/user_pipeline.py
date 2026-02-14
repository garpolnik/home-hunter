"""
Per-user pipeline: fetches, enriches, scores, and generates personalized
maps and newsletters for individual approved subscribers.
"""

import copy
import json
import logging
import time
from datetime import datetime
from pathlib import Path

from src.config import (
    AppConfig,
    FilterConfig,
    LocationConfig,
    load_config,
)
from src.db import Database
from src.dedup.deduplicator import Deduplicator
from src.main import get_enabled_enrichers, get_enabled_fetchers
from src.map_generator import generate_map
from src.market_analyzer import analyze_market
from src.models import AreaStats
from src.newsletter.generator import NewsletterGenerator
from src.scoring.engine import ScoringEngine

logger = logging.getLogger(__name__)


def build_user_config(base_config: AppConfig, user_request: dict) -> AppConfig:
    """Build an AppConfig tailored to a user's search_request.

    Copies the base config (sources, enrichment, scoring, DB settings)
    and overrides search locations/filters from the user's preferences.
    """
    config = copy.deepcopy(base_config)

    zip_codes = json.loads(user_request["zip_codes"]) if isinstance(user_request["zip_codes"], str) else user_request["zip_codes"]
    prefs = json.loads(user_request["preferences"]) if isinstance(user_request["preferences"], str) else user_request["preferences"]

    # Build locations from user's ZIP codes
    config.search.locations = [
        LocationConfig(type="zip", value=z) for z in zip_codes
    ]

    # Build filters from user preferences
    config.search.filters = FilterConfig(
        min_price=prefs.get("min_price", 0),
        max_price=prefs.get("max_price", 999999999),
        min_beds=prefs.get("min_beds", 0),
        min_baths=prefs.get("min_baths", 0),
        min_sqft=prefs.get("min_sqft", 0),
        property_types=prefs.get("property_types", ["single_family", "townhouse", "condo"]),
        max_dom_multiplier=base_config.search.filters.max_dom_multiplier,
        max_dom_absolute=base_config.search.filters.max_dom_absolute,
    )

    config.newsletter.recipients = [user_request["email"]]

    return config


def get_user_listings(db: Database, user_request: dict, config: AppConfig | None = None) -> list:
    """Query the shared listings DB for listings matching a user's criteria.

    Filters already-fetched, enriched, scored listings by the user's
    ZIP codes, price range, beds, baths, sqft, and property types.
    Uses max_dom_display and min_deal_score_display from config for display filters.
    """
    zip_codes = json.loads(user_request["zip_codes"]) if isinstance(user_request["zip_codes"], str) else user_request["zip_codes"]
    prefs = json.loads(user_request["preferences"]) if isinstance(user_request["preferences"], str) else user_request["preferences"]

    max_dom = config.search.filters.max_dom_display if config else 60
    min_score = config.search.filters.min_deal_score_display if config else 50.0

    placeholders = ",".join("?" * len(zip_codes))
    params = list(zip_codes)

    conditions = [
        f"zip_code IN ({placeholders})",
        "status = 'active'",
        "(days_on_market IS NULL OR days_on_market < ?)",
        "(deal_score IS NOT NULL AND deal_score >= ?)",
    ]
    params.extend([max_dom, min_score])

    min_price = prefs.get("min_price", 0)
    max_price = prefs.get("max_price", 999999999)
    if min_price > 0:
        conditions.append("price >= ?")
        params.append(min_price)
    if max_price < 999999999:
        conditions.append("price <= ?")
        params.append(max_price)

    min_beds = prefs.get("min_beds", 0)
    if min_beds > 0:
        conditions.append("(bedrooms IS NULL OR bedrooms >= ?)")
        params.append(min_beds)

    min_baths = prefs.get("min_baths", 0)
    if min_baths > 0:
        conditions.append("(bathrooms IS NULL OR bathrooms >= ?)")
        params.append(min_baths)

    min_sqft = prefs.get("min_sqft", 0)
    if min_sqft > 0:
        conditions.append("(sqft IS NULL OR sqft >= ?)")
        params.append(min_sqft)

    prop_types = prefs.get("property_types", [])
    if prop_types:
        pt_placeholders = ",".join("?" * len(prop_types))
        conditions.append(f"(property_type IS NULL OR property_type IN ({pt_placeholders}))")
        params.extend(prop_types)

    where = " AND ".join(conditions)
    sql = f"SELECT * FROM listings WHERE {where}"

    cursor = db.conn.execute(sql, params)
    return [db._row_to_listing(row) for row in cursor.fetchall()]


def run_for_user(
    user_request: dict,
    base_config: AppConfig,
    db: Database,
    fetch_new: bool = True,
    send_email: bool = True,
) -> dict:
    """Run the pipeline for a single user.

    If fetch_new=True, fetches fresh listings from APIs for the user's ZIPs.
    If fetch_new=False, just re-generates map/newsletter from existing DB data.

    Returns a summary dict with listings_matched, new_listings, email_sent, errors.
    """
    start = time.time()
    request_id = user_request["id"]
    email = user_request["email"]
    errors = []

    logger.info(f"=== Running pipeline for user {email} (request {request_id}) ===")

    user_config = build_user_config(base_config, user_request)

    if fetch_new:
        # Phase 1: Fetch for this user's locations/filters
        raw_listings = []
        for fetcher in get_enabled_fetchers(user_config):
            try:
                listings = fetcher.fetch_all()
                raw_listings.extend(listings)
            except Exception as e:
                logger.exception(f"Fetcher {fetcher.source_name} failed for user {email}")
                errors.append(str(e))

        if raw_listings:
            # Phase 2: Dedup
            deduplicator = Deduplicator()
            unique = deduplicator.process(raw_listings)

            # Phase 3: Reconcile
            new_listings, updated_listings = db.reconcile(unique)

            # Phase 4: Enrich new
            enrichers = get_enabled_enrichers(user_config)
            for listing in new_listings:
                for enricher in enrichers:
                    try:
                        enricher.enrich(listing)
                    except Exception:
                        logger.exception(f"Enricher failed for {listing.address}")

            # Persist all
            all_fetched = new_listings + updated_listings
            db.upsert_listings(all_fetched)

            # Phase 5: Score unscored
            area_stats = db.compute_area_stats()
            needs_scoring = [l for l in all_fetched if l.deal_score is None]
            if needs_scoring:
                scorer = ScoringEngine(user_config.scoring.weights, user_config)
                for listing in needs_scoring:
                    zip_stats = area_stats.get(listing.zip_code, AreaStats(area_key=listing.zip_code))
                    listing.deal_score, listing.score_breakdown = scorer.score(listing, zip_stats)
                db.upsert_listings(needs_scoring)

    # Query DB for all listings matching user criteria
    all_user_listings = get_user_listings(db, user_request, user_config)
    new_user_listings = [l for l in all_user_listings if l.is_new]

    access_token = user_request.get("access_token") or request_id

    # Generate per-user map
    data_dir = Path(base_config.database.path).parent
    map_path = str(data_dir / f"user_{access_token}_map.html")
    generate_map(all_user_listings, map_path)
    db.update_user_output(request_id, map_path=map_path)

    # Generate per-user newsletter
    area_stats = db.compute_area_stats()
    market_report = analyze_market(all_user_listings, new_user_listings, area_stats)
    generator = NewsletterGenerator(user_config)
    newsletter_html = generator.render(new_user_listings, all_user_listings, area_stats, market_report)

    newsletter_path = str(data_dir / f"user_{access_token}_newsletter.html")
    Path(newsletter_path).parent.mkdir(parents=True, exist_ok=True)
    with open(newsletter_path, "w") as f:
        f.write(newsletter_html)
    db.update_user_output(request_id, newsletter_path=newsletter_path)

    # Send email via SendGrid
    email_sent = False
    if send_email and all_user_listings:
        from src.newsletter.sendgrid_sender import SendGridSender

        sender = SendGridSender(user_config)
        try:
            map_url = f"https://homehunter.casa/u/{access_token}/map"
            sent = sender.send(
                newsletter_html,
                recipient=email,
                new_count=len(new_user_listings),
                map_url=map_url,
            )
            email_sent = sent > 0
        except Exception as e:
            logger.exception(f"Failed to send email to {email}")
            errors.append(str(e))

    duration = time.time() - start
    summary = {
        "listings_matched": len(all_user_listings),
        "new_listings": len(new_user_listings),
        "email_sent": email_sent,
        "errors": "; ".join(errors),
        "duration_seconds": round(duration, 1),
    }

    db.log_user_run(request_id, summary)
    db.update_user_run_status(request_id, "completed")

    logger.info(
        f"=== User {email}: {len(all_user_listings)} matches, "
        f"{len(new_user_listings)} new, "
        f"email={'sent' if email_sent else 'skipped'} ({duration:.1f}s) ==="
    )

    return summary


def run_all_approved_users(config_path: str = "config/config.yaml"):
    """Monthly cron entry point: run pipeline for all approved users.

    Fetches once per unique set of ZIP codes across all users,
    then generates per-user outputs from the shared DB.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    base_config = load_config(config_path)
    db = Database(base_config.database.path)

    approved = db.get_approved_subscribers()
    if not approved:
        logger.info("No approved subscribers. Nothing to do.")
        db.close()
        return

    logger.info(f"Running pipeline for {len(approved)} approved user(s)")

    # Collect all unique ZIP codes across all users
    all_zips = set()
    for user in approved:
        zips = json.loads(user["zip_codes"]) if isinstance(user["zip_codes"], str) else user["zip_codes"]
        all_zips.update(zips)

    # Build a bulk config with all ZIPs and broadest possible filters
    bulk_request = {
        "zip_codes": json.dumps(list(all_zips)),
        "preferences": json.dumps({
            "min_price": 0,
            "max_price": 999999999,
            "min_beds": 0,
            "min_baths": 0,
            "min_sqft": 0,
            "property_types": ["single_family", "townhouse", "condo", "multi_family"],
        }),
        "email": "",
    }
    bulk_config = build_user_config(base_config, bulk_request)

    # Single bulk fetch/dedup/enrich/score
    raw_listings = []
    for fetcher in get_enabled_fetchers(bulk_config):
        try:
            listings = fetcher.fetch_all()
            raw_listings.extend(listings)
        except Exception:
            logger.exception(f"Fetcher {fetcher.source_name} failed during bulk fetch")

    if raw_listings:
        deduplicator = Deduplicator()
        unique = deduplicator.process(raw_listings)
        new_listings, updated_listings = db.reconcile(unique)

        enrichers = get_enabled_enrichers(bulk_config)
        for listing in new_listings:
            for enricher in enrichers:
                try:
                    enricher.enrich(listing)
                except Exception:
                    logger.exception(f"Enricher failed for {listing.address}")

        all_fetched = new_listings + updated_listings
        db.upsert_listings(all_fetched)

        area_stats = db.compute_area_stats()
        needs_scoring = [l for l in all_fetched if l.deal_score is None]
        if needs_scoring:
            scorer = ScoringEngine(bulk_config.scoring.weights, bulk_config)
            for listing in needs_scoring:
                zip_stats = area_stats.get(listing.zip_code, AreaStats(area_key=listing.zip_code))
                listing.deal_score, listing.score_breakdown = scorer.score(listing, zip_stats)
            db.upsert_listings(needs_scoring)

    logger.info("Bulk fetch/enrich/score complete. Generating per-user outputs...")

    # Generate per-user outputs (no fetch, just filter + generate)
    for user in approved:
        try:
            db.update_user_run_status(user["id"], "running")
            run_for_user(user, base_config, db, fetch_new=False, send_email=True)
        except Exception:
            logger.exception(f"Pipeline failed for user {user['email']}")
            db.update_user_run_status(user["id"], "failed")

    db.close()
    logger.info("=== All user pipelines complete ===")
