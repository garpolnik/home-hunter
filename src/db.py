import json
import secrets
import sqlite3
import statistics
import uuid
from datetime import datetime
from pathlib import Path

from src.models import AreaStats, Listing, PriceHistoryEntry


SCHEMA = """
CREATE TABLE IF NOT EXISTS listings (
    id TEXT PRIMARY KEY,
    normalized_address TEXT NOT NULL,
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    source_url TEXT,
    source_urls TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    county TEXT,
    latitude REAL,
    longitude REAL,
    price INTEGER,
    property_type TEXT,
    bedrooms INTEGER,
    bathrooms REAL,
    sqft INTEGER,
    lot_sqft INTEGER,
    year_built INTEGER,
    stories INTEGER,
    has_garage BOOLEAN,
    garage_spaces INTEGER,
    has_basement BOOLEAN,
    has_pool BOOLEAN,
    has_fireplace BOOLEAN,
    hoa_monthly REAL,
    list_date TEXT,
    days_on_market INTEGER,
    status TEXT,
    price_history TEXT,
    photo_url TEXT,
    redfin_estimate INTEGER,
    zestimate INTEGER,
    annual_tax REAL,
    tax_rate REAL,
    walk_score INTEGER,
    transit_score INTEGER,
    bike_score INTEGER,
    flood_zone TEXT,
    flood_risk_rating TEXT,
    school_rating REAL,
    commute_minutes TEXT,
    deal_score REAL,
    score_breakdown TEXT,
    first_seen TEXT,
    last_seen TEXT,
    is_new BOOLEAN DEFAULT 1,
    UNIQUE(normalized_address, source)
);

CREATE TABLE IF NOT EXISTS area_stats (
    area_key TEXT PRIMARY KEY,
    median_price INTEGER,
    median_price_per_sqft REAL,
    median_lot_size INTEGER,
    median_dom INTEGER,
    sample_size INTEGER,
    computed_at TEXT
);

CREATE TABLE IF NOT EXISTS run_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT,
    listings_fetched INTEGER,
    new_listings INTEGER,
    emails_sent INTEGER,
    errors TEXT
);

CREATE TABLE IF NOT EXISTS search_requests (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    zip_codes TEXT NOT NULL,
    preferences TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT NOT NULL,
    reviewed_at TEXT,
    access_token TEXT UNIQUE,
    map_path TEXT,
    newsletter_path TEXT,
    last_run_at TEXT,
    run_status TEXT
);

CREATE TABLE IF NOT EXISTS user_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL REFERENCES search_requests(id),
    run_date TEXT NOT NULL,
    listings_matched INTEGER DEFAULT 0,
    new_listings INTEGER DEFAULT 0,
    email_sent BOOLEAN DEFAULT 0,
    errors TEXT DEFAULT '',
    duration_seconds REAL DEFAULT 0
);
"""

# Columns to add to search_requests if migrating from an older schema
_SEARCH_REQUESTS_MIGRATIONS = [
    ("access_token", "TEXT UNIQUE"),
    ("map_path", "TEXT"),
    ("newsletter_path", "TEXT"),
    ("last_run_at", "TEXT"),
    ("run_status", "TEXT"),
]


class Database:
    def __init__(self, db_path: str = "data/listings.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self._migrate_search_requests()

    def _migrate_search_requests(self):
        """Add new columns to search_requests if they don't exist (safe for existing DBs)."""
        cursor = self.conn.execute("PRAGMA table_info(search_requests)")
        existing_cols = {row["name"] for row in cursor.fetchall()}
        for col_name, col_type in _SEARCH_REQUESTS_MIGRATIONS:
            if col_name not in existing_cols:
                self.conn.execute(
                    f"ALTER TABLE search_requests ADD COLUMN {col_name} {col_type}"
                )
        self.conn.commit()

    def close(self):
        self.conn.close()

    def _listing_to_row(self, listing: Listing) -> dict:
        return {
            "id": listing.id or str(uuid.uuid4())[:16],
            "normalized_address": listing.normalized_address or "",
            "source": listing.source.value,
            "source_id": listing.source_id,
            "source_url": listing.source_url,
            "source_urls": json.dumps(listing.source_urls),
            "address": listing.address,
            "city": listing.city,
            "state": listing.state,
            "zip_code": listing.zip_code,
            "county": listing.county,
            "latitude": listing.latitude,
            "longitude": listing.longitude,
            "price": listing.price,
            "property_type": listing.property_type.value if listing.property_type else None,
            "bedrooms": listing.bedrooms,
            "bathrooms": listing.bathrooms,
            "sqft": listing.sqft,
            "lot_sqft": listing.lot_sqft,
            "year_built": listing.year_built,
            "stories": listing.stories,
            "has_garage": listing.has_garage,
            "garage_spaces": listing.garage_spaces,
            "has_basement": listing.has_basement,
            "has_pool": listing.has_pool,
            "has_fireplace": listing.has_fireplace,
            "hoa_monthly": listing.hoa_monthly,
            "list_date": listing.list_date.isoformat() if listing.list_date else None,
            "days_on_market": listing.days_on_market,
            "status": listing.status,
            "price_history": json.dumps([e.model_dump(mode="json") for e in listing.price_history]),
            "photo_url": listing.photo_url,
            "redfin_estimate": listing.redfin_estimate,
            "zestimate": listing.zestimate,
            "annual_tax": listing.annual_tax,
            "tax_rate": listing.tax_rate,
            "walk_score": listing.walk_score,
            "transit_score": listing.transit_score,
            "bike_score": listing.bike_score,
            "flood_zone": listing.flood_zone,
            "flood_risk_rating": listing.flood_risk_rating,
            "school_rating": listing.school_rating,
            "commute_minutes": json.dumps(listing.commute_minutes) if listing.commute_minutes else None,
            "deal_score": listing.deal_score,
            "score_breakdown": json.dumps(listing.score_breakdown) if listing.score_breakdown else None,
            "first_seen": listing.first_seen.isoformat() if listing.first_seen else datetime.now().isoformat(),
            "last_seen": listing.last_seen.isoformat() if listing.last_seen else datetime.now().isoformat(),
            "is_new": listing.is_new,
        }

    def _row_to_listing(self, row: sqlite3.Row) -> Listing:
        d = dict(row)
        price_history = []
        if d.get("price_history"):
            for entry in json.loads(d["price_history"]):
                price_history.append(PriceHistoryEntry(**entry))

        source_urls = {}
        if d.get("source_urls"):
            source_urls = json.loads(d["source_urls"])

        from src.models import ListingSource, PropertyType

        return Listing(
            id=d["id"],
            source=ListingSource(d["source"]),
            source_id=d["source_id"],
            source_url=d.get("source_url", ""),
            source_urls=source_urls,
            address=d["address"],
            city=d["city"],
            state=d["state"],
            zip_code=d["zip_code"],
            county=d.get("county"),
            latitude=d.get("latitude"),
            longitude=d.get("longitude"),
            price=d["price"],
            property_type=PropertyType(d["property_type"]) if d.get("property_type") else None,
            bedrooms=d.get("bedrooms"),
            bathrooms=d.get("bathrooms"),
            sqft=d.get("sqft"),
            lot_sqft=d.get("lot_sqft"),
            year_built=d.get("year_built"),
            stories=d.get("stories"),
            has_garage=d.get("has_garage"),
            garage_spaces=d.get("garage_spaces"),
            has_basement=d.get("has_basement"),
            has_pool=d.get("has_pool"),
            has_fireplace=d.get("has_fireplace"),
            hoa_monthly=d.get("hoa_monthly"),
            list_date=d.get("list_date"),
            days_on_market=d.get("days_on_market"),
            status=d.get("status", "active"),
            price_history=price_history,
            photo_url=d.get("photo_url"),
            redfin_estimate=d.get("redfin_estimate"),
            zestimate=d.get("zestimate"),
            annual_tax=d.get("annual_tax"),
            tax_rate=d.get("tax_rate"),
            walk_score=d.get("walk_score"),
            transit_score=d.get("transit_score"),
            bike_score=d.get("bike_score"),
            flood_zone=d.get("flood_zone"),
            flood_risk_rating=d.get("flood_risk_rating"),
            school_rating=d.get("school_rating"),
            commute_minutes=json.loads(d["commute_minutes"]) if d.get("commute_minutes") else None,
            deal_score=d.get("deal_score"),
            score_breakdown=json.loads(d["score_breakdown"]) if d.get("score_breakdown") else None,
            normalized_address=d.get("normalized_address"),
            first_seen=datetime.fromisoformat(d["first_seen"]) if d.get("first_seen") else None,
            last_seen=datetime.fromisoformat(d["last_seen"]) if d.get("last_seen") else None,
            is_new=bool(d.get("is_new", True)),
        )

    def upsert_listing(self, listing: Listing):
        row = self._listing_to_row(listing)
        columns = ", ".join(row.keys())
        placeholders = ", ".join(f":{k}" for k in row.keys())
        update_cols = ", ".join(f"{k} = :{k}" for k in row.keys() if k != "id")

        sql = f"""
            INSERT INTO listings ({columns}) VALUES ({placeholders})
            ON CONFLICT(normalized_address, source) DO UPDATE SET {update_cols}
        """
        self.conn.execute(sql, row)
        self.conn.commit()

    def upsert_listings(self, listings: list[Listing]):
        for listing in listings:
            self.upsert_listing(listing)

    def reconcile(self, listings: list[Listing]) -> tuple[list[Listing], list[Listing]]:
        """Compare incoming listings against DB. Returns (new_listings, updated_listings)."""
        new_listings = []
        updated_listings = []
        now = datetime.now()

        for listing in listings:
            cursor = self.conn.execute(
                "SELECT * FROM listings WHERE normalized_address = ? AND source = ?",
                (listing.normalized_address, listing.source.value),
            )
            existing = cursor.fetchone()

            if existing is None:
                listing.first_seen = now
                listing.last_seen = now
                listing.is_new = True
                listing.id = str(uuid.uuid4())[:16]
                new_listings.append(listing)
            else:
                existing_listing = self._row_to_listing(existing)
                listing.id = existing_listing.id
                listing.first_seen = existing_listing.first_seen
                listing.last_seen = now
                listing.is_new = False
                # Preserve enrichment data from previous runs if not re-fetched
                if listing.walk_score is None and existing_listing.walk_score is not None:
                    listing.walk_score = existing_listing.walk_score
                    listing.transit_score = existing_listing.transit_score
                    listing.bike_score = existing_listing.bike_score
                if listing.flood_risk_rating is None and existing_listing.flood_risk_rating is not None:
                    listing.flood_zone = existing_listing.flood_zone
                    listing.flood_risk_rating = existing_listing.flood_risk_rating
                if listing.commute_minutes is None and existing_listing.commute_minutes is not None:
                    listing.commute_minutes = existing_listing.commute_minutes
                if listing.school_rating is None and existing_listing.school_rating is not None:
                    listing.school_rating = existing_listing.school_rating
                # Preserve AI deal score if price hasn't changed
                if existing_listing.deal_score is not None and listing.price == existing_listing.price:
                    listing.deal_score = existing_listing.deal_score
                    listing.score_breakdown = existing_listing.score_breakdown
                updated_listings.append(listing)

        return new_listings, updated_listings

    def get_all_active_listings(self) -> list[Listing]:
        cursor = self.conn.execute("SELECT * FROM listings WHERE status = 'active'")
        return [self._row_to_listing(row) for row in cursor.fetchall()]

    def compute_area_stats(self) -> dict[str, AreaStats]:
        """Compute median stats per ZIP code from all active listings in DB."""
        cursor = self.conn.execute(
            "SELECT zip_code, price, sqft, lot_sqft, days_on_market FROM listings WHERE status = 'active'"
        )
        rows = cursor.fetchall()

        by_zip: dict[str, list[dict]] = {}
        for row in rows:
            d = dict(row)
            zip_code = d["zip_code"]
            if zip_code not in by_zip:
                by_zip[zip_code] = []
            by_zip[zip_code].append(d)

        stats = {}
        now = datetime.now()
        for zip_code, listings in by_zip.items():
            prices = [l["price"] for l in listings if l["price"]]
            ppsf_values = [
                l["price"] / l["sqft"]
                for l in listings
                if l["price"] and l["sqft"] and l["sqft"] > 0
            ]
            lot_sizes = [l["lot_sqft"] for l in listings if l.get("lot_sqft")]
            doms = [l["days_on_market"] for l in listings if l.get("days_on_market") is not None]

            area = AreaStats(
                area_key=zip_code,
                median_price=int(statistics.median(prices)) if prices else None,
                median_price_per_sqft=round(statistics.median(ppsf_values), 2) if ppsf_values else None,
                median_lot_size=int(statistics.median(lot_sizes)) if lot_sizes else None,
                median_dom=int(statistics.median(doms)) if doms else None,
                sample_size=len(listings),
                computed_at=now,
            )
            stats[zip_code] = area

            # Persist to area_stats table
            self.conn.execute(
                """INSERT OR REPLACE INTO area_stats
                   (area_key, median_price, median_price_per_sqft, median_lot_size,
                    median_dom, sample_size, computed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (area.area_key, area.median_price, area.median_price_per_sqft,
                 area.median_lot_size, area.median_dom, area.sample_size,
                 area.computed_at.isoformat()),
            )

        self.conn.commit()
        return stats

    # --- Search request management ---

    def create_search_request(self, email: str, zip_codes: list[str], preferences: dict) -> str:
        """Create a pending search request. Returns the request ID."""
        request_id = str(uuid.uuid4())[:16]
        self.conn.execute(
            """INSERT INTO search_requests (id, email, zip_codes, preferences, status, created_at)
               VALUES (?, ?, ?, ?, 'pending', ?)""",
            (request_id, email, json.dumps(zip_codes), json.dumps(preferences),
             datetime.now().isoformat()),
        )
        self.conn.commit()
        return request_id

    def get_pending_requests(self) -> list[dict]:
        """Return all pending search requests."""
        cursor = self.conn.execute(
            "SELECT * FROM search_requests WHERE status = 'pending' ORDER BY created_at DESC"
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_all_requests(self) -> list[dict]:
        """Return all search requests."""
        cursor = self.conn.execute(
            "SELECT * FROM search_requests ORDER BY created_at DESC"
        )
        return [dict(row) for row in cursor.fetchall()]

    def update_request_status(self, request_id: str, status: str):
        """Update a search request status to 'approved' or 'rejected'."""
        self.conn.execute(
            "UPDATE search_requests SET status = ?, reviewed_at = ? WHERE id = ?",
            (status, datetime.now().isoformat(), request_id),
        )
        self.conn.commit()

    def get_request(self, request_id: str) -> dict | None:
        """Get a single search request by ID."""
        cursor = self.conn.execute(
            "SELECT * FROM search_requests WHERE id = ?", (request_id,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_approved_subscribers(self) -> list[dict]:
        """Return all approved search requests (active subscribers)."""
        cursor = self.conn.execute(
            "SELECT * FROM search_requests WHERE status = 'approved' ORDER BY created_at"
        )
        return [dict(row) for row in cursor.fetchall()]

    def log_run(self, listings_fetched: int, new_listings: int, emails_sent: int = 0, errors: str = ""):
        self.conn.execute(
            "INSERT INTO run_log (run_date, listings_fetched, new_listings, emails_sent, errors) VALUES (?, ?, ?, ?, ?)",
            (datetime.now().isoformat(), listings_fetched, new_listings, emails_sent, errors),
        )
        self.conn.commit()

    # --- Per-user pipeline support ---

    def set_access_token(self, request_id: str) -> str:
        """Generate and store a unique access token for a user. Returns the token."""
        token = secrets.token_hex(16)
        self.conn.execute(
            "UPDATE search_requests SET access_token = ? WHERE id = ?",
            (token, request_id),
        )
        self.conn.commit()
        return token

    def get_request_by_token(self, token: str) -> dict | None:
        """Look up an approved search request by its access token."""
        cursor = self.conn.execute(
            "SELECT * FROM search_requests WHERE access_token = ? AND status = 'approved'",
            (token,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def update_user_output(self, request_id: str, map_path: str | None = None, newsletter_path: str | None = None):
        """Update the generated output file paths for a user."""
        updates = []
        params = []
        if map_path is not None:
            updates.append("map_path = ?")
            params.append(map_path)
        if newsletter_path is not None:
            updates.append("newsletter_path = ?")
            params.append(newsletter_path)
        if updates:
            params.append(request_id)
            self.conn.execute(
                f"UPDATE search_requests SET {', '.join(updates)} WHERE id = ?",
                params,
            )
            self.conn.commit()

    def update_user_run_status(self, request_id: str, status: str):
        """Update the run status for a user (running/completed/failed)."""
        self.conn.execute(
            "UPDATE search_requests SET run_status = ?, last_run_at = ? WHERE id = ?",
            (status, datetime.now().isoformat(), request_id),
        )
        self.conn.commit()

    def log_user_run(self, request_id: str, summary: dict):
        """Log a per-user pipeline run."""
        self.conn.execute(
            """INSERT INTO user_runs (request_id, run_date, listings_matched, new_listings,
               email_sent, errors, duration_seconds)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                request_id,
                datetime.now().isoformat(),
                summary.get("listings_matched", 0),
                summary.get("new_listings", 0),
                summary.get("email_sent", False),
                summary.get("errors", ""),
                summary.get("duration_seconds", 0),
            ),
        )
        self.conn.commit()
