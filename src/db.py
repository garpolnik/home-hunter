import json
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
"""


class Database:
    def __init__(self, db_path: str = "data/listings.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript(SCHEMA)
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

    def log_run(self, listings_fetched: int, new_listings: int, emails_sent: int = 0, errors: str = ""):
        self.conn.execute(
            "INSERT INTO run_log (run_date, listings_fetched, new_listings, emails_sent, errors) VALUES (?, ?, ?, ?, ?)",
            (datetime.now().isoformat(), listings_fetched, new_listings, emails_sent, errors),
        )
        self.conn.commit()
