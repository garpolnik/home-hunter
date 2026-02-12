from __future__ import annotations

import yaml
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class LocationConfig:
    type: str  # "zip", "city", "county"
    value: str
    state: str = ""


@dataclass
class FilterConfig:
    min_price: int = 0
    max_price: int = 999999999
    min_beds: int = 0
    max_beds: int = 99
    min_baths: float = 0
    property_types: list[str] = field(default_factory=lambda: ["single_family", "townhouse", "condo"])
    min_sqft: int = 0
    max_sqft: int = 999999
    min_year_built: int | None = None
    max_year_built: int | None = None
    exclude_hoa_over: float | None = None
    # Dynamic listing age filter: exclude listings older than (area median DOM * multiplier).
    # E.g., if median DOM is 30 days and multiplier is 4, max is 120 days.
    # Set to None to disable dynamic filtering.
    max_dom_multiplier: float = 4.0
    # Absolute cap in days - never show listings older than this regardless of market
    max_dom_absolute: int = 365


@dataclass
class SearchConfig:
    locations: list[LocationConfig] = field(default_factory=list)
    filters: FilterConfig = field(default_factory=FilterConfig)


@dataclass
class SourceConfig:
    enabled: bool = True
    max_results_per_location: int = 350
    request_delay_seconds: float = 3.0


@dataclass
class SourcesConfig:
    redfin: SourceConfig = field(default_factory=lambda: SourceConfig())
    realtor: SourceConfig = field(default_factory=lambda: SourceConfig(max_results_per_location=200, request_delay_seconds=1.0))
    zillow: SourceConfig = field(default_factory=lambda: SourceConfig(enabled=False, max_results_per_location=200, request_delay_seconds=2.0))


@dataclass
class CommuteTarget:
    name: str
    address: str


@dataclass
class EnrichmentModuleConfig:
    enabled: bool = True


@dataclass
class CommuteConfig:
    enabled: bool = True
    targets: list[CommuteTarget] = field(default_factory=list)


@dataclass
class EnrichmentConfig:
    walkscore: EnrichmentModuleConfig = field(default_factory=EnrichmentModuleConfig)
    flood_zone: EnrichmentModuleConfig = field(default_factory=EnrichmentModuleConfig)
    commute: CommuteConfig = field(default_factory=CommuteConfig)


DEFAULT_WEIGHTS = {
    "price_vs_estimate": 0.20,
    "price_per_sqft": 0.12,
    "days_on_market": 0.08,
    "price_reductions": 0.08,
    "lot_size_value": 0.05,
    "hoa_cost": 0.05,
    "tax_rate": 0.04,
    "school_rating": 0.07,
    "walk_score": 0.05,
    "flood_risk": 0.06,
    "commute_time": 0.06,
    "property_age": 0.04,
    "bed_bath_value": 0.05,
    "features_bonus": 0.05,
}


@dataclass
class ScoringConfig:
    weights: dict[str, float] = field(default_factory=lambda: dict(DEFAULT_WEIGHTS))
    top_deal_threshold: float = 70.0
    claude_model: str = "claude-haiku-4-5-20251001"


@dataclass
class NewsletterSection:
    name: str
    filter: str
    sort: str = "-deal_score"
    limit: int = 10


@dataclass
class NewsletterConfig:
    recipients: list[str] = field(default_factory=list)
    from_email: str = "deals@example.com"
    from_name: str = "Home Deal Finder"
    subject_template: str = "Home Deal Finder: {new_count} New Listings - {month} {year}"
    max_listings_in_email: int = 30
    sections: list[NewsletterSection] = field(default_factory=lambda: [
        NewsletterSection(name="Top Deals", filter="deal_score >= 70", sort="-deal_score", limit=10),
        NewsletterSection(name="New This Month", filter="is_new == True", sort="-deal_score", limit=15),
        NewsletterSection(name="Price Drops", filter="has_price_reduction == True", sort="-reduction_pct", limit=10),
    ])


@dataclass
class DatabaseConfig:
    path: str = "data/listings.db"


@dataclass
class AppConfig:
    search: SearchConfig = field(default_factory=SearchConfig)
    sources: SourcesConfig = field(default_factory=SourcesConfig)
    enrichment: EnrichmentConfig = field(default_factory=EnrichmentConfig)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)
    newsletter: NewsletterConfig = field(default_factory=NewsletterConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)


def _build_locations(raw: list[dict]) -> list[LocationConfig]:
    return [LocationConfig(type=loc["type"], value=str(loc["value"]), state=loc.get("state", "")) for loc in raw]


def _build_commute_targets(raw: list[dict]) -> list[CommuteTarget]:
    return [CommuteTarget(name=t["name"], address=t["address"]) for t in raw]


def _build_newsletter_sections(raw: list[dict]) -> list[NewsletterSection]:
    return [NewsletterSection(**s) for s in raw]


def load_config(path: str | Path = "config/config.yaml") -> AppConfig:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    if not raw:
        return AppConfig()

    config = AppConfig()

    # Search
    if "search" in raw:
        s = raw["search"]
        if "locations" in s:
            config.search.locations = _build_locations(s["locations"])
        if "filters" in s:
            f = s["filters"]
            config.search.filters = FilterConfig(
                min_price=f.get("min_price", 0),
                max_price=f.get("max_price", 999999999),
                min_beds=f.get("min_beds", 0),
                max_beds=f.get("max_beds", 99),
                min_baths=f.get("min_baths", 0),
                property_types=f.get("property_types", ["single_family", "townhouse", "condo"]),
                min_sqft=f.get("min_sqft", 0),
                max_sqft=f.get("max_sqft", 999999),
                min_year_built=f.get("min_year_built"),
                max_year_built=f.get("max_year_built"),
                exclude_hoa_over=f.get("exclude_hoa_over"),
                max_dom_multiplier=f.get("max_dom_multiplier", 4.0),
                max_dom_absolute=f.get("max_dom_absolute", 365),
            )

    # Sources
    if "sources" in raw:
        for name in ("redfin", "realtor", "zillow"):
            if name in raw["sources"]:
                src = raw["sources"][name]
                source_cfg = SourceConfig(
                    enabled=src.get("enabled", True),
                    max_results_per_location=src.get("max_results_per_location", 350),
                    request_delay_seconds=src.get("request_delay_seconds", 3.0),
                )
                setattr(config.sources, name, source_cfg)

    # Enrichment
    if "enrichment" in raw:
        e = raw["enrichment"]
        if "walkscore" in e:
            config.enrichment.walkscore = EnrichmentModuleConfig(enabled=e["walkscore"].get("enabled", True))
        if "flood_zone" in e:
            config.enrichment.flood_zone = EnrichmentModuleConfig(enabled=e["flood_zone"].get("enabled", True))
        if "commute" in e:
            c = e["commute"]
            config.enrichment.commute = CommuteConfig(
                enabled=c.get("enabled", True),
                targets=_build_commute_targets(c.get("targets", [])),
            )

    # Scoring
    if "scoring" in raw:
        sc = raw["scoring"]
        weights = dict(DEFAULT_WEIGHTS)
        if "weights" in sc:
            weights.update(sc["weights"])
        config.scoring = ScoringConfig(
            weights=weights,
            top_deal_threshold=sc.get("top_deal_threshold", 70.0),
            claude_model=sc.get("claude_model", "claude-haiku-4-5-20251001"),
        )

    # Newsletter
    if "newsletter" in raw:
        n = raw["newsletter"]
        config.newsletter.recipients = n.get("recipients", [])
        config.newsletter.from_email = n.get("from_email", config.newsletter.from_email)
        config.newsletter.from_name = n.get("from_name", config.newsletter.from_name)
        config.newsletter.subject_template = n.get("subject_template", config.newsletter.subject_template)
        config.newsletter.max_listings_in_email = n.get("max_listings_in_email", 30)
        if "sections" in n:
            config.newsletter.sections = _build_newsletter_sections(n["sections"])

    # Database
    if "database" in raw:
        config.database.path = raw["database"].get("path", "data/listings.db")

    return config
