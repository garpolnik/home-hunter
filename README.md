# Home Deal Finder

Aggregates home listings from Redfin, Realtor.com, and Zillow, deduplicates them, scores each listing as a "deal" using 14 weighted criteria, and delivers results via a monthly email newsletter and an interactive map. Designed for personal use in any US market.

## Quick Start

### 1. Clone and install

```bash
git clone git@github.com:garpolnik/home-hunter.git
cd home-hunter
pip install -r requirements.txt
```

### 2. Configure your search

```bash
cp config/config.example.yaml config/config.yaml
```

Edit `config/config.yaml` with your target area and preferences:

```yaml
search:
  locations:
    - type: zip
      value: "08002"          # Cherry Hill, NJ
    - type: city
      value: "Haddonfield"
      state: "NJ"

  filters:
    min_price: 250000
    max_price: 550000
    min_beds: 3
    min_baths: 2.0
    min_sqft: 1500
    property_types:
      - single_family
      - townhouse
```

### 3. Set up API keys

Copy the environment template and fill in your keys:

```bash
cp .env.example .env
```

| Variable | Required | How to get it |
|----------|----------|---------------|
| `RAPIDAPI_KEY` | For Realtor.com | Sign up at [rapidapi.com](https://rapidapi.com), subscribe to "Realty in US" (free tier: 500 req/mo) |
| `SENDGRID_API_KEY` | For email delivery | Sign up at [sendgrid.com](https://sendgrid.com) (free tier: 100 emails/day) |
| `WALKSCORE_API_KEY` | Optional | Sign up at [walkscore.com/professional/api.php](https://www.walkscore.com/professional/api.php) (free: 5k req/day) |
| `GOOGLE_MAPS_API_KEY` | Optional | [Google Cloud Console](https://console.cloud.google.com) > Distance Matrix API |

Load them before running:

```bash
source .env
# or export them individually:
export SENDGRID_API_KEY=SG.xxxxx
export RAPIDAPI_KEY=xxxxx
```

### 4. Run the scanner

```bash
python -m src
```

This executes the full pipeline:
1. Fetches listings from all enabled sources
2. Deduplicates across sources
3. Enriches with walk score, flood zone, and commute data
4. Filters out stale listings based on market conditions
5. Scores every listing on 14 criteria (0-100 deal score)
6. Generates an interactive map at `data/map.html`
7. Sends an HTML newsletter via SendGrid (or saves to `data/latest_newsletter.html` if no API key)

You can also pass a custom config path:

```bash
python -m src path/to/my-config.yaml
```

### 5. Launch the web GUI

```bash
python -m src.web.app
```

Open [http://localhost:5000](http://localhost:5000) in your browser. The web GUI lets you:

- **Configure searches** - enter ZIP codes, price range, beds/baths, sqft, property types, and feature preferences (garage, pool, basement)
- **Subscribe to the newsletter** - enter your email and preferences to receive monthly reports
- **View the interactive map** - at [http://localhost:5000/map](http://localhost:5000/map), see all listings as color-coded markers with filter controls
- **Read the latest newsletter** - at [http://localhost:5000/newsletter](http://localhost:5000/newsletter)

## Automated Monthly Runs (GitHub Actions)

The included workflow runs the scanner on the 1st of every month at 8:00 AM UTC.

### Setup

1. Push the repo to GitHub
2. Go to **Settings > Secrets and variables > Actions**
3. Add these repository secrets:
   - `RAPIDAPI_KEY`
   - `SENDGRID_API_KEY`
   - `WALKSCORE_API_KEY` (optional)
   - `GOOGLE_MAPS_API_KEY` (optional)
4. Make sure `config/config.yaml` is committed with your search preferences
5. Update the `newsletter.recipients` list in your config with your email address

The workflow will automatically commit the updated database, map, and newsletter back to the repo after each run.

To trigger a run manually: go to **Actions > Monthly Home Deal Scan > Run workflow**.

## Configuration Reference

### Search Locations

Three location types are supported:

```yaml
search:
  locations:
    - type: zip
      value: "08002"
    - type: city
      value: "Haddonfield"
      state: "NJ"
    - type: county
      value: "Camden"
      state: "NJ"
```

### Search Filters

```yaml
search:
  filters:
    min_price: 250000
    max_price: 550000
    min_beds: 3
    max_beds: 5
    min_baths: 2.0
    property_types: [single_family, townhouse, condo]
    min_sqft: 1500
    min_year_built: 1960
    exclude_hoa_over: 400         # Skip listings with HOA above this

    # Dynamic listing age filter - removes stale listings based on market speed.
    # Threshold = area median days-on-market * multiplier.
    # Hot market (15-day median, 4x) = hides listings older than 60 days.
    # Slow market (60-day median, 4x) = hides listings older than 240 days.
    max_dom_multiplier: 4.0
    max_dom_absolute: 365         # Hard cap in days, never show older than this
```

### Data Sources

```yaml
sources:
  redfin:
    enabled: true
    max_results_per_location: 350
    request_delay_seconds: 3       # Rate limiting
  realtor:
    enabled: true
    max_results_per_location: 200
    request_delay_seconds: 1
  zillow:
    enabled: false                 # Enable if you have a RapidAPI key for Zillow
```

### Enrichment

```yaml
enrichment:
  walkscore:
    enabled: true                  # Requires WALKSCORE_API_KEY
  flood_zone:
    enabled: true                  # Free, no key needed (FEMA API)
  commute:
    enabled: true                  # Requires GOOGLE_MAPS_API_KEY
    targets:
      - name: "Office"
        address: "1600 Market St, Philadelphia, PA"
      - name: "Parents"
        address: "123 Oak St, Vineland, NJ"
```

### Deal Scoring Weights

All weights are customizable and should sum to approximately 1.0:

```yaml
scoring:
  weights:
    price_vs_estimate: 0.20        # Price below Redfin/Zestimate
    price_per_sqft: 0.12           # $/sqft vs area median
    days_on_market: 0.08           # Longer DOM = more negotiating leverage
    price_reductions: 0.08         # Seller motivation signal
    school_rating: 0.07            # School district quality
    flood_risk: 0.06               # Flood insurance cost/risk
    commute_time: 0.06             # Avg commute to your targets
    hoa_cost: 0.05                 # Monthly HOA penalty
    lot_size_value: 0.05           # Lot size vs area median
    walk_score: 0.05               # Walkability
    bed_bath_value: 0.05           # Rooms per dollar
    features_bonus: 0.05           # Garage, basement, pool, fireplace
    tax_rate: 0.04                 # Effective property tax rate
    property_age: 0.04             # Newer = less maintenance risk
  top_deal_threshold: 70           # Min score for "Top Deals" newsletter section
```

### Newsletter

```yaml
newsletter:
  recipients:
    - "you@example.com"
    - "partner@example.com"
  from_email: "deals@yourdomain.com"
  from_name: "Home Deal Finder"
  subject_template: "Home Deal Finder: {new_count} New Listings - {month} {year}"
```

## Project Structure

```
home-hunter/
├── src/
│   ├── main.py                    # Pipeline orchestrator
│   ├── config.py                  # YAML config loader
│   ├── models.py                  # Data models (Listing, AreaStats)
│   ├── db.py                      # SQLite database layer
│   ├── security.py                # Input sanitization for external API data
│   ├── map_generator.py           # Interactive Leaflet.js map
│   ├── fetchers/
│   │   ├── base.py                # Abstract fetcher
│   │   ├── redfin.py              # Redfin Stingray API
│   │   └── realtor.py             # Realtor.com via RapidAPI
│   ├── enrichers/
│   │   ├── walkscore.py           # Walk/transit/bike scores
│   │   ├── flood_zone.py          # FEMA flood zone lookup
│   │   └── commute.py             # Google Maps commute times
│   ├── scoring/
│   │   ├── engine.py              # Weighted composite scorer
│   │   └── criteria.py            # 14 scoring functions
│   ├── dedup/
│   │   └── deduplicator.py        # Cross-source deduplication
│   ├── newsletter/
│   │   ├── generator.py           # HTML email builder
│   │   ├── sender.py              # SendGrid integration
│   │   └── templates/
│   │       └── newsletter.html    # Jinja2 email template
│   └── web/
│       ├── app.py                 # Flask web GUI
│       └── templates/
│           └── index.html         # Search & subscribe form
├── config/
│   ├── config.yaml                # Your config (not committed)
│   └── config.example.yaml        # Example config
├── data/                          # Generated files (DB, map, newsletter)
├── .github/workflows/
│   └── monthly_scan.yml           # Monthly cron job
├── requirements.txt
└── pyproject.toml
```

## How the Deal Score Works

Each listing receives a composite score from 0 to 100. The score is a weighted average of 14 sub-scores, each ranging from 0.0 to 1.0:

| Score | Meaning |
|-------|---------|
| 70-100 | Great deal - strong value relative to market |
| 50-69 | Good deal - some favorable factors |
| 0-49 | Below average - overpriced or unfavorable factors |

The interactive map and newsletter use color coding: green (70+), yellow (50-69), red (<50).

Key scoring signals:
- **Price vs Estimate (20%)** - The biggest signal. A listing priced 15%+ below its Redfin estimate or Zestimate scores 1.0.
- **Price per SqFt (12%)** - Compared to the ZIP code median. 30%+ below median scores 1.0.
- **Days on Market (8%)** - Listings sitting longer give you more negotiating power.
- **Price Reductions (8%)** - Multiple price drops indicate a motivated seller.

## How Deduplication Works

The same property often appears on all three sites. The deduplicator:

1. **Normalizes addresses** - standardizes abbreviations (Street->St, Avenue->Ave), removes punctuation, combines with city/state/zip
2. **Geo-matches** - if two listings are within ~11 meters of each other AND have similar prices AND matching bed/bath counts, they're merged
3. **Merges data** - keeps the richest data from each source (Redfin estimate from Redfin, Zestimate from Zillow, best photos, etc.)

## Security

- All external API data passes through input sanitization (`src/security.py`) before use
- XSS pattern detection and HTML escaping on text fields
- Jinja2 template injection pattern detection
- URL validation against trusted domains
- Numeric range validation (price 0-100M, coordinates, etc.)
- Dependencies are pinned to specific versions in `requirements.txt`
- Web GUI validates and sanitizes all form inputs (email regex, ZIP format, numeric bounds)

## Disclaimer

This tool is for **personal use only**. Data is sourced from public real estate listings. Deal scores are algorithmic estimates based on market data and should not be considered financial advice. Always consult with a real estate professional before making purchasing decisions.
