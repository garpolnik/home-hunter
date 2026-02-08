"""
Generates an interactive Leaflet.js HTML map of all active listings.
Color-coded markers by deal score with popups showing listing details.
Uses OpenStreetMap tiles (free, no API key needed).
"""

import json
import logging
from pathlib import Path

from src.models import Listing

logger = logging.getLogger(__name__)

MAP_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Home Deal Finder - Live Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
      integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
      crossorigin="anonymous" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
        integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
        crossorigin="anonymous"></script>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
  #map { height: 100vh; width: 100%; }
  .header-bar {
    position: fixed; top: 0; left: 0; right: 0; z-index: 1000;
    background: rgba(26, 54, 93, 0.95); color: white;
    padding: 10px 20px; display: flex; align-items: center;
    justify-content: space-between; backdrop-filter: blur(4px);
  }
  .header-bar h1 { font-size: 18px; font-weight: 600; }
  .header-bar .stats { font-size: 13px; opacity: 0.85; }
  .legend {
    position: fixed; bottom: 30px; right: 10px; z-index: 1000;
    background: white; padding: 12px 16px; border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.2); font-size: 13px;
  }
  .legend-item { display: flex; align-items: center; margin: 4px 0; }
  .legend-dot { width: 14px; height: 14px; border-radius: 50%; margin-right: 8px; border: 2px solid rgba(0,0,0,0.2); }
  .filter-bar {
    position: fixed; top: 46px; left: 0; right: 0; z-index: 999;
    background: rgba(255,255,255,0.95); padding: 8px 20px;
    display: flex; gap: 12px; align-items: center; flex-wrap: wrap;
    border-bottom: 1px solid #e2e8f0; backdrop-filter: blur(4px);
    font-size: 13px;
  }
  .filter-bar label { font-weight: 500; color: #4a5568; }
  .filter-bar input, .filter-bar select {
    padding: 4px 8px; border: 1px solid #cbd5e0; border-radius: 4px;
    font-size: 13px;
  }
  .popup-content { min-width: 250px; }
  .popup-content h3 { font-size: 15px; margin-bottom: 4px; color: #2d3748; }
  .popup-content .price { font-size: 20px; font-weight: 700; color: #2d3748; margin: 4px 0; }
  .popup-content .details { font-size: 13px; color: #4a5568; line-height: 1.6; }
  .popup-content .score-badge {
    display: inline-block; padding: 2px 10px; border-radius: 10px;
    color: white; font-weight: 700; font-size: 14px; margin: 4px 0;
  }
  .popup-content .links { margin-top: 8px; }
  .popup-content .links a { font-size: 12px; color: #3182ce; margin-right: 10px; text-decoration: none; }
  .popup-content .links a:hover { text-decoration: underline; }
</style>
</head>
<body>
<div class="header-bar">
  <h1>Home Deal Finder - Live Map</h1>
  <span class="stats" id="statsText"></span>
</div>
<div class="filter-bar">
  <label>Min Score:</label>
  <input type="range" id="minScore" min="0" max="100" value="0" oninput="filterListings()">
  <span id="minScoreVal">0</span>
  <label>Max Price:</label>
  <input type="number" id="maxPrice" placeholder="Any" step="10000" oninput="filterListings()">
  <label>Min Beds:</label>
  <select id="minBeds" onchange="filterListings()">
    <option value="0">Any</option><option value="1">1+</option><option value="2">2+</option>
    <option value="3">3+</option><option value="4">4+</option><option value="5">5+</option>
  </select>
  <label>Type:</label>
  <select id="propType" onchange="filterListings()">
    <option value="">All</option><option value="single_family">Single Family</option>
    <option value="townhouse">Townhouse</option><option value="condo">Condo</option>
  </select>
</div>
<div class="legend">
  <div class="legend-item"><div class="legend-dot" style="background:#38a169;"></div> Great Deal (70+)</div>
  <div class="legend-item"><div class="legend-dot" style="background:#d69e2e;"></div> Good Deal (50-69)</div>
  <div class="legend-item"><div class="legend-dot" style="background:#e53e3e;"></div> Below Average (&lt;50)</div>
  <div class="legend-item"><div class="legend-dot" style="background:#a0aec0;"></div> Not Scored</div>
</div>
<div id="map"></div>
<script>
const LISTINGS = __LISTINGS_JSON__;

const map = L.map('map', { zoomControl: true }).setView([39.85, -75.0], 10);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: '&copy; OpenStreetMap contributors',
  maxZoom: 19,
}).addTo(map);

// Offset map down to account for header + filter bar
map.getContainer().style.marginTop = '90px';
map.getContainer().style.height = 'calc(100vh - 90px)';
map.invalidateSize();

let markers = [];
let markerLayer = L.layerGroup().addTo(map);

function getColor(score) {
  if (score === null || score === undefined) return '#a0aec0';
  if (score >= 70) return '#38a169';
  if (score >= 50) return '#d69e2e';
  return '#e53e3e';
}

function createMarker(listing) {
  const color = getColor(listing.deal_score);
  const marker = L.circleMarker([listing.latitude, listing.longitude], {
    radius: 8, fillColor: color, color: '#fff', weight: 2,
    opacity: 1, fillOpacity: 0.85,
  });

  const scoreBg = listing.deal_score !== null
    ? `background:${color}` : 'background:#a0aec0';
  const scoreText = listing.deal_score !== null
    ? listing.deal_score.toFixed(1) : 'N/A';

  let linksHtml = '';
  if (listing.source_urls) {
    for (const [src, url] of Object.entries(listing.source_urls)) {
      if (url) linksHtml += `<a href="${url}" target="_blank">${src.charAt(0).toUpperCase() + src.slice(1)}</a>`;
    }
  }

  const fmt = (n) => n ? n.toLocaleString() : 'N/A';

  marker.bindPopup(`
    <div class="popup-content">
      <h3>${listing.address}</h3>
      <div>${listing.city}, ${listing.state} ${listing.zip_code}</div>
      <div class="price">$${listing.price.toLocaleString()}</div>
      <span class="score-badge" style="${scoreBg}">Score: ${scoreText}</span>
      <div class="details">
        ${listing.bedrooms || '?'} bed &bull; ${listing.bathrooms || '?'} bath &bull; ${fmt(listing.sqft)} sqft<br>
        ${listing.lot_sqft ? fmt(listing.lot_sqft) + ' lot sqft &bull; ' : ''}
        ${listing.year_built ? 'Built ' + listing.year_built + ' &bull; ' : ''}
        ${listing.days_on_market !== null ? listing.days_on_market + ' DOM' : ''}<br>
        ${listing.hoa_monthly ? '$' + listing.hoa_monthly + '/mo HOA &bull; ' : ''}
        ${listing.walk_score ? 'Walk: ' + listing.walk_score + ' &bull; ' : ''}
        ${listing.flood_risk_rating ? 'Flood: ' + listing.flood_risk_rating : ''}
      </div>
      <div class="links">${linksHtml}</div>
    </div>
  `);

  marker._listingData = listing;
  return marker;
}

function filterListings() {
  const minScore = parseInt(document.getElementById('minScore').value);
  document.getElementById('minScoreVal').textContent = minScore;
  const maxPrice = parseInt(document.getElementById('maxPrice').value) || Infinity;
  const minBeds = parseInt(document.getElementById('minBeds').value);
  const propType = document.getElementById('propType').value;

  markerLayer.clearLayers();
  let shown = 0;
  markers.forEach(m => {
    const d = m._listingData;
    const score = d.deal_score !== null ? d.deal_score : 0;
    if (score >= minScore && d.price <= maxPrice
        && (d.bedrooms || 0) >= minBeds
        && (!propType || d.property_type === propType)) {
      markerLayer.addLayer(m);
      shown++;
    }
  });
  document.getElementById('statsText').textContent =
    `Showing ${shown} of ${markers.length} listings`;
}

// Initialize markers
LISTINGS.forEach(l => {
  if (l.latitude && l.longitude) {
    markers.push(createMarker(l));
  }
});
markers.forEach(m => markerLayer.addLayer(m));

// Fit bounds to all markers
if (markers.length > 0) {
  const group = L.featureGroup(markers);
  map.fitBounds(group.getBounds().pad(0.1));
}

document.getElementById('statsText').textContent =
  `Showing ${markers.length} of ${markers.length} listings`;
</script>
</body>
</html>"""


def _listing_to_map_dict(listing: Listing) -> dict | None:
    """Convert listing to a JSON-safe dict for the map. Only include what the map needs."""
    if listing.latitude is None or listing.longitude is None:
        return None

    return {
        "address": listing.address,
        "city": listing.city,
        "state": listing.state,
        "zip_code": listing.zip_code,
        "latitude": listing.latitude,
        "longitude": listing.longitude,
        "price": listing.price,
        "property_type": listing.property_type.value if listing.property_type else None,
        "bedrooms": listing.bedrooms,
        "bathrooms": listing.bathrooms,
        "sqft": listing.sqft,
        "lot_sqft": listing.lot_sqft,
        "year_built": listing.year_built,
        "days_on_market": listing.days_on_market,
        "hoa_monthly": listing.hoa_monthly,
        "deal_score": listing.deal_score,
        "walk_score": listing.walk_score,
        "flood_risk_rating": listing.flood_risk_rating,
        "source_urls": listing.source_urls,
    }


def generate_map(listings: list[Listing], output_path: str = "data/map.html"):
    """Generate an interactive HTML map file from listings."""
    map_listings = []
    for listing in listings:
        d = _listing_to_map_dict(listing)
        if d:
            map_listings.append(d)

    logger.info(f"Generating map with {len(map_listings)} geo-located listings")

    listings_json = json.dumps(map_listings, default=str)
    html = MAP_TEMPLATE.replace("__LISTINGS_JSON__", listings_json)

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w") as f:
        f.write(html)

    logger.info(f"Map saved to {output_path}")
