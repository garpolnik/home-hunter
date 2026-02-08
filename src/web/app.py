"""
Flask web application for Home Deal Finder.
Provides a GUI for configuring searches, subscribing to newsletters,
and viewing the interactive map.
"""

import json
import logging
import os
import re
from pathlib import Path

from flask import Flask, flash, redirect, render_template, request, send_file, url_for

logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder="templates")
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(32))

DATA_DIR = Path("data")
CONFIG_DIR = Path("config")

# Email validation pattern
EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
# ZIP code validation
ZIP_PATTERN = re.compile(r"^\d{5}$")


def _load_subscribers() -> list[dict]:
    """Load subscriber list from JSON file."""
    path = DATA_DIR / "subscribers.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return []


def _save_subscribers(subscribers: list[dict]):
    """Save subscriber list to JSON file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(DATA_DIR / "subscribers.json", "w") as f:
        json.dump(subscribers, f, indent=2)


def _sanitize_input(value: str, max_length: int = 200) -> str:
    """Sanitize user input to prevent injection."""
    if not isinstance(value, str):
        return ""
    # Strip whitespace and null bytes
    value = value.strip().replace("\x00", "")
    # Truncate
    value = value[:max_length]
    return value


@app.route("/")
def index():
    """Main page with search form and subscription."""
    return render_template("index.html")


@app.route("/subscribe", methods=["POST"])
def subscribe():
    """Handle newsletter subscription form."""
    # Validate and sanitize all inputs
    email = _sanitize_input(request.form.get("email", ""), 254)
    if not EMAIL_PATTERN.match(email):
        flash("Please enter a valid email address.", "error")
        return redirect(url_for("index"))

    # Build search preferences from form
    zip_codes_raw = _sanitize_input(request.form.get("zip_codes", ""), 500)
    zip_codes = [z.strip() for z in zip_codes_raw.split(",") if ZIP_PATTERN.match(z.strip())]
    if not zip_codes:
        flash("Please enter at least one valid 5-digit ZIP code.", "error")
        return redirect(url_for("index"))

    min_price = request.form.get("min_price", "0")
    max_price = request.form.get("max_price", "999999999")
    min_beds = request.form.get("min_beds", "0")
    min_baths = request.form.get("min_baths", "0")
    min_sqft = request.form.get("min_sqft", "0")

    # Validate numeric inputs
    try:
        min_price = max(0, int(min_price))
        max_price = max(0, int(max_price))
        min_beds = max(0, int(min_beds))
        min_baths = max(0, int(min_baths))
        min_sqft = max(0, int(min_sqft))
    except ValueError:
        flash("Please enter valid numbers for price, beds, baths, and sqft.", "error")
        return redirect(url_for("index"))

    if min_price >= max_price:
        flash("Min price must be less than max price.", "error")
        return redirect(url_for("index"))

    property_types = request.form.getlist("property_types")
    valid_types = {"single_family", "townhouse", "condo", "multi_family"}
    property_types = [pt for pt in property_types if pt in valid_types]
    if not property_types:
        property_types = ["single_family", "townhouse", "condo"]

    wants_garage = request.form.get("garage") == "on"
    wants_pool = request.form.get("pool") == "on"
    wants_basement = request.form.get("basement") == "on"

    subscriber = {
        "email": email,
        "zip_codes": zip_codes,
        "min_price": min_price,
        "max_price": max_price,
        "min_beds": min_beds,
        "min_baths": min_baths,
        "min_sqft": min_sqft,
        "property_types": property_types,
        "wants_garage": wants_garage,
        "wants_pool": wants_pool,
        "wants_basement": wants_basement,
    }

    # Save subscriber
    subscribers = _load_subscribers()
    # Update existing or add new
    existing = next((s for s in subscribers if s["email"] == email), None)
    if existing:
        existing.update(subscriber)
        flash("Your preferences have been updated!", "success")
    else:
        subscribers.append(subscriber)
        flash("You've been subscribed! You'll receive your first report on the 1st of next month.", "success")

    _save_subscribers(subscribers)
    return redirect(url_for("index"))


@app.route("/unsubscribe", methods=["POST"])
def unsubscribe():
    """Handle unsubscribe request."""
    email = _sanitize_input(request.form.get("email", ""), 254)
    subscribers = _load_subscribers()
    subscribers = [s for s in subscribers if s["email"] != email]
    _save_subscribers(subscribers)
    flash("You've been unsubscribed.", "success")
    return redirect(url_for("index"))


@app.route("/map")
def live_map():
    """Serve the interactive map."""
    map_path = DATA_DIR / "map.html"
    if map_path.exists():
        return send_file(map_path)
    return render_template("no_map.html")


@app.route("/newsletter")
def latest_newsletter():
    """Serve the latest newsletter."""
    newsletter_path = DATA_DIR / "latest_newsletter.html"
    if newsletter_path.exists():
        return send_file(newsletter_path)
    return "<h1>No newsletter generated yet.</h1><p>Run the scanner first.</p>", 404


def create_app():
    """Factory function for creating the Flask app."""
    return app


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
