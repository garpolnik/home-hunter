"""
Flask web application for Home Deal Finder.
Provides a GUI for configuring searches, subscribing to newsletters,
viewing the interactive map, and admin approval of search requests.
"""

import json
import logging
import os
import re
import threading
from functools import wraps
from pathlib import Path

from flask import (
    Flask,
    abort,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)

from src.db import Database

logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder="templates")
app.secret_key = os.environ.get("FLASK_SECRET_KEY", os.urandom(32))

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"
DB_PATH = os.environ.get("DB_PATH", "data/listings.db")

# Email validation pattern
EMAIL_PATTERN = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
# ZIP code validation
ZIP_PATTERN = re.compile(r"^\d{5}$")

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")

# Limit concurrent background pipeline runs
_pipeline_semaphore = threading.Semaphore(2)


def _get_db() -> Database:
    """Get a database connection."""
    return Database(DB_PATH)


def _sanitize_input(value: str, max_length: int = 200) -> str:
    """Sanitize user input to prevent injection."""
    if not isinstance(value, str):
        return ""
    value = value.strip().replace("\x00", "")
    value = value[:max_length]
    return value


def admin_required(f):
    """Decorator that checks for a valid admin token in the query string."""
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.args.get("token", "")
        if not ADMIN_TOKEN or token != ADMIN_TOKEN:
            abort(403)
        return f(*args, **kwargs)
    return decorated


@app.route("/")
def index():
    """Main page with search form and subscription."""
    return render_template("index.html")


@app.route("/subscribe", methods=["POST"])
def subscribe():
    """Handle newsletter subscription — creates a pending search request."""
    email = _sanitize_input(request.form.get("email", ""), 254)
    if not EMAIL_PATTERN.match(email):
        flash("Please enter a valid email address.", "error")
        return redirect(url_for("index"))

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

    preferences = {
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

    db = _get_db()
    try:
        db.create_search_request(email, zip_codes, preferences)
    finally:
        db.close()

    flash(
        "Your search request has been submitted! "
        "You'll receive an email once your request is approved.",
        "success",
    )
    return redirect(url_for("index"))


@app.route("/unsubscribe", methods=["POST"])
def unsubscribe():
    """Handle unsubscribe request."""
    email = _sanitize_input(request.form.get("email", ""), 254)
    if not email:
        flash("Please enter your email address.", "error")
        return redirect(url_for("index"))

    db = _get_db()
    try:
        # Mark any approved requests for this email as rejected (effectively unsubscribed)
        approved = db.get_approved_subscribers()
        for sub in approved:
            if sub["email"] == email:
                db.update_request_status(sub["id"], "rejected")
    finally:
        db.close()

    flash("You've been unsubscribed.", "success")
    return redirect(url_for("index"))


@app.route("/map")
def live_map():
    """Serve the interactive map."""
    map_path = DATA_DIR / "map.html"
    if map_path.exists():
        return send_file(map_path)
    return "<h1>No map generated yet.</h1><p>Check back after the next scan.</p>", 404


@app.route("/newsletter")
def latest_newsletter():
    """Serve the latest newsletter."""
    newsletter_path = DATA_DIR / "latest_newsletter.html"
    if newsletter_path.exists():
        return send_file(newsletter_path)
    return "<h1>No newsletter generated yet.</h1><p>Check back after the next scan.</p>", 404


# --- Per-user private routes ---


@app.route("/u/<token>")
def user_dashboard(token):
    """User's personal dashboard with links to their map and newsletter."""
    db = _get_db()
    try:
        req = db.get_request_by_token(token)
    finally:
        db.close()

    if not req:
        abort(404)

    # Parse JSON fields for display
    req["zip_codes"] = json.loads(req["zip_codes"]) if isinstance(req["zip_codes"], str) else req["zip_codes"]
    req["preferences"] = json.loads(req["preferences"]) if isinstance(req["preferences"], str) else req["preferences"]

    return render_template("user_dashboard.html", req=req, token=token)


@app.route("/u/<token>/map")
def user_map(token):
    """Serve a user's personalized map via their private token."""
    db = _get_db()
    try:
        req = db.get_request_by_token(token)
    finally:
        db.close()

    if not req:
        abort(404)

    map_path = req.get("map_path")
    if map_path and Path(map_path).exists():
        return send_file(Path(map_path).resolve())

    return render_template("no_map.html"), 202


@app.route("/u/<token>/newsletter")
def user_newsletter(token):
    """Serve a user's personalized newsletter."""
    db = _get_db()
    try:
        req = db.get_request_by_token(token)
    finally:
        db.close()

    if not req:
        abort(404)

    newsletter_path = req.get("newsletter_path")
    if newsletter_path and Path(newsletter_path).exists():
        return send_file(Path(newsletter_path).resolve())

    return "<h1>Your newsletter is being generated.</h1><p>Check back in a few minutes.</p>", 202


# --- Admin routes ---


@app.route("/admin/requests")
@admin_required
def admin_requests():
    """Admin dashboard showing all search requests."""
    db = _get_db()
    try:
        requests_list = db.get_all_requests()
        # Parse JSON fields for display
        for req in requests_list:
            req["zip_codes"] = json.loads(req["zip_codes"])
            req["preferences"] = json.loads(req["preferences"])
    finally:
        db.close()

    return render_template(
        "admin_requests.html",
        requests=requests_list,
        token=ADMIN_TOKEN,
    )


@app.route("/admin/approve/<request_id>", methods=["POST"])
@admin_required
def admin_approve(request_id):
    """Approve a search request, generate access token, trigger pipeline in background."""
    db = _get_db()
    try:
        req = db.get_request(request_id)
        if not req:
            abort(404)
        db.update_request_status(request_id, "approved")
        access_token = db.set_access_token(request_id)
        db.update_user_run_status(request_id, "running")
        # Re-fetch with token
        req = db.get_request(request_id)
    finally:
        db.close()

    # Trigger pipeline in background thread
    def _run_pipeline():
        with _pipeline_semaphore:
            from src.config import load_config
            from src.user_pipeline import run_for_user

            pipeline_db = Database(DB_PATH)
            try:
                config = load_config()
                # Send welcome email first
                from src.newsletter.sendgrid_sender import SendGridSender

                sender = SendGridSender(config)
                dashboard_url = f"https://homehunter.casa/u/{access_token}"
                sender.send_welcome(req["email"], dashboard_url)

                run_for_user(req, config, pipeline_db, fetch_new=True, send_email=True)
            except Exception:
                logger.exception(f"Background pipeline failed for {req['email']}")
                pipeline_db.update_user_run_status(request_id, "failed")
            finally:
                pipeline_db.close()

    thread = threading.Thread(target=_run_pipeline, daemon=True)
    thread.start()

    flash(
        f"Request from {req['email']} approved. Pipeline running in background. "
        f"Private URL: https://homehunter.casa/u/{access_token}",
        "success",
    )
    return redirect(url_for("admin_requests", token=ADMIN_TOKEN))


@app.route("/admin/reject/<request_id>", methods=["POST"])
@admin_required
def admin_reject(request_id):
    """Reject a pending search request."""
    db = _get_db()
    try:
        req = db.get_request(request_id)
        if not req:
            abort(404)
        db.update_request_status(request_id, "rejected")
    finally:
        db.close()

    flash(f"Request from {req['email']} rejected.", "success")
    return redirect(url_for("admin_requests", token=ADMIN_TOKEN))


@app.route("/admin/runs")
@admin_required
def admin_runs():
    """Admin page showing recent per-user pipeline run history."""
    db = _get_db()
    try:
        runs = db.get_user_runs(limit=50)
    finally:
        db.close()

    return render_template("admin_runs.html", runs=runs, token=ADMIN_TOKEN)


@app.route("/admin/rerun/<request_id>", methods=["POST"])
@admin_required
def admin_rerun(request_id):
    """Re-run the pipeline for an approved user."""
    db = _get_db()
    try:
        req = db.get_request(request_id)
        if not req or req["status"] != "approved":
            abort(404)
        db.update_user_run_status(request_id, "running")
    finally:
        db.close()

    access_token = req.get("access_token", "")

    def _run_pipeline():
        with _pipeline_semaphore:
            from src.config import load_config
            from src.user_pipeline import run_for_user

            pipeline_db = Database(DB_PATH)
            try:
                config = load_config()
                run_for_user(req, config, pipeline_db, fetch_new=True, send_email=True)
            except Exception:
                logger.exception(f"Re-run pipeline failed for {req['email']}")
                pipeline_db.update_user_run_status(request_id, "failed")
            finally:
                pipeline_db.close()

    thread = threading.Thread(target=_run_pipeline, daemon=True)
    thread.start()

    flash(f"Pipeline re-run started for {req['email']}.", "success")
    return redirect(url_for("admin_requests", token=ADMIN_TOKEN))


def create_app():
    """Factory function for creating the Flask app."""
    return app


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
