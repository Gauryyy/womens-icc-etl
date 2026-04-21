import logging
import json
import os
import secrets
import sqlite3
from functools import wraps
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
from authlib.integrations.flask_client import OAuth
from flask import Flask, flash, g, jsonify, redirect, render_template, request, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash

app = Flask(__name__)

# Secret key for sessions
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")

# Setup logging
BASE_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
DATABASE_PATH = os.path.join(BASE_DIR, "users.db")
log_file = os.path.join(PROJECT_ROOT, "logs", "web.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

def load_google_client_config():
    client_id = os.getenv("GOOGLE_CLIENT_ID", "").strip()
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
    redirect_uri = os.getenv("GOOGLE_REDIRECT_URI", "").strip()

    credentials_path = os.getenv("GOOGLE_CLIENT_SECRETS_FILE", "").strip()
    if not credentials_path:
        credentials_path = os.path.join(PROJECT_ROOT, "config", "google_oauth.json")

    if os.path.exists(credentials_path):
        with open(credentials_path, "r", encoding="utf-8") as credentials_file:
            payload = json.load(credentials_file)

        web_config = payload.get("web", {})
        client_id = client_id or web_config.get("client_id", "").strip()
        client_secret = client_secret or web_config.get("client_secret", "").strip()

        redirect_uris = web_config.get("redirect_uris") or []
        if redirect_uris and not redirect_uri:
            redirect_uri = redirect_uris[0].strip()

    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
    }


google_client_config = load_google_client_config()

# OAuth setup
oauth = OAuth(app)
google = oauth.register(
    name="google",
    client_id=google_client_config["client_id"],
    client_secret=google_client_config["client_secret"],
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)

# Load analytics data
data_dir = os.path.join(PROJECT_ROOT, "data", "processed")
matches_df = pd.read_csv(os.path.join(data_dir, "matches.csv"))
deliveries_df = pd.read_csv(os.path.join(data_dir, "deliveries.csv"))

# Merge to add bowling_team
merged_df = deliveries_df.merge(matches_df[["match_id", "team1", "team2"]], on="match_id", how="left")
merged_df["bowling_team"] = merged_df.apply(
    lambda row: row["team2"] if row["batting_team"] == row["team1"] else row["team1"], axis=1
)


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


def init_db():
    db = sqlite3.connect(DATABASE_PATH)
    try:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                password TEXT NOT NULL
            )
            """
        )
        db.commit()
    finally:
        db.close()


@app.teardown_appcontext
def close_db(_error):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def get_user_by_email(email):
    return get_db().execute("SELECT * FROM users WHERE email = ?", (email.lower(),)).fetchone()


def get_user_by_id(user_id):
    return get_db().execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()


def create_user(name, email, password_hash):
    db = get_db()
    cursor = db.execute(
        "INSERT INTO users (name, email, password) VALUES (?, ?, ?)",
        (name.strip(), email.lower(), password_hash),
    )
    db.commit()
    return get_user_by_id(cursor.lastrowid)


def login_user(user_row, provider="password"):
    session["user_id"] = user_row["id"]
    session["user"] = {
        "id": user_row["id"],
        "name": user_row["name"],
        "email": user_row["email"],
        "provider": provider,
    }


def current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    user = get_user_by_id(user_id)
    if not user:
        session.clear()
        return None
    return {
        "id": user["id"],
        "name": user["name"],
        "email": user["email"],
        "provider": session.get("user", {}).get("provider", "password"),
    }


def login_required(view_func):
    @wraps(view_func)
    def wrapped_view(*args, **kwargs):
        user = current_user()
        if user is None:
            flash("Please log in to continue.", "warning")
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)

    return wrapped_view


def get_google_redirect_uri():
    configured_redirect = google_client_config["redirect_uri"]
    if configured_redirect:
        return configured_redirect

    redirect_uri = url_for("auth_callback", _external=True)
    parsed = urlsplit(redirect_uri)

    # Google rejects 0.0.0.0 as an OAuth redirect host. When the app is
    # running locally, normalize it to localhost so it can match the console.
    if parsed.hostname in {"0.0.0.0", "::", "[::]"}:
        host = parsed.netloc.replace(parsed.hostname, "localhost")
        redirect_uri = urlunsplit((parsed.scheme, host, parsed.path, parsed.query, parsed.fragment))

    return redirect_uri


@app.before_request
def sync_session_user():
    user = current_user()
    if user:
        session["user"] = user


@app.route("/")
def home():
    if current_user():
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/register", methods=["GET", "POST"])
def register():
    if current_user():
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")

        if not name or not email or not password:
            flash("Name, email, and password are required.", "danger")
            return render_template("register.html")

        if get_user_by_email(email):
            flash("An account with that email already exists.", "danger")
            return render_template("register.html")

        password_hash = generate_password_hash(password)
        user = create_user(name, email, password_hash)
        login_user(user, provider="password")
        logger.info("User registered with email/password: %s", email)
        flash("Registration successful. Welcome to Cricket Analytics!", "success")
        return redirect(url_for("dashboard"))

    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user():
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        user = get_user_by_email(email)

        if user is None or not check_password_hash(user["password"], password):
            flash("Invalid email or password.", "danger")
            return render_template("login.html")

        login_user(user, provider="password")
        logger.info("User logged in with email/password: %s", email)
        flash("You are now logged in.", "success")
        return redirect(url_for("dashboard"))

    return render_template("login.html")


@app.route("/login/google")
def login_google():
    if not google_client_config["client_id"] or not google_client_config["client_secret"]:
        logger.error("Google OAuth credentials are missing")
        flash("Google login is not configured on this server yet.", "danger")
        return redirect(url_for("login"))

    redirect_uri = get_google_redirect_uri()
    logger.info("Starting Google OAuth with redirect URI: %s", redirect_uri)
    return google.authorize_redirect(redirect_uri, prompt="select_account", access_type="offline")


@app.route("/auth/callback")
def auth_callback():
    token = google.authorize_access_token()
    if not token:
        flash("Google login could not be completed.", "danger")
        return redirect(url_for("login"))

    google_user = token.get("userinfo")
    if not google_user:
        google_user = google.get("userinfo").json()
    email = google_user.get("email", "").strip().lower()
    name = google_user.get("name") or email.split("@")[0]

    if not email:
        flash("Google account did not return an email address.", "danger")
        return redirect(url_for("login"))

    user = get_user_by_email(email)
    if user is None:
        user = create_user(name, email, generate_password_hash(secrets.token_urlsafe(32)))
        logger.info("User auto-created from Google OAuth: %s", email)

    login_user(user, provider="google")
    logger.info("User logged in with Google OAuth: %s", email)
    flash("Signed in with Google.", "success")
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
@login_required
def dashboard():
    user = current_user()
    logger.info("Dashboard accessed by: %s", user["email"])
    return render_template("dashboard.html", user=user)


@app.route("/logout")
def logout():
    session.clear()
    logger.info("User logged out")
    flash("You have been logged out.", "info")
    return redirect(url_for("login"))


@app.route("/api/players")
@login_required
def get_players():
    logger.info("Players list requested")
    players = deliveries_df["batter"].unique().tolist()
    return jsonify(players)


@app.route("/api/teams")
@login_required
def get_teams():
    logger.info("Teams list requested")
    teams = pd.concat([matches_df["team1"], matches_df["team2"]]).unique().tolist()
    return jsonify(teams)


@app.route("/api/player_stats/<player>", defaults={"opponent": None})
@app.route("/api/player_stats/<player>/<opponent>")
@login_required
def player_stats(player, opponent=None):
    logger.info("Player stats requested: %s vs %s", player, opponent or "all")
    if opponent:
        player_data = merged_df[(merged_df["batter"] == player) & (merged_df["bowling_team"] == opponent)]
    else:
        player_data = deliveries_df[deliveries_df["batter"] == player]

    if player_data.empty:
        return jsonify({"error": "Player not found or no matches against opponent"})

    total_runs = player_data["runs_total"].sum()
    matches = player_data["match_id"].nunique()
    average = total_runs / matches if matches > 0 else 0
    return jsonify({"total_runs": int(total_runs), "average": round(average, 2), "matches": matches})


@app.route("/api/team_vs_team/<team1>/<team2>")
@login_required
def team_vs_team(team1, team2):
    logger.info("Team vs team stats requested: %s vs %s", team1, team2)
    head_to_head_matches = matches_df[
        ((matches_df["team1"] == team1) & (matches_df["team2"] == team2))
        | ((matches_df["team1"] == team2) & (matches_df["team2"] == team1))
    ]
    matches_played = len(head_to_head_matches)

    team1_wins = (head_to_head_matches["winner"] == team1).sum()
    team2_wins = (head_to_head_matches["winner"] == team2).sum()

    team1_data = merged_df[(merged_df["batting_team"] == team1) & (merged_df["bowling_team"] == team2)]
    team1_runs = team1_data["runs_total"].sum()
    team1_avg = team1_runs / matches_played if matches_played > 0 else 0

    team2_data = merged_df[(merged_df["batting_team"] == team2) & (merged_df["bowling_team"] == team1)]
    team2_runs = team2_data["runs_total"].sum()
    team2_avg = team2_runs / matches_played if matches_played > 0 else 0

    return jsonify(
        {
            "team1": team1,
            "team2": team2,
            "matches_played": matches_played,
            "team1_wins": int(team1_wins),
            "team2_wins": int(team2_wins),
            "team1_runs_against_team2": int(team1_runs),
            "team1_avg_runs_per_match": round(team1_avg, 2),
            "team2_runs_against_team1": int(team2_runs),
            "team2_avg_runs_per_match": round(team2_avg, 2),
        }
    )


init_db()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
