import json
import logging
import os
import secrets
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from functools import wraps
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
from authlib.integrations.flask_client import OAuth
from flask import Flask, flash, g, jsonify, redirect, render_template, request, session, url_for
from flask.json.provider import DefaultJSONProvider
from werkzeug.security import check_password_hash, generate_password_hash


class CricketJSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        def clean_obj(inner_obj):
            if isinstance(inner_obj, dict):
                return {k: clean_obj(v) for k, v in inner_obj.items()}
            if isinstance(inner_obj, list):
                return [clean_obj(i) for i in inner_obj]
            if hasattr(inner_obj, "tolist"):
                return inner_obj.tolist()
            if pd.isna(inner_obj):
                return None
            return inner_obj

        return super().dumps(clean_obj(obj), **kwargs)


app = Flask(__name__)
app.json = CricketJSONProvider(app)
app.secret_key = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")

BASE_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, ".."))
DATABASE_PATH = os.path.join(BASE_DIR, "users.db")
DATA_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
LOG_FILE = os.path.join(PROJECT_ROOT, "logs", "web.log")
NEW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "new")

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(message)s")
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

    return {"client_id": client_id, "client_secret": client_secret, "redirect_uri": redirect_uri}


google_client_config = load_google_client_config()

oauth = OAuth(app)
google = oauth.register(
    name="google",
    client_id=google_client_config["client_id"],
    client_secret=google_client_config["client_secret"],
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)

matches_df = pd.DataFrame()
deliveries_df = pd.DataFrame()
player_stats_df = pd.DataFrame()
team_stats_df = pd.DataFrame()
match_summary_df = pd.DataFrame()
merged_df = pd.DataFrame()


def read_csv_if_exists(filename, parse_dates=None):
    file_path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(file_path):
        return pd.DataFrame()
    return pd.read_csv(file_path, parse_dates=parse_dates)


def load_data():
    global matches_df, deliveries_df, player_stats_df, team_stats_df, match_summary_df, merged_df

    try:
        matches_df = read_csv_if_exists("matches.csv", parse_dates=["date"])
        deliveries_df = read_csv_if_exists("deliveries.csv")
        player_stats_df = read_csv_if_exists("player_stats.csv")
        team_stats_df = read_csv_if_exists("team_stats.csv")
        match_summary_df = read_csv_if_exists("match_summary.csv")

        if not deliveries_df.empty:
            if "wicket_kind" not in deliveries_df.columns:
                deliveries_df["wicket_kind"] = None
            if "player_out" not in deliveries_df.columns:
                deliveries_df["player_out"] = None

        merged_df = pd.DataFrame()
        if not matches_df.empty and not deliveries_df.empty:
            merged_df = deliveries_df.merge(
                matches_df[["match_id", "team1", "team2", "date", "venue", "winner"]],
                on="match_id",
                how="left",
            )
            merged_df["bowling_team"] = merged_df.apply(
                lambda row: row["team2"] if row["batting_team"] == row["team1"] else row["team1"],
                axis=1,
            )

        logger.info(
            "Data loaded: %s matches, %s deliveries, %s player stats, %s team stats, %s match summaries",
            len(matches_df),
            len(deliveries_df),
            len(player_stats_df),
            len(team_stats_df),
            len(match_summary_df),
        )
    except Exception as exc:
        logger.error("Error loading data: %s", exc)
        matches_df = pd.DataFrame()
        deliveries_df = pd.DataFrame()
        player_stats_df = pd.DataFrame()
        team_stats_df = pd.DataFrame()
        match_summary_df = pd.DataFrame()
        merged_df = pd.DataFrame()


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
                password_hash TEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        user_columns = {row[1] for row in db.execute("PRAGMA table_info(users)").fetchall()}
        if "password" in user_columns and "password_hash" not in user_columns:
            db.execute("ALTER TABLE users ADD COLUMN password_hash TEXT")
            db.execute("UPDATE users SET password_hash = password WHERE password_hash IS NULL")
        elif "password" in user_columns and "password_hash" in user_columns:
            db.execute("UPDATE users SET password_hash = password WHERE password_hash IS NULL AND password IS NOT NULL")
            
        if "password" in user_columns:
            try:
                db.execute("ALTER TABLE users DROP COLUMN password")
            except sqlite3.OperationalError as e:
                logger.warning(f"Could not drop password column: {e}")
                
        if "created_at" not in user_columns:
            db.execute("ALTER TABLE users ADD COLUMN created_at DATETIME")
            db.execute("UPDATE users SET created_at = CURRENT_TIMESTAMP WHERE created_at IS NULL")
        if "last_active" not in user_columns:
            db.execute("ALTER TABLE users ADD COLUMN last_active DATETIME")
        if "deleted_at" not in user_columns:
            db.execute("ALTER TABLE users ADD COLUMN deleted_at DATETIME")
        if "status" not in user_columns:
            db.execute("ALTER TABLE users ADD COLUMN status TEXT NOT NULL DEFAULT 'active'")
            db.execute("UPDATE users SET status = 'active' WHERE status IS NULL")

        db.execute("CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_users_deleted_at ON users(deleted_at)")
        db.execute("CREATE INDEX IF NOT EXISTS idx_users_last_active ON users(last_active)")

        db.commit()
    finally:
        db.close()


@app.teardown_appcontext
def close_db(_error):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def utc_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_user_by_email(email, include_deleted=False):
    if include_deleted:
        return get_db().execute("SELECT * FROM users WHERE email = ?", (email.lower(),)).fetchone()
    return get_db().execute(
        "SELECT * FROM users WHERE email = ? AND deleted_at IS NULL",
        (email.lower(),),
    ).fetchone()


def get_user_by_id(user_id, include_deleted=False):
    if include_deleted:
        return get_db().execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    return get_db().execute(
        "SELECT * FROM users WHERE id = ? AND deleted_at IS NULL",
        (user_id,),
    ).fetchone()


def update_user_last_active(user_id):
    get_db().execute(
        "UPDATE users SET last_active = ? WHERE id = ? AND deleted_at IS NULL",
        (utc_timestamp(), user_id),
    )
    get_db().commit()


def set_user_status(user_id, status):
    get_db().execute(
        "UPDATE users SET status = ? WHERE id = ? AND deleted_at IS NULL",
        (status, user_id),
    )
    get_db().commit()


def soft_delete_user(user_id):
    timestamp = utc_timestamp()
    get_db().execute(
        "UPDATE users SET deleted_at = ?, status = 'disabled' WHERE id = ? AND deleted_at IS NULL",
        (timestamp, user_id),
    )
    get_db().commit()


def fetch_users(include_deleted=False):
    query = """
        SELECT id, name, email, created_at, last_active, status, deleted_at
        FROM users
    """
    params = ()
    if not include_deleted:
        query += " WHERE deleted_at IS NULL"
    query += " ORDER BY id"
    return get_db().execute(query, params).fetchall()


def create_user(name, email, password_hash):
    db = get_db()
    cursor = db.execute(
        "INSERT INTO users (name, email, password_hash, last_active, status) VALUES (?, ?, ?, ?, 'active')",
        (name.strip(), email.lower(), password_hash, utc_timestamp()),
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
    if not user or user["status"] != "active":
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
    if parsed.hostname in {"0.0.0.0", "::", "[::]"}:
        host = parsed.netloc.replace(parsed.hostname, "localhost")
        redirect_uri = urlunsplit((parsed.scheme, host, parsed.path, parsed.query, parsed.fragment))
    return redirect_uri


@app.before_request
def sync_session_user():
    user = current_user()
    if user:
        session["user"] = user
        if request.path.startswith("/api") or request.path.startswith("/admin/users"):
            update_user_last_active(user["id"])


def apply_filters(df, filters):
    if df.empty:
        return df

    filtered_df = df.copy()
    team_a = filters.get("team_a") or filters.get("team")
    team_b = filters.get("team_b")

    if team_a and team_a != "All" and team_b and team_b != "All":
        filtered_df = filtered_df[
            ((filtered_df["team1"] == team_a) & (filtered_df["team2"] == team_b))
            | ((filtered_df["team1"] == team_b) & (filtered_df["team2"] == team_a))
        ]
    elif team_a and team_a != "All":
        filtered_df = filtered_df[(filtered_df["team1"] == team_a) | (filtered_df["team2"] == team_a)]

    if filters.get("venue") and filters["venue"] != "All" and "venue" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["venue"] == filters["venue"]]
    if filters.get("date_start") and "date" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["date"] >= filters["date_start"]]
    if filters.get("date_end") and "date" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["date"] <= filters["date_end"]]
    return filtered_df


def safe_records(dataframe):
    if dataframe.empty:
        return []
    return dataframe.where(pd.notnull(dataframe), None).to_dict(orient="records")


def wickets_series(dataframe):
    if dataframe.empty or "wicket_kind" not in dataframe.columns:
        return pd.Series(dtype="object")
    return dataframe["wicket_kind"]


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

        user = create_user(name, email, generate_password_hash(password))
        logger.info("User registered with email/password: %s", email)
        flash("Registration successful. Please log in.", "success")
        return redirect(url_for("login"))

    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user():
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        user = get_user_by_email(email)

        stored_password_hash = user["password_hash"] if user is not None else None
        if user is None or not stored_password_hash or not check_password_hash(stored_password_hash, password):
            flash("Invalid email or password.", "danger")
            return render_template("login.html")
        if user["status"] != "active":
            flash("Your account is disabled.", "danger")
            return render_template("login.html")

        login_user(user, provider="password")
        update_user_last_active(user["id"])
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
    elif user["status"] != "active":
        flash("Your account is disabled.", "danger")
        return redirect(url_for("login"))

    login_user(user, provider="google")
    update_user_last_active(user["id"])
    logger.info("User logged in with Google OAuth: %s", email)
    flash("Signed in with Google.", "success")
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
@login_required
def dashboard():
    logger.info("Dashboard accessed by: %s", current_user()["email"])
    return render_template("dashboard.html", user=current_user())


@app.route("/admin")
@login_required
def admin():
    logger.info("Admin page accessed by: %s", current_user()["email"])
    return render_template("admin.html", user=current_user())


@app.route("/logout")
def logout():
    session.clear()
    logger.info("User logged out")
    flash("You have been logged out.", "info")
    return redirect(url_for("login"))


@app.route("/api/log_interaction", methods=["POST"])
@login_required
def log_interaction():
    data = request.json or {}
    action = data.get("action", "Unknown")
    metadata = data.get("metadata", {})
    logger.info(
        "USER_INTERACTION | USER: %s | ACTION: %s | METADATA: %s",
        current_user()["email"],
        action,
        json.dumps(metadata),
    )
    return jsonify({"status": "success"})


@app.route("/api/filter_options")
@login_required
def filter_options():
    if matches_df.empty:
        return jsonify({"teams": [], "players": [], "venues": [], "date_range": {"min": "", "max": ""}})

    teams = pd.concat([matches_df["team1"], matches_df["team2"]]).dropna().unique().tolist()
    players = deliveries_df["batter"].dropna().unique().tolist() if not deliveries_df.empty else []
    venues = matches_df["venue"].dropna().unique().tolist() if "venue" in matches_df.columns else []

    date_min = ""
    date_max = ""
    if "date" in matches_df.columns and not matches_df["date"].dropna().empty:
        date_min = matches_df["date"].min().strftime("%Y-%m-%d")
        date_max = matches_df["date"].max().strftime("%Y-%m-%d")

    return jsonify(
        {
            "teams": sorted(teams),
            "players": sorted(players),
            "venues": sorted(venues),
            "date_range": {"min": date_min, "max": date_max},
        }
    )


@app.route("/api/filter_players")
@login_required
def filter_players():
    team_a = request.args.get("team_a", "All")
    team_b = request.args.get("team_b", "All")

    if deliveries_df.empty:
        return jsonify([])

    if team_a == "All" and team_b == "All":
        players = deliveries_df["batter"].dropna().unique().tolist()
    else:
        teams = [team for team in [team_a, team_b] if team != "All"]
        players = deliveries_df[deliveries_df["batting_team"].isin(teams)]["batter"].dropna().unique().tolist()

    return jsonify(sorted(players))


@app.route("/api/dashboard_stats", methods=["POST"])
@login_required
def dashboard_stats():
    filters = request.json or {}
    filtered_matches = apply_filters(matches_df, filters)
    match_ids = filtered_matches["match_id"].unique() if not filtered_matches.empty else []

    filtered_deliveries = merged_df[merged_df["match_id"].isin(match_ids)] if not merged_df.empty else pd.DataFrame()

    if filters.get("player") and filters["player"] != "All" and not filtered_deliveries.empty:
        filtered_deliveries = filtered_deliveries[filtered_deliveries["batter"] == filters["player"]]

    if not filtered_deliveries.empty and (filters.get("over_min") or filters.get("over_max")):
        over_min = float(filters.get("over_min", 0))
        over_max = float(filters.get("over_max", 50))
        filtered_deliveries = filtered_deliveries[
            (filtered_deliveries["over"] >= over_min) & (filtered_deliveries["over"] <= over_max)
        ]

    total_matches = len(match_ids)
    total_runs = int(filtered_deliveries["runs_total"].sum()) if not filtered_deliveries.empty else 0
    total_wickets = int(wickets_series(filtered_deliveries).notna().sum()) if not filtered_deliveries.empty else 0
    avg_score = round(total_runs / (total_matches * 2), 2) if total_matches > 0 else 0

    top_scorers = pd.DataFrame()
    top_bowlers = pd.DataFrame()
    if not filtered_deliveries.empty:
        top_scorers = (
            filtered_deliveries.groupby("batter")["runs_total"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
            .reset_index()
        )
        top_bowlers = (
            filtered_deliveries.groupby("bowler")["wicket_kind"].count().sort_values(ascending=False).head(5).reset_index()
            if "wicket_kind" in filtered_deliveries.columns
            else pd.DataFrame(columns=["bowler", "wicket_kind"])
        )

    team_wins = filtered_matches["winner"].value_counts().head(1) if not filtered_matches.empty else pd.Series(dtype="int64")
    most_wins_team = team_wins.index[0] if not team_wins.empty else "N/A"
    most_wins_count = int(team_wins.values[0]) if not team_wins.empty else 0

    fifties = 0
    centuries = 0
    highest_score = 0
    highest_score_team = "N/A"
    if not filtered_deliveries.empty:
        batter_scores = filtered_deliveries.groupby(["match_id", "batter"])["runs_total"].sum()
        fifties = int((batter_scores >= 50).sum())
        centuries = int((batter_scores >= 100).sum())
        match_scores = filtered_deliveries.groupby(["match_id", "batting_team"])["runs_total"].sum()
        if not match_scores.empty:
            highest_score = int(match_scores.max())
            highest_score_team = match_scores.idxmax()[1]

    recent_matches = filtered_matches.sort_values("date", ascending=False).head(5).copy() if not filtered_matches.empty else pd.DataFrame()
    if not recent_matches.empty and "date" in recent_matches.columns:
        recent_matches["date"] = recent_matches["date"].dt.strftime("%Y-%m-%d")

    return jsonify(
        {
            "kpis": {
                "total_matches": int(total_matches),
                "total_runs": total_runs,
                "total_wickets": total_wickets,
                "avg_score": float(avg_score),
            },
            "top_scorers": safe_records(top_scorers),
            "top_bowlers": safe_records(top_bowlers),
            "insights": {
                "most_wins_team": str(most_wins_team),
                "most_wins_count": most_wins_count,
                "highest_score": highest_score,
                "highest_score_team": str(highest_score_team),
                "fifties": fifties,
                "centuries": centuries,
            },
            "recent_matches": safe_records(recent_matches),
        }
    )


@app.route("/api/charts/runs_distribution", methods=["POST"])
@login_required
def runs_distribution():
    filters = request.json or {}
    filtered_matches = apply_filters(matches_df, filters)
    match_ids = filtered_matches["match_id"].unique() if not filtered_matches.empty else []
    filtered_deliveries = merged_df[merged_df["match_id"].isin(match_ids)] if not merged_df.empty else pd.DataFrame()

    if filtered_deliveries.empty:
        return jsonify({})

    if filters.get("player") and filters["player"] != "All":
        opponent_distribution = (
            filtered_deliveries.groupby("bowling_team")["runs_total"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
        )
        return jsonify(opponent_distribution.to_dict())

    phases = {
        "Powerplay (0-6)": int(filtered_deliveries[filtered_deliveries["over"] < 6]["runs_total"].sum()),
        "Middle (6-15)": int(
            filtered_deliveries[
                (filtered_deliveries["over"] >= 6) & (filtered_deliveries["over"] < 15)
            ]["runs_total"].sum()
        ),
        "Death (15-20)": int(filtered_deliveries[filtered_deliveries["over"] >= 15]["runs_total"].sum()),
    }
    return jsonify(phases)


@app.route("/api/charts/player_trends", methods=["POST"])
@login_required
def player_trends():
    filters = request.json or {}
    if merged_df.empty:
        return jsonify({"player": "N/A", "dates": [], "runs": []})

    player = filters.get("player")
    if not player or player == "All":
        player = merged_df.groupby("batter")["runs_total"].sum().idxmax()

    player_data = merged_df[merged_df["batter"] == player].sort_values("date")
    if player_data.empty:
        return jsonify({"player": player, "dates": [], "runs": []})

    trends = player_data.groupby("date")["runs_total"].sum().reset_index()
    return jsonify(
        {
            "player": player,
            "dates": [date.strftime("%Y-%m-%d") if hasattr(date, "strftime") else str(date) for date in trends["date"]],
            "runs": [int(runs) for runs in trends["runs_total"]],
        }
    )


@app.route("/api/players")
@login_required
def get_players():
    players = deliveries_df["batter"].dropna().unique().tolist() if not deliveries_df.empty else []
    return jsonify(players)


@app.route("/api/teams")
@login_required
def get_teams():
    teams = pd.concat([matches_df["team1"], matches_df["team2"]]).dropna().unique().tolist() if not matches_df.empty else []
    return jsonify(teams)


@app.route("/api/player_stats/<player>", defaults={"opponent": None})
@app.route("/api/player_stats/<player>/<opponent>")
@login_required
def player_stats(player, opponent=None):
    if merged_df.empty:
        return jsonify({"error": "No player data available"})

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
    if matches_df.empty or merged_df.empty:
        return jsonify(
            {
                "team1": team1,
                "team2": team2,
                "matches_played": 0,
                "team1_wins": 0,
                "team2_wins": 0,
                "team1_runs_against_team2": 0,
                "team1_avg_runs_per_match": 0,
                "team2_runs_against_team1": 0,
                "team2_avg_runs_per_match": 0,
            }
        )

    head_to_head_matches = matches_df[
        ((matches_df["team1"] == team1) & (matches_df["team2"] == team2))
        | ((matches_df["team1"] == team2) & (matches_df["team2"] == team1))
    ]
    matches_played = len(head_to_head_matches)
    team1_wins = (head_to_head_matches["winner"] == team1).sum()
    team2_wins = (head_to_head_matches["winner"] == team2).sum()

    team1_data = merged_df[(merged_df["batting_team"] == team1) & (merged_df["bowling_team"] == team2)]
    team2_data = merged_df[(merged_df["batting_team"] == team2) & (merged_df["bowling_team"] == team1)]

    team1_runs = team1_data["runs_total"].sum()
    team2_runs = team2_data["runs_total"].sum()
    team1_avg = team1_runs / matches_played if matches_played > 0 else 0
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


@app.route("/api/admin/users")
@app.route("/admin/users", methods=["GET"])
@login_required
def admin_users():
    include_deleted = request.args.get("include_deleted", "false").lower() in {"1", "true", "yes"}
    db_users = fetch_users(include_deleted=include_deleted)
    users = []
    for row in db_users:
        users.append(
            {
                "id": row["id"],
                "name": row["name"],
                "email": row["email"],
                "role": "Authenticated User",
                "last_active": row["last_active"] or "Never",
                "status": row["status"],
                "active": row["status"] == "active",
                "deleted_at": row["deleted_at"],
            }
        )
    return jsonify(users)


@app.route("/admin/users/<int:user_id>/status", methods=["PATCH"])
@login_required
def update_admin_user_status(user_id):
    payload = request.get_json(silent=True) or {}
    status = payload.get("status", "").strip().lower()
    if status not in {"active", "disabled"}:
        return jsonify({"error": "Invalid status. Allowed values: active, disabled"}), 400

    user = get_user_by_id(user_id)
    if user is None:
        return jsonify({"error": "User not found"}), 404

    set_user_status(user_id, status)
    updated_user = get_user_by_id(user_id)
    return jsonify(
        {
            "id": updated_user["id"],
            "status": updated_user["status"],
            "last_active": updated_user["last_active"],
        }
    )


@app.route("/admin/users/<int:user_id>", methods=["DELETE"])
@login_required
def soft_delete_admin_user(user_id):
    user = get_user_by_id(user_id)
    if user is None:
        return jsonify({"error": "User not found"}), 404

    soft_delete_user(user_id)
    return jsonify({"status": "deleted", "user_id": user_id, "deleted_at": utc_timestamp()})


@app.route("/api/admin/metrics")
@login_required
def admin_metrics():
    def get_dir_size(path):
        total = 0
        try:
            for entry in os.scandir(path):
                if entry.is_file():
                    total += entry.stat().st_size
                elif entry.is_dir():
                    total += get_dir_size(entry.path)
        except OSError:
            return total
        return total

    def format_size(size_bytes):
        value = float(size_bytes)
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if value < 1024.0:
                return f"{value:.1f} {unit}"
            value /= 1024.0
        return f"{value:.1f} PB"

    db_user_count = get_db().execute(
        "SELECT COUNT(*) AS count FROM users WHERE deleted_at IS NULL"
    ).fetchone()["count"]
    active_sessions = 1 if current_user() else 0

    return jsonify(
        {
            "total_users": int(db_user_count),
            "active_sessions": int(active_sessions),
            "queries_24h": len(matches_df) + len(deliveries_df),
            "data_size": format_size(get_dir_size(os.path.join(PROJECT_ROOT, "data"))),
        }
    )


@app.route("/api/admin/upload-file", methods=["POST"])
@login_required
def admin_upload_file():
    if "file" not in request.files:
        return jsonify({"status": "error", "message": "No file part"}), 400

    file = request.files["file"]
    if file.filename == "":
        return jsonify({"status": "error", "message": "No selected file"}), 400

    if file and file.filename.endswith(".json"):
        os.makedirs(NEW_DATA_DIR, exist_ok=True)
        target_path = os.path.join(NEW_DATA_DIR, file.filename)
        file.save(target_path)
        logger.info("Admin uploaded new file: %s", file.filename)
        return jsonify({"status": "success", "message": "File uploaded successfully. ETL can now be run."})

    return jsonify({"status": "error", "message": "Invalid file type. Only JSON allowed."}), 400


@app.route("/api/admin/upload", methods=["POST"])
@login_required
def admin_trigger_etl():
    has_new_files = os.path.exists(NEW_DATA_DIR) and any(name.endswith(".json") for name in os.listdir(NEW_DATA_DIR))
    if not has_new_files:
        return jsonify({"status": "ok", "message": "All is okay. No new data to process."})

    logger.info("Admin triggered ETL pipeline execution")
    try:
        result = subprocess.run(
            [sys.executable, "src/main.py"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        load_data()
        return jsonify(
            {
                "status": "success",
                "message": "ETL Pipeline completed successfully",
                "output": result.stdout,
            }
        )
    except subprocess.CalledProcessError as exc:
        logger.error("ETL Pipeline failed: %s", exc.stderr)
        return jsonify({"status": "error", "message": "ETL Pipeline failed", "error": exc.stderr}), 500


init_db()
load_data()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
