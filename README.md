# Women's ICC ETL and Analytics App

This repository contains a modular ETL pipeline built with Python and Pandas to process women's cricket match data from Cricsheet, plus a Flask web app for analytics exploration.

The web app now includes:
- email/password registration and login
- Google OAuth login
- session-based protected dashboard access
- SQLite user storage for authentication

***

## Features

- Extracts and processes Cricsheet JSON datasets from ZIP archives
- Transforms nested cricket data into flat, analysis-ready CSV files
- Serves analytics through a Flask dashboard
- Supports user registration and login with hashed passwords
- Supports Google OAuth sign-in alongside local login
- Uses SQLite for storing app users
- Protects dashboard and API routes with session-based authentication

***

## Project Structure

```text
womens-icc-etl/
|-- config/
|   |-- config.yaml
|   |-- google_oauth.example.json
|-- data/
|   |-- raw/
|   |-- processed/
|-- src/
|   |-- extract.py
|   |-- transform.py
|   |-- load.py
|   |-- utils.py
|   |-- main.py
|-- analytics/
|   |-- analysis.py
|-- web/
|   |-- app.py
|   |-- users.db
|   |-- static/
|   |   |-- css/
|   |   |   |-- styles.css
|   |   |-- js/
|   |       |-- script.js
|   |-- templates/
|       |-- login.html
|       |-- register.html
|       |-- dashboard.html
|-- logs/
|   |-- etl.log
|   |-- web.log
|-- .gitignore
|-- Dockerfile
|-- requirements.txt
|-- README.md
```

Note:
- `config/google_oauth.json` is intentionally ignored by Git and should stay local only.
- `web/users.db` is created automatically when the web app starts.

***

## ETL Overview

```text
ZIP File
   ->
Extract
   ->
Transform
   ->
Load CSV files
   ->
Analytics Dashboard
```

Generated output files:
- `data/processed/matches.csv`
- `data/processed/deliveries.csv`

***

## Authentication

The Flask app supports two login methods:
- Local auth using name, email, and password
- Google OAuth using a Google Cloud OAuth client

Local auth details:
- user data is stored in `web/users.db`
- passwords are hashed with `werkzeug.security.generate_password_hash`
- login validation uses `check_password_hash`
- duplicate email registration is blocked

Protected route:
- `/dashboard`

Auth routes:
- `/register`
- `/login`
- `/login/google`
- `/auth/callback`
- `/logout`

***

## Local Setup

### 1. Create a virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```powershell
pip install -r requirements.txt
```

### 3. Run the ETL pipeline

```powershell
python .\src\main.py
```

### 4. Configure Google OAuth

Copy the example file and add your real credentials:

```powershell
Copy-Item .\config\google_oauth.example.json .\config\google_oauth.json
```

Then update `config/google_oauth.json` with your real Google OAuth client values.

Required redirect URI in Google Cloud Console:

```text
http://localhost:5000/auth/callback
```

Important:
- create a `Web application` OAuth client in Google Cloud Console
- if your app is in `Testing`, add your Google account under `Test users`
- do not commit `config/google_oauth.json`

### 5. Run the Flask web app

```powershell
python .\web\app.py
```

Then open:

[http://127.0.0.1:5000](http://127.0.0.1:5000)

***

## Configuration

Pipeline configuration lives in:

`config/config.yaml`

Example:

```yaml
paths:
  zip_path: "data/raw/icc_womens_cricket_world_cup_female_json.zip"
  extract_path: "data/raw"
  processed_path: "data/processed"

web:
  host: "127.0.0.1"
  port: 5000
  debug: true
```

***

## Security Notes

- Keep real Google OAuth credentials only in `config/google_oauth.json`
- `config/google_oauth.json` is ignored by Git
- Commit `config/google_oauth.example.json` instead
- Rotate OAuth client secrets immediately if they are ever exposed

***

## Logs

App logs are written to:
- `logs/etl.log`
- `logs/web.log`

These are local runtime files and should not be committed.

***

## Tech Stack

- Python
- Pandas
- Flask
- SQLite
- Authlib
- Jinja2
- Bootstrap

***

## Data Source

Cricsheet: [https://cricsheet.org/](https://cricsheet.org/)
