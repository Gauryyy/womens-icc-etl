import json
import os
from glob import glob


def transform_data(extract_path, logger):
    files = glob(f"{extract_path}/*.json")

    matches = []
    deliveries = []

    logger.info(f"Processing {len(files)} files")

    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        info = data.get("info", {})
        match_id = os.path.basename(file).replace(".json", "")

        # Match table
        matches.append({
            "match_id": match_id,
            "date": (info.get("dates") or [None])[0],
            "team1": (info.get("teams") or [None, None])[0],
            "team2": (info.get("teams") or [None, None])[1],
            "venue": info.get("venue"),
            "city": info.get("city"),
            "winner": info.get("outcome", {}).get("winner"),
        })

        # Deliveries table
        for i, innings in enumerate(data.get("innings", [])):

            # ✅ Handle BOTH formats (new + old)
            if isinstance(innings, dict) and "team" in innings:
                inning_data = innings
            else:
                # old format: {"1st innings": {...}}
                inning_data = list(innings.values())[0]

            team = inning_data.get("team")

            for over_data in inning_data.get("overs", []):
                over = over_data.get("over")

                for ball_idx, delivery in enumerate(over_data.get("deliveries", [])):
                    deliveries.append({
                        "match_id": match_id,
                        "innings": i + 1,
                        "batting_team": team,
                        "over": over,
                        "ball": ball_idx + 1,
                        "batter": delivery.get("batter"),
                        "bowler": delivery.get("bowler"),
                        "runs_total": delivery.get("runs", {}).get("total"),
                    })

    logger.info("Transformation complete")

    return matches, deliveries