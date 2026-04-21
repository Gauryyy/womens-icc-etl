from flask import Flask, render_template, jsonify
import pandas as pd
import os
import logging

app = Flask(__name__)

from src.main import run_pipeline

@app.route("/run_pipeline/<int:user_id>")
def run_pipeline_route(user_id):
    try:
        run_pipeline(user_id)
        return "Pipeline executed successfully"
    except Exception as e:
        return str(e)

# Setup logging
log_file = os.path.join(os.path.dirname(__file__), '..', 'logs', 'web.log')
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# Load data
data_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')
matches_df = pd.read_csv(os.path.join(data_dir, 'matches.csv'))
deliveries_df = pd.read_csv(os.path.join(data_dir, 'deliveries.csv'))

# Merge to add bowling_team
merged_df = deliveries_df.merge(matches_df[['match_id', 'team1', 'team2']], on='match_id', how='left')
merged_df['bowling_team'] = merged_df.apply(
    lambda row: row['team2'] if row['batting_team'] == row['team1'] else row['team1'], axis=1
)

@app.route('/')
def index():
    logger.info("Home page accessed")
    return render_template('index.html')

@app.route('/api/players')
def get_players():
    logger.info("Players list requested")
    players = deliveries_df['batter'].unique().tolist()
    return jsonify(players)

@app.route('/api/teams')
def get_teams():
    logger.info("Teams list requested")
    teams = pd.concat([matches_df['team1'], matches_df['team2']]).unique().tolist()
    return jsonify(teams)

@app.route('/api/player_stats/<player>', defaults={'opponent': None})
@app.route('/api/player_stats/<player>/<opponent>')
def player_stats(player, opponent=None):
    logger.info(f"Player stats requested: {player} vs {opponent or 'all'}")
    if opponent:
        # Filter to matches where bowling_team == opponent
        player_data = merged_df[(merged_df['batter'] == player) & (merged_df['bowling_team'] == opponent)]
    else:
        player_data = deliveries_df[deliveries_df['batter'] == player]
    
    if player_data.empty:
        return jsonify({'error': 'Player not found or no matches against opponent'})
    
    total_runs = player_data['runs_total'].sum()
    matches = player_data['match_id'].nunique()
    average = total_runs / matches if matches > 0 else 0
    return jsonify({
        'total_runs': int(total_runs),
        'average': round(average, 2),
        'matches': matches
    })

@app.route('/api/team_vs_team/<team1>/<team2>')
def team_vs_team(team1, team2):
    logger.info(f"Team vs team stats requested: {team1} vs {team2}")
    # Matches between the teams
    head_to_head_matches = matches_df[
        ((matches_df['team1'] == team1) & (matches_df['team2'] == team2)) |
        ((matches_df['team1'] == team2) & (matches_df['team2'] == team1))
    ]
    matches_played = len(head_to_head_matches)
    
    team1_wins = (head_to_head_matches['winner'] == team1).sum()
    team2_wins = (head_to_head_matches['winner'] == team2).sum()
    
    # Runs scored by team1 against team2
    team1_data = merged_df[(merged_df['batting_team'] == team1) & (merged_df['bowling_team'] == team2)]
    team1_runs = team1_data['runs_total'].sum()
    team1_avg = team1_runs / matches_played if matches_played > 0 else 0
    
    # Runs scored by team2 against team1
    team2_data = merged_df[(merged_df['batting_team'] == team2) & (merged_df['bowling_team'] == team1)]
    team2_runs = team2_data['runs_total'].sum()
    team2_avg = team2_runs / matches_played if matches_played > 0 else 0
    
    return jsonify({
        'team1': team1,
        'team2': team2,
        'matches_played': matches_played,
        'team1_wins': int(team1_wins),
        'team2_wins': int(team2_wins),
        'team1_runs_against_team2': int(team1_runs),
        'team1_avg_runs_per_match': round(team1_avg, 2),
        'team2_runs_against_team1': int(team2_runs),
        'team2_avg_runs_per_match': round(team2_avg, 2)
    })

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)