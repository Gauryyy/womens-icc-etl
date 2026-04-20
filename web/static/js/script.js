document.addEventListener('DOMContentLoaded', function() {
    loadPlayers();
    loadTeams();
});

function loadPlayers() {
    fetch('/api/players')
        .then(response => response.json())
        .then(players => {
            const select = document.getElementById('playerSelect');
            players.forEach(player => {
                const option = document.createElement('option');
                option.value = player;
                option.textContent = player;
                select.appendChild(option);
            });
        });
}

function loadTeams() {
    fetch('/api/teams')
        .then(response => response.json())
        .then(teams => {
            const select1 = document.getElementById('team1Select');
            const select2 = document.getElementById('team2Select');
            const opponentSelect = document.getElementById('opponentSelect');
            teams.forEach(team => {
                const option1 = document.createElement('option');
                option1.value = team;
                option1.textContent = team;
                select1.appendChild(option1);

                const option2 = document.createElement('option');
                option2.value = team;
                option2.textContent = team;
                select2.appendChild(option2);

                const optionOpp = document.createElement('option');
                optionOpp.value = team;
                optionOpp.textContent = team;
                opponentSelect.appendChild(optionOpp);
            });
        });
}

function getPlayerStats() {
    const player = document.getElementById('playerSelect').value;
    const opponent = document.getElementById('opponentSelect').value;
    if (!player) return;

    let url = `/api/player_stats/${encodeURIComponent(player)}`;
    if (opponent) {
        url += `/${encodeURIComponent(opponent)}`;
    }

    fetch(url)
        .then(response => response.json())
        .then(data => {
            const div = document.getElementById('playerStats');
            if (data.error) {
                div.textContent = data.error;
            } else {
                const opponentText = opponent ? ` against ${opponent}` : '';
                div.innerHTML = `
                    <h3>${player}${opponentText}</h3>
                    <p>Total Runs: ${data.total_runs}</p>
                    <p>Average: ${data.average}</p>
                    <p>Matches: ${data.matches}</p>
                `;
            }
        });
}

function getTeamVsTeam() {
    const team1 = document.getElementById('team1Select').value;
    const team2 = document.getElementById('team2Select').value;
    if (!team1 || !team2) return;

    fetch(`/api/team_vs_team/${encodeURIComponent(team1)}/${encodeURIComponent(team2)}`)
        .then(response => response.json())
        .then(data => {
            const div = document.getElementById('teamVsTeamResult');
            div.innerHTML = `
                <h3>${data.team1} vs ${data.team2}</h3>
                <p>Matches Played: ${data.matches_played}</p>
                <table border="1">
                    <tr>
                        <th>Metric</th>
                        <th>${data.team1}</th>
                        <th>${data.team2}</th>
                    </tr>
                    <tr>
                        <td>Wins</td>
                        <td>${data.team1_wins}</td>
                        <td>${data.team2_wins}</td>
                    </tr>
                    <tr>
                        <td>Total Runs Against Opponent</td>
                        <td>${data.team1_runs_against_team2}</td>
                        <td>${data.team2_runs_against_team1}</td>
                    </tr>
                    <tr>
                        <td>Average Runs Per Match</td>
                        <td>${data.team1_avg_runs_per_match}</td>
                        <td>${data.team2_avg_runs_per_match}</td>
                    </tr>
                </table>
            `;
        });
}