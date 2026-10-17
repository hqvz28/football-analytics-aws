-- ==============================================================================
-- Analytical Queries & Football KPIs (Amazon Athena / Presto SQL)
-- Project: Football Analytics Data Lake on AWS
-- Author: Hoang Quoc Viet
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- KPI 1: Complete League Standings & Form Analysis
-- Calculates: Matches Played, W/D/L, Goals For/Against, Goal Differential, Points, PPG
-- ------------------------------------------------------------------------------
SELECT
    season,
    league,
    team_name,
    COUNT(match_id) AS matches_played,
    COUNT(CASE WHEN result = 'W' THEN 1 END) AS wins,
    COUNT(CASE WHEN result = 'D' THEN 1 END) AS draws,
    COUNT(CASE WHEN result = 'L' THEN 1 END) AS losses,
    SUM(goals_for) AS goals_for,
    SUM(goals_against) AS goals_against,
    SUM(goals_for) - SUM(goals_against) AS goal_difference,
    SUM(points) AS total_points,
    ROUND(CAST(SUM(points) AS DOUBLE) / COUNT(match_id), 2) AS points_per_game,
    ROUND(AVG(possession_pct), 1) AS avg_possession_pct,
    DENSE_RANK() OVER (
        PARTITION BY season, league 
        ORDER BY SUM(points) DESC, (SUM(goals_for) - SUM(goals_against)) DESC, SUM(goals_for) DESC
    ) AS league_rank
FROM football_analytics_db.v_team_match_performance
GROUP BY season, league, team_name
ORDER BY season DESC, league, league_rank;


-- ------------------------------------------------------------------------------
-- KPI 2: Home Field Advantage & Venue Dominance Comparison
-- Compares win rates, average goals, and points earned at Home vs. Away across leagues
-- ------------------------------------------------------------------------------
SELECT
    league,
    season,
    COUNT(match_id) AS total_matches,
    ROUND(100.0 * COUNT(CASE WHEN match_outcome = 'HOME_WIN' THEN 1 END) / COUNT(match_id), 2) AS home_win_pct,
    ROUND(100.0 * COUNT(CASE WHEN match_outcome = 'DRAW' THEN 1 END) / COUNT(match_id), 2) AS draw_pct,
    ROUND(100.0 * COUNT(CASE WHEN match_outcome = 'AWAY_WIN' THEN 1 END) / COUNT(match_id), 2) AS away_win_pct,
    ROUND(AVG(home_score), 2) AS avg_home_goals,
    ROUND(AVG(away_score), 2) AS avg_away_goals,
    ROUND(AVG(total_goals), 2) AS avg_total_goals_per_match
FROM football_analytics_db.match_stats
GROUP BY league, season
ORDER BY avg_total_goals_per_match DESC;


-- ------------------------------------------------------------------------------
-- KPI 3: Attacking Efficiency & Shot Conversion Leaders
-- Measures shot volume vs. clinical finishing (Accuracy & Conversion Rates)
-- ------------------------------------------------------------------------------
SELECT
    season,
    league,
    team_name,
    COUNT(match_id) AS matches_played,
    SUM(goals_for) AS total_goals,
    SUM(shots) AS total_shots,
    SUM(shots_on_target) AS total_shots_on_target,
    ROUND(100.0 * SUM(shots_on_target) / NULLIF(SUM(shots), 0), 2) AS shot_accuracy_pct,
    ROUND(100.0 * SUM(goals_for) / NULLIF(SUM(shots), 0), 2) AS shot_conversion_rate_pct,
    ROUND(CAST(SUM(shots) AS DOUBLE) / NULLIF(SUM(goals_for), 0), 2) AS shots_needed_per_goal
FROM football_analytics_db.v_team_match_performance
GROUP BY season, league, team_name
HAVING COUNT(match_id) >= 10
ORDER BY shot_conversion_rate_pct DESC
LIMIT 20;


-- ------------------------------------------------------------------------------
-- KPI 4: Disciplinary & Aggression Matrix
-- Ranks teams by fouls, yellow cards, red cards, and weighted disciplinary points
-- (Disciplinary Points = 1 pt per foul, 5 pts per yellow, 15 pts per red)
-- ------------------------------------------------------------------------------
SELECT
    league,
    team_name,
    COUNT(match_id) AS matches_played,
    SUM(fouls) AS total_fouls,
    ROUND(AVG(fouls), 1) AS fouls_per_match,
    SUM(yellow_cards) AS total_yellow_cards,
    SUM(red_cards) AS total_red_cards,
    SUM(fouls) + (SUM(yellow_cards) * 5) + (SUM(red_cards) * 15) AS disciplinary_index_score
FROM football_analytics_db.v_team_match_performance
GROUP BY league, team_name
ORDER BY disciplinary_index_score DESC;


-- ------------------------------------------------------------------------------
-- KPI 5: Match Entertainment & Goal Probability Indicators (Over 2.5 / BTTS)
-- Used for sports betting analytics, media insights, and fan engagement metrics
-- ------------------------------------------------------------------------------
SELECT
    league,
    season,
    COUNT(match_id) AS total_matches,
    SUM(is_over_2_5_goals) AS matches_over_2_5_goals,
    ROUND(100.0 * SUM(is_over_2_5_goals) / COUNT(match_id), 2) AS pct_over_2_5_goals,
    SUM(both_teams_scored) AS matches_btts,
    ROUND(100.0 * SUM(both_teams_scored) / COUNT(match_id), 2) AS pct_both_teams_scored,
    COUNT(CASE WHEN total_goals >= 4 THEN 1 END) AS matches_4_plus_goals
FROM football_analytics_db.match_stats
GROUP BY league, season
ORDER BY pct_over_2_5_goals DESC;


-- ------------------------------------------------------------------------------
-- KPI 6: Rolling 5-Match Momentum & Form Trajectory
-- Window functions calculating moving averages for points and goals conceded
-- ------------------------------------------------------------------------------
WITH team_match_ordered AS (
    SELECT
        season,
        league,
        team_name,
        match_date,
        matchday,
        venue,
        goals_for,
        goals_against,
        points,
        result,
        ROW_NUMBER() OVER (PARTITION BY season, team_name ORDER BY match_date) AS match_seq
    FROM football_analytics_db.v_team_match_performance
)
SELECT
    season,
    league,
    team_name,
    matchday,
    match_date,
    venue,
    result,
    points,
    -- 5-match moving average points
    ROUND(AVG(points) OVER (
        PARTITION BY season, team_name 
        ORDER BY match_seq 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_5_match_avg_points,
    -- 5-match sum of goals scored
    SUM(goals_for) OVER (
        PARTITION BY season, team_name 
        ORDER BY match_seq 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS rolling_5_match_goals_scored
FROM team_match_ordered
ORDER BY team_name, match_date;


-- ------------------------------------------------------------------------------
-- KPI 7: Resilience & Comeback Kings (Halftime Deficit to Win/Draw)
-- Identifies teams with the highest mental fortitude turning matches around
-- ------------------------------------------------------------------------------
SELECT
    team_name,
    COUNT(*) AS trailing_at_halftime_count,
    COUNT(CASE WHEN result = 'W' THEN 1 END) AS comeback_wins,
    COUNT(CASE WHEN result = 'D' THEN 1 END) AS comeback_draws,
    ROUND(
        100.0 * COUNT(CASE WHEN result IN ('W', 'D') THEN 1 END) / COUNT(*),
        2
    ) AS point_recovery_rate_pct
FROM (
    SELECT
        home_team_name AS team_name,
        halftime_home_score,
        halftime_away_score,
        CASE WHEN home_score > away_score THEN 'W' WHEN home_score = away_score THEN 'D' ELSE 'L' END AS result
    FROM football_analytics_db.match_stats
    WHERE halftime_home_score < halftime_away_score

    UNION ALL

    SELECT
        away_team_name AS team_name,
        halftime_away_score,
        halftime_home_score,
        CASE WHEN away_score > home_score THEN 'W' WHEN away_score = home_score THEN 'D' ELSE 'L' END AS result
    FROM football_analytics_db.match_stats
    WHERE halftime_away_score < halftime_home_score
)
GROUP BY team_name
HAVING COUNT(*) >= 5
ORDER BY comeback_wins DESC, point_recovery_rate_pct DESC
LIMIT 15;
