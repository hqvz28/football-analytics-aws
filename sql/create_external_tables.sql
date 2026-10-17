-- ==============================================================================
-- DDL Script: AWS Glue Data Catalog & Amazon Athena External Tables
-- Project: Football Analytics Data Lake on AWS
-- Author: Hoang Quoc Viet
-- ==============================================================================

-- 1. Create Analytics Database
CREATE DATABASE IF NOT EXISTS football_analytics_db
COMMENT 'Production Football Match Analytics and Performance Data Lake'
LOCATION 's3://football-analytics-lake-processed/athena-db/';

-- 2. Create Match Statistics External Table (Parquet Partitioned by Season & League)
CREATE EXTERNAL TABLE IF NOT EXISTS football_analytics_db.match_stats (
    match_id INT COMMENT 'Unique identifier for the match',
    match_datetime TIMESTAMP COMMENT 'Match kickoff timestamp in UTC',
    match_date DATE COMMENT 'Match kickoff date',
    matchday INT COMMENT 'Matchday or round number in competition',
    stage STRING COMMENT 'Competition stage (e.g. REGULAR_SEASON, GROUP_STAGE)',
    status STRING COMMENT 'Match status (FINISHED, TIMED, IN_PLAY)',
    
    -- Home Team
    home_team_id INT COMMENT 'Unique identifier for the home team',
    home_team_name STRING COMMENT 'Full name of the home team',
    home_score INT COMMENT 'Full time goals scored by home team',
    halftime_home_score INT COMMENT 'Half time goals scored by home team',
    home_shots INT COMMENT 'Total shots taken by home team',
    home_shots_on_target INT COMMENT 'Shots on target by home team',
    home_possession_pct DOUBLE COMMENT 'Possession percentage of home team',
    home_fouls INT COMMENT 'Fouls committed by home team',
    home_yellow_cards INT COMMENT 'Yellow cards issued to home team',
    home_red_cards INT COMMENT 'Red cards issued to home team',
    home_points INT COMMENT 'Points earned by home team (3/1/0)',
    home_shot_conversion_rate DOUBLE COMMENT 'Goals per total shots for home team',
    home_shot_accuracy DOUBLE COMMENT 'Shots on target ratio for home team',

    -- Away Team
    away_team_id INT COMMENT 'Unique identifier for the away team',
    away_team_name STRING COMMENT 'Full name of the away team',
    away_score INT COMMENT 'Full time goals scored by away team',
    halftime_away_score INT COMMENT 'Half time goals scored by away team',
    away_shots INT COMMENT 'Total shots taken by away team',
    away_shots_on_target INT COMMENT 'Shots on target by away team',
    away_possession_pct DOUBLE COMMENT 'Possession percentage of away team',
    away_fouls INT COMMENT 'Fouls committed by away team',
    away_yellow_cards INT COMMENT 'Yellow cards issued to away team',
    away_red_cards INT COMMENT 'Red cards issued to away team',
    away_points INT COMMENT 'Points earned by away team (3/1/0)',
    away_shot_conversion_rate DOUBLE COMMENT 'Goals per total shots for away team',
    away_shot_accuracy DOUBLE COMMENT 'Shots on target ratio for away team',

    -- Match Analytical Derived Metrics
    total_goals INT COMMENT 'Sum of home and away goals',
    goal_differential INT COMMENT 'Home goals minus away goals',
    match_outcome STRING COMMENT 'Categorical outcome: HOME_WIN, AWAY_WIN, DRAW',
    is_over_2_5_goals INT COMMENT 'Binary flag: 1 if total goals > 2.5, else 0',
    both_teams_scored INT COMMENT 'Binary flag: 1 if both teams scored, else 0',
    processed_timestamp TIMESTAMP COMMENT 'Timestamp when record was transformed by Glue PySpark'
)
PARTITIONED BY (
    season STRING COMMENT 'Season year (e.g. 2023, 2024)',
    league STRING COMMENT 'Competition code (e.g. PL, PD, SA, BL1)'
)
STORED AS PARQUET
LOCATION 's3://football-analytics-lake-processed/matches/processed/match_stats/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'has_encrypted_data' = 'false',
    'classification' = 'parquet'
);

-- 3. Recover Partitions in Athena
MSCK REPAIR TABLE football_analytics_db.match_stats;

-- 4. Create Normalized Team-Level View (Unpivoting Home & Away for Team Analytics)
CREATE OR REPLACE VIEW football_analytics_db.v_team_match_performance AS
SELECT
    season,
    league,
    match_id,
    match_date,
    matchday,
    home_team_id AS team_id,
    home_team_name AS team_name,
    away_team_id AS opponent_id,
    away_team_name AS opponent_name,
    'HOME' AS venue,
    home_score AS goals_for,
    away_score AS goals_against,
    (home_score - away_score) AS goal_diff,
    home_points AS points,
    CASE 
        WHEN home_score > away_score THEN 'W'
        WHEN home_score = away_score THEN 'D'
        ELSE 'L'
    END AS result,
    home_shots AS shots,
    home_shots_on_target AS shots_on_target,
    home_possession_pct AS possession_pct,
    home_fouls AS fouls,
    home_yellow_cards AS yellow_cards,
    home_red_cards AS red_cards,
    home_shot_conversion_rate AS shot_conversion_rate
FROM football_analytics_db.match_stats

UNION ALL

SELECT
    season,
    league,
    match_id,
    match_date,
    matchday,
    away_team_id AS team_id,
    away_team_name AS team_name,
    home_team_id AS opponent_id,
    home_team_name AS opponent_name,
    'AWAY' AS venue,
    away_score AS goals_for,
    home_score AS goals_against,
    (away_score - home_score) AS goal_diff,
    away_points AS points,
    CASE 
        WHEN away_score > home_score THEN 'W'
        WHEN away_score = home_score THEN 'D'
        ELSE 'L'
    END AS result,
    away_shots AS shots,
    away_shots_on_target AS shots_on_target,
    away_possession_pct AS possession_pct,
    away_fouls AS fouls,
    away_yellow_cards AS yellow_cards,
    away_red_cards AS red_cards,
    away_shot_conversion_rate AS shot_conversion_rate
FROM football_analytics_db.match_stats;
