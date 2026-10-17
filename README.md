# Serverless Football Analytics Data Lakehouse on AWS

[![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![AWS S3](https://img.shields.io/badge/Amazon%20S3-Raw%20%26%20Processed%20Zones-569A31?style=for-the-badge&logo=amazon-s3&logoColor=white)](https://aws.amazon.com/s3/)
[![AWS Glue](https://img.shields.io/badge/AWS%20Glue-Serverless%20ETL-FF9900?style=for-the-badge&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/glue/)
[![Amazon Athena](https://img.shields.io/badge/Amazon%20Athena-Serverless%20SQL-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/athena/)
[![Amazon QuickSight](https://img.shields.io/badge/Amazon%20QuickSight-BI%20Dashboards-3F88C5?style=for-the-badge&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/quicksight/)

An end-to-end cloud Data Engineering lakehouse architecture that ingests, transforms, catalogs, and analyzes European football match data at scale using **Python**, **AWS S3**, **AWS Glue (PySpark)**, **Amazon Athena**, and **Amazon QuickSight**.

---

## 📑 Project Presentation

> 🎯 **Executive Presentation & Architectural Walkthrough**:  
> Explore the full project deck, design trade-offs, and analytics visualizations on Canva:  
> 👉 **[View Project Presentation Slide](https://canva.link/6pz4tgpmeqcc508)**

---

## 📌 Table of Contents
- [Architecture Overview](#-architecture-overview)
- [Key Engineering Highlights](#-key-engineering-highlights)
- [Pipeline Workflow](#-pipeline-workflow)
- [Repository Structure](#-repository-structure)
- [Data Modeling & Partitioning](#-data-modeling--partitioning)
- [Analytics & Football KPIs](#-analytics--football-kpis)
- [Deployment & Execution Guide](#-deployment--execution-guide)
  - [1. Configuration & Prerequisites](#1-configuration--prerequisites)
  - [2. Ingest Match Data (Python + Boto3)](#2-ingest-match-data-python--boto3)
  - [3. Transform & Aggregate (AWS Glue PySpark)](#3-transform--aggregate-aws-glue-pyspark)
  - [4. Query with Amazon Athena](#4-query-with-amazon-athena)
  - [5. Connect Amazon QuickSight](#5-connect-amazon-quicksight)
- [Author](#-author)

---

## 🏗 Architecture Overview

The platform implements a multi-tier **Medallion Lakehouse Architecture** on AWS:

```
+------------------------------------------------------------------------------------------------------------------+
|                                    Football Analytics Serverless Lakehouse on AWS                                |
+------------------------------------------------------------------------------------------------------------------+

   [ Football REST API ]
   (football-data.org / API-Football)
             |
             | (1) Ingestion: Python + Requests + Boto3
             v
   +-----------------------------------------------------------------------------------+
   | Amazon S3 (Raw Zone - Bronze Lake)                                                |
   | s3://football-analytics-lake-raw/matches/raw/league={league}/season={season}/*.json|
   +-----------------------------------------------------------------------------------+
             |
             | (2) Distributed ETL: AWS Glue 4.0 Job (PySpark)
             v
   +-----------------------------------------------------------------------------------+
   | AWS Glue PySpark Processing Engine                                                |
   | - Schema normalization & nested JSON array flattening                             |
   | - Deduplication via Window ranking over match_id                                  |
   | - Analytical KPI calculation (Outcomes, Over/Under 2.5, BTTS, Conversion Rates)   |
   +-----------------------------------------------------------------------------------+
             |
             | (3) Optimized Parquet Writes (Snappy Compression)
             v
   +-----------------------------------------------------------------------------------+
   | Amazon S3 (Processed Zone - Silver/Gold Lake)                                     |
   | s3://football-analytics-lake-processed/matches/processed/match_stats/             |
   | Partitioned by: [ season={YYYY} / league={CODE} ]                                 |
   +-----------------------------------------------------------------------------------+
             |
             | (4) Schema Discovery & Metadata Registration
             v
   +-----------------------------------------------------------------------------------+
   | AWS Glue Data Catalog                                                             |
   | Database: football_analytics_db  |  Table: match_stats                            |
   +-----------------------------------------------------------------------------------+
             |
             | (5) Interactive Serverless Ad-hoc SQL (Presto / Trino engine)
             v
   +-----------------------------------------------------------------------------------+
   | Amazon Athena                                                                     |
   | External Tables & Analytical Views (v_team_match_performance)                     |
   +-----------------------------------------------------------------------------------+
             |
             | (6) Business Intelligence & Storytelling
             v
   +-----------------------------------------------------------------------------------+
   | Amazon QuickSight BI Dashboards                                                   |
   | Team Standings, Momentum Trajectory, Venue Advantage, Shot Efficiency            |
   +-----------------------------------------------------------------------------------+
```

---

## 💡 Key Engineering Highlights

- **Serverless & Scalable**: Pure pay-per-use architecture with zero server management overhead across S3, Glue, and Athena.
- **Cost-Optimized Columnar Storage**: Processed data is stored in Apache Parquet format with Snappy compression, minimizing Athena scan costs and maximizing query performance by up to 90%.
- **Two-Tier Partitioning Strategy**: Hive-style partitioning by `season={year}` and `league={code}` enables efficient partition pruning when querying specific competitions or historical seasons.
- **Fault-Tolerant Ingestion**: API client includes automated rate-limit detection, exponential backoff, retry mechanisms, and structured logging.
- **Robust Feature Engineering**: Computes derived sports metrics at write-time in PySpark:
  - Match outcome categorizations (`HOME_WIN`, `AWAY_WIN`, `DRAW`)
  - Points allocation (3 for win, 1 for draw, 0 for loss)
  - Over/Under 2.5 goals and Both Teams To Score (BTTS) indicators
  - Shot conversion rates and shot accuracy metrics

---

## 📁 Repository Structure

```bash
football-analytics-aws/
├── config/
│   └── pipeline_config.json        # Central pipeline settings, S3 bucket names, Glue & Athena configs
├── scripts/
│   ├── ingestion/
│   │   └── fetch_football_data.py  # Python ingestion script streaming API data to S3 Raw Zone
│   └── transformation/
│       └── transform_match_stats.py # AWS Glue PySpark job transforming JSON to partitioned Parquet
├── sql/
│   ├── create_external_tables.sql  # Athena DDL for database, external tables, and normalized views
│   └── analytical_kpis.sql         # Athena queries for league standings, form, and tactical KPIs
├── .gitignore                      # Git ignore rules for Python, Spark, and AWS credentials
├── requirements.txt                # Python project dependencies (boto3, pyspark, pandas, requests)
└── README.md                       # Comprehensive project documentation
```

---

## 📊 Data Modeling & Partitioning

### Processed Schema (`football_analytics_db.match_stats`)

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `match_id` | `INT` | Unique identifier for each fixture |
| `match_datetime` | `TIMESTAMP` | UTC kickoff date and time |
| `matchday` | `INT` | Competition round number |
| `home_team_name` / `away_team_name` | `STRING` | Competing club names |
| `home_score` / `away_score` | `INT` | Full time final scores |
| `total_goals` | `INT` | Total match goal sum |
| `goal_differential` | `INT` | Goal margin (`home_score - away_score`) |
| `match_outcome` | `STRING` | Outcome category: `HOME_WIN`, `AWAY_WIN`, `DRAW` |
| `home_shots` / `away_shots` | `INT` | Total match shots attempted |
| `home_shot_conversion_rate` | `DOUBLE` | Goals scored divided by total shots |
| `is_over_2_5_goals` | `INT` | Flag indicating high-scoring games (> 2.5 goals) |
| `both_teams_scored` | `INT` | Flag indicating BTTS |
| `season` *(Partition Key)* | `STRING` | Season starting year (e.g. `2023`, `2024`) |
| `league` *(Partition Key)* | `STRING` | League competition code (`PL`, `PD`, `SA`, `BL1`) |

---

## 📈 Analytics & Football KPIs

The repository includes ready-to-run Amazon Athena queries in [`sql/analytical_kpis.sql`](file:///Users/hoaqviet/.gemini/antigravity-ide/scratch/football-analytics-aws/sql/analytical_kpis.sql):

1. **Dynamic League Standings**: Computes Wins, Draws, Losses, Goal Difference, Points, and Points Per Game (PPG).
2. **Home Field Advantage Index**: Quantifies home win percentages and average venue goal differentials by league.
3. **Clinical Finishing Leaders**: Ranks clubs by shot conversion rates and shots needed per goal.
4. **Aggression & Disciplinary Matrix**: Weights fouls, yellow cards, and red cards into a composite discipline rating.
5. **Goal Market Volatility**: Calculates empirical probabilities for Over/Under 2.5 goals and BTTS for sports betting modeling.
6. **Rolling 5-Match Momentum**: Uses SQL window functions `AVG(...) OVER (ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)` to monitor team form and trajectories.
7. **Halftime Comeback Resilience**: Tracks teams that overcome halftime deficits to rescue points.

---

## ⚡ Deployment & Execution Guide

### 1. Configuration & Prerequisites
- Python 3.9+ installed
- AWS CLI configured with appropriate IAM permissions (`S3FullAccess`, `AWSGlueConsoleFullAccess`, `AmazonAthenaFullAccess`)
- Install project dependencies:
  ```bash
  pip install -r requirements.txt
  ```

### 2. Ingest Match Data (Python + Boto3)
Run the ingestion script to pull match fixtures across target leagues and upload raw payloads to S3:
```bash
python scripts/ingestion/fetch_football_data.py \
  --league PL \
  --league PD \
  --season 2023 \
  --season 2024
```
*Tip: Add `--dry-run` to test API connectivity without performing S3 writes.*

### 3. Transform & Aggregate (AWS Glue PySpark)
Submit the PySpark job to AWS Glue 4.0 or run in a local Spark environment:
```bash
# Example AWS CLI Glue Job invocation:
aws glue start-job-run \
  --job-name glue-transform-football-match-stats \
  --arguments='{
    "--S3_RAW_PATH":"s3://football-analytics-lake-raw/matches/raw/",
    "--S3_PROCESSED_PATH":"s3://football-analytics-lake-processed/matches/processed/match_stats/"
  }'
```

### 4. Query with Amazon Athena
1. Open the Amazon Athena Query Editor.
2. Run [`sql/create_external_tables.sql`](file:///Users/hoaqviet/.gemini/antigravity-ide/scratch/football-analytics-aws/sql/create_external_tables.sql) to register the database, table, and unpivoted team performance view.
3. Run `MSCK REPAIR TABLE football_analytics_db.match_stats;` to discover all partitions.
4. Execute analytical queries from [`sql/analytical_kpis.sql`](file:///Users/hoaqviet/.gemini/antigravity-ide/scratch/football-analytics-aws/sql/analytical_kpis.sql).

### 5. Connect Amazon QuickSight
1. In Amazon QuickSight, create a new Dataset with **Athena** as data source.
2. Select catalog `AwsDataCatalog` and database `football_analytics_db`.
3. Choose the table `match_stats` or view `v_team_match_performance`.
4. Build interactive visual sheets:
   - League table standings leaderboard
   - Home vs. away win percentage donuts
   - Shot conversion vs. goals scatter plot
   - Rolling form line charts

---

## 👨‍💻 Author

**Hoang Quoc Viet**
- **Portfolio**: [https://hqvz28.github.io](https://hqvz28.github.io)
- **LinkedIn**: [https://www.linkedin.com/in/qviet28/](https://www.linkedin.com/in/qviet28/)
- **GitHub**: [@hqvz28](https://github.com/hqvz28)
- **Project Presentation**: [Canva Presentation Deck](https://canva.link/6pz4tgpmeqcc508)
