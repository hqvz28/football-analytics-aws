# Serverless Football Analytics Data Lakehouse on AWS

[![Python](https://img.shields.io/badge/Python-3.9%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/Apache%20Spark-PySpark%20%26%20MLlib-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)](https://spark.apache.org/)
[![Terraform](https://img.shields.io/badge/Terraform-Infrastructure%20as%20Code-844FBA?style=for-the-badge&logo=terraform&logoColor=white)](https://www.terraform.io/)
[![AWS S3](https://img.shields.io/badge/Amazon%20S3-Raw%20%26%20Processed%20Zones-569A31?style=for-the-badge&logo=amazon-s3&logoColor=white)](https://aws.amazon.com/s3/)
[![AWS Glue](https://img.shields.io/badge/AWS%20Glue-Serverless%20ETL-FF9900?style=for-the-badge&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/glue/)
[![Amazon Athena](https://img.shields.io/badge/Amazon%20Athena-Serverless%20SQL-232F3E?style=for-the-badge&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/athena/)
[![Amazon QuickSight](https://img.shields.io/badge/Amazon%20QuickSight-BI%20Dashboards-3F88C5?style=for-the-badge&logo=amazon-aws&logoColor=white)](https://aws.amazon.com/quicksight/)
[![CI/CD](https://img.shields.io/badge/GitHub%20Actions-Automated%20CI%2FCD-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)](https://github.com/features/actions)

An enterprise-grade cloud Data Engineering lakehouse architecture on AWS featuring **Infrastructure as Code (Terraform)**, **Distributed Ingestion**, **Batch ETL (AWS Glue 4.0 PySpark)**, **Machine Learning (Spark MLlib)**, **Automated Data Quality Gates**, and **Serverless SQL Analytics (Amazon Athena)**.

---

## 📑 Project Presentation

> 🎯 **Executive Presentation & Architectural Walkthrough**:  
> Explore the full project deck, design trade-offs, and analytics visualizations on Canva:  
> 👉 **[View Project Presentation Slide](https://canva.link/6pz4tgpmeqcc508)**

---

## 📌 Table of Contents
- [Enterprise Architecture Overview](#-enterprise-architecture-overview)
- [Key Engineering Pillars](#-key-engineering-pillars)
- [Repository Structure](#-repository-structure)
- [Infrastructure as Code (Terraform)](#-infrastructure-as-code-terraform)
- [Machine Learning & Predictive Modeling (Spark MLlib)](#-machine-learning--predictive-modeling-spark-mllib)
- [Automated Data Quality Gatekeeper](#-automated-data-quality-gatekeeper)
- [Analytics & Presto SQL KPIs](#-analytics--presto-sql-kpis)
- [Continuous Integration & Deployment (CI/CD)](#-continuous-integration--deployment-cicd)
- [Deployment & Execution Guide](#-deployment--execution-guide)
- [Author](#-author)

---

## 🏗 Enterprise Architecture Overview

```
+------------------------------------------------------------------------------------------------------------------+
|                                    Football Analytics Serverless Lakehouse on AWS                                |
+------------------------------------------------------------------------------------------------------------------+

   [ Infrastructure as Code: Terraform ] ---> Provisions S3 Buckets, IAM Policies, Glue Job & Athena Workgroup
   
   [ Football REST API ]
             |
             | (1) Ingestion: Python + Requests + Boto3 (Rate-limiting & Exponential Backoff)
             v
   +-----------------------------------------------------------------------------------+
   | Amazon S3 (Raw Zone - Bronze Lake)                                                |
   | s3://football-analytics-lake-raw/matches/raw/league={league}/season={season}/*.json|
   | [Lifecycle: Auto-transition to Glacier after 90 days]                             |
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
             | (3) Quality Audit Gatekeeper: validate_data_quality.py
             v
   +-----------------------------------------------------------------------------------+
   | Data Quality Gate (Null Checks, Uniqueness, Domain Constraints, Range Checks)     |
   +-----------------------------------------------------------------------------------+
             |
             +-----------------------------------------+
             |                                         |
             v                                         v
   +-----------------------------------+     +-----------------------------------+
   | Amazon S3 (Processed Zone - Gold) |     | Machine Learning: Spark MLlib     |
   | Parquet (Snappy Compressed)       |     | predict_match_outcome.py          |
   | Partitioned by: season / league   |     | (Random Forest Match Predictions) |
   +-----------------------------------+     +-----------------------------------+
             |                                         |
             | (4) Schema Discovery & Metadata         v
             v                               +-----------------------------------+
   +-----------------------------------+     | S3 Predictions & Power Rankings   |
   | AWS Glue Data Catalog             |     +-----------------------------------+
   | Database: football_analytics_db   |
   +-----------------------------------+
             |
             | (5) Interactive Serverless Ad-hoc SQL (Presto / Trino engine)
             v
   +-----------------------------------------------------------------------------------+
   | Amazon Athena (External Tables & Analytical Views)                                |
   +-----------------------------------------------------------------------------------+
             |
             | (6) Business Intelligence & Storytelling
             v
   +-----------------------------------------------------------------------------------+
   | Amazon QuickSight BI Dashboards                                                   |
   | Standings Leaderboard, Venue Dominance, Attacking Shot Accuracy, Momentum Trends |
   +-----------------------------------------------------------------------------------+
```

---

## 💡 Key Engineering Pillars

1. **Infrastructure as Code (IaC)**: 100% reproducible AWS environment managed through modular Terraform scripts with automated S3 lifecycle rules and strict least-privilege IAM policies.
2. **Columnar Parquet Optimization**: Achieves up to 90% storage savings and query cost reductions by converting raw nested JSON into Snappy-compressed Parquet partitioned by `season` and `league`.
3. **Machine Learning Pipeline**: Uses Apache Spark MLlib (`RandomForestClassifier`, `StandardScaler`, `VectorAssembler`) to forecast match outcomes and evaluate team tactical differentials.
4. **Automated Data Quality Gatekeeper**: Enforces pre-serving data integrity rules (primary key uniqueness, non-null guarantees, score domain constraints, possession percentages) before exposing tables to Athena.
5. **CI/CD Automation**: GitHub Actions pipeline validates code styling (`flake8`), executes unit tests with `pytest`, and validates Terraform configurations on every commit.

---

## 📁 Repository Structure

```bash
football-analytics-aws/
├── .github/
│   └── workflows/
│       └── ci-cd.yml               # GitHub Actions CI/CD for testing & Terraform validation
├── config/
│   └── pipeline_config.json        # Central pipeline settings, S3 bucket names, Glue & Athena configs
├── scripts/
│   ├── ingestion/
│   │   └── fetch_football_data.py  # Python ingestion script streaming API data to S3 Raw Zone
│   ├── transformation/
│   │   └── transform_match_stats.py # AWS Glue PySpark job transforming JSON to partitioned Parquet
│   ├── ml/
│   │   └── predict_match_outcome.py # Spark MLlib Random Forest match outcome predictive pipeline
│   └── quality/
│       └── validate_data_quality.py # Automated data quality gatekeeper script
├── sql/
│   ├── create_external_tables.sql  # Athena DDL for database, external tables, and normalized views
│   └── analytical_kpis.sql         # Athena queries for league standings, form, and tactical KPIs
├── terraform/                      # Infrastructure as Code
│   ├── main.tf                     # S3, IAM, Glue Job, Catalog & Athena Workgroup definitions
│   ├── variables.tf                # Configurable deployment parameters
│   └── outputs.tf                  # Infrastructure endpoints and resource ARNs
├── tests/                          # Unit and integration tests
│   ├── test_ingestion.py           # Ingestion logic and rate-limit backoff tests
│   └── test_transformations.py     # Data transformation and KPI calculation unit tests
├── .gitignore                      # Git ignore rules for Python, Spark, and AWS credentials
├── requirements.txt                # Python project dependencies
└── README.md                       # Comprehensive project documentation
```

---

## ☁️ Infrastructure as Code (Terraform)

Deploy all AWS resources in minutes:

```bash
cd terraform
terraform init
terraform plan
terraform apply -auto-approve
```

Resources provisioned:
- S3 Buckets: Raw (with Glacier transition lifecycle), Staging, and Processed (with versioning and SSE-S3 encryption).
- Public Access Blocks for 100% cloud storage security compliance.
- IAM Service Roles and custom policies for AWS Glue execution.
- AWS Glue Data Catalog database (`football_analytics_db`).
- AWS Glue 4.0 Job (`glue-transform-football-match-stats`).
- Amazon Athena dedicated Workgroup with enforced query result encryption.

---

## 🤖 Machine Learning & Predictive Modeling (Spark MLlib)

The repository includes a distributed machine learning pipeline in [`scripts/ml/predict_match_outcome.py`](file:///Users/hoaqviet/.gemini/antigravity-ide/scratch/football-analytics-aws/scripts/ml/predict_match_outcome.py):

- **Feature Engineering**: Calculates shot differential, shots on target differential, possession ratio, and disciplinary imbalance.
- **Model Architecture**: Random Forest Classifier with standard scaling across 50 estimators.
- **Evaluation**: Evaluates prediction accuracy and multi-class F1-scores.
- **Run Locally or in Glue**:
  ```bash
  python scripts/ml/predict_match_outcome.py
  ```

---

## 🛡️ Automated Data Quality Gatekeeper

The pipeline incorporates an automated data quality gatekeeper in [`scripts/quality/validate_data_quality.py`](file:///Users/hoaqviet/.gemini/antigravity-ide/scratch/football-analytics-aws/scripts/quality/validate_data_quality.py):

- **Non-Empty Check**: Verifies non-zero record count.
- **Primary Key Integrity**: Asserts 0 nulls and 100% uniqueness on `match_id`.
- **Domain Constraints**: Validates that `match_outcome` strictly equals `HOME_WIN`, `AWAY_WIN`, or `DRAW`.
- **Score Range Verification**: Ensures non-negative goals and valid possession percentages (`0 <= pct <= 100`).

---

## 📈 Analytics & Presto SQL KPIs

Ready-to-run queries in [`sql/analytical_kpis.sql`](file:///Users/hoaqviet/.gemini/antigravity-ide/scratch/football-analytics-aws/sql/analytical_kpis.sql):
1. **Dynamic League Standings & Form**: Matches played, wins/draws/losses, goal differential, total points, points per game.
2. **Home Field Advantage Index**: Comparative venue dominance ratios across leagues.
3. **Clinical Finishing Leaders**: Shot conversion and accuracy rankings.
4. **Aggression & Disciplinary Matrix**: Composite discipline scores.
5. **Over/Under 2.5 Goals Probability**: Goal market volatility indicators.
6. **Rolling 5-Match Momentum**: SQL window functions tracking team trajectories.
7. **Comeback Resilience**: Analysis of teams recovering points after trailing at halftime.

---

## 🔄 Continuous Integration & Deployment (CI/CD)

The GitHub Actions workflow in [`.github/workflows/ci-cd.yml`](file:///Users/hoaqviet/.gemini/antigravity-ide/scratch/football-analytics-aws/.github/workflows/ci-cd.yml) automatically runs on every push and pull request:
- Flake8 syntax and linting checks.
- Pytest unit tests execution.
- Terraform formatting and schema validation.

---

## 👨‍💻 Author

**Hoang Quoc Viet**
- **Portfolio**: [https://hqvz28.github.io](https://hqvz28.github.io)
- **LinkedIn**: [https://www.linkedin.com/in/qviet28/](https://www.linkedin.com/in/qviet28/)
- **GitHub**: [@hqvz28](https://github.com/hqvz28)
- **Project Presentation**: [Canva Presentation Deck](https://canva.link/6pz4tgpmeqcc508)
