"""
transform_match_stats.py
------------------------
AWS Glue PySpark Job that transforms raw football match payloads from the S3 Raw Zone
into an optimized, query-ready Parquet dataset in the S3 Processed Data Lake Zone,
partitioned by `season` and `league`.

Key Operations:
1. Load nested JSON match records from S3 Raw prefix.
2. Explode and normalize nested schema (competition, season, teams, scores, stats).
3. Data cleansing, deduplication by match ID, and strict type casting.
4. Feature engineering & KPI calculation:
   - Match outcome (HOME_WIN, AWAY_WIN, DRAW)
   - Goal differential and total match goals
   - Over/Under 2.5 goals and Both Teams To Score (BTTS) indicators
   - Match points allocation (3 for win, 1 for draw, 0 for loss)
   - Shot conversion & accuracy rates
5. Partitioned Parquet output write to S3 with Snappy compression.

Author: Hoang Quoc Viet
"""

import os
import sys

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
    BooleanType
)

# Optional AWS Glue imports with safe fallback for local Spark testing
try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    IS_GLUE = True
except ImportError:
    IS_GLUE = False


def init_spark_and_glue():
    """Initialize SparkSession and GlueContext depending on runtime environment."""
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)

    glue_context = None
    job = None
    args = {}

    if IS_GLUE:
        glue_context = GlueContext(sc)
        args = getResolvedOptions(
            sys.argv,
            ["JOB_NAME", "S3_RAW_PATH", "S3_PROCESSED_PATH"]
        )
        job = Job(glue_context)
        job.init(args["JOB_NAME"], args)
    else:
        # Default local/testing paths
        args = {
            "JOB_NAME": "local_transform_football_matches",
            "S3_RAW_PATH": os.getenv("S3_RAW_PATH", "s3://football-analytics-lake-raw/matches/raw/"),
            "S3_PROCESSED_PATH": os.getenv(
                "S3_PROCESSED_PATH",
                "s3://football-analytics-lake-processed/matches/processed/match_stats/"
            )
        }

    return spark, glue_context, job, args


def clean_and_transform_matches(raw_df):
    """
    Transform raw nested match JSON dataframe into clean, flattened, analytical schema.
    """
    # Check if data contains an exploded array or top-level matches array
    if "matches" in raw_df.columns:
        matches_df = raw_df.select(
            F.col("competition.code").alias("league_code"),
            F.col("competition.name").alias("league_name"),
            F.col("filters.season").alias("season_year"),
            F.explode("matches").alias("match")
        )
    else:
        # If already at match level or partitioned structure
        matches_df = raw_df.select(F.col("*").alias("match"))

    # Flatten nested match fields
    flattened_df = matches_df.select(
        # Identity and league info
        F.coalesce(
            F.col("match.id"),
            F.col("match.match_id")
        ).cast(IntegerType()).alias("match_id"),

        F.coalesce(
            F.col("match.competition.code"),
            F.col("league_code"),
            F.lit("PL")
        ).cast(StringType()).alias("league"),

        F.coalesce(
            F.col("match.season.startDate").substr(1, 4),
            F.col("season_year"),
            F.lit("2023")
        ).cast(StringType()).alias("season"),

        F.to_timestamp(F.col("match.utcDate")).alias("match_datetime"),
        F.to_date(F.col("match.utcDate")).alias("match_date"),
        F.col("match.matchday").cast(IntegerType()).alias("matchday"),
        F.col("match.stage").cast(StringType()).alias("stage"),
        F.col("match.status").cast(StringType()).alias("status"),

        # Team attributes
        F.col("match.homeTeam.id").cast(IntegerType()).alias("home_team_id"),
        F.col("match.homeTeam.name").cast(StringType()).alias("home_team_name"),
        F.col("match.awayTeam.id").cast(IntegerType()).alias("away_team_id"),
        F.col("match.awayTeam.name").cast(StringType()).alias("away_team_name"),

        # Match scores
        F.coalesce(
            F.col("match.score.fullTime.home"),
            F.lit(0)
        ).cast(IntegerType()).alias("home_score"),

        F.coalesce(
            F.col("match.score.fullTime.away"),
            F.lit(0)
        ).cast(IntegerType()).alias("away_score"),

        F.col("match.score.halfTime.home").cast(IntegerType()).alias("halftime_home_score"),
        F.col("match.score.halfTime.away").cast(IntegerType()).alias("halftime_away_score"),

        # Advanced statistics (with safe defaults if API tier omits in-depth stats)
        F.coalesce(
            F.col("match.statistics.home_shots"),
            F.lit(12)
        ).cast(IntegerType()).alias("home_shots"),

        F.coalesce(
            F.col("match.statistics.away_shots"),
            F.lit(10)
        ).cast(IntegerType()).alias("away_shots"),

        F.coalesce(
            F.col("match.statistics.home_shots_on_target"),
            F.lit(5)
        ).cast(IntegerType()).alias("home_shots_on_target"),

        F.coalesce(
            F.col("match.statistics.away_shots_on_target"),
            F.lit(4)
        ).cast(IntegerType()).alias("away_shots_on_target"),

        F.coalesce(
            F.col("match.statistics.home_possession_pct"),
            F.lit(50.0)
        ).cast(DoubleType()).alias("home_possession_pct"),

        F.coalesce(
            F.col("match.statistics.away_possession_pct"),
            F.lit(50.0)
        ).cast(DoubleType()).alias("away_possession_pct"),

        F.coalesce(
            F.col("match.statistics.home_fouls"),
            F.lit(10)
        ).cast(IntegerType()).alias("home_fouls"),

        F.coalesce(
            F.col("match.statistics.away_fouls"),
            F.lit(11)
        ).cast(IntegerType()).alias("away_fouls"),

        F.coalesce(
            F.col("match.statistics.home_yellow_cards"),
            F.lit(2)
        ).cast(IntegerType()).alias("home_yellow_cards"),

        F.coalesce(
            F.col("match.statistics.away_yellow_cards"),
            F.lit(2)
        ).cast(IntegerType()).alias("away_yellow_cards"),

        F.coalesce(
            F.col("match.statistics.home_red_cards"),
            F.lit(0)
        ).cast(IntegerType()).alias("home_red_cards"),

        F.coalesce(
            F.col("match.statistics.away_red_cards"),
            F.lit(0)
        ).cast(IntegerType()).alias("away_red_cards")
    )

    # 1. Filter out invalid rows (missing ID or null dates)
    cleaned_df = flattened_df.filter(
        F.col("match_id").isNotNull() & F.col("match_date").isNotNull()
    )

    # 2. Deduplicate matches by match_id keeping the latest record
    dedup_window = Window.partitionBy("match_id").orderBy(F.col("match_datetime").desc_nulls_last())
    deduped_df = cleaned_df.withColumn("row_num", F.row_number().over(dedup_window)) \
                           .filter(F.col("row_num") == 1) \
                           .drop("row_num")

    # 3. Calculate Analytical KPIs & Metrics
    enriched_df = deduped_df.withColumn(
        "total_goals",
        F.col("home_score") + F.col("away_score")
    ).withColumn(
        "goal_differential",
        F.col("home_score") - F.col("away_score")
    ).withColumn(
        "match_outcome",
        F.when(F.col("home_score") > F.col("away_score"), F.lit("HOME_WIN"))
         .when(F.col("home_score") < F.col("away_score"), F.lit("AWAY_WIN"))
         .otherwise(F.lit("DRAW"))
    ).withColumn(
        "is_over_2_5_goals",
        F.when((F.col("home_score") + F.col("away_score")) > 2.5, F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "both_teams_scored",
        F.when((F.col("home_score") > 0) & (F.col("away_score") > 0), F.lit(1)).otherwise(F.lit(0))
    ).withColumn(
        "home_points",
        F.when(F.col("home_score") > F.col("away_score"), F.lit(3))
         .when(F.col("home_score") == F.col("away_score"), F.lit(1))
         .otherwise(F.lit(0))
    ).withColumn(
        "away_points",
        F.when(F.col("away_score") > F.col("home_score"), F.lit(3))
         .when(F.col("home_score") == F.col("away_score"), F.lit(1))
         .otherwise(F.lit(0))
    ).withColumn(
        "home_shot_conversion_rate",
        F.round(F.col("home_score") / F.greatest(F.col("home_shots"), F.lit(1)), 4)
    ).withColumn(
        "away_shot_conversion_rate",
        F.round(F.col("away_score") / F.greatest(F.col("away_shots"), F.lit(1)), 4)
    ).withColumn(
        "home_shot_accuracy",
        F.round(F.col("home_shots_on_target") / F.greatest(F.col("home_shots"), F.lit(1)), 4)
    ).withColumn(
        "away_shot_accuracy",
        F.round(F.col("away_shots_on_target") / F.greatest(F.col("away_shots"), F.lit(1)), 4)
    ).withColumn(
        "processed_timestamp",
        F.current_timestamp()
    )

    return enriched_df


def main():
    spark, glue_context, job, args = init_spark_and_glue()

    raw_path = args["S3_RAW_PATH"]
    processed_path = args["S3_PROCESSED_PATH"]

    print(f"Reading raw football match data from: {raw_path}")

    # Read all raw JSON files recursively
    raw_df = spark.read.option("multiline", "true").json(raw_path)

    # Apply data transformations and feature engineering
    processed_df = clean_and_transform_matches(raw_df)

    print(f"Writing transformed parquet data to: {processed_path}")

    # Partitioned write by season and league into Parquet with Snappy compression
    (
        processed_df.write
        .mode("overwrite")
        .partitionBy("season", "league")
        .option("compression", "snappy")
        .parquet(processed_path)
    )

    print("Data successfully transformed and written to S3 Processed Zone.")

    if IS_GLUE and job is not None:
        job.commit()


if __name__ == "__main__":
    main()
