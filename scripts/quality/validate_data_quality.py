"""
validate_data_quality.py
------------------------
Automated Data Quality Gatekeeper for Football Analytics Lakehouse.
Performs pre-flight and post-transformation checks to ensure high data integrity
before data is served to Amazon Athena and QuickSight.

Author: Hoang Quoc Viet
"""

import sys
from typing import Dict, Any, List
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class DataQualityGate:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.results: List[Dict[str, Any]] = []

    def log_result(self, check_name: str, passed: bool, details: str):
        status = "PASSED" if passed else "FAILED"
        print(f"[{status}] {check_name}: {details}")
        self.results.append({
            "check": check_name,
            "status": status,
            "details": details
        })

    def validate_processed_matches(self, df) -> bool:
        """Run all data quality rules against processed match dataset."""
        print("Running Data Quality Checks on Processed Match Stats...")
        all_passed = True
        total_rows = df.count()

        # Check 1: Non-empty dataset
        if total_rows == 0:
            self.log_result("Dataset Non-Empty Check", False, "Dataset contains 0 rows.")
            return False
        self.log_result("Dataset Non-Empty Check", True, f"Found {total_rows} records.")

        # Check 2: Null check on Primary Key (match_id)
        null_ids = df.filter(F.col("match_id").isNull()).count()
        passed = (null_ids == 0)
        self.log_result("Null Primary Key Check (match_id)", passed, f"Null match_id count: {null_ids}")
        if not passed:
            all_passed = False

        # Check 3: Uniqueness check on match_id
        distinct_ids = df.select("match_id").distinct().count()
        passed = (distinct_ids == total_rows)
        self.log_result(
            "Uniqueness Check (match_id)",
            passed,
            f"Unique IDs: {distinct_ids} vs Total Rows: {total_rows}"
        )
        if not passed:
            all_passed = False

        # Check 4: Domain constraint on match_outcome
        valid_outcomes = ["HOME_WIN", "AWAY_WIN", "DRAW"]
        invalid_outcomes = df.filter(~F.col("match_outcome").isin(valid_outcomes)).count()
        passed = (invalid_outcomes == 0)
        self.log_result(
            "Domain Validity Check (match_outcome)",
            passed,
            f"Invalid outcome values: {invalid_outcomes}"
        )
        if not passed:
            all_passed = False

        # Check 5: Range constraint on scores (scores must be >= 0)
        negative_scores = df.filter((F.col("home_score") < 0) | (F.col("away_score") < 0)).count()
        passed = (negative_scores == 0)
        self.log_result(
            "Non-negative Score Check",
            passed,
            f"Negative score records: {negative_scores}"
        )
        if not passed:
            all_passed = False

        # Check 6: Possession percentage validity (0 <= possession <= 100)
        invalid_possession = df.filter(
            (F.col("home_possession_pct") < 0.0) | (F.col("home_possession_pct") > 100.0)
        ).count()
        passed = (invalid_possession == 0)
        self.log_result(
            "Possession Range Check",
            passed,
            f"Out-of-range possession records: {invalid_possession}"
        )
        if not passed:
            all_passed = False

        return all_passed


def main():
    spark = SparkSession.builder.appName("DataQualityValidator").getOrCreate()
    dataset_path = sys.argv[1] if len(sys.argv) > 1 else "s3://football-analytics-lake-processed/matches/processed/match_stats/"

    print(f"Reading dataset for quality audit: {dataset_path}")
    df = spark.read.parquet(dataset_path)

    gate = DataQualityGate(spark)
    success = gate.validate_processed_matches(df)

    if not success:
        print("\n❌ DATA QUALITY AUDIT FAILED! Pipeline execution halted.")
        sys.exit(1)

    print("\n✅ ALL DATA QUALITY CHECKS PASSED SUCCESSFULLY.")
    sys.exit(0)


if __name__ == "__main__":
    main()
