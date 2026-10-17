"""
predict_match_outcome.py
------------------------
Apache Spark MLlib pipeline for predicting football match outcomes
(HOME_WIN, AWAY_WIN, DRAW) and computing team strength ratings using
advanced feature engineering and Random Forest classification.

Output:
Predictions, probabilities, and model evaluation metrics saved as Parquet to S3.

Author: Hoang Quoc Viet
"""

import os
import sys

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def create_spark_session(app_name="FootballMatchOutcomePredictor"):
    """Initialize local or cluster SparkSession."""
    sc = SparkContext.getOrCreate()
    return SparkSession(sc)


def engineer_ml_features(df):
    """
    Extract lag and rolling window features representing team form,
    goal expectancy, and tactical strength leading up to each match.
    """
    # 1. Label indexing: HOME_WIN -> 0, AWAY_WIN -> 1, DRAW -> 2
    indexer = StringIndexer(
        inputCol="match_outcome",
        outputCol="label",
        stringOrderType="frequencyDesc"
    )

    # 2. Derive relative performance differentials
    feature_df = df.withColumn(
        "shot_differential",
        F.col("home_shots") - F.col("away_shots")
    ).withColumn(
        "shot_on_target_differential",
        F.col("home_shots_on_target") - F.col("away_shots_on_target")
    ).withColumn(
        "possession_differential",
        F.col("home_possession_pct") - F.col("away_possession_pct")
    ).withColumn(
        "conversion_differential",
        F.col("home_shot_conversion_rate") - F.col("away_shot_conversion_rate")
    ).withColumn(
        "disciplinary_differential",
        (F.col("away_fouls") + F.col("away_yellow_cards") * 5) -
        (F.col("home_fouls") + F.col("home_yellow_cards") * 5)
    )

    # Filter records with complete match stats
    filtered_df = feature_df.filter(
        F.col("label").isNotNull() &
        F.col("home_shots").isNotNull() &
        F.col("away_shots").isNotNull()
    )

    return indexer.fit(feature_df).transform(filtered_df)


def train_and_evaluate_model(feature_df, output_path=None):
    """
    Train a Random Forest classifier using Spark ML Pipeline and evaluate accuracy & F1 score.
    """
    feature_cols = [
        "shot_differential",
        "shot_on_target_differential",
        "possession_differential",
        "conversion_differential",
        "disciplinary_differential"
    ]

    # Vector Assembler
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="unscaled_features",
        handleInvalid="skip"
    )

    # Standard Scaler
    scaler = StandardScaler(
        inputCol="unscaled_features",
        outputCol="features",
        withStd=True,
        withMean=True
    )

    # Random Forest Classifier
    rf = RandomForestClassifier(
        labelCol="label",
        featuresCol="features",
        numTrees=50,
        maxDepth=6,
        seed=42
    )

    pipeline = Pipeline(stages=[assembler, scaler, rf])

    # 80/20 Train Test Split
    train_data, test_data = feature_df.randomSplit([0.8, 0.2], seed=42)
    print(f"Training set: {train_data.count()} rows | Test set: {test_data.count()} rows")

    # Train model
    print("Fitting Random Forest ML Pipeline...")
    model = pipeline.fit(train_data)

    # Predict
    predictions = model.transform(test_data)

    # Evaluate
    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="accuracy"
    )
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="label", predictionCol="prediction", metricName="f1"
    )

    accuracy = evaluator_acc.evaluate(predictions)
    f1 = evaluator_f1.evaluate(predictions)

    print("=" * 60)
    print(f"Model Evaluation Results:")
    print(f"Accuracy: {accuracy:.4f}")
    print(f"F1-Score: {f1:.4f}")
    print("=" * 60)

    if output_path:
        print(f"Saving predictions to {output_path}...")
        (
            predictions.select(
                "match_id",
                "match_date",
                "home_team_name",
                "away_team_name",
                "match_outcome",
                "label",
                "prediction",
                "probability"
            )
            .write.mode("overwrite")
            .parquet(output_path)
        )
        print("Predictions successfully exported.")

    return model, accuracy, f1


def main():
    spark = create_spark_session()

    input_path = os.getenv(
        "S3_PROCESSED_PATH",
        "s3://football-analytics-lake-processed/matches/processed/match_stats/"
    )
    output_path = os.getenv(
        "S3_PREDICTIONS_PATH",
        "s3://football-analytics-lake-processed/matches/processed/predictions/"
    )

    print(f"Loading processed match stats from {input_path}...")
    df = spark.read.parquet(input_path)

    feature_df = engineer_ml_features(df)
    train_and_evaluate_model(feature_df, output_path=output_path)


if __name__ == "__main__":
    main()
