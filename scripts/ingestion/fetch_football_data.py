"""
fetch_football_data.py
----------------------
Ingestion script that fetches football fixture and match statistics data from 
a football API (such as football-data.org or API-Football) using `requests`,
and streams raw JSON payloads into an AWS S3 Raw Data Lake Zone using `boto3`.

Author: Hoang Quoc Viet
"""

import argparse
import datetime
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import boto3
from botocore.exceptions import ClientError
import requests

# Set up structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("football_ingestion")


def load_config(config_path: str = "config/pipeline_config.json") -> Dict[str, Any]:
    """Load configuration from JSON config file with fallback to defaults."""
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    logger.warning("Config file not found at %s. Using default environment configuration.", config_path)
    return {
        "aws": {"region": os.getenv("AWS_DEFAULT_REGION", "us-east-1")},
        "storage": {
            "s3_buckets": {"raw": os.getenv("S3_RAW_BUCKET", "football-analytics-lake-raw")},
            "s3_prefixes": {"raw_matches": "matches/raw/"}
        },
        "data_sources": {
            "base_url": "https://api.football-data.org/v4",
            "target_leagues": [{"id": "PL", "name": "Premier League"}],
            "seasons": ["2023", "2024"]
        }
    }


class FootballDataIngestion:
    """Manages ingestion of football match data into AWS S3 Raw Zone."""

    def __init__(self, config: Dict[str, Any], api_key: Optional[str] = None):
        self.config = config
        self.aws_region = config.get("aws", {}).get("region", "us-east-1")
        self.raw_bucket = (
            config.get("storage", {}).get("s3_buckets", {}).get("raw", "football-analytics-lake-raw")
            .replace("s3://", "")
        )
        self.prefix = config.get("storage", {}).get("s3_prefixes", {}).get("raw_matches", "matches/raw/")
        self.base_url = config.get("data_sources", {}).get("base_url", "https://api.football-data.org/v4")
        self.api_key = api_key or os.getenv("FOOTBALL_API_KEY", "")

        # Initialize boto3 S3 client
        self.s3_client = boto3.client("s3", region_name=self.aws_region)

    def _get_headers(self) -> Dict[str, str]:
        headers = {"User-Agent": "FootballAnalyticsPipeline/1.0"}
        if self.api_key:
            headers["X-Auth-Token"] = self.api_key
        return headers

    def fetch_matches(
        self,
        league_id: str,
        season: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Fetch matches data for a specific league and season from the REST API.
        Implements error handling and rate-limit backoff.
        """
        endpoint = f"/competitions/{league_id}/matches"
        url = urljoin(self.base_url, endpoint)

        params: Dict[str, Any] = {"season": season}
        if date_from:
            params["dateFrom"] = date_from
        if date_to:
            params["dateTo"] = date_to

        logger.info("Fetching matches from %s with params %s", url, params)

        max_retries = 3
        backoff_seconds = 2

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.get(url, headers=self._get_headers(), params=params, timeout=30)
                if response.status_code == 429:
                    wait_time = int(response.headers.get("Retry-After", backoff_seconds * attempt))
                    logger.warning("Rate limit hit (429). Retrying in %d seconds...", wait_time)
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.RequestException as e:
                logger.error("Attempt %d failed to fetch from API: %s", attempt, str(e))
                if attempt == max_retries:
                    raise
                time.sleep(backoff_seconds * attempt)

        return {}

    def upload_to_s3(
        self,
        data: Dict[str, Any],
        league_id: str,
        season: str,
        dry_run: bool = False
    ) -> str:
        """
        Upload JSON response payload to S3 Raw Zone with partition structure:
        s3://<raw-bucket>/matches/raw/league=<league_id>/season=<season>/matches_<timestamp>.json
        """
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
        s3_key = f"{self.prefix.rstrip('/')}/league={league_id}/season={season}/matches_{timestamp}.json"

        json_bytes = json.dumps(data, indent=2).encode("utf-8")

        if dry_run:
            logger.info("[DRY RUN] Would upload %d bytes to s3://%s/%s", len(json_bytes), self.raw_bucket, s3_key)
            return f"s3://{self.raw_bucket}/{s3_key}"

        try:
            logger.info("Uploading %d bytes to s3://%s/%s", len(json_bytes), self.raw_bucket, s3_key)
            self.s3_client.put_object(
                Bucket=self.raw_bucket,
                Key=s3_key,
                Body=json_bytes,
                ContentType="application/json",
                Metadata={
                    "league": league_id,
                    "season": season,
                    "ingestion_timestamp": timestamp,
                    "source": "football_data_api"
                }
            )
            logger.info("Successfully ingested data to S3: s3://%s/%s", self.raw_bucket, s3_key)
            return f"s3://{self.raw_bucket}/{s3_key}"

        except ClientError as e:
            logger.error("Failed to upload object to S3: %s", str(e))
            raise

    def run_pipeline(
        self,
        leagues: Optional[List[str]] = None,
        seasons: Optional[List[str]] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        dry_run: bool = False
    ) -> List[str]:
        """Execute ingestion across specified leagues and seasons."""
        target_leagues = leagues or [
            item["id"] for item in self.config.get("data_sources", {}).get("target_leagues", [])
        ]
        target_seasons = seasons or self.config.get("data_sources", {}).get("seasons", ["2023"])

        uploaded_uris = []
        logger.info("Starting ingestion for leagues: %s, seasons: %s", target_leagues, target_seasons)

        for league in target_leagues:
            for season in target_seasons:
                try:
                    payload = self.fetch_matches(league, season, date_from, date_to)
                    if payload and "matches" in payload:
                        matches_count = len(payload.get("matches", []))
                        logger.info("Fetched %d matches for %s (Season %s)", matches_count, league, season)
                        uri = self.upload_to_s3(payload, league, season, dry_run=dry_run)
                        uploaded_uris.append(uri)
                    else:
                        logger.warning("No matches returned for league %s season %s", league, season)

                    # Polite sleep to respect API limits
                    time.sleep(1.0)

                except Exception as e:
                    logger.error("Error processing %s season %s: %s", league, season, str(e))

        logger.info("Ingestion completed. Total files written: %d", len(uploaded_uris))
        return uploaded_uris


def main():
    parser = argparse.ArgumentParser(description="Ingest football match data into AWS S3 Raw Zone.")
    parser.add_argument("--config", default="config/pipeline_config.json", help="Path to config JSON")
    parser.add_argument("--league", action="append", help="Target league ID (e.g. PL, PD, SA, BL1)")
    parser.add_argument("--season", action="append", help="Target season year (e.g. 2023, 2024)")
    parser.add_argument("--date-from", dest="date_from", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--date-to", dest="date_to", help="End date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Perform a dry run without uploading to S3")
    args = parser.parse_args()

    config = load_config(args.config)
    ingestion = FootballDataIngestion(config)
    ingestion.run_pipeline(
        leagues=args.league,
        seasons=args.season,
        date_from=args.date_from,
        date_to=args.date_to,
        dry_run=args.dry_run
    )


if __name__ == "__main__":
    main()
