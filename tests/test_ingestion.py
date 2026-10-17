"""
Unit tests for football data ingestion module.
"""

import sys
import unittest
from unittest.mock import MagicMock, patch

# Provide mock fallback for cloud libraries if running in minimal test environment
if "boto3" not in sys.modules:
    sys.modules["boto3"] = MagicMock()
if "botocore" not in sys.modules:
    sys.modules["botocore"] = MagicMock()
if "botocore.exceptions" not in sys.modules:
    mock_exceptions = MagicMock()
    mock_exceptions.ClientError = Exception
    sys.modules["botocore.exceptions"] = mock_exceptions
if "requests" not in sys.modules:
    sys.modules["requests"] = MagicMock()

from scripts.ingestion.fetch_football_data import FootballDataIngestion, load_config


class TestFootballDataIngestion(unittest.TestCase):

    def setUp(self):
        self.mock_config = {
            "aws": {"region": "us-east-1"},
            "storage": {
                "s3_buckets": {"raw": "test-raw-bucket"},
                "s3_prefixes": {"raw_matches": "matches/raw/"}
            },
            "data_sources": {
                "base_url": "https://api.football-data.org/v4",
                "target_leagues": [{"id": "PL", "name": "Premier League"}],
                "seasons": ["2023"]
            }
        }

    @patch("boto3.client")
    def test_init_and_headers(self, mock_boto):
        ingestion = FootballDataIngestion(self.mock_config, api_key="secret-test-token")
        headers = ingestion._get_headers()

        self.assertEqual(headers.get("X-Auth-Token"), "secret-test-token")
        self.assertIn("FootballAnalyticsPipeline", headers.get("User-Agent", ""))

    @patch("boto3.client")
    def test_upload_dry_run(self, mock_boto):
        ingestion = FootballDataIngestion(self.mock_config)
        sample_data = {"matches": [{"id": 1, "score": {"fullTime": {"home": 2, "away": 1}}}]}

        s3_uri = ingestion.upload_to_s3(sample_data, league_id="PL", season="2023", dry_run=True)

        self.assertTrue(s3_uri.startswith("s3://test-raw-bucket/matches/raw/league=PL/season=2023/"))
        self.assertTrue(s3_uri.endswith(".json"))

    @patch("requests.get")
    @patch("boto3.client")
    def test_fetch_matches_success(self, mock_boto, mock_requests_get):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"count": 1, "matches": [{"id": 100}]}
        mock_requests_get.return_value = mock_response

        ingestion = FootballDataIngestion(self.mock_config)
        payload = ingestion.fetch_matches("PL", "2023")

        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["matches"][0]["id"], 100)


if __name__ == "__main__":
    unittest.main()
