"""
Unit tests for data extraction module.
"""
import unittest
from unittest.mock import Mock, patch
import requests

from extract import extract_data
from utils.http import fetch_with_retry


class TestFetchWithRetry(unittest.TestCase):
    """Test retry logic for API requests."""

    @patch('utils.http.requests.get')
    @patch('utils.http.time.sleep')
    def test_successful_request(self, mock_sleep, mock_get):
        """Should return data on successful request."""
        mock_response = Mock()
        mock_response.json.return_value = {"value": [{"test": "data"}]}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        result = fetch_with_retry("http://test.url")

        self.assertEqual(result, {"value": [{"test": "data"}]})
        mock_get.assert_called_once()
        mock_sleep.assert_not_called()

    @patch('utils.http.requests.get')
    @patch('utils.http.time.sleep')
    def test_retry_on_failure(self, mock_sleep, mock_get):
        """Should retry with exponential backoff on failure."""
        mock_response = Mock()
        mock_response.json.return_value = {"value": []}
        mock_response.raise_for_status = Mock()

        mock_get.side_effect = [
            requests.RequestException("Connection error"),
            requests.RequestException("Timeout"),
            mock_response
        ]

        result = fetch_with_retry("http://test.url", max_retries=3)

        self.assertEqual(mock_get.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch('utils.http.requests.get')
    @patch('utils.http.time.sleep')
    def test_max_retries_exhausted(self, mock_sleep, mock_get):
        """Should raise exception after max retries."""
        mock_get.side_effect = requests.RequestException("Persistent error")

        with self.assertRaises(requests.RequestException):
            fetch_with_retry("http://test.url", max_retries=2)

        self.assertEqual(mock_get.call_count, 2)


class TestExtractData(unittest.TestCase):
    """Test data extraction with pagination."""

    @patch('extract.fetch_with_retry')
    @patch('extract.save_checkpoint')
    @patch('extract.time.sleep')
    def test_pagination_stops_on_incomplete_page(self, mock_sleep, mock_checkpoint, mock_fetch):
        """Should stop when receiving less than PAGE_SIZE records."""
        mock_fetch.side_effect = [
            {"value": [{"id": i} for i in range(100)]},
            {"value": [{"id": i} for i in range(30)]}
        ]

        result = extract_data()

        self.assertEqual(len(result), 130)
        self.assertEqual(mock_fetch.call_count, 2)

    @patch('extract.fetch_with_retry')
    @patch('extract.save_checkpoint')
    @patch('extract.time.sleep')
    def test_resume_from_checkpoint(self, mock_sleep, mock_checkpoint, mock_fetch):
        """Should resume from checkpoint page."""
        mock_fetch.return_value = {"value": [{"id": 1}] * 50}

        extract_data(start_from={"page": 5})

        first_call_url = mock_fetch.call_args_list[0][0][0]
        self.assertIn("$skip=500", first_call_url)


if __name__ == '__main__':
    unittest.main()
