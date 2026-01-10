"""
Integration tests for the full ETL pipeline.
"""
import unittest
from unittest.mock import Mock, patch, MagicMock

from extract import extract_data
from transform import transform_data
from load import load_data


class TestETLIntegration(unittest.TestCase):
    """Test full ETL workflow."""

    @patch('extract.fetch_with_retry')
    @patch('extract.save_checkpoint')
    @patch('extract.time.sleep')
    def test_extract_to_transform_pipeline(self, mock_sleep, mock_checkpoint, mock_fetch):
        """Should extract and transform data end-to-end."""
        mock_fetch.side_effect = [
            {
                "value": [
                    {
                        "SpatialDim": "USA",
                        "TimeDim": 2020,
                        "NumericValue": 78.5,
                        "Dim1": "SEX_BTSX"
                    },
                    {
                        "SpatialDim": "CAN",
                        "TimeDim": 2020,
                        "NumericValue": 82.1,
                        "Dim1": "SEX_BTSX"
                    }
                ] * 50
            },
            {"value": []}
        ]

        # Extract
        raw_data = extract_data()
        self.assertEqual(len(raw_data), 100)

        # Transform
        clean_data = transform_data(raw_data)
        self.assertGreater(len(clean_data), 0)

        # Verify structure
        for record in clean_data:
            self.assertIn("country_code", record)
            self.assertIn("year", record)
            self.assertIn("value", record)
            self.assertIn("indicator_code", record)

    def test_data_structure_compatibility(self):
        """Should verify transform output matches load input schema."""
        raw_data = [
            {
                "SpatialDim": "USA",
                "TimeDim": 2020,
                "NumericValue": 78.5,
                "Dim1": "SEX_BTSX"
            }
        ]

        clean_data = transform_data(raw_data)

        # Verify structure matches what load expects
        required_fields = ["country_code", "indicator_code", "indicator_name",
                          "year", "value", "source_url"]

        for record in clean_data:
            for field in required_fields:
                self.assertIn(field, record)

            # Verify types match database schema
            self.assertIsInstance(record["country_code"], str)
            self.assertIsInstance(record["year"], int)
            self.assertIsInstance(record["value"], float)


if __name__ == '__main__':
    unittest.main()
