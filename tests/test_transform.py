"""
Unit tests for data transformation module.
"""
import unittest
from transform import transform_data


class TestTransformData(unittest.TestCase):
    """Test data transformation and validation."""

    def test_empty_input(self):
        """Should handle empty input gracefully."""
        result = transform_data([])
        self.assertEqual(result, [])

    def test_basic_transformation(self):
        """Should transform valid data correctly."""
        raw_data = [
            {
                "SpatialDim": "USA",
                "TimeDim": 2020,
                "NumericValue": 78.5,
                "Dim1": "SEX_BTSX"
            }
        ]

        result = transform_data(raw_data)

        self.assertEqual(len(result), 1)
        record = result[0]
        self.assertEqual(record["country_code"], "USA")
        self.assertEqual(record["year"], 2020)
        self.assertEqual(record["value"], 78.5)
        self.assertEqual(record["indicator_code"], "WHOSIS_000001")

    def test_removes_null_values(self):
        """Should filter out records with null critical fields."""
        raw_data = [
            {"SpatialDim": "USA", "TimeDim": 2020, "NumericValue": 78.5},
            {"SpatialDim": None, "TimeDim": 2020, "NumericValue": 80.0},
            {"SpatialDim": "CAN", "TimeDim": None, "NumericValue": 82.0},
            {"SpatialDim": "GBR", "TimeDim": 2020, "NumericValue": None},
        ]

        result = transform_data(raw_data)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["country_code"], "USA")

    def test_validates_year_range(self):
        """Should filter out invalid years."""
        raw_data = [
            {"SpatialDim": "USA", "TimeDim": 1899, "NumericValue": 50.0},
            {"SpatialDim": "CAN", "TimeDim": 2020, "NumericValue": 82.0},
            {"SpatialDim": "GBR", "TimeDim": 2031, "NumericValue": 85.0},
        ]

        result = transform_data(raw_data)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["country_code"], "CAN")

    def test_validates_value_range(self):
        """Should filter out negative values."""
        raw_data = [
            {"SpatialDim": "USA", "TimeDim": 2020, "NumericValue": -5.0},
            {"SpatialDim": "CAN", "TimeDim": 2020, "NumericValue": 82.0},
        ]

        result = transform_data(raw_data)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["value"], 82.0)

    def test_removes_duplicates(self):
        """Should remove duplicate country/year combinations."""
        raw_data = [
            {"SpatialDim": "USA", "TimeDim": 2020, "NumericValue": 78.5},
            {"SpatialDim": "USA", "TimeDim": 2020, "NumericValue": 79.0},
            {"SpatialDim": "CAN", "TimeDim": 2020, "NumericValue": 82.0},
        ]

        result = transform_data(raw_data)

        self.assertEqual(len(result), 2)
        countries = [r["country_code"] for r in result]
        self.assertEqual(countries, ["USA", "CAN"])

    def test_type_conversion(self):
        """Should convert year to int and value to float."""
        raw_data = [
            {"SpatialDim": "USA", "TimeDim": "2020", "NumericValue": "78.5"}
        ]

        result = transform_data(raw_data)

        self.assertIsInstance(result[0]["year"], int)
        self.assertIsInstance(result[0]["value"], float)


if __name__ == '__main__':
    unittest.main()
