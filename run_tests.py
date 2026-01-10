"""
Test runner script.
Runs all unit tests for the ETL pipeline.
"""
import sys
import unittest

# Discover and run all tests
loader = unittest.TestLoader()
suite = loader.discover('tests', pattern='test_*.py')
runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)

# Exit with error code if tests failed
sys.exit(not result.wasSuccessful())
