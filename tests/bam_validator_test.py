__author__ = 'Amr Abouelleil'

import unittest
from SRA_submission_tool.file_service import BamValidator
from tests import TEST_PATH


class BamValidatorTests(unittest.TestCase):
    def setUp(self):
        print "Starting Bam Validator testing..."
        self.bv = BamValidator()

    def test_validation_runner_returns_no_errors_on_good_bam(self):
        result = self.bv.validate_bam(TEST_PATH + "good_test.bam")
        self.assertTrue(result)

    def test_validation_runner_bad_bam_stores_errors(self):
        result = self.bv.validate_bam(TEST_PATH + "bad_test.bam")
        self.assertFalse(result)

    def tearDown(self):
        print "Bam Validator testing complete."


if __name__ == '__main__':
    unittest.main()

