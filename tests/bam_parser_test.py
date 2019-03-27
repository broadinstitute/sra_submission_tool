__author__ = 'Amr Abouelleil'

import unittest
from SRA_submission_tool.file_service import BamParser
from tests import BAM, WALK_UP_BAM


class BamParserTest(unittest.TestCase):

    def setUp(self):
        self.bp = BamParser()

    def test_bam_parser(self):
        self.assertIsInstance(self.bp.parse_header(BAM), dict)

    def test_walkup_bam_parser(self):
        self.assertIsInstance(self.bp.parse_header(WALK_UP_BAM), dict)

if __name__ == '__main__':
    unittest.main()
