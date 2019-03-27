__author__ = 'Amr Abouelleil'

import unittest
from tests import BAM
from SRA_submission_tool.screening_service import ScreeningService


class ScreenTest(unittest.TestCase):
    def setUp(self):
        print "Starting ScreenTest..."
        self.ss = ScreeningService()

    def test_screen_human(self):
        self.ss.screen_human(read_file=BAM, file_type="bam", library_layout="paired",
                             output_dir="/cil/shed/resources/sra_submission_tool/test_files/")
        self.assertEqual(True, False)

def tearDown(self):
        print "Ending ScreenTest..."


if __name__ == '__main__':
    unittest.main()
