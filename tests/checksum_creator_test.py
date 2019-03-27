__author__ = 'Amr Abouelleil'

import unittest
import SRA_submission_tool.constants as c
from SRA_submission_tool.file_service import ChecksumCreator


class ChecksumCreatorUnitTests(unittest.TestCase):
    def setUp(self):
        print "Starting Checksum Creator testing..."
        self.md5_result = "9f5baff90de4199fa2de67f2239ac797"

    def test_hashlib_md5_result_equals_md5_result(self):
        cc = ChecksumCreator(c.test_file_path + "good_test.bam")
        self.assertEqual(self.md5_result, cc.create_checksum())

    def test_write_checksum_file_name_is_correct(self):
        cc = ChecksumCreator(c.test_file_path + "good_test.bam")
        checksum = cc.create_checksum()
        self.assertEqual(cc.write_checksum(checksum), c.test_file_path + "good_test.bam.md5")

    def tearDown(self):
        print "Checksum Creator testing complete."


if __name__ == '__main__':
    unittest.main()
