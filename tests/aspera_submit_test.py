__author__ = 'Amr Abouelleil'

import unittest
from SRA_submission_tool.transfer_service import AsperaTransfer
from tests import TEST_FILE
from SRA_submission_tool import constants as c

class AsperaSubmitTest(unittest.TestCase):

    def setUp(self):
        self.at = AsperaTransfer()

    def test_aspera_submission_returns_zero_code_when_successful(self):
        run = self.at.aspera_submit(c.asp_acct, TEST_FILE, 'Test')
        self.assertEqual(0, run)

if __name__ == '__main__':
    unittest.main()
