__author__ = 'Amr Abouelleil'

import unittest
import subprocess
import os
from SRA_submission_tool import constants as c
from SRA_submission_tool.transfer_service import AsperaTransfer

class TransferServiceTests(unittest.TestCase):
    def setUp(self):
        pass

    def test_retrieval(self):
        cmd = "/broad/software/free/Linux/redhat_5_x86_64/pkgs/aspera_connect_linux_x86_64-3.1.1/connect/bin/ascp -i " \
              + c.key_file_path + " --overwrite=always -QT -l100m -k1 " \
              + c.asp_acct + ":submit/Test/tmpr16pqk/report.xml /cil/shed/resources/SRA_submission_tool/test_files/tmpr16pqk/"
        print cmd
        subprocess.check_call([cmd], shell=True)
        self.assertEqual(True, os.path.isfile("/cil/shed/resources/SRA_submission_tool/test_files/tmpr16pqk/report.xml"))
        os.remove("/cil/shed/resources/SRA_submission_tool/test_files/tmpr16pqk/report.xml")

    def test_retrieval_service(self):
        ts = AsperaTransfer()
        ts.aspera_retrieve(account=c.asp_acct, source_file='/Test/tmpr16pqk/report.xml',
                           dest_dir="/cil/shed/resources/SRA_submission_tool/test_files/tmpr16pqk/")
        self.assertEqual(True, os.path.isfile("/cil/shed/resources/SRA_submission_tool/test_files/tmpr16pqk/report.xml"))
        os.remove("/cil/shed/resources/SRA_submission_tool/test_files/tmpr16pqk/report.xml")

    def tearDown(self):
        pass

if __name__ == '__main__':
    unittest.main()
