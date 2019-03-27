__author__ = 'Amr Abouelleil'

import unittest
from tests import MANUAL_CSV, PROD_BATCH_CSV, PB_CSV
import subprocess


class SquidConnectorTests(unittest.TestCase):
    def setUp(self):
        print "Starting System Tests...\n"
        self.base_cmd = "sh /home/unix/amr/dev/python/sra_submission_tool/bin/run_submission_dev.sh"

    def test_prod_submit_single_no_attributes(self):
        cmd = self.base_cmd + " prod_single G88070 /gsap/garage-bacterial/ALE_repository/assembly/B636/sra/G88070/G88070.C65NMACXX.5.Pond-390037.bam -s GENOMIC -m RANDOM -p \"protocol placeholder text\" --force"
        print cmd
        subprocess.check_call([cmd], stdout=subprocess.PIPE, shell=True)

    def test_prod_submit_single_with_attributes(self):
        cmd = self.base_cmd + " prod_single G88078 /gsap/garage-bacterial/ALE_repository/assembly/B636/sra/G88078/G88078.C65NMACXX.2.Solexa-318743.bam -s GENOMIC --force -m RANDOM -p \"protocol placeholder text\" -a \"foo:bar|x:y|a:b\""
        print cmd
        subprocess.check_call([cmd], stdout=subprocess.PIPE, shell=True)

    def test_prod_submit_batch(self):
        cmd = self.base_cmd + " prod_batch " + PROD_BATCH_CSV + " --force"
        print cmd
        subprocess.check_call([cmd], stdout=subprocess.PIPE, shell=True)

    def test_manual_pacbio(self):
        cmd = self.base_cmd + " manual " + PB_CSV + " --force"
        print cmd
        subprocess.check_call([cmd], stdout=subprocess.PIPE, shell=True)

    def test_manual_submit(self):
        cmd = self.base_cmd + " manual " + MANUAL_CSV + " --force"
        print cmd
        subprocess.check_call([cmd], stdout=subprocess.PIPE, shell=True)

    def tearDown(self):
        print "Test complete."

if __name__ == '__main__':
    unittest.main()
