__author__ = 'Amr Abouelleil'

import unittest
from SRA_submission_tool.file_service import PacBioService
from SRA_submission_tool.file_service import archive_maker
from tests import PACBIO_FILE


class PacBioServiceTest(unittest.TestCase):
    def setUp(self):
        print "Starting PacBioService testing..."
        self.pbs = PacBioService()

    def test_create_pacbio_file_list(self):
        root_name = "/cil/shed/resources/SRA_submission_tool/test_files/pacbio/B01_1/Analysis_Results/m151205_044215_00120_c100902282550000001823204904231631_s1_p0"
        file_data = self.pbs.get_pacbio_files_data(PACBIO_FILE)
        self.assertEqual(file_data['file_list'][0], '/cil/shed/resources/SRA_submission_tool/test_files/pacbio/B01_1/m151205_044215_00120_c100902282550000001823204904231631_s1_p0.mcd.h5')
        self.assertEqual(file_data['file_list'][1], root_name + ".1.bax.h5")
        self.assertEqual(file_data['file_list'][2], root_name + ".2.bax.h5")
        self.assertEqual(file_data['file_list'][3], root_name + ".3.bax.h5")

    def test_archive_pacbio(self):
        archive_maker(file_list=self.pbs.get_pacbio_files_data(PACBIO_FILE)['file_list'],
                      archive_dest="/cil/shed/resources/SRA_submission_tool/test_files/pacbio/pb.test")


    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
