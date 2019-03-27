__author__ = 'Amr Abouelleil'

import unittest
from SRA_submission_tool.oracle_connector import OracleConnector
import cx_Oracle
#from third_party.sqlobject import *
import pprint


class OracleConnectorTest(unittest.TestCase):

    def setUp(self):
        oc = OracleConnector()
        self.con = oc.connect("seq20/wmlalos!@seqprod.broadinstitute.org:1521/seqprod")
        self.cur = self.con.cursor()

    def test_oracle_connection(self):
        self.assertEqual(self.con.version, '11.2.0.4.0')

    def test_query_get_fixed_run_name(self):
        sql = """
              SELECT DISTINCT rgb.project, mdm_info.mdm_global_id, rgb.lane, rgb.screened, ngr.name, rgb.*, ngr.*, mdm_info.*, sf.*
              FROM read_group_bam@BASS_LINK rgb JOIN mdm_info@BASS_LINK mdm_info ON mdm_info.mdm_id = rgb.mdm_id JOIN storedfile@BASS_LINK sf ON sf.locationid = mdm_info.mdm_location_id LEFT JOIN next_generation_run ngr ON ngr.barcode = rgb.run_barcode
              WHERE rgb.run_barcode = 'C686TACXX150305' AND rgb.lane = 7 AND rgb.library_name = 'Solexa-316435'
              """
        self.cur.execute(sql)
        row = self.cur.fetchone()
        self.cur.close()
        self.assertEqual("150305_SL-HEL_0262_BFCC686TACXX", row[4])

    def test_qeuery_get_variable_run_name(self):
        run_barcode = 'C686TACXX150305'
        lane = '7'
        library = 'Solexa-316435'
        sql = """
              SELECT DISTINCT rgb.project, mdm_info.mdm_global_id, rgb.lane, rgb.screened, ngr.name, rgb.*, ngr.*, mdm_info.*, sf.*
              FROM read_group_bam@BASS_LINK rgb JOIN mdm_info@BASS_LINK mdm_info ON mdm_info.mdm_id = rgb.mdm_id JOIN storedfile@BASS_LINK sf ON sf.locationid = mdm_info.mdm_location_id LEFT JOIN next_generation_run ngr ON ngr.barcode = rgb.run_barcode
              WHERE rgb.run_barcode = '""" + run_barcode + """' AND rgb.lane = """ + lane + """AND rgb.library_name = '""" + library + """'"""
        self.cur.execute(sql)
        row = self.cur.fetchone()
        self.cur.close()
        self.assertEqual("150305_SL-HEL_0262_BFCC686TACXX", row[4])

    def test_get_run_name_function(self):
        oc = OracleConnector()
        connection = oc.connect("seq20/wmlalos!@seqprod.broadinstitute.org:1521/seqprod")
        run_name = oc.get_run_name(self.con, 'C686TACXX150305', '7', 'Solexa-316435')
        self.assertEqual("150305_SL-HEL_0262_BFCC686TACXX", run_name)
        connection.close()

    def tearDown(self):
        self.con.close()


if __name__ == '__main__':
    unittest.main()
