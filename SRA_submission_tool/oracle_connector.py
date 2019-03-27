import cx_Oracle
import logging
__author__ = 'Amr Abouelleil'


class OracleConnector(object):

    def __init__(self):
        self.logger = logging.getLogger('sra_tool.oracle_connector.OracleConnector')

    def connect(self, connection_string):
        return cx_Oracle.connect(connection_string)

    def get_run_name(self, connection, run_barcode, lane, library):
        sql = """
              SELECT DISTINCT rgb.project, mdm_info.mdm_global_id, rgb.lane, rgb.screened, ngr.name, rgb.*, ngr.*, mdm_info.*, sf.*
              FROM read_group_bam@BASS_LINK rgb JOIN mdm_info@BASS_LINK mdm_info ON mdm_info.mdm_id = rgb.mdm_id JOIN storedfile@BASS_LINK sf ON sf.locationid = mdm_info.mdm_location_id LEFT JOIN next_generation_run ngr ON ngr.barcode = rgb.run_barcode
              WHERE rgb.run_barcode = '""" + run_barcode + """' AND rgb.lane = """ + lane + """AND rgb.library_name = '""" + library + """'"""
        self.logger.info("SQL query constructed: " + sql)
        cur = connection.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()
        self.logger.info("run_name retrieved:" + row[4])
        return row[4]
