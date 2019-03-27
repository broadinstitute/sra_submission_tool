import sqlite3
import SRA_submission_tool.constants as c
import logging
import time
import sys
import shutil
import os


class SubmissionDBService(object):

    def __init__(self, db, logger = None):
        if logger:
            self.logger = logging.getLogger(logger + '.submission_db.SubmissionDBService')
        else:
            self.logger = logging.getLogger('sra_tool.submission_db.SubmissionDBService')
        self.db = db

    def get_new_spuid(self, read_file):
        conn = sqlite3.connect(self.db, timeout=c.global_db_timeout)
        cursor = conn.cursor()
        try:
            insert_string = ('''INSERT INTO submissions (spuid_date, read_file) VALUES(\"'''
                             + str(time.strftime("%Y-%m-%d %H:%M:%S")) + '''\", \"''' + read_file +'''\")''')
            self.logger.debug("Creating new SPUID with SQL string:" + insert_string)
            cursor.execute(insert_string)
            conn.commit()
            new_spuid = str(cursor.lastrowid)
            self.logger.debug("New SPUID generated:" + new_spuid)
        except sqlite3.OperationalError as e:
            self.logger.critical("Could not obtain new SPUID from database. Aborting." + str(e))
            sys.exit(-1)
        conn.close()
        return new_spuid

    def update_sub_data(self, spuid, column_id, column_value):
        if column_value:
            update_string = '''UPDATE submissions SET ''' + column_id + '''=\'''' + column_value.replace('\'', '') \
                            + '''\' WHERE spuid=\'''' + str(spuid) + '''\''''
        else:
            update_string = '''UPDATE submissions SET ''' + column_id + '''=\'''' + str(column_value) \
                            + '''\' WHERE spuid=\'''' + str(spuid) + '''\''''
        self.logger.debug("Attempting update:" + update_string)
        try:
            conn = sqlite3.connect(self.db, timeout=c.global_db_timeout)
            cursor = conn.cursor()
            cursor.execute(update_string)
            conn.commit()
            conn.close()
            self.logger.debug("Submission DB updated:" + update_string)
        except sqlite3.OperationalError as e:
            self.logger.error("Problem updating database with " + str(column_id) + ": " + str(e) + "Aborting submission.")
            self.logger.error("Update string:" + update_string)

    def check_submission_exists(self, platform_unit):
        try:
            conn = sqlite3.connect(self.db, timeout=c.global_db_timeout)
            cursor = conn.cursor()
            check_string = '''SELECT DISTINCT spuid, temp_path, submission_status FROM submissions WHERE platform_unit=\''''\
                           + platform_unit + '''\''''
            self.logger.debug("Query: " + check_string)
            cursor.execute(check_string)
            results = cursor.fetchall()
            conn.close()
            self.logger.debug("Check results: " + str(results))
            return results

        except sqlite3.OperationalError as e:
            self.logger.critical("Problem checking database for existing SPUID. Aborting submission." + str(e))

    def get_previous_submission_info(self, read_file):
        try:
            conn = sqlite3.connect(self.db, timeout=c.global_db_timeout)
            cursor = conn.cursor()
            check_string = '''SELECT DISTINCT spuid, temp_path, submission_status FROM submissions WHERE read_file=\''''\
                           + read_file + '''\''''
            cursor.execute(check_string)
            results = cursor.fetchall()
            conn.close()
            prev_spuid = str(results[0][0])
            prev_temp = str(results[0][1])
            self.logger.debug("Previous SPUID " + prev_spuid + " found. Processing as resubmission.")
            self.logger.debug("Previous temp dir " + prev_temp)
            self.logger.debug("Clearing " + prev_temp + " contents.")
            if os.path.isdir(prev_temp):
                shutil.rmtree(prev_temp)
            return prev_spuid, prev_temp
        except sqlite3.OperationalError as e:
            self.logger.critical("Problem retrieving previous submission information. Aborting submission." + str(e))

    def reset_resub(self, spuid):
        self.logger.critical("Resetting DB data for resubmission of SPUID " + str(spuid))

        for key in c.reset_fields:
            self.update_sub_data(spuid, key, None)