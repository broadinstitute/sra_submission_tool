__author__ = 'Amr Abouelleil'

import unittest
import sqlite3
from SRA_submission_tool.submission_db import SubmissionDBService
import os


class DBTests(unittest.TestCase):
    def setUp(self):
        self.db = "/home/unix/amr/dev/python/SRA_submission_tool/tests/test.db"
        self.sds = SubmissionDBService(self.db)
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE submissions (spuid, g_number, read_file, release_date, temp_path, xml_attributes, ncbi_submission_id, submission_status, response_message, response_severity)''')
        self.sds.insert_sub_data("BI_TEST", "G00000", "test_file.bam", "2016-01-01", "temp/path", "Foo")
        conn.commit()
        conn.close()

    def test_update_row(self):
        self.sds.update_sub_data(spuid="BI_TEST", column_id="ncbi_submission_id", column_value="XYZ")
        self.sds.update_sub_data(spuid="BI_TEST", column_id="submission_status", column_value="123")
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        cursor.execute('''SELECT * FROM submissions WHERE spuid="BI_TEST"''')
        conn.commit()
        self.assertEqual(cursor.fetchall()[0][6], "XYZ")
        cursor.execute('''SELECT * FROM submissions WHERE spuid="BI_TEST"''')
        conn.commit()
        self.assertEqual(cursor.fetchall()[0][7], "123")
        conn.close()

    def test_update_with_sql(self):
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        cursor.execute('''UPDATE submissions SET response_message='some response' WHERE spuid="BI_TEST"''')
        cursor.execute('''UPDATE submissions SET response_severity='low' WHERE spuid="BI_TEST"''')
        cursor.execute('''SELECT * FROM submissions WHERE spuid="BI_TEST"''')
        conn.commit()
        self.assertEqual(cursor.fetchall()[0][9], "low")
        conn.close()

    def test_delete_row(self):
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        cursor.execute('''DELETE FROM submissions WHERE spuid=\'BI_TEST\'''')
        cursor.execute('''SELECT * FROM submissions''')
        conn.commit()
        self.assertEqual(len(cursor.fetchall()), 0)
        conn.close()

    def tearDown(self):
        os.remove(self.db)

if __name__ == '__main__':
    unittest.main()
