import SRA_submission_tool.constants as c
import subprocess
import logging
import time
import sys
from SRA_submission_tool.submission_db import SubmissionDBService
__author__ = "Amr Abouelleil"


class AsperaTransfer(object):
    """
    A class for creating submission and retrieval objects.
    """
    def __init__(self, spuid=None):
        self.logger = logging.getLogger('sra_tool.transfer_service.AsperaTransfer')
        self.sdb = SubmissionDBService(c.submission_db)
        self.spuid = spuid

    def aspera_submit(self, account, source_file, dest_dir):
        """
        A method for submitting a directory to a destination via aspera.
        :param account: The aspera account to use for authentication with destination server.
        :param source_file: The source file or directory to submit.
        :param dest_dir: The destination directory for submission.
        :return:
        """
        cmd = 'ascp -i ' \
              + c.key_file_path + ' --overwrite=always -QT -l100m -k1 -d ' + source_file + " "\
              + account + ":submit/" + dest_dir
        self.logger.debug("Aspera upload command: " + cmd)
        try:
            subprocess.check_call([cmd], stdout=subprocess.PIPE, shell=True)
            self.sdb.update_sub_data(self.spuid, "submission_status", "uploading")
            self.logger.info("Aspera upload Complete.")
        except subprocess.CalledProcessError as e:
            self.logger.error("Upload subprocess call failed:" + str(e))
            self.logger.error("Status : FAIL", e.returncode, e.output)
            self.sdb.update_sub_data(self.spuid, "submission_status", "upload failure")
            sys.exit(1)

    def aspera_retrieve(self, account, source_file, dest_dir, retry):
        """
        A method for retrieving data from a remote location.
        :param account: The aspera account to use for authentication with remote server.
        :param source_file: The remote source file or directory to download
        :param dest_dir: the local destination for the data
        :return:
        """
        cmd = 'ascp -i ' \
              + c.key_file_path + ' --overwrite=always -QT -l100m -k1 ' + account + ":submit/" + source_file + " " \
              + dest_dir
        self.logger.info("Aspera retrieve command: " + cmd)
        downloaded = False
        attempts = 0
        if retry:
            while not downloaded:
                try:
                    subprocess.check_call([cmd], stdout=subprocess.PIPE, shell=True)
                    downloaded = True
                except subprocess.CalledProcessError as e:
                    self.logger.error(msg=str(e) + "\nRetrying download in sixty seconds...\n")
                    time.sleep(60)
                    attempts += 1
                if attempts > 30:
                    self.logger.critical("Maximum download attempts exceeded. Aborting submission.")
                    self.sdb.update_sub_data(self.spuid, "submission_status", "upload failure")
                    sys.exit(-1)
        else:
            try:
                subprocess.check_call([cmd], stdout=subprocess.PIPE, shell=True)
                self.sdb.update_sub_data(self.spuid, "submission_status", "report retrieved")
                self.logger.info("Report.xml retrieved.")
            except subprocess.CalledProcessError as e:
                self.logger.error(msg=str(e))
