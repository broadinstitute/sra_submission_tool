__author__ = "Amr Abouelleil"

from transfer_service import AsperaTransfer
import constants as c
import logging
import sys
import time
import xml.etree.ElementTree as ET
from submission_db import SubmissionDBService
import os


class SubmissionMonitor(object):
    """
    Some docstring
    """
    def __init__(self):
        self.logger = logging.getLogger('sra_tool.sub_monitor_service.SubmissionMonitor')
        self.logger.info("Starting SubmissionMonitor Service.")

    def monitor_submission(self, remote_sub_path, local_sub_dir, spuid_suffix, retry):
        at = AsperaTransfer()
        exit_flag = False
        dbs = SubmissionDBService(c.submission_db)
        dbs.update_sub_data(spuid_suffix, "submission_status", "monitoring")
        time_lapsed = 0
        attempts = 0
        print("retry=" + str(retry))
        while not exit_flag:
            attempts += 1
            at.aspera_retrieve(account=c.asp_acct, source_file=remote_sub_path, dest_dir=local_sub_dir, retry=retry)
            if os.path.exists(local_sub_dir + '/report.xml'):
                os.chmod(local_sub_dir + '/report.xml', 0777)
            tree = ET.parse(local_sub_dir + '/report.xml')
            self.logger.debug('Monitoring ' + local_sub_dir + '/report.xml')
            root = tree.getroot()
            if root.attrib['status'] == 'processing' or root.attrib['status'] == 'submitted':
                if time_lapsed <= c.report_check_interval:
                    dbs.update_sub_data(spuid=spuid_suffix,
                                        column_id='submission_status', column_value=root.attrib['status'])
                    try:
                        dbs.update_sub_data(spuid=spuid_suffix,
                                            column_id='ncbi_submission_id',
                                            column_value=root.attrib['submission_id'])
                    except KeyError as e:
                        self.logger.error("No submission_id assigned yet." + str(e))
                        dbs.update_sub_data(spuid=spuid_suffix,
                                            column_id='ncbi_submission_id',
                                            column_value='')
                dbs.update_sub_data(spuid=spuid_suffix, column_id='submission_status',
                                    column_value=root.attrib['status'])
                self.logger.info('Report.xml still processing. Rechecking in ' + str(c.report_check_interval)
                                 + ' seconds...')
                # this sleep time is to give NCBI time to finish processing the submission
                time.sleep(c.report_check_interval)
                time_lapsed += c.report_check_interval
                if time_lapsed > c.max_report_check_time:
                    dbs.update_sub_data(spuid=spuid_suffix,
                                        column_id='submission_status',
                                        column_value=root.attrib['status'] + "-time_out")
                    update_dict = dict()
                    update_dict['response_severity'] = "sra tool time-out."
                    update_dict['response_message'] = "Submission did not receive a terminal response."
                    for k, v in update_dict.items():
                        try:
                            dbs.update_sub_data(spuid=spuid_suffix,
                                                column_id=k, column_value=v)
                        except KeyError as e:
                            self.logger.error("Unable to update " + str(k) + ":" + str(v) + "." + str(e))
                    self.logger.error("Maximum monitoring time exceeded. Aborting submissions.")
                    sys.exit(-1)
            elif root.attrib['status'] == 'processed-error':
                dbs.update_sub_data(spuid_suffix, "submission_status", root.attrib['status'])
                update_dict = {'ncbi_submission_id': root.attrib['submission_id'],
                               'submission_status': root.attrib['status']}
                try:
                    update_dict['response_severity'] = root[0][0][0].attrib['severity']
                except KeyError as e:
                    self.logger.error("Unable to assign response severity. Assigning none value." + str(e))
                    update_dict['response_severity'] = "None"
                try:
                    update_dict['response_message'] = root[0][0][0].text
                except KeyError as e:
                    self.logger.error("Unable to assign response message. Assigning none value." + str(e))
                    update_dict['response_message'] = "None"
                for k, v in update_dict.items():
                    try:
                        dbs.update_sub_data(spuid=spuid_suffix,
                                            column_id=k, column_value=v)
                    except KeyError as e:
                        self.logger.error("Unable to update " + str(k) + ":" + str(v) + "." + str(e))
                self.logger.info("Processing failed with status processed-error. Submission DB updated.")
                error_msg = "NCBI returned processed-error for submission: " \
                            + "Severity=" + update_dict['response_severity'] \
                            + " Message=" + update_dict['response_message']
                self.logger.info("Attempts made: " + str(attempts) + " Trying again.")
                time.sleep(60)
                if attempts >= 3:
                    self.logger.critical(msg=error_msg + " Aborting after " + str(attempts) + " attempts!")
                    dbs.update_sub_data(spuid_suffix, "submission_status", root.attrib['status'])
                    sys.exit(-1)
            elif root.attrib['status'] == 'processed-ok':
                update_dict = {'ncbi_submission_id': root.attrib['submission_id'],
                               'submission_status': root.attrib['status']}
                try:
                    update_dict['response_severity'] = root[0][0][0].attrib['severity']
                except KeyError as e:
                    self.logger.error("Unable to assign response severity. Assigning none value." + str(e))
                    update_dict['response_severity'] = "None"
                try:
                    update_dict['response_message'] = root[0][0][0].text
                except KeyError as e:
                    self.logger.error("Unable to assign response message. Assigning none value." + str(e))
                    update_dict['response_message'] = "None"
                try:
                    update_dict['accession'] = root[0][0][0].attrib['accession']
                except KeyError as e:
                    self.logger.error("Unable to assign accession number. Assigning none value." + str(e))
                    update_dict['accession_number'] = "None"
                for k, v in update_dict.items():
                    try:
                        dbs.update_sub_data(spuid=spuid_suffix,
                                            column_id=k, column_value=v)
                    except KeyError as e:
                        self.logger.error("Unable to update " + str(k) + ":" + str(v) + "." + str(e))
                self.logger.info("Processing completed with status processed-ok. Submission DB updated.")
                exit_flag = True
            elif root.attrib['status'] == 'failed':
                dbs.update_sub_data(spuid=spuid_suffix,
                                    column_id='submission_status', column_value=root.attrib['status'])
                dbs.update_sub_data(spuid=spuid_suffix,
                                    column_id='response_severity', column_value=root[0].attrib['severity'])
                dbs.update_sub_data(spuid=spuid_suffix,
                                    column_id='response_message', column_value=root[0].text)
                self.logger.critical("NCBI returned submission 'failed' status for SPUID " + str(spuid_suffix))
                self.logger.info("Attempts made: " + str(attempts) + " Trying again.")
                time.sleep(60)
                if attempts >= 3:
                    self.logger.critical(msg="Aborting after " + str(attempts) + " failed attempts!")
                    dbs.update_sub_data(spuid_suffix, "submission_status", root.attrib['status'])
                    sys.exit(-1)
            else:
                dbs.update_sub_data(spuid=spuid_suffix,
                                    column_id='submission_status',
                                    column_value=root.attrib['status'] + "-unrecognized")
                self.logger.critical(msg='Unrecognized submission status received from NCBI:' + root.attrib['status'] +
                                         " Aborting!")
                sys.exit(-1)

