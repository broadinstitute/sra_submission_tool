#!/usr/bin/env python
__author__ = "Amr Abouelleil"

import sys
import argparse
import time
import logging
import SRA_submission_tool.constants as c
from SRA_submission_tool.zamboni_service import Zamboni
from SRA_submission_tool.process_handler import ProcessHandler
from SRA_submission_tool.file_service import BamParser
from SRA_submission_tool import LevelFilter
import csv
import getpass
import os
from SRA_submission_tool.submission_db import SubmissionDBService
import shutil
import traceback

parser = argparse.ArgumentParser(description="Description: A tool for single or batch SRA submissions via Aspera.",
                                 usage="SRA_submission_tool.py <command> [<args>]",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

sub = parser.add_subparsers()
prod_single_cmd = sub.add_parser("prod_single", description='Submit a production single file set to the SRA.',
                                 usage='SRA_submission_tool.py prod_single [<read_file>]',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
prod_single_cmd.add_argument('g_project', action='store', help='The G project number associated with the bam file.')
prod_single_cmd.add_argument('read_file', action='store', help='Path to the bam file to submit.')
prod_single_cmd.add_argument('-r', '--release_date',  default=time.strftime('%Y-%m-%d'),
                             help='The date the data should be released on.')
prod_single_cmd.add_argument('-m', '--library_selection_method', action='store', required=True,
                             help='Specify the library selection method for the bam file.'
                                  'Accepted terms are:' + str(c.vocab_dict['selection']))
prod_single_cmd.add_argument('-p', '--library_protocol', action='store',
                             required=True, help='Describe the library construction protocol. '
                                                 'Must be enclosed in quotes.')
prod_single_cmd.add_argument('-s', '--library_source', action='store', required=True,
                              help='Specify the library source material. '
                                   'Accepted terms are:' + str(c.vocab_dict['source']))
prod_single_cmd.add_argument('-a', '--additional_attributes', default=None, help='Pass any additional xml attributes.')
prod_single_cmd.add_argument('-n', '--notification_email', dest='notificationEmailAddresses', action='store',
                             default=getpass.getuser() + "@broadinstitute.org",
                             help='email address to send notifications to.')
prod_single_cmd.add_argument("-H", '--host_screen', action="store_true", default=False,
                             help='Screen out host contamination from read file prior to submission. Off by default.')
prod_single_cmd.add_argument("-F", '--force', action="store_true", default=False,
                             help='Resubmit previously submitted data even if it was successfully submitted before.')
prod_single_cmd.add_argument('-D', '--dry_run', action="store_true", help=argparse.SUPPRESS)
prod_single_cmd.add_argument('-T', '--trim', action="store", help="Path to sequence file for trimming.", default="trim.seq")


batch_cmd = sub.add_parser("prod_batch", description='Submit a batch of production files to the SRA.',
                           usage='SRA_submission_tool.py prod_batch [<batch_input_file>]',
                           formatter_class=argparse.ArgumentDefaultsHelpFormatter)
batch_cmd.add_argument('batch_input_file', help='Path to the batch input file. Format must be CSV with a header'
                                                ' row as follows:' + str(c.prod_batch_fields))
batch_cmd.add_argument('-n', '--notification_email', dest='notificationEmailAddresses', action='store',
                         default=getpass.getuser() + "@broadinstitute.org",
                       help='email address to send notifications to.')
batch_cmd.add_argument("-H", '--host_screen', action="store_true", default=False,
                       help='Screen out host contamination from read file prior to submission. Off by default.')
batch_cmd.add_argument("-F", '--force', action="store_true", default=False,
                             help='Resubmit running or previously '
                                  'submitted data even if it was successfully submitted before.')
batch_cmd.add_argument('-D', '--dry_run', action="store_true", help=argparse.SUPPRESS)
batch_cmd.add_argument('-T', '--trim', action="store", help="Path to sequence file for trimming.", default="trim.seq")

manual_cmd = sub.add_parser("manual", description='Submit one or more data files to the SRA while providing all XML '
                                                  'data via CSV file. Use for files with no metadata in databases.',
                            usage="SRA_submission_tool.py manual <batch_input_file>")
manual_cmd.add_argument('batch_input_file', help='Path to batch input file for manual submissions. Required headers are'
                                                 ':' + str(c.manual_sub_fields))
manual_cmd.add_argument("-H", '--host_screen', action="store_true", default=False,
                        help='Screen out host contamination from read file prior to submission. Off by default.')
manual_cmd.add_argument("-F", '--force', action="store_true", default=False,
                             help='Resubmit running or previously '
                                  'submitted data even if it was successfully submitted before.')
manual_cmd.add_argument('-n', '--notification_email', dest='notificationEmailAddresses', action='store',
                        default=getpass.getuser() + "@broadinstitute.org",
                        help='email address to send notifications to.')
manual_cmd.add_argument('-D', '--dry_run', action="store_true", help=argparse.SUPPRESS)
manual_cmd.add_argument('-T', '--trim', action="store", help="Path to sequence file for trimming.", default="trim.seq")


def main():
    root_logger = logging.getLogger('sra_tool')
    run_logger = logging.getLogger('run_submission')
    root_logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(c.run_log_file)
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.addFilter(LevelFilter(40))
    formatter = logging.Formatter('[%(asctime)s | %(name)s | %(levelname)s] %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    run_logger.propagate = False
    run_logger.addHandler(fh)
    run_logger.addHandler(ch)
    mode = sys.argv[1]
    run_logger.info("\n=========RUN SUBMISSION LOG RECORD BEGIN=========")
    run_logger.debug('Processing Mode: ' + mode)
    args_dict = vars(parser.parse_args())
    run_logger.debug('Run Submission Arguments: ' + str(args_dict))
    zamboni = Zamboni()
    ph = ProcessHandler()
    bp = BamParser()
    sdb = SubmissionDBService(c.submission_db, logger='run_submission')

    if mode == "prod_single":
        try:
            if not os.path.isfile(args_dict['read_file']):
                run_logger.critical(args_dict['read_file'] + " does not exist. Please supply an existing read file.")
                zamboni.disconnect()
                sys.exit(-1)
            # check to see if this is a resubmission, if it is, use the same temp dir as what's stored in the database.
            header_dict = bp.parse_header(bam_file=['read_file'])
            prev_sub_data_info = sdb.check_submission_exists(platform_unit=header_dict['PU'])
            if len(prev_sub_data_info) == 1:
                if prev_sub_data_info[0][1] and prev_sub_data_info[0][2]:
                    if prev_sub_data_info[0][2] in c.run_states and not args_dict['force']:
                        run_logger.critical(args_dict['read_file'] + " is currently being processed for submission. "
                                                                     "Please wait until current submission completes "
                                                                     "before resubmitting.")
                        zamboni.disconnect()
                        sys.exit(0)
                    elif prev_sub_data_info[0][2] == 'processed-ok' and not args_dict['force']:
                        run_logger.critical(args_dict['read_file'] + " previously submitted successfully. "
                                                                     "Aborting submission.")
                        zamboni.disconnect()
                        sys.exit(0)
                    elif (prev_sub_data_info[0][2] == 'processed-ok' or prev_sub_data_info[0][2] == 'requested') and args_dict['force']:
                        run_logger.info(args_dict['read_file']
                                         + " running or previously submitted successfully "
                                           "but resubmitting as directed by user.")
                        # resubmit the data with same info as previous submission
                        args_dict['spuid'] = prev_sub_data_info[0][0]
                        args_dict['temp_dir'] = prev_sub_data_info[0][1].rstrip("/")
                        sdb.reset_resub(args_dict['spuid'])
                        if os.path.isdir(args_dict['temp_dir']):
                            shutil.rmtree(args_dict['temp_dir'])
                        os.mkdir(args_dict['temp_dir'])
                    else:
                        # resubmit the data with same info as previous submission
                        args_dict['spuid'] = prev_sub_data_info[0][0]
                        args_dict['temp_dir'] = prev_sub_data_info[0][1].rstrip("/")
                        sdb.reset_resub(args_dict['spuid'])
                        if os.path.isdir(args_dict['temp_dir']):
                            shutil.rmtree(args_dict['temp_dir'])
                        os.mkdir(args_dict['temp_dir'])
                        run_logger.info(args_dict['read_file']\
                                        + " previously submitted unsuccessfully. Resubmitting with same SPUID.")
            else:
                run_logger.info(args_dict['read_file']
                                + " never submitted. Processing as new submission.")
                args_dict['spuid'] = sdb.get_new_spuid(args_dict['read_file'])
                args_dict['temp_dir'] = c.temp_root + c.spuid_prefix + str(args_dict['spuid'])
                os.mkdir(args_dict['temp_dir'], 0777)
            sdb.update_sub_data(args_dict['spuid'], 'temp_path', args_dict['temp_dir'])
            sdb.update_sub_data(args_dict['spuid'], 'platform_unit', header_dict['PU'])
            os.chmod(args_dict['temp_dir'], 0777)
            args_dict['temp_string'] = args_dict['temp_dir'].split("/")[-1]
            if not args_dict['additional_attributes']:
                args_dict['additional_attributes'] = "None"
            print ("Initiating single SRA submission of production data...\n")
            zamboni.start_workflow("ProdSraSubWorkflow", args_dict)
            sdb.update_sub_data(args_dict['spuid'], 'submission_status', 'requested')
        except:
            sdb.update_sub_data(args_dict['spuid'], 'submission_status', 'launch failed')
            traceback.print_exc()
            traceback.print_stack()
            sys.exit(1)

    elif mode == "prod_batch":
        print ("\nInitiating batch SRA submission of production data...\n")
        try:
            with open(args_dict['batch_input_file']) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    try:
                        if args_dict['dry_run']:
                            row['dry_run'] = True
                        if args_dict['host_screen']:
                            row['host_screen'] = True
                        if args_dict['trim']:
                            row['trim'] = args_dict['trim']
                        if row["read_file"] == '':
                            run_logger.info("No read file detected for entry" + row["g_project"] + ". Do you have blank rows?")
                            continue
                        ph.check_row(row)
                        if not os.path.isfile(row['read_file']):
                            run_logger.critical(row['read_file'] + " does not exist. Please supply an existing read file.")
                            zamboni.disconnect()
                            sys.exit(-1)
                        header_dict = bp.parse_header(row['read_file'])
                        prev_sub_data_info = sdb.check_submission_exists(platform_unit=header_dict['PU'])
                        if len(prev_sub_data_info) == 1:
                            if prev_sub_data_info[0][1] and prev_sub_data_info[0][2]:
                                if prev_sub_data_info[0][2] in c.run_states and not args_dict['force']:
                                    run_logger.critical(row['read_file'] + " is currently being processed for submission. "
                                                                           "Please wait until current submission completes "
                                                                           "before resubmitting.")
                                    continue
                                elif prev_sub_data_info[0][2] == 'processed-ok' and not args_dict['force']:
                                    run_logger.critical(row['read_file'] + " previously submitted successfully. "
                                                                           "Aborting submission.")
                                    continue
                                elif (prev_sub_data_info[0][2] == 'processed-ok' or prev_sub_data_info[0][2] == 'requested') \
                                        and args_dict['force']:
                                    run_logger.info(row['read_file']
                                                    + " running or previously submitted successfully "
                                                      "but resubmitting as directed by user.")
                                    # resubmit the data with same info as previous submission
                                    row['spuid'] = prev_sub_data_info[0][0]
                                    row['temp_dir'] = prev_sub_data_info[0][1].rstrip("/")
                                    sdb.reset_resub(row['spuid'])
                                    if os.path.isdir(row['temp_dir']):
                                        shutil.rmtree(row['temp_dir'])
                                    os.mkdir(row['temp_dir'])
                                else:
                                    # resubmit the data with same info as previous submission
                                    row['spuid'] = prev_sub_data_info[0][0]
                                    row['temp_dir'] = prev_sub_data_info[0][1].rstrip("/")
                                    sdb.reset_resub(row['spuid'])
                                    if os.path.isdir(row['temp_dir']):
                                        shutil.rmtree(row['temp_dir'])
                                    os.mkdir(row['temp_dir'])
                                    run_logger.info(row['read_file']
                                                    + " previously submitted unsuccessfully. Resubmitting with same SPUID.")
                        else:
                            run_logger.info(row['read_file']
                                            + " never submitted. Processing as new submission.")
                            row['spuid'] = sdb.get_new_spuid(row['read_file'])
                            row['temp_dir'] = c.temp_root + c.spuid_prefix + str(row['spuid'])
                            sdb.update_sub_data(row['spuid'], 'temp_path', row['temp_dir'])
                            sdb.update_sub_data(row['spuid'], 'platform_unit', header_dict['PU'])
                            os.mkdir(row['temp_dir'], 0777)
                        os.chmod(row['temp_dir'], 0777)
                        row['temp_string'] = row['temp_dir'].split("/")[-1]
                        row['notificationEmailAddresses'] = args_dict['notificationEmailAddresses']
                        zamboni.start_workflow("ProdSraSubWorkflow", row)
                        run_logger.debug("Row sent to Zamboni:" + str(row))
                        sdb.update_sub_data(row['spuid'], 'submission_status', 'requested')
                    except:
                        zamboni.disconnect()
                        sdb.update_sub_data(row['spuid'], 'submission_status', 'launch failed')
                        traceback.print_exc()
                        traceback.print_stack()
                        sys.exit(1)
                csvfile.close()
        except IOError:
            zamboni.disconnect()
            run_logger.error('ERROR:{} not found.'.format(args_dict['batch_input_file']))
            print('{} not found.'.format(args_dict['batch_input_file']))
            sys.exit(-1)
    elif mode == "manual":
        print ("Initiating SRA submission of data in CSV file...\n")
        try:
            with open(args_dict['batch_input_file']) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    run_logger.info('Processing row:\n{}'.format(row))
                    # try:
                    if args_dict['dry_run']:
                        run_logger.info("Dry run selected.")
                        row['dry_run'] = True
                    if args_dict['host_screen']:
                        run_logger.info("Host screen selected.")
                        row['host_screen'] = True
                    if args_dict['trim']:
                        run_logger.info("Trimming selected")
                        row['trim'] = args_dict['trim']
                    if row["read_file"] == '':
                        run_logger.info("No read file detected for entry. Do you have blank rows? Skipping...")
                        continue
                    ph.check_row(row)
                    try:
                        pass #if not os.path.isfile(row['reference_file']) and '.' in row['reference_file']:
                        #    run_logger.critical(
                        #        row['reference_file'] + " does not exist. Please supply an existing reference file.")
                        #    zamboni.disconnect()
                        #    sys.exit(-1)
                    except KeyError:
                        run_logger.warning('Reference file not specified.')
                    if not os.path.isfile(row['read_file']):
                        run_logger.critical(row['read_file'] + " does not exist. Please supply an existing read file.")
                        zamboni.disconnect()
                        sys.exit(-1)
                    if 'bam' in row['file_type']:
                        run_logger.info('{} is a bam file.'.format(row['file_type']))
                        header_dict = bp.parse_header(row['read_file'])
                        if 'PU' in header_dict:
                            run_logger.debug('Platform unit:{}'.format(header_dict['PU']))
                            prev_sub_data_info = sdb.check_submission_exists(platform_unit=header_dict['PU'])
                        else:
                            run_logger.debug('No platform unit detected.')
                            prev_sub_data_info = sdb.check_submission_exists(platform_unit=row['read_file'])
                    else:
                        run_logger.info('{} is not a bam file.'.format(row['read_file']))
                        prev_sub_data_info = sdb.check_submission_exists(platform_unit=row['read_file'])
                    run_logger.debug('Previous submission data: {}'.format(prev_sub_data_info))
                    if len(prev_sub_data_info) == 1:
                        if prev_sub_data_info[0][1] and prev_sub_data_info[0][2]:
                            if prev_sub_data_info[0][2] in c.run_states and not args_dict['force']:
                                run_logger.critical(row['read_file'] + " is currently being processed for submission."
                                                                       "Please wait until current submission completes"
                                                                       "before resubmitting.")
                                continue
                            elif prev_sub_data_info[0][2] == 'processed-ok' and not args_dict['force']:
                                run_logger.critical(row['read_file'] + " previously submitted successfully."
                                                                       "Aborting submission.")
                                continue
                            elif (prev_sub_data_info[0][2] == 'processed-ok' or prev_sub_data_info[0][2] == 'requested')\
                                    and args_dict['force']:
                                run_logger.info(row['read_file']
                                                + " running or previously submitted successfully "
                                                  "but resubmitting as directed by user.")
                                # resubmit the data with same info as previous submission
                                row['spuid'] = prev_sub_data_info[0][0]
                                row['temp_dir'] = prev_sub_data_info[0][1]
                                sdb.reset_resub(row['spuid'])
                                if os.path.isdir(row['temp_dir']):
                                    shutil.rmtree(row['temp_dir'])
                                os.mkdir(row['temp_dir'])
                            else:
                                # resubmit the data with same info as previous submission
                                row['spuid'] = prev_sub_data_info[0][0]
                                row['temp_dir'] = prev_sub_data_info[0][1]
                                sdb.reset_resub(row['spuid'])
                                if os.path.isdir(row['temp_dir']):
                                    shutil.rmtree(row['temp_dir'])
                                os.mkdir(row['temp_dir'])
                                run_logger.info(row['read_file']
                                                + " previously submitted unsuccessfully. Resubmitting with same SPUID.")
                        else:
                            run_logger.info(row['read_file']
                                            + " submitted but missing data. Processing as new submission.")
                            row['spuid'] = sdb.get_new_spuid(row['read_file'])
                            row['temp_dir'] = c.temp_root + c.spuid_prefix + str(row['spuid'])
                            sdb.update_sub_data(row['spuid'], 'temp_path', row['temp_dir'])
                            os.mkdir(row['temp_dir'], 0777)
                    else:
                        run_logger.info(row['read_file']
                                        + " never submitted. Processing as new submission.")
                        row['spuid'] = sdb.get_new_spuid(row['read_file'])
                        row['temp_dir'] = c.temp_root + c.spuid_prefix + str(row['spuid'])
                        sdb.update_sub_data(row['spuid'], 'temp_path', row['temp_dir'])
                        if 'bam' in row['file_type'] and 'PU' in header_dict:
                            sdb.update_sub_data(row['spuid'], 'platform_unit', header_dict['PU'])
                        else:
                            sdb.update_sub_data(row['spuid'], 'platform_unit', row['read_file'])
                        os.mkdir(row['temp_dir'], 0777)
                    os.chmod(row['temp_dir'], 0777)
                    row['temp_string'] = row['temp_dir'].split("/")[-1]
                    row['notificationEmailAddresses'] = args_dict['notificationEmailAddresses']
                    zamboni.start_workflow("ManualSraSubWorkflow", row)
		    print("ROW SENT TO ZAMBONI:" + str(row))
                    run_logger.debug("Row sent to Zamboni:" + str(row))
                    sdb.update_sub_data(row['spuid'], 'submission_status', 'requested')
                    # except KeyError as e:
                    #     zamboni.disconnect()
                    #     sdb.update_sub_data(row['spuid'], 'submission_status', 'KeyError')
                    #     sdb.update_sub_data(row['spuid'], 'response_message', str(e))
                    #     traceback.print_exc()
                    #     traceback.print_stack()
                    #     sys.exit(1)
        except IOError:
            zamboni.disconnect()
            import traceback
            traceback.print_exc()
            run_logger.critical('ERROR:{} not found.'.format(args_dict['batch_input_file']))
            print('ERROR:{} not found.'.format(args_dict['batch_input_file']))
            sys.exit(-1)
    else:
        run_logger.critical("{} is not a recognized submission mode.".format(mode))
    run_logger.info("\n=========RUN SUBMISSION LOG RECORD END=========\n")
    logging.shutdown()
    zamboni.disconnect()

if __name__ == "__main__":
    sys.exit(main())
