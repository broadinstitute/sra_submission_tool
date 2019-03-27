#!/usr/bin/env python
__author__ = "Amr Abouelleil"

import sys
import argparse
import time
import logging
import SRA_submission_tool.constants as c
from SRA_submission_tool.process_handler import ProcessHandler
import os

parser = argparse.ArgumentParser(description="Description: A tool for single or batch SRA submissions via Aspera.",
                                 usage="SRA_submission_tool.py <command> [<args>]")

sub = parser.add_subparsers()
prod_single_cmd = sub.add_parser("prod_single", description='Submit a production single file set to the SRA.',
                                 usage='SRA_submission_tool.py prod_single [<bam_file>]',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
prod_single_cmd.add_argument('g_project', action='store', help='The G project number associated with the bam file.')
prod_single_cmd.add_argument('read_file', action='store', help='Path to the bam file to submit.')
prod_single_cmd.add_argument('-r', '--release_date', default=time.strftime('%Y-%m-%d'),
                             help='The date the data should be released on.')
prod_single_cmd.add_argument('-m', '--library_selection_method', action='store', required=True,
                             help='Specify the library selection method for the bam file.'
                             'Accepted terms are:' + str(c.vocab_dict['selection']))
prod_single_cmd.add_argument('-p', '--library_protocol', action='store', required=True,
                             help='Describe the library construction protocol')
prod_single_cmd.add_argument('-s', '--library_source', action='store', required=True,
                             help='Specify the library source material. '
                                  'Accepted terms are:' + str(c.vocab_dict['source']))
prod_single_cmd.add_argument('-a', '--additional_attributes', default=None,
                             help='Pass any additional xml attributes.')
prod_single_cmd.add_argument('-t', '--temp_dir', action='store', required=True)
prod_single_cmd.add_argument('-d', '--spuid', action='store', required=True)
prod_single_cmd.add_argument("-H", '--host_screen', action="store_true", default=False,
                             help='Screen out host contamination from read file prior to submission. Off by default.')
prod_single_cmd.add_argument('-D', '--dry_run', action="store_true", help=argparse.SUPPRESS)
prod_single_cmd.add_argument('-T', '--trim', action="store", help="Path to sequence file for trimming.", default="trim.seq")


manual_cmd = sub.add_parser("manual", description='Submit one or more data files to the SRA while providing all XML '
                                                  'data. Use for files with no metadata in databases.',
                            usage="SRA_submission_tool.py manual <batch_input_file>")
manual_cmd.add_argument('read_file', action='store', help='Path to the bam file to submit.')
manual_cmd.add_argument('-f', '--file_type', action='store', help='The type of file being submitted.', required=True)
manual_cmd.add_argument('-e', '--contact_email', action='store', help='The sub contact\'s email address', required=True)
manual_cmd.add_argument('-F', '--first_name', action='store', required=True)
manual_cmd.add_argument('-L', '--last_name', action='store', required=True)
manual_cmd.add_argument('-b', '--ncbi_bioproject_id', action='store', required=True)
manual_cmd.add_argument('-B', '--ncbi_biosample_id', action='store', required=True)
manual_cmd.add_argument('-l', '--library_name', action='store', required=True)
manual_cmd.add_argument('-o', '--library_layout', action='store', required=True)
manual_cmd.add_argument('-P', '--platform', action='store', required=True)
manual_cmd.add_argument('-i', '--instrument_model', action='store', required=True)
manual_cmd.add_argument('-a', '--additional_attributes', action='store')
manual_cmd.add_argument('-S', '--library_strategy', action='store', required=True)
manual_cmd.add_argument('-s', '--library_source', action='store', required=True)
manual_cmd.add_argument('-r', '--release_date', default=time.strftime('%Y-%m-%d'),
                        help='The date the data should be released on.')
manual_cmd.add_argument('-m', '--library_selection_method', action='store', required=True,
                        help='Specify the library selection method for the bam file.'
                                  'Accepted terms are:' + str(c.vocab_dict['selection']))
manual_cmd.add_argument('-p', '--library_protocol', action='store', required=True,
                        help='Describe the library construction protocol')
manual_cmd.add_argument('-t', '--temp_dir', action='store', required=True)
manual_cmd.add_argument('-d', '--spuid', action='store', required=True)
manual_cmd.add_argument('-R', '--reference_file', action='store')
manual_cmd.add_argument("-H", '--host_screen', action="store_true", default=False,
                        help='Screen out host contamination from read file prior to submission. Off by default.')
manual_cmd.add_argument('-D', '--dry_run', action="store_true", help=argparse.SUPPRESS)
manual_cmd.add_argument('-T', '--trim', action="store", help="Path to sequence file for trimming.", default="trim.seq")


def main():
    args_dict = vars(parser.parse_args())
    mode = sys.argv[1]
    logger = logging.getLogger('sra_tool')
    logger.setLevel(logging.DEBUG)
    if not os.path.exists(args_dict['temp_dir']):
        os.makedirs(args_dict['temp_dir'])
    log_dir = c.log_root + args_dict['temp_dir'].split('/')[-1]
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = log_dir + "/sra.log"
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(asctime)s | %(name)s | %(levelname)s] %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.info("\n=========SRA SUBMISSION TOOL LOG RECORD BEGIN=========")
    logger.debug('Processing Mode: ' + mode)
    logger.debug('SRA Submission Tool arguments: ' + str(args_dict))
    logger.debug('Log directory set to ' + log_file)
    processor = ProcessHandler()
    if mode == "prod_single":
        logger.debug("Read File:" + args_dict['read_file'])
        processor.process_prod_single(args_dict)
    elif mode == "manual":
        logger.debug("Manual Read File:" + args_dict['read_file'])
        processor.process_manual(args_dict)
    logger.info("\n=========SRA SUBMISSION TOOL LOG RECORD END=========\n")
    logging.shutdown()

if __name__ == "__main__":
    sys.exit(main())
