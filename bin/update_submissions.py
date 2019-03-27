__author__ = 'Amr Abouelleil'

import argparse
import sys
import logging
import xml.etree.ElementTree as ET
from SRA_submission_tool.submission_db import SubmissionDBService
import SRA_submission_tool.constants as c
import sqlite3
from SRA_submission_tool.transfer_service import AsperaTransfer
import threading
import os

parser = argparse.ArgumentParser(prog="update_submission",
                                 description="A tool to update the ssub database when errors create bad data.")

input_group = parser.add_mutually_exclusive_group()
input_group.add_argument('-r', '--update_by_report', action='store', help='Full path to report.xml file.')
input_group.add_argument('-s', '--update_by_spuid', action='append', help='By spuid number(ommit BI prefix).')
parser.add_argument('-a', '--key_value_pair', action='append', help='A single key/value pair to update(k:v).')
parser.add_argument('-A', '--update_all', action='store_true', help='Crawl submission DB and update any unsuccessful submissions')
parser.add_argument('-b', '--update_batch', action='store', help='update a comma-sep list of spuids.')
parser.add_argument('-R', '--update_range', action='store', help='updated a range of spuids(sep with hyphen.)')
parser.add_argument('-D', '--database', action='store', help="Path to db file.", default=c.submission_db)


def update_by_report(report, database):
    tree = ET.parse(report)
    root = tree.getroot()
    if root.attrib['status'] == 'processed-ok':
        update_dict = {'submission_status': root.attrib['status'], 'accession': root[0][0][0].attrib['accession']}
    spuid_number = root[0][0][0].attrib['spuid'].split('_')[1]
    for k, v in update_dict.items():
        try:
            database.update_sub_data(spuid=spuid_number,
                                column_id=k, column_value=v)
        except KeyError as e:
            print "Unable to update " + str(k) + ":" + str(v) + "." + str(e)


def update_by_spuid(spuid_list, kv_pairs, database):
    for kv_pair in kv_pairs:
        k = kv_pair.split(':')[0]
        v = kv_pair.split(':')[1]
        for spuid_number in spuid_list:
            try:
                database.update_sub_data(spuid=spuid_number, column_id=k, column_value=v)
            except KeyError as e:
                print "Unable to update " + str(k) + ":" + str(v) + "." + str(e)


def update_all(db, kv_update=None):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute("SELECT spuid, temp_path, submission_status FROM submissions WHERE submission_status <> 'processed-ok'")
    table = cursor.fetchall()
    for row in table:
        if "abandoned" not in row[2]:
            t = threading.Thread(target=update_one(spuid_suffix=row[0], dest=row[1] + "/", db=db, kv_update=kv_update))
            t.start()
    conn.close()


def update_batch(db, list, kv_update=None):
    list_split = list.split(",")
    conn = sqlite3.connect(db)
    for spuid in list_split:
        cursor = conn.cursor()
        cursor.execute("SELECT spuid, temp_path, submission_status FROM submissions WHERE spuid=" + spuid)
        row = cursor.fetchall()
        t = threading.Thread(target=update_one(spuid_suffix=row[0][0], dest=row[0][1] + "/", db=db, kv_update=kv_update))
        t.start()
    conn.close()


def update_range(db, list, kv_update=None):
    list_split = list.split("-")
    conn = sqlite3.connect(db)
    for spuid in range(int(list_split[0]), int(list_split[-1]) + 1):
        cursor = conn.cursor()
        cursor.execute("SELECT spuid, temp_path, submission_status FROM submissions WHERE spuid=" + str(spuid))
        row = cursor.fetchall()
        t = threading.Thread(target=update_one(spuid_suffix=row[0][0], dest=row[0][1] + "/", db=db, kv_update=kv_update))
        t.start()
    conn.close()


def update_one(spuid_suffix, dest, db, kv_update=None):
    print("\n---Checking BI_" + str(spuid_suffix) + "---")
    dbs = SubmissionDBService(db)
    if kv_update:
        for kv_pair in kv_update:
            k = kv_pair.split(':')[0]
            v = kv_pair.split(':')[1]
            try:
                print ("Updating SPUID " + str(spuid_suffix))
                dbs.update_sub_data(spuid=spuid_suffix, column_id=k, column_value=v)
            except KeyError as e:
                print "Unable to update " + str(k) + ":" + str(v) + "." + str(e)
    else:
        if not os.path.exists(dest):
            os.mkdir(dest, 0777)
        report_xml = "report.xml"
        source = c.sra_root_dir + "/" + c.spuid_prefix + str(spuid_suffix) + "/" + report_xml
        at = AsperaTransfer()
        at.aspera_retrieve(c.asp_acct, source, dest, False)
        if os.path.exists(dest + report_xml):
            tree = ET.parse(dest + report_xml)
            root = tree.getroot()
            if root.attrib['status'] == 'processed-error':
                update_dict = {'ncbi_submission_id': root.attrib['submission_id'],
                                   'submission_status': root.attrib['status']}
                try:
                    update_dict['response_severity'] = root[0][0][0].attrib['severity']
                except KeyError as e:
                    print("Unable to assign response severity. Assigning none value." + str(e))
                    update_dict['response_severity'] = "None"
                try:
                    update_dict['response_message'] = root[0][0][0].text
                except KeyError as e:
                    print("Unable to assign response message. Assigning none value." + str(e))
                    update_dict['response_message'] = "None"
                for k, v in update_dict.items():
                    try:
                        dbs.update_sub_data(spuid=spuid_suffix,
                                            column_id=k, column_value=v)
                    except KeyError as e:
                        print("Unable to update " + str(k) + ":" + str(v) + "." + str(e))
                print("Processing failed with status processed-error. Submission DB updated.")
            elif root.attrib['status'] == 'processed-ok':
                update_dict = {'ncbi_submission_id': root.attrib['submission_id'],
                               'submission_status': root.attrib['status']}
                try:
                    update_dict['response_severity'] = root[0][0][0].attrib['severity']
                except KeyError as e:
                    print("Unable to assign response severity. Assigning none value." + str(e))
                    update_dict['response_severity'] = "None"
                try:
                    update_dict['response_message'] = root[0][0][0].text
                except KeyError as e:
                    print("Unable to assign response message. Assigning none value." + str(e))
                    update_dict['response_message'] = "None"
                try:
                    update_dict['accession'] = root[0][0][0].attrib['accession']
                except KeyError as e:
                    print("Unable to assign accession number. Assigning none value." + str(e))
                    update_dict['accession_number'] = "None"
                for k, v in update_dict.items():
                    try:
                        dbs.update_sub_data(spuid=spuid_suffix,
                                            column_id=k, column_value=v)
                    except KeyError as e:
                        print("Unable to update " + str(k) + ":" + str(v) + "." + str(e))
                print("Processing completed with status processed-ok. Submission DB updated.")
            elif root.attrib['status'] == 'failed':
                dbs.update_sub_data(spuid=spuid_suffix,
                                    column_id='submission_status', column_value=root.attrib['status'])
                dbs.update_sub_data(spuid=spuid_suffix,
                                    column_id='response_severity', column_value=root[0].attrib['severity'])
                dbs.update_sub_data(spuid=spuid_suffix,
                                    column_id='response_message', column_value=root[0].text)
                print("NCBI returned submission 'failed' status for SPUID " + str(spuid_suffix))

            elif root.attrib['status'] == 'processing' or root.attrib['status'] == 'submitted':
                dbs.update_sub_data(spuid=spuid_suffix,
                                    column_id='submission_status', column_value=root.attrib['status'])
                try:
                    dbs.update_sub_data(spuid=spuid_suffix,
                                        column_id='ncbi_submission_id',
                                        column_value=root.attrib['submission_id'])
                except KeyError as e:
                    print("No submission_id assigned yet." + str(e))
                    dbs.update_sub_data(spuid=spuid_suffix,
                                        column_id='ncbi_submission_id',
                                        column_value='')


def main():
    logging.basicConfig()
    args_dict = vars(parser.parse_args())
    dbs = SubmissionDBService(args_dict["database"])
    if args_dict['update_by_report']:
        update_by_report(args_dict['update_by_report'], dbs)
    elif args_dict['update_by_spuid']:
        update_by_spuid(args_dict['update_by_spuid'], args_dict['key_value_pair'], dbs)
    elif args_dict['update_all']:
        update_all(c.submission_db, args_dict['key_value_pair'])
    elif args_dict['update_batch']:
        update_batch(c.submission_db, args_dict['update_batch'], args_dict['key_value_pair'])
    elif args_dict['update_range']:
        update_range(c.submission_db, args_dict['update_range'], args_dict['key_value_pair'])

if __name__ == "__main__":
    sys.exit(main())
