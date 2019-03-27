__author__ = "Amr Abouelleil"
import os
import urllib2
import json
from string import Template
from SRA_submission_tool.file_service import BamParser
from SRA_submission_tool.oracle_connector import OracleConnector
from SRA_submission_tool import constants as c
from lxml import etree
from SRA_submission_tool.manhattan_service import RetrievalService
import sys
import logging
import csv
from submission_db import SubmissionDBService
from SRA_submission_tool.file_service import PacBioService


class XmlCreator(object):
    """
    A class for creating a submissions XML file for a given bam file.
    """

    def __init__(self, g_number=None):
        if g_number:
            self.rs = RetrievalService(g_number)
        self.logger = logging.getLogger('sra_tool.xml_service.XmlCreator')

    def create_xml_file(self, read_file, deliverable_fields, release_date, spuid, selection_method, construct_protocol,
                        library_source, temp_path, additional_attributes=None):
        """
        A sub_processor function that calls various subordinate functions to create the xml file.
        :param bam_file: A string that is the bam file path containing the read data.
        :param deliverable_fields: a list object containing the manhattan deliverable fields to be queried for the xml.
        :param release_date: a date string indicating the date when NCBI should release the data.
        :param spuid: A unique identifier string that gives NCBI a way to refer to the submission.
        :param selection_method: a controlled string (vocabulary.txt doc) that indicates how data was selected.
        :param construct_protocol: an uncontrolled string describing how the library was constructed.
        :param temp_path: path to temporary directory
        :param additional_attributes: string of additional attributes to go into XML
        :return: the path of the xml file created by the function
        """
        xml_template = os.path.dirname(__file__) + "/template.xml"
        xml_handle = open(xml_template, 'r')
        src = Template(xml_handle.read())
        if additional_attributes:
            self.logger.info("Additional attributes detected.")
            xml_dict, ref = self.create_xml_dict(deliverable_fields, read_file, construct_protocol, selection_method,
                                            library_source, spuid.split("_")[1], additional_attributes)
        else:
            self.logger.info("No additional attributes detected.")
            xml_dict, ref = self.create_xml_dict(deliverable_fields, read_file, construct_protocol, selection_method,
                                            library_source, spuid.split("_")[1])
        xml_dict['release_date'] = release_date
        xml_dict['spuid'] = spuid
        if selection_method in c.vocab_dict['selection']:
            xml_dict['library_selection_method'] = selection_method
        else:
            self.logger.warning('WARNING:', selection_method, "is not an NCBI-approved selection method word.")
            self.logger.warning('Acceptable words:\n' + str(c.vocab_dict['selection']))
            xml_dict['library_selection_method'] = selection_method
        xml_dict['library_protocol'] = construct_protocol
        file_list = [read_file, ref]
        xml_dict['file_block'] = self.generate_file_block(file_list)
        os.symlink(ref, temp_path + "/" + ref.split('/')[-1])
        src.safe_substitute(xml_dict)
        self.write_xml_file(xml_dict, temp_path)
        return xml_dict

    def write_xml_file(self, xml_dict, temp_path):
        xml_template = os.path.dirname(__file__) + "/template.xml"
        xml_handle = open(xml_template, 'r')
        src = Template(xml_handle.read())
        text = src.safe_substitute(xml_dict)
        xml_file = temp_path + '/submission.xml'
        xml_handle = open(xml_file, 'w')
        xml_handle.write(text)
        self.logger.info("XML written to " + xml_file)
        xml_handle.close()
        v = XmlValidator()
        v.validate_from_file(xml_file, c.schema_file)
        return xml_file

    def create_xml_dict(self, deliverable_fields, read_file, construction_protocol, selection_method, library_source,
                        spuid_suffix, additional_attributes=None):
        """
        Function for creating a dictionary of fields to fill out in the xml template.
        :param deliverable_fields: list of deliverable fields to pull from manhattan.
        :param read_file: path to the bam file.
        :param construction_protocol:
        :param selection_method:
        :param additional_attributes:
        :return: a dictionary of xml fields for the templating process.
        """
        xml_fields = dict()
        xml_fields.update(self.get_deliverables(deliverable_fields))
        xml_fields.update(self.get_description_info())
        attributes, reference = self.get_bam_library_info(read_file, spuid_suffix)
        attributes['library_construction_protocol'] = construction_protocol
        attributes['library_selection'] = selection_method
        attributes['library_source'] = library_source
        if additional_attributes and ":" in additional_attributes:
            att_list = additional_attributes.split('|')
            for att in att_list:
                attributes[att.split(':')[0]] = att.split(':')[1]
        attributes_dict = {'attributes_block': self.generate_attributes_block(attributes)}
        xml_fields.update(self.get_file_information(read_file))
        xml_fields.update(attributes_dict)
        self.logger.debug("XML dictionary created with following values:" + str(xml_fields))
        return xml_fields, reference

    def generate_file_record(self, name, f_type):
        file_dict = {'file_name': name, 'file_data_type': f_type}
        self.logger.debug('File record generated' + str(file_dict))
        return file_dict

    def generate_file_block(self, file_list):
        self.logger.debug("File block generator received file list:" + str(file_list))
        block = ""
        template_string = "      <File file_path=\"$file_name\">\n        <DataType>$file_data_type</DataType>\n      </File>"
        src = Template(template_string)
        count = 0
        for f in file_list:
            file_dict = self.generate_file_record(f.split("/")[-1], "generic-data")
            if self.generate_file_record(f.split("/")[-1], "generic-data")['file_name'] == '.':
                continue #skip the file block if file name is empty
            file_string = src.safe_substitute(file_dict)
            count += 1
            if count == len(file_list):
                block += file_string
            else:
                block += file_string + "\n"
        self.logger.debug("File block generated with following values" + str(file_list))
        return block

    def generate_attribute(self, key, value):
        attribute_dict = {'attribute_name': key, 'attribute_value': value}
        self.logger.debug('Attribute generated:' + str(attribute_dict))
        return attribute_dict

    def generate_attributes_block(self, att_dict):
        block = ""
        template_string = "      <Attribute name=\"$attribute_name\">$attribute_value</Attribute>"
        src = Template(template_string)
        count = 0
        for key, value in att_dict.items():
            attribute_string = src.safe_substitute(self.generate_attribute(key, value))
            count += 1
            if count == len(att_dict):
                block += attribute_string
            else:
                block += attribute_string + "\n"
        self.logger.debug("Attributes block generated with following values" + str(att_dict))
        return block

    def get_file_information(self, data_file):
        """
        Simple function to get the file name into a dict that can be used to update the xml_dict in create_xml_dict
        function.
        :param data_file: the data file (bam, pacbio, etc)
        :return: a dict containing the file name and file type, which NCBI instructs to just leave as 'generic-data'.
        """
        file_dict = {'file_name': data_file.split('/')[-1], 'file_data_type': 'generic-data'}
        self.logger.debug("File information:" + str(file_dict))
        return file_dict

    def get_deliverables(self, deliverable_fields):
        """
        A function that calls the retrieval service to retrieve the values of the deliverable fields from Manhattan.
        :param deliverable_fields:
        :return:
        """

        deliverables = self.rs.retrieve_deliverables(deliverable_fields)
        self.logger.debug("Deliverables fetched:" + str(deliverables))
        return deliverables

    def get_bam_library_info(self, bam_file, spuid_suffix):
        """
        A function to get the library information required by the submission template from various sources.
        :param bam_file: The path to the bam/data file.
        :return:
        """
        # retrieval of data from bam header
        bp = BamParser()
        header_dict = bp.parse_header(bam_file)
        self.logger.debug("Bam header parsed:" + str(header_dict))
        library_dict = {'library_name': header_dict['LB'], 'run_barcode': header_dict['PU'].split('.')[0],
                        'lane': header_dict['PU'].split('.')[1], 'platform': header_dict['PL'], 'library_source': '',
                        'library_strategy': '', 'library_layout': '', 'reference_file': header_dict['UR'],
                        'instrument_model': None, 'run_date': header_dict['DT'],
                        'read_group_platform_unit': header_dict['PU']}
        '''
         read_group_bam will have to be replaced with something that interprets file extensions once we start supporting
         files other than BAM.
        '''
        # retrieval of data from mercury
        oc = OracleConnector()
        con = oc.connect(connection_string=c.seq20_string)
        self.logger.info("Oracle Connection Established")
        run_name = oc.get_run_name(connection=con, run_barcode=library_dict['run_barcode'],
                                   lane=library_dict['lane'], library=library_dict['library_name'])
        self.logger.debug("Run_name retrieved:" + run_name)
        con.close()
        mercury_path = c.mercury_url + run_name
        self.logger.debug("Mercury URL:" + mercury_path)
        while not library_dict['instrument_model']:
            response = urllib2.urlopen(mercury_path)
            mercury_data = json.load(response)
            library_dict['instrument_model'] = mercury_data['sequencerModel']
            library_dict['instrument_name']  = mercury_data['sequencer']
            library_dict['flowcell_barcode'] = mercury_data['flowcellBarcode']
        library_dict['run_name'] = run_name
        for lane in mercury_data['lanes']:
            if lane['name'] == library_dict['lane']:
                for info in lane['libraries']:
                    if info['library'] == library_dict['library_name']:
                        library_dict['library_strategy'] = self.convert_vocabulary('strategy',
                                                                                   info['analysisType'].split('.')[0],
                                                                                   spuid_suffix)
                        library_dict['root_sample'] = info['rootSample']
                        library_dict['sample_id'] = info['sampleId']
                        library_dict['work_request_id'] = info['workRequestId']
                        library_dict['species'] = info['species']
                        library_dict['lsid'] = info['lsid']
                        library_dict['project_id'] = info['project']
                        library_dict['product_part_number'] = info['productPartNumber']
                        library_dict['analysis_type'] = info ['analysisType']
                        library_dict['gssr_id'] = info['gssrBarcodes'][0]
                        library_dict['research_project'] = info['researchProjectId']
                        library_dict['product_order'] = info['productOrderKey']
                        library_dict['initiative'] = info['initiative']
                        library_dict['research_project_name'] = info['researchProjectName']
                        library_dict['reference_sequence'] = info['referenceSequence']
                        if info['molecularIndexingScheme']['sequences'][0]['hint'] == 'P7':
                            library_dict['library_layout'] = 'paired'
                        else:
                            library_dict['library_layout'] = 'single'
        self.logger.debug("Final library dictionary:" + str(library_dict))
        return library_dict, library_dict['reference_file']

    def convert_vocabulary(self, ncbi_type, word, spuid_suffix):
        """
        A function for converting Broad vocabulary to NCBI vocabulary.
        :param ncbi_type: A string that refers to the type of word (see vocabulary.txt file), for example, 'strategy'
        or 'layout' or 'source'.
        :param word: The Broad institute word to look up.
        :return: Returns a string that is the NCBI equivalent to the Broad word.
        """
        vocab_dict = c.vocab_dict
        dbs = SubmissionDBService(c.submission_db)
        try:
            new_word = vocab_dict[ncbi_type][word]
            self.logger.debug("Translating " + word + " to " + new_word)
            return new_word
        except KeyError as e:
            self.logger.critical(word + " is not a valid " + ncbi_type + " word.")
            dbs.update_sub_data(spuid=spuid_suffix,
                                column_id='submission_status',
                                column_value="failed-vocab")
            dbs.update_sub_data(spuid=spuid_suffix,
                                column_id='response-message',
                                column_value= word + " is not a valid " + ncbi_type + " word.")
            dbs.update_sub_data(spuid=spuid_suffix,
                                column_id='response-severity',
                                column_value='critical')
            raise e

    def get_description_info(self):
        """
        Gets the submission xml description block values, such as contact information.
        :return: A dictionary containing the description info.
        """
        initiative_dict = self.rs.retrieve_initiative_data()
        contact_username = initiative_dict['broad_scientist_ids'][0]
        description_dict = dict()
        with open(c.people_csv, 'rb') as csvfile:
            people_reader = csv.DictReader(csvfile)
            for row in people_reader:
                if row["username"] == contact_username:
                    description_dict = {'first_name': row['first_name'],
                                        'last_name': row['last_name'],
                                        'contact_email': row['email']
                                        }
        self.logger.debug("Description info retrieved:" + str(description_dict))
        return description_dict

    def create_xml_from_dict(self, xml_dict, temp_path, file_type):
        """
        A method for creating the xml metadata file for the SRA submission from a dictionary.
        :param xml_dict: The dictionary containing the xml metadata.
        :param temp_path: The full path where the metadata file should be written.
        :return
        """
        xml_dict['spuid'] = c.spuid_prefix + xml_dict['spuid']
        attributes = {'library_construction_protocol': xml_dict['library_protocol'],
                      'library_selection': xml_dict['library_selection_method'],
                      'library_strategy': xml_dict['library_strategy'], 'library_layout': xml_dict['library_layout'],
                      'instrument_model': xml_dict['instrument_model'], 'library_name': xml_dict['library_name'],
                      'platform': xml_dict['platform'], 'library_source': xml_dict['library_source']
                      }
        if ':' in xml_dict['additional_attributes']:
            self.logger.info("Additional attributes found.")
            att_list = xml_dict['additional_attributes'].split('|')
            for att in att_list:
                attributes[att.split(':')[0]] = att.split(':')[1]
        self.logger.debug("Filetype identified:" + file_type)
        if 'PacBio_HDF5' in file_type:
            pbs = PacBioService()
            pb_files = pbs.get_pacbio_files_data(xml_dict['read_file'])['file_list']
            xml_dict['file_list'] = []
            for f in pb_files:
                self.logger.debug("Adding to file list:" + f.split('/')[-1])
                xml_dict['file_list'].append(f.split('/')[-1])
        else:
            xml_dict['file_list'] = [xml_dict['read_file']]
            xml_dict.update(self.get_file_information(xml_dict['read_file']))
        if xml_dict['reference_file']:
            self.logger.info('reference file found:' + xml_dict['reference_file'])
            if not os.path.exists(xml_dict['reference_file']):	    
                os.symlink(xml_dict['reference_file'], temp_path + "/" + xml_dict['reference_file'].split('/')[-1])
            xml_dict['file_list'].append(xml_dict['reference_file'])
        else:
            self.logger.info('no reference file found.')
        self.logger.debug('file_list:' + ", ".join(xml_dict['file_list']))

        xml_dict['file_block'] = self.generate_file_block(xml_dict['file_list'])
        xml_dict['attributes_block'] = self.generate_attributes_block(attributes)
        xml_file = self.write_xml_file(xml_dict, temp_path)
        return xml_file


class XmlValidator(object):
    """
    A class for creating objects that validate XML against a schema file.
    """
    def __init__(self):
        self.logger = logging.getLogger('sra_tool.xml_service.XmlValidator')

    def validate_from_file(self, xml_file, schema):
        """
        A function that validates an xml file against the given schema.
        :param xml_file:
        :param schema:
        :return: A boolean value, True if the schema is valid, false if not.
        Also prints to STDOUT a confirmation string.
        """
        xmlschema = etree.XMLSchema(file=schema)
        xml = etree.parse(xml_file)
        if xmlschema.validate(xml):
            self.logger.debug(xml_file + " is valid against " + c.schema_file)
            return xmlschema.validate(xml)
        else:
            log = xmlschema.error_log
            self.logger.error(xml_file + "is not valid against" + c.schema_file)
            for error in iter(log):
                self.logger.error("Reason:" + error.message)
                sys.exit(-1)

