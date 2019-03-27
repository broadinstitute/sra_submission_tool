import unittest
import time
import SRA_submission_tool.constants as c
import tempfile
from SRA_submission_tool.xml_service import XmlCreator
from SRA_submission_tool.process_handler import ProcessHandler
from tests import BAM
import logging
import csv
from tests import MANUAL_CSV, TEST_PATH
from SRA_submission_tool.oracle_connector import OracleConnector
import urllib2
__author__ = 'Amr Abouelleil'


class XmlCreatorTests(unittest.TestCase):
    def setUp(self):
        self.g_project = 'G87948'
        self.x = XmlCreator(self.g_project)
        logger = logging.getLogger('sra_tool')
        logger.setLevel(logging.DEBUG)
        fh = logging.FileHandler(c.log_file)
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('[%(asctime)s | %(name)s | %(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(ch)

    def test_create_xml_file(self):
        self.x.create_xml_file(deliverable_fields=['ncbi_biosample_id', 'ncbi_bioproject_id'],
                               release_date=time.strftime('%Y-%m-%d'), spuid='123XYZ', read_file=BAM,
                               construct_protocol="User-specified construction protocol",
                               temp_path=tempfile.mkdtemp(dir=c.temp_root), selection_method="RANDOM")

    def test_xml_creator_get_deliverables(self):
        xml_dict = self.x.get_deliverables(['ncbi_biosample_id', 'ncbi_bioproject_id'])
        print xml_dict
        self.assertEqual('SAMN03280170', xml_dict['ncbi_biosample_id'])
        self.assertEqual('PRJNA271899', xml_dict['ncbi_bioproject_id'])
        self.assertEqual('Ashlee', xml_dict['contact_first'])
        self.assertEqual('Earl', xml_dict['contact_last'])
        self.assertEqual('aearl@broadinstitute.org', xml_dict['contact_email'])

    def test_get_deliverables(self):
        deliverables_dict = self.x.get_deliverables(['ncbi_biosample_id', 'ncbi_bioproject_id'])
        self.assertEqual('SAMN03280170', deliverables_dict['ncbi_biosample_id'])
        self.assertEqual('PRJNA271899', deliverables_dict['ncbi_bioproject_id'])

    def test_get_contact_info(self):
        contact_dict = self.x.get_description_info()
        print contact_dict
        self.assertIsInstance(contact_dict, dict)

    def test_get_bam_library_info(self):
        self.x.get_bam_library_info(BAM)

    def test_mercury_does_not_return_none_for_instrumentModel(self):
        oc = OracleConnector()
        con = oc.connect(connection_string=c.seq20_string)
        run_name = oc.get_run_name(connection=con, run_barcode='C65NMACXX150311',
                                   lane=5, library='Pond-390016')
        mercury_path = c.mercury_url + run_name
        self.logger.debug("Mercury URL:" + mercury_path)
        response = urllib2.urlopen(mercury_path)
        mercury_data = json.load(response)
        library_dict['instrument_model'] = mercury_data['sequencerModel']
        library_dict['instrument_name'] = mercury_data['sequencer']
        library_dict['run_name'] = run_name

    def test_xml_creator_manual(self):
        ph = ProcessHandler()
        with open(MANUAL_CSV) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                self.x.create_xml_from_dict(ph.check_row(row), TEST_PATH)

    def tearDown(self):
        print "Testing complete..."


if __name__ == '__main__':
    unittest.main()
