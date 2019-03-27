__author__ = 'Amr Abouelleil'

import unittest
from tests import SUB_XML_BAD, SUB_XML_EXAMPLE
from SRA_submission_tool.xml_service import XmlValidator
import SRA_submission_tool.constants as c


class XmlValidatorTests(unittest.TestCase):

    def Setup(self):
        print "Starting XML Validator testing..."

    def test_submission_example_evals_true(self):
        v = XmlValidator()
        self.assertEquals(True, v.validate_from_file(SUB_XML_EXAMPLE, c.schema_file))

    def tearDown(self):
        print "Testing complete!"


if __name__ == '__main__':
    unittest.main()
