import constants as c
import subprocess
import hashlib
import logging
import tarfile
import sys
from SRA_submission_tool.submission_db import SubmissionDBService
__author__ = "Amr Abouelleil"


class BamValidator(object):
    """
    A class for creating bam validator objects that run ValidateSamFile.
    """
    def __init__(self):
        self.logger = logging.getLogger('sra_tool.bam_service.BamValidator')
        self.dbs = SubmissionDBService(c.submission_db)

    def validate_bam(self, bam_file, spuid):
        """
        A method that runs ValidateSamFile to check bam files for errors.
        :param bam_file:
        :return: A dict with stdout and stderr of ValidateSamFile which can be analyzed to determine errors if any.
        """
        self.dbs.update_sub_data(spuid, column_id="submission_status", column_value="validating")
        cmd = "java -jar " + c.picard_validate_path + " I=" + bam_file + " IGNORE=INVALID_VERSION_NUMBER"
        self.logger.info("validate bam : " + cmd)
        try:
            proc = subprocess.Popen([cmd], stdout=subprocess.PIPE, shell=True)
            (out, err) = proc.communicate()
            if 'ERROR' in out.split('\n')[0]:
                self.logger.error("bam validation errors found: " + str(out))
                self.dbs.update_sub_data(spuid, column_id="submission_status",
                                         column_value="not validated")
                return False
            else:
                self.dbs.update_sub_data(spuid, column_id="submission_status", column_value="validated")
                self.logger.info("No bam validation errors found.")
                return True
        except:
            self.dbs.update_sub_data(spuid, column_id="submission_status", column_value="validation error")


class ChecksumCreator(object):
    """
    A class for checksum creator objects using the md5 algorithm.
    """
    def __init__(self, target_file):
        self.target_file = target_file

    def create_checksum(self):
        """
        A method for creating a checksum of any file.
        :param
        :return: checksum string for file
        """
        file_handle = open(self.target_file, 'rb')
        checksum = hashlib.md5(file_handle.read()).hexdigest()
        file_handle.close()
        return checksum

    def write_checksum(self, checksum):
        """
        A function for writing a checksum for a file to another file with appropriate naming.
        :param checksum: the md5 checksum as calculated by hashlib.
        :return: name of the output file
        """
        out_file_name = self.target_file + ".md5"
        file_handle = open(out_file_name, "wb")
        file_handle.write(checksum)
        file_handle.close()
        return out_file_name


class BamParser(object):
    """
    A class for parsing bam files using samtools. Currently just parses bam header into a dictionary.
    """

    def __init__(self):
        self.logger = logging.getLogger('sra_tool.file_service.BamParser')

    def list_to_dict(self, delim, in_list):
        """
        A simple list to dictionary converter.
        :param delim: The delimiter that seperates keys and values in each list element.
        :param in_list: The input list.
        :return:
        """
        new_dict = dict()
        for element in in_list:
            if ":" in element:
                new_dict[element.split(delim)[0]] = element.split(delim)[1]
        self.logger.debug("List converted to dict:" + str(new_dict))
        return new_dict

    def parse_header(self, bam_file):
        """
        A wrapper method that calls samtools to parse the header information.
        :param bam_file: The bam file to parse
        :return: A dictionary of bam header information.
        """
        cmd = c.samtools_path + " view -H " + bam_file
        self.logger.debug(msg="Header Parse cmd:" + cmd)
        proc = subprocess.Popen([cmd], stdout=subprocess.PIPE, shell=True)
        stdout, stderr = proc.communicate()
        read_group_string = stdout[stdout.find("@RG")+1:stdout.find("@PG")]
        seq_string = stdout[stdout.find("@SQ")+1:stdout.find("@RG")]
        header_dict = self.list_to_dict(":", read_group_string.split())
        seq_dict = self.list_to_dict(":", seq_string.split())
        header_dict.update(seq_dict)
        self.logger.debug(msg="Header dict:" + str(header_dict))
        return header_dict


class PacBioService(object):
    """
    A service for operations dealing with PacBio data
    """
    def __init__(self):
        self.logger = logging.getLogger('sra_tool.file_service.PacBioService')

    def get_pacbio_files_data(self, h5_file):
        """
        A method that returns a list of info regarding pacbio files
        :param h5_file: the pacbio bas.h5 read file
        :return: A dictionary containing the location of analysis files as well as the root name of the pacbio read file.
        """
        analysis_dir = "/".join(h5_file.split('/')[0:-1]) + "/"
        if "bas" in h5_file:
            root_name = h5_file.split('/')[-1].replace('.bas.h5', '')
            self.logger.info("Root name set to" + root_name)
        else:
            self.logger.critical("Unrecognized PacBio file type:" + h5_file)
            sys.exit(1)
        file_list = [h5_file, analysis_dir + root_name + ".1.bax.h5",
                     analysis_dir + root_name + ".2.bax.h5", analysis_dir + root_name + ".3.bax.h5"]

        file_data = {'analysis_dir': analysis_dir, 'root_name': root_name, 'file_list': file_list}
        self.logger.debug(msg="File Data retrieved:" + str(file_data))
        return file_data


def archive_maker(archive_dest, file_list):
    """
    A function for making several files into a tar.gz file.
    :param archive_dest: The destination in which the archive file should go.
    :param file_list: List of files to archive.
    :return:
    """
    logger = logging.getLogger('sra_tool.file_service.archive_maker')
    logger.debug("Archive Destination:" + archive_dest)
    logger.debug("Archive File List:" + str(file_list))
    archive = tarfile.open(archive_dest, "w:gz")
    for name in file_list:
        archive.add(name)
    archive.close()


def rehead_bam(in_bam, bam_file, temp_dir, header):
    logger = logging.getLogger('sra_tool.file_service.rehead_bam')
    sam_file = temp_dir + "/temp.sam"
    sam_cmd = c.samtools_path + " view -h -o " + sam_file + " " + in_bam
    logger.info("Running " + sam_cmd)
    subprocess.check_call(sam_cmd, stdout=subprocess.PIPE, shell=True)
    reheaded_bam_path = temp_dir + "/" + header + ".reheaded.screened.bam"
    rehead_cmd = c.samtools_path + " reheader " + sam_file + " " + bam_file + " > " + reheaded_bam_path
    logger.info("Running " + rehead_cmd)
    subprocess.check_call(rehead_cmd, stdout=subprocess.PIPE, shell=True)
    return reheaded_bam_path


def unalign_bam(bam_file, out_bam):
    logger = logging.getLogger('sra_tool.file_service.unalign_bam')
    cmd = "java -jar " + c.picard_revert_sam + " I=" + bam_file + " O=" + out_bam
    subprocess.check_call([cmd], stdout=subprocess.PIPE, shell=True)
    logger.info("Running " + cmd)
    return out_bam
