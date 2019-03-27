import constants as c
import logging
import subprocess
import os
import shutil
from SRA_submission_tool.file_service import rehead_bam
__author__ = "Amr Abouelleil"


class ScreeningService(object):
    """
    A service for screening read files for contamination
    """
    def __init__(self):
        self.base_cmd = c.blender_path + " filter --remove_host --threads 4"
        self.logger = logging.getLogger('sra_tool.screening_service.ScreeningService')
        self.logger.info("Starting Screening Service.")

    def screen_human(self, read_file, library_layout, file_type, spuid, output_dir):
        output_header = ".".join(read_file.split("/")[-1].split(".")[0:-1])
        self.logger.info("Output header assigned:" + output_header)
        screen_dir = c.screen_temp_dir + spuid + "/"
        if not os.path.exists(screen_dir):
            os.mkdir(screen_dir, 0777)
        screen_cmd = self.base_cmd + " --reference " + c.human_reference + " --output_format " \
                     + file_type + " --output_directory " + screen_dir + " --output_header " + output_header
        if library_layout == "paired":
            screen_cmd += " --paired_bam_file " + read_file
        elif library_layout == "single":
            screen_cmd += " --unpaired_bam_file " + read_file
        try:
            self.logger.info("Screening command issued via subprocess:" + screen_cmd)
            subprocess.check_call(screen_cmd, stdout=subprocess.PIPE, shell=True)
        except subprocess.CalledProcessError as e:
            self.logger.error("Subprocess call failed:" + str(e))
            self.logger.info("Trying system call:" + screen_cmd)
            os.system(screen_cmd)
        finally:
            screened_bam = screen_dir + output_header + ".filter-bmtagger." + library_layout + ".sample.1.bam"
            reheaded_bam = rehead_bam(temp_dir=screen_dir, in_bam=read_file, bam_file=screened_bam,
                                      header=output_header)
            self.logger.info("Moving screened bam:" + reheaded_bam + " to " + output_dir)
            final_bam = output_dir + "/" + read_file.split("/")[-1]
            shutil.move(reheaded_bam, final_bam)
            self.logger.info("Deleting temp screening dir: " + screen_dir)
            shutil.rmtree(screen_dir)
            return final_bam
