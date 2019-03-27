__author__ = 'Amr Abouelleil'

picard_validate_path = "/cil/shed/apps/external/picard/current/bin/ValidateSamFile.jar"
picard_revert_sam = "/cil/shed/apps/external/picard/current/bin/RevertSam.jar"
asp_acct = "asp-bi@upload.ncbi.nlm.nih.gov"
key_file_path = "/cil/shed/resources/sra_submission_tool/keys/sra_tool.openssh"
samtools_path = "/broad/software/groups/gtba/software/samtools_0.1.18/bin/samtools"
dmslist_path = "/prodinfo/prodapps/dmsClient/dmsList"
mercury_url = "https://mercury.broadinstitute.org:8443/Mercury/rest/IlluminaRun/query?runName="
vocab_dict = {'source': ['GENOMIC', 'TRANSCRIPTOMIC', 'METAGENOMIC', 'METATRANSCRIPTOMIC', 'SYNTHETIC', 'VIRAL RNA',
                         'OTHER'],
              'selection': ['RANDOM', 'PCR', 'RANDOM PCR', 'RT-PCR', 'HMPR', 'MF', 'CF-S', 'CF-M', 'CF-H', 'CF-T',
                            'MDA', 'MSLL', 'cDNA', 'ChIP', 'MNase', 'DNAse', 'Hybrid Selection',
                            'Reduced Representation', 'Restriction Digest', '5-methylcytidine antibody',
                            'MBD2 protein methyl-CpG binding domain', 'CAGE', 'RACE', 'size fractionation', 'PolyA',
                            'Padlock probes capture method', 'other', 'unspecified'],
              'strategy': {'WholeGenomeShotgun': 'WGS', 'IlluminaStufferlessJump': 'WGS',
                           'cDNAShotgunReadTwoSense': 'RNA-Seq', 'cDNAShotgunStrandAgnostic': 'RNA-Seq',
                           'SixteenS': 'AMPLICON', '16S': 'AMPLICON', 'TraCS': 'Tn-Seq', 'WGSWithRef': 'WGS',
                           'PacBio': 'WGS', 'Bacterial 454 Assembly': 'WGS', 'HybridSelection': 'WGS',
                           'Bacterial 454 Alignment and Assembly': 'WGS', 'Unspecified': 'OTHER'}
}
schema_file = '/cil/shed/resources/sra_submission_tool/test_files/submission.xsd'
seq20_string = 'seq20/wmlalos!@seqprod.broadinstitute.org:1521/seqprod'
temp_root = '/cil/shed/resources/sra_submission_tool/temp/'
log_file = '/cil/shed/resources/sra_submission_tool/logs/sra_test.log'
run_log_file = '/cil/shed/resources/sra_submission_tool/logs/run_submission.log'
log_root = '/cil/shed/resources/sra_submission_tool/logs/'
prod_batch_fields = ['g_project', 'release_date', 'library_selection_method', 'library_protocol', 'read_file']
manual_sub_fields = ['contact_email', 'first_name', 'last_name', 'release_date', 'ncbi_bioproject_id',
                     'ncbi_biosample_id', 'library_name', 'library_strategy', 'library_source',
                     'library_selection_method', 'library_layout', 'library_protocol', 'platform', 'instrument_model',
                     'file_type', 'read_file']
run_states = ['requested', 'processing', 'submitted']
max_report_check_time = 259200
report_check_interval = 60
global_db_timeout = 2400
transfer_speed = 5349908  # estimated bytes per second
reset_fields = ['response_message', 'response_severity', 'xml_attributes', 'release_date', 'ncbi_submission_id']
people_csv = "/cil/shed/resources/sra_submission_tool/contacts/people.csv"

blender_path = "/cil/shed/apps/internal/blender/blender.sh"
human_reference = "/seq/references/Homo_sapiens_assembly38/v0/Homo_sapiens_assembly38.fasta"
screen_temp_dir = "/cil/shed/resources/sra_submission_tool/temp/screening/"
#For testing
# spuid_prefix = "BI_TEST_"
# submission_db = "/cil/shed/resources/sra_submission_tool/db/submission_test.db"
# sra_root_dir = 'Test'

#For live
spuid_prefix = "BI_"
submission_db = "/cil/shed/resources/sra_submission_tool/db/submission.db"
sra_root_dir = 'Production'