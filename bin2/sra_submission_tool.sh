source /broad/software/scripts/useuse
use Python-2.7
#use .aspera_connect_linux_x86_64-3.1.1
use .aspera-3.7.4
use .lxml-3.5.0-python-2.7.1-sqlite3-rtrees
use .cx-oracle-5.0.2-python-2.7.1-sqlite3-rtrees-oracle-full-client-11.1
use .cx-oracle-5.0.2-python-2.7.1-sqlite3-rtrees-oracle-full-client-11.1
export PYTHONPATH=$PYTHONPATH:/cil/shed/apps/internal/sra_submission_tool:/cil/shed/apps/internal/vesper/libs:/cil/shed/apps/internal/vesper/:/home/unix/amr/.conda/envs/sra_tool/lib/python2.7/site-packages/
/broad/software/free/Linux/redhat_5_x86_64/pkgs/python_2.7.1-sqlite3-rtrees/bin/python /cil/shed/apps/internal/sra_submission_tool/bin/sra_submission_tool.py "$@"
