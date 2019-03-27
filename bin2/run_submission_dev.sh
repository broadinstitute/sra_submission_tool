source /broad/software/scripts/useuse
reuse -q Python-2.7
reuse -q Java-1.8
reuse -q .lxml-3.5.0-python-2.7.1-sqlite3-rtrees
reuse -q .aspera_connect_linux_x86_64-3.1.1
reuse -q .cx-oracle-5.0.2-python-2.7.1-sqlite3-rtrees-oracle-full-client-11.1
export PYTHONPATH=$PYTHONPATH:/home/unix/amr/dev/python/sra_submission_tool:/cil/shed/apps/internal/vesper/libs:/cil/shed/apps/internal/vesper/:/home/unix/amr/.conda/envs/sra_tool/lib/python2.7/site-packages/
/broad/software/free/Linux/redhat_5_x86_64/pkgs/python_2.7.1-sqlite3-rtrees/bin/python /home/unix/amr/dev/python/sra_submission_tool/run_submission.py "$@"
