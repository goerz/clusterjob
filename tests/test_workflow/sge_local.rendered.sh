#!/bin/bash
#$ -N test_clj
#$ -l h_rt=00:05:00
#$ -j y
#$ -S /bin/bash
#$ -o clusterjob_test.out
#$ -cwd

echo "####################################################"
echo "Job id         : $JOB_ID"
echo "Job name       : $JOB_NAME"
echo "Workdir        : $SGE_O_WORKDIR"
echo "Submission Host: $SGE_O_HOST"
echo "Compute Node   : $HOSTNAME"
echo "Job started on" `hostname` `date`
echo "Current directory:" `pwd`
echo "####################################################"

cd $SGE_O_WORKDIR

sleep 60

echo "Job Finished: " `date`
exit 0
