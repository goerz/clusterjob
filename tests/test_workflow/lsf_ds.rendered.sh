#!/bin/bash
#BSUB -n 1
#BSUB -J test_clj
#BSUB -q deflt_centos
#BSUB -M 20
#BSUB -W 5
#BSUB -o clusterjob_test.out

echo "####################################################"
echo "Job id         : $LSB_JOBID"
echo "Job name       : $LSB_JOBNAME"
echo "Workdir        : $LS_SUBCWD"
echo "Submission Host: `hostname`"
echo "Compute Node   : $LSB_HOSTS"
echo "Job started on" `hostname` `date`
echo "Current directory:" `pwd`
echo "####################################################"

sleep 180

echo "Job Finished: " `date`
exit 0
