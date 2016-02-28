#!/bin/bash
#PBS -N test_clj
#PBS -q debug
#PBS -l walltime=00:05:00
#PBS -l select=1:ncpus=32:mpiprocs=32
#PBS -A XXXXXXXXXXXXX
#PBS -j oe
#PBS -o clusterjob_test.out

echo "####################################################"
echo "Job id         : $PBS_JOBID"
echo "Job name       : $PBS_JOBNAME"
echo "Workdir        : $PBS_O_WORKDIR"
echo "Submission Host: $PBS_O_HOST"
echo "Compute Node   : `cat $PBS_NODEFILE`"
echo "Job started on" `hostname` `date`
echo "Current directory:" `pwd`
echo "####################################################"

echo "Computer node:"
aprun -n1 hostname
sleep 60

echo "Job Finished: " `date`
exit 0

