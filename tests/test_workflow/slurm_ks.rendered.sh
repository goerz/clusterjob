#!/bin/bash
#SBATCH --job-name=test_clj
#SBATCH --partition=exec
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=20
#SBATCH --output=clusterjob_test.out

echo "####################################################"
echo "Job id         : $SLURM_JOB_ID"
echo "Job name       : $SLURM_JOB_NAME"
echo "Workdir        : $SLURM_SUBMIT_DIR"
echo "Submission Host: $SLURM_SUBMIT_HOST"
echo "Compute Node   : $SLURM_JOB_NODELIST"
echo "Job started on" `hostname` `date`
echo "Current directory:" `pwd`
echo "####################################################"

sleep 180

echo "Job Finished: " `date`
exit 0
