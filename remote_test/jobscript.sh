echo "####################################################"
echo "Job id         : $XXX_JOB_ID"
echo "Job name       : $XXX_JOB_NAME"
echo "Workdir        : $XXX_WORKDIR"
echo "Submission Host: $XXX_HOST"
echo "Compute Node   : $XXX_NODELIST"
echo "Job started on" `hostname` `date`
echo "Current directory:" `pwd`
echo "####################################################"

cd $XXX_WORKDIR # superfluous for slurm

echo "####################################################"
echo "Full Environment:"
printenv | tee out.log
echo "####################################################"

echo "####################################################"
echo "External command"
./hello_world.sh | tee -a out.log
echo "####################################################"

sleep 90

echo "Job Finished: " `date`
exit 0
