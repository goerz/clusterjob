echo "####################################################"
echo "Job id         : $CLUSTERJOB_ID"
echo "Job name       : $CLUSTERJOB_NAME"
echo "Workdir        : $CLUSTERJOB_WORKDIR"
echo "Submission Host: $CLUSTERJOB_SUBMIT_HOST"
echo "Compute Node   : $CLUSTERJOB_NODELIST"
echo "Job started on" `hostname` `date`
echo "Current directory:" `pwd`
echo "####################################################"

cd $CLUSTERJOB_WORKDIR # superfluous for slurm

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
