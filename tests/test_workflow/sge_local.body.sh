
echo "####################################################"
echo "Job id         : $CLUSTERJOB_ID"
echo "Job name       : $CLUSTERJOB_NAME"
echo "Workdir        : $CLUSTERJOB_WORKDIR"
echo "Submission Host: $CLUSTERJOB_SUBMIT_HOST"
echo "Compute Node   : $CLUSTERJOB_NODELIST"
echo "Job started on" `hostname` `date`
echo "Current directory:" `pwd`
echo "####################################################"

cd $CLUSTERJOB_WORKDIR

sleep 60

echo "Job Finished: " `date`
exit 0
