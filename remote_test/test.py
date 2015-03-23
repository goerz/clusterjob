#!/usr/bin/env python
from clusterjob import Job
from clusterjob.utils import read_file
from clusterjob.status import str_status

###############################################################################
Job.default_remote = 'clusteruser@mycluster'
Job.default_backend = 'slurm'
###############################################################################

job = Job(read_file('./jobscript.sh'), jobname='test', time='00:03:00',
          nodes=1, threads=1, mem=8, workdir='~/jobs/remote_test',
          prologue=read_file('./prologue.sh'),
          epilogue=read_file('./epilogue.sh'))

ar = job.submit(verbose=True)
print str_status[ar.get()]