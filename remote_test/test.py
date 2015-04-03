#!/usr/bin/env python
from clusterjob import Job
from clusterjob.utils import read_file
from clusterjob.status import str_status
import logging
Job.debug_cmds = True

###############################################################################
Job.default_remote = 'clusteruser@mycluster'
Job.default_backend = 'slurm'
###############################################################################

logging.basicConfig(level=logging.DEBUG)

job = Job(read_file('./jobscript.sh'), jobname='test', time='00:03:00',
          nodes=1, threads=1, mem=8, rootdir='~/jobs', workdir='./remote_test',
          prologue=read_file('./prologue.sh'),
          epilogue=read_file('./epilogue.sh'))

print "\n*** Submitting Job ***\n"
ar = job.submit()

print "\n*** Cancelling Job ***\n"
ar.cancel()

print "\n*** Resubmitting Job ***\n"
ar = job.submit(retry=True)

print "\n*** Waiting for  Job to finish ***\n"
print str_status[ar.get()]

print "\n*** DONE ***\n"
