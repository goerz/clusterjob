"""
LPBS backend
"""
from __future__ import absolute_import
from .pbs import PbsBackend

class LPbsBackend(PbsBackend):
    """LPBS Backend"""

    name = 'lpbs'
    extension = 'pbs'
    prefix = '#PBS'

    def cmd_submit(self, jobscript):
        """Given a :class:`~clusterjob.JobScript` instance, return a ``lqsub``
        command that submits the job to the scheduler, as a list of program
        arguments.
        """
        return ['lqsub', jobscript.filename]

    def cmd_status(self, run, finished=False):
        """Given a :class:`~clusterjob.AsyncResult` instance, return a
        ``lqstat`` command that queries the scheduler for the job status."""
        return ['lqstat', str(run.job_id)]

    def cmd_cancel(self, run):
        """Given a :class:`~clusterjob.AsyncResult` instance, return a
        ``lqdel`` command that cancels the run, as a list of command arguments.
        """
        return ['lqdel', str(run.job_id)]
