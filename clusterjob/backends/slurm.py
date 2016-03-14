from __future__ import absolute_import
import re

from ..status import PENDING, RUNNING, COMPLETED, CANCELLED, FAILED
from .. import ClusterjobBackend


class SlurmBackend(ClusterjobBackend):
    """SLURM Backend

    Attributes:
        name (str): Name of the backend
        extension (str): Extension for job scripts
        prefix(str): The prefix for every line in the resource header
        status_mapping (dict): mapping of Slurm string status codes to
            clusterjob integer status codes
        resource_replacements (dict): mapping of the common clusterjob resource
            keys to command line options of the `qsub` command.
        job_vars(dict): mapping of *core environment variables* to
            Slurm-specific environment variables.

    """
    name = 'slurm'
    extension = 'slr'
    prefix = '#SBATCH'

    def __init__(self):
        self.status_mapping = {
            'RUNNING'    : RUNNING,
            'CANCELLED'  : CANCELLED,
            'COMPLETED'  : COMPLETED,
            'CONFIGURING': PENDING,
            'COMPLETING' : RUNNING,
            'FAILED'     : FAILED,
            'NODE_FAIL'  : FAILED,
            'PENDING'    : PENDING,
            'PREEMPTED'  : FAILED,
            'SUSPENDED'  : PENDING,
            'TIMEOUT'    : FAILED,
        }
        self.resource_replacements = {
            'jobname': '--job-name',
            'queue'  : '--partition',
            'time'   : '--time',
            'nodes'  : '--nodes',
            'ppn'    : '--tasks-per-node',
            'threads': '--cpus-per-task',
            'mem'    : '--mem',
            'stdout' : '--output',
            'stderr' : '--error',
        }
        self.job_vars = {
            '$CLUSTERJOB_ID'         : '$SLURM_JOB_ID',
            '$CLUSTERJOB_WORKDIR'    : '$SLURM_SUBMIT_DIR',
            '$CLUSTERJOB_SUBMIT_HOST': '$SLURM_SUBMIT_HOST',
            '$CLUSTERJOB_NAME'       : '$SLURM_JOB_NAME',
            '$CLUSTERJOB_ARRAY_INDEX': '$SLURM_ARRAY_TASK_ID',
            '$CLUSTERJOB_NODELIST'   : '$SLURM_JOB_NODELIST',
            '${CLUSTERJOB_ID}'         : '${SLURM_JOB_ID}',
            '${CLUSTERJOB_WORKDIR}'    : '${SLURM_SUBMIT_DIR}',
            '${CLUSTERJOB_SUBMIT_HOST}': '${SLURM_SUBMIT_HOST}',
            '${CLUSTERJOB_NAME}'       : '${SLURM_JOB_NAME}',
            '${CLUSTERJOB_ARRAY_INDEX}': '${SLURM_ARRAY_TASK_ID}',
            '${CLUSTERJOB_NODELIST}'   : '${SLURM_JOB_NODELIST}',
        }
    def cmd_submit(self, jobscript):
        """Given a :class:`~clusterjob.JobScript` instance, return a ``sbatch``
        command that submits the job to the scheduler, as a list of program
        arguments.
        """
        return ['sbatch', jobscript.filename]

    def get_job_id(self, response):
        """Given the stdout from the command returned by :meth:`cmd_submit`,
        return a job ID"""
        match = re.search('Submitted batch job (\d+)\s*$', response)
        if match:
            return match.group(1)
        else:
            return None

    def cmd_status(self, run, finished=False):
        """Given a :class:`~clusterjob.AsyncResult` instance, return a command
        that queries the scheduler for the job status, as a list of command
        arguments.  If ``finished=True``, the scheduler is queried via
        ``sacct``. Otherwise, ``squeue`` is used.
        """
        if finished:
            return ['sacct', '--format=state', '-n', '-j %s' % run.job_id]
        else:
            return ['squeue', '-h', '-o %T', '-j %s' % run.job_id]

    def get_status(self, response, finished=False):
        """Given the stdout from the command returned by :meth:`cmd_status`,
        return one of the status code defined in :mod:`clusterjob.status`"""
        for line in response.split("\n"):
            if line.strip() in self.status_mapping:
                return self.status_mapping[line.strip()]
        return None

    def cmd_cancel(self, run):
        """Given a :class:`~clusterjob.AsyncResult` instance, return an
        ``scancel`` command that cancels the run, as a list of command
        arguments.
        """
        return ['scancel', str(run.job_id)]

    def resource_headers(self, jobscript):
        """Given a :class:`~clusterjob.JobScript` instance, return a list of
        lines that encode the resource requirements, to be added at the top of
        the rendered job script
        """
        lines = []
        for (key, val) in jobscript.resources.items():
            if key in self.resource_replacements:
                slurm_key = self.resource_replacements[key]
                val = str(val).strip()
            else:
                slurm_key = key
            if not slurm_key.startswith('-'):
                if len(slurm_key) == 1:
                    slurm_key = '-%s' % slurm_key
                else:
                    slurm_key = '--%s' % slurm_key
            if val is None:
                continue
            if type(val) is bool:
                if val:
                    lines.append("%s %s" % (self.prefix, slurm_key))
            else:
                if slurm_key.startswith('--'):
                    lines.append('%s %s=%s'
                                  % (self.prefix, slurm_key, str(val)))
                else:
                    lines.append('%s %s %s'
                                 % (self.prefix, slurm_key, str(val)))
        return lines

    def replace_body_vars(self, body):
        """Given a multiline string that is the body of the job script, replace
        the placeholders for environment variables with backend-specific
        realizations, and return the modified body
        """
        for key, val in self.job_vars.items():
            body = body.replace(key, val)
        return body

