"""
LSF backend
"""
from __future__ import absolute_import

import re
from ..status import PENDING, RUNNING, COMPLETED, CANCELLED, FAILED
from ..utils import time_to_seconds
from .. import ClusterjobBackend

def time_to_minutes(val):
    return str(int(time_to_seconds(val) / 60))


class LsfBackend(ClusterjobBackend):
    """LSF Backend

    Attributes:
        name (str): Name of the backend
        extension (str): Extension for job scripts
        prefix(str): The prefix for every line in the resource header
        resource_replacements (dict): mapping of the common clusterjob resource
            keys to command line options of the `bsub` command.
        job_vars(dict): mapping of *core environment variables* to
            LSF-specific environment variables.
    """

    name = 'lsf'
    extension = 'lsf'
    prefix = '#BSUB'

    def __init__(self):
        self.status_mapping = {
            'PEND'  : PENDING,
            'PSUSP' : PENDING,
            'RUN'   : RUNNING,
            'USUSP' : PENDING,
            'SSUSP' : PENDING,
            'DONE'  : COMPLETED,
            'EXIT'  : FAILED,
            'UNKWN' : PENDING,
            'WAIT'  : PENDING,
            'ZOMBI' : FAILED,
        }
        self.resource_replacements = {
            'jobname': '-J',
            'queue'  : '-q',
            'time'   : '-W',
            'mem'    : '-M',
            'stdout' : '-o',
            'stderr' : '-e',
        }
        self.job_vars = {
            '$CLUSTERJOB_ID'         : '$LSB_JOBID',
            '$CLUSTERJOB_WORKDIR'    : '$LS_SUBCWD',
            '$CLUSTERJOB_SUBMIT_HOST': '`hostname`', # Not available in LSF
            '$CLUSTERJOB_NAME'       : '$LSB_JOBNAME',
            '$CLUSTERJOB_ARRAY_INDEX': '$LSB_JOBINDEX',
            '$CLUSTERJOB_NODELIST'   : '$LSB_HOSTS',
            '${CLUSTERJOB_ID}'         : '${LSB_JOBID}',
            '${CLUSTERJOB_WORKDIR}'    : '${LS_SUBCWD}',
            '${CLUSTERJOB_SUBMIT_HOST}': '`hostname`',
            '${CLUSTERJOB_NAME}'       : '${LSB_JOBNAME}',
            '${CLUSTERJOB_ARRAY_INDEX}': '${LSB_JOBINDEX}',
            '${CLUSTERJOB_NODELIST}'   : '${LSB_HOSTS}',
        }
    def cmd_submit(self, jobscript):
        """Given a :class:`~clusterjob.JobScript` instance, return a ``bsub``
        command that submits the job to the scheduler, as a string.
        Specifically, the jobscript is piped into ``bsub`` for instant
        scheduling.
        """
        return 'bsub < "%s"' % jobscript.filename

    def get_job_id(self, response):
        """Given the stdout from the command returned by :meth:`cmd_submit`,
        return a job ID"""
        match = re.search('Job <([^>]+)> is submitted', response)
        if match:
            return match.group(1)
        else:
            return None

    def cmd_status(self, run, finished=False):
        """Given a :class:`~clusterjob.AsyncResult` instance, return a
        ``bjobs`` command that queries the scheduler for the job status, as a
        list of command arguments.  The same command is used for running or
        finished jobs.
        """
        return ['bjobs', '-a', str(run.job_id)]

    def get_status(self, response, finished=False):
        """Given the stdout from the command returned by :meth:`cmd_status`,
        return one of the status code defined in :mod:`clusterjob.status`"""
        status_pos = 0
        for line in response.split("\n"):
            if line.startswith('JOBID'):
                try:
                    status_pos = line.find('STAT')
                except ValueError:
                    return None
            else:
                status = line[status_pos:].split()[0]
                if status in self.status_mapping:
                    return self.status_mapping[status]
        return None

    def cmd_cancel(self, run):
        """Given a :class:`~clusterjob.AsyncResult` instance, return an
        ``bkill`` command that cancels the run, as a list of command
        arguments.
        """
        return ['bkill', str(run.job_id)]

    def resource_headers(self, jobscript):
        """Given a :class:`~clusterjob.JobScript` instance, return a list of
        lines that encode the resource requirements, to be added at the top of
        the rendered job script
        """
        resources = jobscript.resources.copy()
        lines = []
        if 'threads' in resources:
            if 'nodes' in resources:
                n_total = resources['nodes'] * resources['threads']
                lines.append('%s -n %d' % (self.prefix, n_total))
                del resources['nodes']
            else:
                lines.append('%s -n %d' % (self.prefix, resources['threads']))
            del resources['threads']
        for (key, val) in resources.items():
            if key in self.resource_replacements:
                lsf_key = self.resource_replacements[key]
                if key == 'time':
                    val = time_to_minutes(val)
            else:
                lsf_key = key
            if val is None:
                continue
            if type(val) is bool:
                if val:
                    lines.append("%s %s" % (self.prefix, lsf_key))
            else:
                lines.append("%s %s %s" % (self.prefix, lsf_key, str(val)))
        return lines

    def replace_body_vars(self, body):
        """Given a multiline string that is the body of the job script, replace
        the placeholders for environment variables with backend-specific
        realizations, and return the modified body
        """
        for key, val in self.job_vars.items():
            body = body.replace(key, val)
        return body
