"""
PBS/TORQUE backend
"""
from __future__ import absolute_import
import re

from ..status import PENDING, RUNNING, COMPLETED
from .. import ClusterjobBackend

class PbsBackend(ClusterjobBackend):
    """PBS/TORQUE Backend

    Attributes:
        name (str): Name of the backend
        extension (str): Extension for job scripts
        prefix (str): The prefix for every line in the resource header
        status_mapping (dict): map single-letter PBS status codes to clusterjob
            integer status codes.
        resource_replacements (dict): mapping of the common clusterjob resource
            keys to command line options of the `qsub` command.
        job_vars (dict): mapping of *core environment variables* to
            PBS-specific environment variables.
    """
    name = 'pbs'
    extension = 'pbs'
    prefix = '#PBS'

    def __init__(self):
        self.status_mapping = {
            'C' : COMPLETED,
            'B' : RUNNING,
            'E' : RUNNING,
            'H' : PENDING,
            'M' : PENDING,
            'Q' : PENDING,
            'R' : RUNNING,
            'T' : PENDING,
            'W' : PENDING,
            'U' : PENDING,
            'S' : PENDING,
            'F' : COMPLETED,
            'X' : COMPLETED,
        }
        self.resource_replacements = {
            'jobname': '-N',
            'queue'  : '-q',
            'time'   : '-l walltime=',
            'mem'    : '-l mem=',
            'stdout' : '-o',
            'stderr' : '-e',
            # nodes and threads are handled separately, in resource_headers
        }
        self.job_vars = {
            '$CLUSTERJOB_ID'         : '$PBS_JOBID',
            '$CLUSTERJOB_WORKDIR'    : '$PBS_O_WORKDIR',
            '$CLUSTERJOB_SUBMIT_HOST': '$PBS_O_HOST',
            '$CLUSTERJOB_NAME'       : '$PBS_JOBNAME',
            '$CLUSTERJOB_ARRAY_INDEX': '$PBS_ARRAYID',
            '$CLUSTERJOB_NODELIST'   : '`cat $PBS_NODEFILE`',
            '${CLUSTERJOB_ID}'         : '${PBS_JOBID}',
            '${CLUSTERJOB_WORKDIR}'    : '${PBS_O_WORKDIR}',
            '${CLUSTERJOB_SUBMIT_HOST}': '${PBS_O_HOST}',
            '${CLUSTERJOB_NAME}'       : '${PBS_JOBNAME}',
            '${CLUSTERJOB_ARRAY_INDEX}': '${PBS_ARRAYID}',
            '${CLUSTERJOB_NODELIST}'   : '`cat $PBS_NODEFILE`',
        }

    def cmd_submit(self, jobscript):
        """Given a :class:`~clusterjob.JobScript` instance, return a ``qsub``
        command that submits the job to the scheduler, as a list of program
        arguments.
        """
        return ['qsub', jobscript.filename]

    def get_job_id(self, response):
        """Given the stdout from the command returned by :meth:`cmd_submit`,
        return a job ID"""
        lines = [line.strip() for line in response.split("\n")
                if line.strip() != '']
        last_line = lines[-1]
        match = re.match('(\d+)\.[\w.-]+$', last_line)
        if match:
            return match.group(1)
        else:
            return None

    def cmd_status(self, run, finished=False):
        """Given a :class:`~clusterjob.AsyncResult` instance, return a
        ``qstat`` command that queries the scheduler for the job status. It is
        assumed that by passing ``-x`` to ``qstat``, results for both running
        and finished jobs can be obtained.
        """
        return ['qstat', '-x', str(run.job_id)]

    def get_status(self, response, finished=False):
        """Given the stdout from the command returned by :meth:`cmd_status`,
        return one of the status code defined in :mod:`clusterjob.status`, or
        None if the status cannot be determined"""
        lines = [line.strip() for line in response.split("\n")
                if line.strip() != '']
        last_line = lines[-1]
        if last_line.startswith('qstat: Unknown Job'):
            return COMPLETED
        else:
            try:
                status = lines[-1].split()[4]
                return self.status_mapping[status]
            except (IndexError, KeyError):
                return None

    def cmd_cancel(self, run):
        """Given a :class:`~clusterjob.AsyncResult` instance, return a ``qdel``
        command that cancels the run, as a list of command arguments.
        """
        return ['qdel', str(run.job_id)]

    def resource_headers(self, jobscript):
        """Given a :class:`~clusterjob.JobScript` instance, return a list of
        lines that encode the resource requirements, to be added at the top of
        the rendered job script
        """
        resources = jobscript.resources.copy()
        lines = []
        if 'nodes' in resources:
            if 'threads' in resources:
                lines.append('%s -l nodes=%s:ppn=%s'
                             % (self.prefix, resources['nodes'],
                                resources['threads']))
                del resources['threads']
            else:
                lines.append('%s -l nodes=%s'
                             % (self.prefix, resources['nodes']))
            del resources['nodes']
        for (key, val) in resources.items():
            if key in self.resource_replacements:
                pbs_key = self.resource_replacements[key]
                if key == 'mem':
                    val = str(val) + "m"
            else:
                pbs_key = key
            if val is None:
                continue
            if type(val) is bool:
                if val:
                    if not pbs_key.startswith('-'):
                        pbs_key = '-' + pbs_key
                    lines.append("%s %s" % (self.prefix, pbs_key))
            else:
                if not pbs_key.startswith('-'):
                    pbs_key = '-l %s=' % pbs_key
                if pbs_key.endswith('='):
                    lines.append('%s %s%s' % (self.prefix, pbs_key, str(val)))
                else:
                    lines.append('%s %s %s' % (self.prefix, pbs_key, str(val)))
        return lines

    def replace_body_vars(self, body):
        """Given a multiline string that is the body of the job script, replace
        the placeholders for environment variables with backend-specific
        realizations, and return the modified body. See the `job_vars`
        attribute for the mappings that are performed.
        """
        for key, val in self.job_vars.items():
            body = body.replace(key, val)
        return body

