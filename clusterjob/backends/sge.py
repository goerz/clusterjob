"""
SGE (Sun Grid Engine) backend
"""
from __future__ import absolute_import

import re
from ..status import RUNNING, COMPLETED
from .. import ClusterjobBackend

class SgeBackend(ClusterjobBackend):
    """SGE Backend

    Attributes:
        name (str): Name of the backend
        extension (str): Extension for job scripts
        prefix(str): The prefix for every line in the resource header
        resource_replacements (dict): mapping of the common clusterjob resource
            keys to command line options of the `qsub` command.
        job_vars(dict): mapping of *core environment variables* to PBS-specific
            environment variables.

    Note:
        Nodes and threads are not directly supported on SGE, but must be set up
        using "parallel environments". The configuration is set by the
        administrator so you have to check what they've called the parallel
        environments::

            %> qconf -spl
            pe1
            omp

        look for one with ``$pe_slots`` in the config::

            %> qconf -sp pe1
            %> qconf -sp omp

        Call ``qsub`` with that environment and number of cores you want to
        use::

            qsub -pe omp 8 -cwd ./myscript

        Depending on how the cluster is set up, it may be necessary to pass the
        shell as e.g. ``-S /bin/bash``. If this definition is missing, the run
        can crash with some very unclear error messages
    """
    def __init__(self):
        self.name = 'sge'
        self.extension = 'sge'
        self.prefix = '#$'
        self.resource_replacements = {
            'jobname': '-N',
            'queue'  : '-q',
            'time'   : '-l h_rt=',
            'mem'    : '-l h_vmem==',
            'stdout' : '-o',
            'stderr' : '-e',
            # nodes and threads are handled separately, in resource_headers
        }
        self.job_vars = {
            '$CLUSTERJOB_ID'         : '$JOB_ID',
            '$CLUSTERJOB_WORKDIR'    : '$SGE_O_WORKDIR',
            '$CLUSTERJOB_SUBMIT_HOST': '$SGE_O_HOST',
            '$CLUSTERJOB_NAME'       : '$JOBNAME',
            '$CLUSTERJOB_ARRAY_INDEX': '$SGE_TASK_ID',
            '$CLUSTERJOB_NODELIST'   : '$HOSTNAME',
            '${CLUSTERJOB_ID}'         : '${JOB_ID}',
            '${CLUSTERJOB_WORKDIR}'    : '${SGE_O_WORKDIR}',
            '${CLUSTERJOB_SUBMIT_HOST}': '${SGE_O_HOST}',
            '${CLUSTERJOB_NAME}'       : '${JOBNAME}',
            '${CLUSTERJOB_ARRAY_INDEX}': '${SGE_TASK_ID}',
            '${CLUSTERJOB_NODELIST}'   : '${HOSTNAME}',
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
        match = re.match(r'Your job (\d+) .* has been submitted$', last_line)
        if match:
            return match.group(1)
        else:
            return None

    def cmd_status(self, run, finished=False):
        """Given a :class:`~clusterjob.AsyncResult` instance, return a
        ``qstat`` command that queries the scheduler for the job status. The
        same command is used for running and finished jobs."""
        # Sadly, qstat -j doesn't give the state, and just 'qstat' doesn't
        # allow to filter for a specific job id
        return ['qstat', '-j %s' % str(run.job_id)]

    def get_status(self, response, finished=False):
        """Given the stdout from the command returned by :meth:`cmd_status`,
        return one of the status code defined in :mod:`clusterjob.status`, or
        None if the status cannot be determined"""
        # Sadly, qstat -j doesn't give the state, and just 'qstat' doesn't allow to
        # filter for a specific job id
        if "Following jobs do not exist" in response:
            return COMPLETED
        else:
            return RUNNING

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
            del resources['nodes']
        if 'threads' in resources:
            del resources['threads']
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
