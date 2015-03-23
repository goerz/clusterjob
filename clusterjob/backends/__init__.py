"""
Collection of backends.

Each submodule defines a `backend` dictionary with the backend options. This
dictionary (and all user-defined backends that may be passed to the
`register_backend` class method of clusterjob.Job) must
have the following structure of keys and values:

    'name': str
        Name of the backend.

    'prefix': str
        prefix to be added before each submission options, in the header of the
        job script. E.g. '#SBATCH' for slurm and '#PBS' for PBS/Torque.

    'extension': str
        Default filename extension for job script files.

    'cmd_submit': tuple of (list, callable)
        The first element of the tuple is the shell command that is used for
        submission. It must take the name of the job script file as a
        parameter. The second element of the tuple is a callable that receives
        the shell output from the submission command and returns the job ID
        that the cluster has assigned to the job as a string

    'cmd_status_running': tuple of (list, callable)
        The first element of the tuple is the shell command that yields some
        information about a running job. The placeholder `{job_id}` can be used
        for the cluster-assigned job ID.  The second element of the tuple is a
        callable that receives the shell output from the command and returns
        one of the status codes defined in the `clusterjob.status` module, or
        None if no status can be determined from the output of the command.

    'cmd_cancel': list
        Shell command that cancels the job on the cluster. The placeholder
        `{job_id}` can be used for the cluster-assigned job ID

    'cmd_status_finished': tuple of (list, callable)
        A fallback if `cmd_status_running` is not able to determine a status,
        e.g. because the command defined there does not return any output for
        jobs that have finished.  It will only be called if
        `cmd_status_running` results in None.

    'translate_options': callable
        The callable receives a dictionary of options (from
        clusterjob.Job.options), and must return an array of command line
        options for the backend's submission script. These, together with the
        backend's prefix will be written to the header of the job submission
        script.

    'default_opts': dict
        Default entries for `clusterjob.Job.options` when a Job instance is
        created with the given backend

    'job_vars': dict
        Mapping of replacements that will be applied to the body of the job
        script. The intention is to adjust the name of environment variables to
        the backend, e.g. '$SLURM_JOB_ID' for SLURM vs. '$PBS_JOBID' for
        PBS/Torque. It must define replacements for the following strings:
        'XXX_JOB_ID'      => var containing cluster assigned job ID
        'XXX_WORKDIR'     => var containing submission directory on the cluster
        'XXX_HOST'        => var containing hostname of submission node
        'XXX_JOB_NAME'    => var containing job name
        'XXX_ARRAY_INDEX' => var containing job array/tast index
        'XXX_NODELIST'    => var containing  hostname(s) of job nodes
"""

import slurm

def check_backend(backend, raise_exception=True):
    """
    Return True if the given backend has the correct structure (as compared
    agains the slurm backend)

    Arguments
    ---------

    backend: dict
        Dictionary of backend options
    raise_exceptions: boolean, optional
        If True (default), raise an AssertionError if the backend does not
        match the required structure. Otherwise, return False.
    """
    template = slurm.backend
    try:
        for key in template:
            assert key in backend, "backend is missing mandatory key %s" % key
        test_options = {
            'name': 'testjob',
            'queue': 'testqueue',
            'time': '01:00:00',
            'nodes': 1,
            'threads': 1,
            'mem': 1024,
            'stdout': 'stdout.log',
            'stderr': 'stderr.log',
        }
        try:
            opt_array = backend['translate_options'](test_options)
            for option in opt_array:
                assert str(option) == option
        except Exception as e:
            raise AssertionError("invalid backend %s: %s", backend['name'], e)
        for key in template['job_vars']:
            assert key in backend['job_vars'], \
            "backend does not recognize job variable %s" % key
    except AssertionError:
        if raise_exception:
            raise
        else:
            return False
    return True

