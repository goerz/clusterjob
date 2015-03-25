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

    'cmd_submit': tuple of (callable, callable)
        The first element of the tuple is a callable that receives the name of
        the job script and must return the command to be used for submission,
        pererrably as a command list, or alternativey as a shell command
        string.
        The second element of the tuple is a callable that receives
        the shell output from the submission command and returns the job ID
        that the cluster has assigned to the job as a string

    'cmd_status_running': tuple of (callable, callable)
        The first element of the tuple is a callable that receives the job ID
        of a running job, and must return the command to be used to check the
        status of the script (as list or string, see 'cmd_submit')
        The second element of the tuple is a
        callable that receives the shell output from that command and returns
        one of the status codes defined in the `clusterjob.status` module, or
        None if no status can be determined from the output of the command.

    'cmd_status_finished': tuple of (list, callable)
        A fallback if `cmd_status_running` is not able to determine a status,
        e.g. because the command defined there does not return any output for
        jobs that have finished.  It will only be called if the interpreted
        result of `cmd_status_running` is None.

    'cmd_cancel': list
        Callable that receives the job ID of a running job, and must return the
        command that can be used to cancel the job (as a list or string).

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
        for key in ['cmd_submit', 'cmd_status_running', 'cmd_status_finished']:
            try:
                cmd1, cmd2 = backend[key]
                cmd1('xxx')
                cmd2('xxx')
            except (TypeError, ValueError) as e:
                raise AssertionError("%s must a a tuple of callables: %s" \
                                     % (key, e))
        try:
            backend['cmd_cancel']('xxx')
        except TypeError as e:
            raise AssertionError("cmd_cancel must a a callable: %s" % (e, ))
        for key in template['job_vars']:
            assert key in backend['job_vars'], \
            "backend does not recognize job variable %s" % key
    except AssertionError:
        if raise_exception:
            raise
        else:
            return False
    return True

