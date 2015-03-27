"""
Abstract description of a Cluster Job
"""
__version__ = "1.1.1"

import os
import subprocess as sp
import tempfile
import cPickle as pickle
from glob import glob
from .utils import set_executable
from textwrap import dedent

class Job(object):
    """
    Class Attributes
    ----------------

    default_opts: dict
        Default values of the `options` attribute for a newly created Job
        instance.

    backends: dict
        Available backends. Maps backend name to a dictionary of backend
        options. See the documentation of `clusterjob.backends.slurm`, for
        details. User-defined backends may be added with the `register_backend`
        class method

    default_backend: str
        The default backend name to be used.

    default_shell: str
        The shell to be used (shebang) for the job script. Also the shebang for
        the prologue and epilogue scripts, if no shebang is present in those
        scripts

    default_remote: str
        The remote to be used when submitting the job script

    default_rootdir: str
        The default root for all working directories.

    cache_folder: str
        Local folder in which to cache the AsyncResult instances resulting from
        job submission

    cache_prefix: str
        prefix for cache filenames

    cache_counter: int
        Internal counter to be used when no cache_id is specified during
        submission

    debug_cmds: boolean
        If set to True, write debug information about all external commands
        (`utils.run_cmd` calls) to stdout.

    Attributes
    ----------

    backend: str
        name of backend, must be a key in the backends class dictionary

    shell: str
        shell that is used to execute runscript

    remote: str or None
        remote server on which to execute submit commands

    rootdir: str
        root directory for workdir

    workdir: str
        work directory (local or remote) in which the job script file will be
        placed, and from which the submission command will be called. Relative
        to `rootdir`.

    filename: str
        Name of file to which the job script will be written (inside
        rootdir/workdir).  If not set explicitly set, the filename will be set
        from the job name (`options['jobname']` attribute) together with a
        backend-specific file extension

    prologue: str
        multiline shell script that will be executed *locally* in the current
        working directory before submitting the job. If the script does not
        contain a shebang, the shell specified in the `shell` attribute will be
        used. The body of the script will be formatted with the Job attributes
        (at submission time); e.g., '{remote}' will be replaced by the value of
        the corresponding attribute. In addition, '{fulldir}' will be replaced
        by

            os.path.join(rootdir, workdir)

        The main purpose of the prologue script is to move data to a remote
        cluster, e.g. via the commands

            ssh {remote} 'mkdir -p {fulldir}'
            rsync -av {workdir}/ {remote}:{fulldir}

    epilogue: str
        multiline shell script that will be executed *locally* in the current
        working directory the first time that the job is known to have
        finished. It will be formatted in the same way as the prologue script
        (at submission time). It's execution will be handled by the AsyncResult
        object resulting from the job submission. The main purpose of the
        epilogue script is to move data from a remote cluster upon completion
        of the job.

    options: dict
        Dictionary of submission options describing resource requirements. Will
        be translated according to the backend and passed to the submission
        command

    jobscript: str
        Multiline job script. Should not contain a shebang or backend-specific
        submission headers. It will be renedered for the given backend by
        adding a shebang, the job submission headers (based on the `options`
        attribute), and by applying the mappings defined in the 'job_vars'
        entry of the backend

    Example
    -------

    >>> script = r'''
    ... echo "####################################################"
    ... echo "Job id: $XXX_JOB_ID"
    ... echo "Job name: $XXX_WORKDIR"
    ... echo "Job started on" `hostname` `date`
    ... echo "Current directory:" `pwd`
    ... echo "####################################################"
    ...
    ... echo "####################################################"
    ... echo "Full Environment:"
    ... printenv
    ... echo "####################################################"
    ...
    ... sleep 90
    ...
    ... echo "Job Finished: " `date`
    ... exit 0
    ... '''
    >>> job = Job(script, backend='slurm', jobname='printenv', queue='test',
    ... time='00:05:00', nodes=1, threads=1, mem=100,
    ...  stdout='printenv.out', stderr='printenv.err')
    >>> print job
    #!/bin/bash
    #SBATCH --output=printenv.out
    #SBATCH --mem=100
    #SBATCH --job-name=printenv
    #SBATCH --partition=test
    #SBATCH --cpus-per-task=1
    #SBATCH --error=printenv.err
    #SBATCH --time=00:05:00
    #SBATCH --nodes=1
    <BLANKLINE>
    echo "####################################################"
    echo "Job id: $SLURM_JOB_ID"
    echo "Job name: $SLURM_SUBMIT_DIR"
    echo "Job started on" `hostname` `date`
    echo "Current directory:" `pwd`
    echo "####################################################"
    <BLANKLINE>
    echo "####################################################"
    echo "Full Environment:"
    printenv
    echo "####################################################"
    <BLANKLINE>
    sleep 90
    <BLANKLINE>
    echo "Job Finished: " `date`
    exit 0
    <BLANKLINE>
    """

    default_opts = {}
    backends = {}
    default_backend = 'slurm'
    default_shell = None
    default_remote = None
    default_rootdir = ''
    cache_folder = None
    cache_prefix = 'clusterjob'
    cache_counter = 0
    debug_cmds = False

    @classmethod
    def register_backend(cls, backend):
        """
        Register a new backend

        `backend` must be a dictionary that follows the same structure as
        `clusterjob.backends.slurm.backend`. If the dictionary is found to have
        the wrong structure, an AssertionError will be raised.
        """
        from . backends import check_backend
        try:
            if check_backend(backend):
                cls.backends[backend['name']] = backend
        except AssertionError as e:
            import pprint
            pp = pprint.PrettyPrinter(indent=4)
            print "Invalid backend:\n%s\n\n%s" % (pp.pformat(backend), e)

    @classmethod
    def clear_cache_folder(cls):
        """Remove all files in the cache_folder"""
        if cls.cache_folder is not None:
            for file in glob(os.path.join(cls.cache_folder, '*')):
                os.unlink(file)

    def __init__(self, jobscript, jobname, **kwargs):
        """
        Keyword Arguments
        -----------------

        The backend, shell, remote, rootdir, workdir, filename, prologue, and
        epilogue arguments specify the value of the corresponding attributes.
        All other keyword arguments are stored in the `options` dict attribute,
        to be used as options for the job sumbmission command (e.g. sbatch for
        slurm or qsub for PBS). At a minimum, the following arguments are
        supported:

        jobname: str
            Name of the job (mandatory)

        queue: str
            Name of queue/partition to which to submit the job

        time: time
            Maximum runtime

        nodes: int
            Required number of nodes

        threads: int
            Required number of threads (cores)

        mem: int
            Required memory

        stdout: str
            name of file to which to write the jobs stdout

        stderr: str
            name of file to which to write the jobs stderr

        Custom backends may define further options, or even support arbitrary
        additional options. For example, in the default SLURM backend,
        unknown options are passed directly as arguments to `sbatch`, where
        single-letter argument names are prepended with '-', multi-letter
        argument names with '--'. An argument with boolean values is passed
        without any value iff the value is True:

            contiguous=True          -> --contiguous
            dependency='after:12454' -> --dependency=after:12454
            F='nodefile.txt'         -> -F nodefile.txt

        All backends are encouraged to implement a similar behavior.
        """
        self.options = {'jobname': jobname}

        self.jobscript = jobscript

        if len(self.backends) == 0:
            # register all available backends
            import pkgutil
            import clusterjob.backends
            for __, module_name, __ \
            in pkgutil.walk_packages(clusterjob.backends.__path__):
                mod = __import__('clusterjob.backends.%s' % module_name,
                                  globals(), locals(), ['backend', ], -1)
                self.register_backend(mod.backend)

        for kw in ['backend', 'shell', 'remote', 'rootdir', 'workdir',
        'filename', 'prologue', 'epilogue']:
            self.__dict__[kw] = None
            if kw in kwargs:
                self.__dict__[kw] = kwargs[kw]
                del kwargs[kw]
            else:
                default_key = 'default_%s' % kw
                if default_key in self.__class__.__dict__:
                    self.__dict__[kw] = self.__class__.__dict__[default_key]
        if self.shell is None:
            self.shell = '/bin/bash'
        if self.rootdir is None:
            self.rootdir = ''
        if self.workdir is None:
            self.workdir = ''
        if self.filename is None:
            self._default_filename()

        self.options.update(self.backends[self.backend]['default_opts'])
        self.options.update(self.default_opts)
        self.options.update(kwargs)

    def _default_filename(self):
        """If self.filename is None, attempt to set it from the jobname"""
        if self.filename is None:
            if 'jobname' in self.options:
                self.filename = "%s.%s" \
                                 % (self.options['jobname'],
                                    self.backends[self.backend]['extension'])

    def __str__(self):
        """Return the string representation of the job, i.e. the fully rendered
        jobscript"""

        opt_translator = self.backends[self.backend]['translate_options']
        opt_array = opt_translator(self.options)
        prefix = self.backends[self.backend]['prefix']
        jobscript = self.jobscript
        var_replacements = self.backends[self.backend]['job_vars']
        for var in var_replacements:
            jobscript = jobscript.replace(var, var_replacements[var])
        jobscript_lines = []
        jobscript_lines.append("#!%s" % self.shell)
        for option in opt_array:
            jobscript_lines.append("%s %s" % (prefix, option))
        for line in jobscript.split("\n"):
            if not line.startswith("#!"):
                jobscript_lines.append(line)
        jobscript = "\n".join(jobscript_lines)
        return jobscript

    def write(self, filename=None):
        """
        Write out the fully rendered jobscript to file. If filename is not
        None, write to the given *local* file. Otherwise, write to the local or
        remote file specified in the filename attribute, in the folder
        specified by the rootdir and workdir attributes. The folder will be
        created if it does not exist already.
        """
        from . utils import run_cmd
        remote = self.remote
        if filename is None:
            self._default_filename()
            filename = self.filename
            filename = os.path.join(self.rootdir, self.workdir, filename)
        else:
            remote = None

        if filename is None:
            raise ValueError("filename not given")
        filepath = os.path.split(filename)[0]
        run_cmd(['mkdir', '-p', filepath], remote, ignore_exit_code=False,
                debug=self.debug_cmds)

        # Write / Upload
        if remote is None:
            with open(filename, 'w') as run_fh:
                run_fh.write(str(self))
            set_executable(filename)
        else:
            tempfilename = tempfile.mkstemp()[1]
            with open(tempfilename, 'w') as run_fh:
                run_fh.write(str(self))
            set_executable(tempfilename)
            try:
                sp.check_output(
                    ['scp', tempfilename, remote+':'+filename],
                    stderr=sp.STDOUT)
            finally:
                os.unlink(tempfilename)

    def _run_prologue(self):
        """Render and run the prologue script"""
        if self.prologue is not None:
            prologue = self.prologue.format(
                           fulldir=os.path.join(self.rootdir, self.workdir),
                           **self.__dict__)
            if not prologue.startswith("#!"):
                prologue = "#!" + self.shell + "\n" + prologue
            tempfilename = tempfile.mkstemp()[1]
            with open(tempfilename, 'w') as prologue_fh:
                prologue_fh.write(prologue)
            set_executable(tempfilename)
            try:
                sp.check_output( [tempfilename, ], stderr=sp.STDOUT)
            except sp.CalledProcessError as e:
                print dedent(r'''
                Prologue script did not exit cleanly.
                CWD: {cwd}
                prologue: ---
                {prologue}
                ---
                response: ---
                {response}
                ---
                ''').format(cwd=os.getcwd(), prologue=self.prologue,
                            response=e.output)
                raise
            finally:
                os.unlink(tempfilename)

    def submit(self, block=False, cache_id=None, verbose=False, force=False,
        retry=True):
        """
        Submit the job.

        Parameters
        ----------

        block: boolean, optional
            If `block` is True, wait until the job is finished, and return the
            exit status code. Otherwise, return an AsyncResult object.

        cache_id: str or None, optional
            An ID uniquely defining the submission, used as identifier for the
            cached AscynResult object. If not given, the cache_id is determined
            internally. If an AsyncResult with a matching cache_id is present
            in the cache_folder, nothing is submitted to the cluster, and the
            cached AsyncResult object is returned

        verbose: boolean, optional
            If True, print information about submission o the screen

        force: boolean, optional
            If True, discard any existing cached AsyncResult object, ensuring
            that the job is sent to the cluster.

        retry: boolean, optional
            If True, and the existing cached AsyncResult indicates that the job
            finished with an error (CANCELLED/FAILED), resubmit the job,
            discard the cache and return a fresh AsyncResult object
        """
        from . status import FAILED, CANCELLED, PENDING, str_status
        from . utils import mkdir, run_cmd, time_to_seconds
        assert self.filename is not None, 'jobscript must have a filename'
        if verbose:
            if self.remote is None:
                print "Submitting job %s locally" \
                        % self.options['jobname']
            else:
                print "Submitting job %s on %s" \
                        % (self.options['jobname'], self.remote)

        submitted = False
        if cache_id is None:
            Job.cache_counter += 1
            cache_id = str(Job.cache_counter)
        else:
            cache_id = str(cache_id)
        cache_file = None

        ar = AsyncResult(backend=self.backends[self.backend])
        ar.debug_cmds = self.debug_cmds

        if self.cache_folder is not None:
            mkdir(self.cache_folder)
            cache_file = os.path.join(self.cache_folder,
                                 "%s.%s.cache" % (self.cache_prefix, cache_id))
            if os.path.isfile(cache_file):
                if force:
                    try:
                        os.unlink(cache_file)
                    except OSError:
                        pass
                else:
                    if verbose:
                        print "Reloading AsyncResults from %s" % cache_file
                    ar.load(cache_file)
                    submitted = True
                    if ar._status >= CANCELLED:
                        if retry:
                            if verbose:
                                print "Cached run %s, resubmitting" \
                                % str_status[ar._status]
                            os.unlink(cache_file)
                            ar = \
                            AsyncResult(backend=self.backends[self.backend])
                            ar.debug_cmds = self.debug_cmds
                            submitted = False

        if not submitted:
            self._run_prologue()
            cmd_submit, id_reader = self.backends[self.backend]['cmd_submit']
            self.write()
            job_id = None
            try:
                cmd = cmd_submit(self.filename)
                job_id = id_reader(run_cmd(cmd, self.remote, self.rootdir,
                                           self.workdir, ignore_exit_code=True,
                                           debug=self.debug_cmds))
                if job_id is None:
                    print "Failed to submit job"
                    status = FAILED
                else:
                    if verbose:
                        print "  Job ID: %s" % job_id
                    status = PENDING
            except sp.CalledProcessError as e:
                print "Failed to submit job: %s" % e
                status = FAILED

            ar.remote = self.remote
            ar.options = self.options.copy()
            ar.cache_file = cache_file
            ar.backend = self.backends[self.backend]
            try:
                ar.sleep_interval \
                = int(time_to_seconds(self.options['time']) / 10)
                if ar.sleep_interval < 10:
                    ar.sleep_interval = 10
                if ar.sleep_interval > 1800:
                    ar.sleep_interval = 1800
            except KeyError:
                ar.sleep_interval = 60
            ar._status = status
            ar.job_id = job_id
            if self.epilogue is not None:
                epilogue = self.epilogue.format(
                              fulldir=os.path.join(self.rootdir, self.workdir),
                              **self.__dict__)
                if not epilogue.startswith("#!"):
                    epilogue = "#!" + self.shell + "\n" + epilogue
                ar.epilogue = epilogue

        if block:
            result = ar.get()
        else:
            result = ar

        ar.dump()

        return result


class AsyncResult(object):
    """
    Result of submitting a cluster job

    Attributes
    ----------

    remote: str or None
        The remote host on which the job is running. Passwordless ssh must be
        set up to reach the remote. A value of None idicates that the job is
        running locally

    options: dict
        copy of the `options` attribute of the Job() instance that created the
        AsyncResult object

    cache_file: str or None
        The full path and name of the file to be used to cache the AsyncResult
        object. The cache file will be written automatically anytime a change
        in status is detected

    backend: dict
        A reference to the backend options dictionary for the backend under
        which the job is running

    sleep_interval: int
        Numer of seconds to sleep between polls to the cluster scheduling
        systems when waiting for the Job to finish

    job_id: str
        The Job ID assigned by the cluster scheduler

    epilogue: str
        Multiline script to be run once when the status changes from "running"
        (pending/running) to "not running" (completed, cancelled, failed).
        The contents of this variable will be written to a temporary file as
        is, and executed as a script in the current working directory.
    """

    debug_cmds = False

    def __init__(self, backend):
        """Create a new AsyncResult instance"""
        from . status import CANCELLED
        self.remote = None
        self.options = {}
        self.cache_file = None
        self.backend = backend
        self.sleep_interval = 10
        self.job_id = ''
        self._status = CANCELLED
        self.epilogue = None

    @property
    def status(self):
        """Return the job status as one of the codes defined in the
        `clusterjob.status` module.
        finished, communicate with the cluster to determine the job's status.
        """
        from . status import COMPLETED, STATUS_CODES
        if self._status >= COMPLETED:
            return self._status
        else:
            from . utils import run_cmd
            cmd_status, status_reader = self.backend['cmd_status_running']
            cmd = cmd_status(self.job_id)
            response = run_cmd(cmd, self.remote, ignore_exit_code=True,
                               debug=self.debug_cmds)
            status = status_reader(response)
            if status is None:
                cmd_status, status_reader = self.backend['cmd_status_finished']
                cmd = cmd_status(self.job_id)
                response = run_cmd(cmd, self.remote, ignore_exit_code=True,
                                   debug=self.debug_cmds)
                status = status_reader(response)
            prev_status = self._status
            self._status = status
            if not self._status in STATUS_CODES:
                raise ValueError("Invalid status code %s", self._status)
            if prev_status != self._status:
                if self._status >= COMPLETED:
                    self.run_epilogue()
                self.dump()
            return self._status

    def get(self, timeout=None):
        """Return status"""
        from . status import COMPLETED
        status = self.status
        if status >= COMPLETED:
            return status
        else:
            self.wait(timeout)
            return self.status

    def dump(self, cache_file=None):
        """Write dump out to file"""
        if cache_file is None:
            cache_file = self.cache_file
        if cache_file is not None:
            self.cache_file = cache_file
            with open(cache_file, 'wb') as pickle_fh:
                pickle.dump((self.remote, self.options, self.sleep_interval,
                             self.job_id, self._status, self.epilogue),
                            pickle_fh)

    def load(self, cache_file):
        """Read dump from file"""
        self.cache_file = cache_file
        with open(cache_file, 'rb') as pickle_fh:
            self.remote, self.options, self.sleep_interval, self.job_id, \
            self._status, self.epilogue = pickle.load(pickle_fh)


    def wait(self, timeout=None):
        """Wait until the result is available or until roughly timeout seconds
        pass."""
        from . status import COMPLETED
        import time
        spent_time = 0
        sleep_seconds = int(self.sleep_interval)
        while self.status < COMPLETED:
            time.sleep(sleep_seconds)
            spent_time += sleep_seconds
            if timeout is not None:
                if spent_time > timeout:
                    return

    def ready(self):
        """Return whether the job has completed."""
        from . status import COMPLETED
        return (self.status >= COMPLETED)

    def successful(self):
        """Return True if the job finished with a COMPLETED status, False if it
        finished with a CANCELLED or FAILED status. Raise an AssertionError if
        the job has not completed"""
        from . status import COMPLETED
        status = self.status
        assert status >= COMPLETED, "status is %s" % status
        return (self.status == COMPLETED)

    def cancel(self):
        """Instruct the cluster to cancel the running job. Has no effect if
        job is not running"""
        from . status import CANCELLED, COMPLETED
        from . utils import run_cmd
        if self.status > COMPLETED:
            return
        cmd_cancel = self.backend['cmd_cancel']
        cmd = cmd_cancel(self.job_id)
        run_cmd(cmd, self.remote, ignore_exit_code=True,
                debug=self.debug_cmds)
        self._status = CANCELLED
        self.dump()

    def run_epilogue(self):
        """
        Run the epilogue script in the current working directory.

        Raise sp.CalledProcessError if the script does not finish with with
        exit code zero.
        """
        if self.epilogue is not None:
            tempfilename = tempfile.mkstemp()[1]
            with open(tempfilename, 'w') as epilogue_fh:
                epilogue_fh.write(self.epilogue)
            set_executable(tempfilename)
            try:
                sp.check_output( [tempfilename, ], stderr=sp.STDOUT)
            except sp.CalledProcessError as e:
                print dedent(r'''
                Epilogue script did not exit cleanly.
                CWD: {cwd}
                epilogue: ---
                {epilogue}
                ---
                response: ---
                {response}
                ---
                ''').format(cwd=os.getcwd(), epilogue=self.epilogue,
                            response=e.output)
                raise
            finally:
                os.unlink(tempfilename)

