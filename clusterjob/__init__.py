"""
Abstract description of a Cluster Job

To see status messages, set

    import logging
    logging.basicConfig(level=logging.DEBUG)
"""
from __future__ import absolute_import

__version__ = "2.0.0-dev"

import os
import subprocess as sp
import tempfile
try:
    import cPickle as pickle
except ImportError:
    import pickle
from glob import glob
from textwrap import dedent
from collections import OrderedDict
import logging
import importlib
import pprint
import pkgutil
import time

import clusterjob.backends
from .status import (STATUS_CODES, COMPLETED, FAILED, CANCELLED, PENDING,
    str_status)
from .utils import set_executable, run_cmd, mkdir, time_to_seconds
from .backends import check_backend

class JobScript(object):
    """
    Class Attributes
    ----------------

    backends: dict
        Available backends. Maps backend name to a dictionary of backend
        options. See the documentation of `clusterjob.backends.slurm`, for
        details. User-defined backends may be added with the `register_backend`
        class method

    cache_folder: str or None
        Local folder in which to cache the AsyncResult instances resulting from
        job submission. If None (default), caching is disabled.

    cache_prefix: str
        Prefix for cache filenames. If caching is enabled, jobs will be stored
        inside `cachefolder` in a file `cache_prefix`.`cache_id`.cache, where
        `cache_id` is defined in the `submit` method.

    debug_cmds: boolean
        If set to True, write debug information about all external commands
        (`utils.run_cmd` calls) to stdout.

    resources: OrderedDict
        Dictionary of *default* resource requirements. Modifying the
        `resources` class attribute affects the default resources for all
        future instantiations.

    Attributes
    ----------

    The following are class attributes, with the expectation that they may be
    shadowed by instance attributes of the same name. This allows to define
    defaults for all jobs by setting the class attribute, and overriding
    them for specific jobs by setting the instance attribute.
    For example,

    >>> jobscript = JobScript(body='echo "Hello"', jobname='test')
    >>> jobscript.shell = '/bin/sh'

    sets the shell for only this specific jobscript, whereas

    >>> JobScript.shell = '/bin/sh'

    sets the class attribute, and thus the default shell for *all* JobScript
    instances, both future and existing instantiation:

    >>> job1 = JobScript(body='echo "Hello"', jobname='test1')
    >>> job2 = JobScript(body='echo "Hello"', jobname='test2')
    >>> assert job1.shell == job2.shell == '/bin/sh'   # class attribute
    >>> JobScript.shell = '/bin/bash'
    >>> assert job1.shell == job2.shell == '/bin/bash' # class attribute
    >>> job1.shell = '/bin/sh'
    >>> assert job1.shell == '/bin/sh'                 # instance attribute
    >>> assert job2.shell == '/bin/bash'               # class attribute

    backend: str
        Name of backend, must be a key in the backends class dictionary.
        Defaults to 'slurm'.

    shell: str
        Shell that is used to execute runscript.  Defaults to '/bin/bash'.

    remote: str or None
        Remote server on which to execute submit commands. If None (default),
        submit locally.

    rootdir: str
        Root directory for workdir, locally or remote. Defaults to '', i.e.,
        the current working directory. The rootdir is guaranteed not to have a
        trailing slash.

    workdir: str
        Work directory (local or remote) in which the job script file will be
        placed, and from which the submission command will be called. Relative
        to `rootdir`. Defaults to '' (current working directory). The workdir
        is guaranteed not to have a trailing slash.

    filename: str or None
        Name of file to which the job script will be written (inside
        rootdir/workdir).  If None (default), the filename will be
        set from the job name (`resources['jobname']` attribute)
        together with a backend-specific file extension

    prologue: str
        multiline shell script that will be executed *locally* in the current
        working directory before submitting the job. Before running, the script
        will be rendered using the `render_script` method.
        A common purpose of the prologue script is to move data to a remote
        cluster, e.g. via the commands

            ssh {remote} 'mkdir -p {rootdir}/{workdir}'
            rsync -av {workdir}/ {remote}:{rootdir}/{workdir}

    epilogue: str
        multiline shell script that will be executed *locally* in the current
        working directory the first time that the job is known to have
        finished. It will be rendered using the `render_script` method at the
        time that the job is submitted.  It's execution will be handled by the
        AsyncResult object resulting from the job submission. The main purpose
        of the epilogue script is to move data from a remote cluster upon
        completion of the job.

    sleep_interval: int or None
        Value for the `sleep_interval` attribute of the AsyncResult instance
        that is created upon submission. If None, the value for that attribute
        will be automatically determined between 10 and 1800 seconds, depending
        on the projected runtime of the job.

    Instance Attributes
    -------------------

    The following attributes are local to any JobScript instance.

    body: str
        Multiline string of shell commands. Should not contain backend-specific
        resource headers. Before submission, it will be rendered using the
        `render_script` method.

    resources: dict
        Dictionary of submission options describing resource requirements. Set
        on instantiation, based on the default values in the `resources` class
        attribute and the keyword arguments passed to the instantiator.


    Examples
    --------

    >>> body = r'''
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
    >>> jobscript = JobScript(body, backend='slurm', jobname='printenv',
    ... queue='test', time='00:05:00', nodes=1, threads=1, mem=100,
    ... stdout='printenv.out', stderr='printenv.err')
    >>> print(jobscript)
    #!/bin/bash
    #SBATCH --job-name=printenv
    #SBATCH --mem=100
    #SBATCH --nodes=1
    #SBATCH --partition=test
    #SBATCH --error=printenv.err
    #SBATCH --output=printenv.out
    #SBATCH --cpus-per-task=1
    #SBATCH --time=00:05:00
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

    Python's ability to add arbitrary attributes to an existing object together
    with the formatting step in rendering the job script allow for a a powerful
    (but hacky) way to use arbitrary template variables in the job script:

    >>> body = r'''
    ... echo {myvar}
    ... '''
    >>> jobscript = JobScript(body, jobname='myvar_test')
    >>> jobscript.myvar = 'Hello'
    >>> print(jobscript)
    #!/bin/bash
    #SBATCH --job-name=myvar_test
    <BLANKLINE>
    echo Hello
    <BLANKLINE>
    """

    # the following class attribute are fall-backs for intended instance
    # attributes. That is, if there is an instance attribute of the same name
    # shadowing the class attribute, the instance attribute is used in any
    # context
    _attributes = ['backend', 'shell', 'remote', 'rootdir', 'workdir',
            'filename', 'prologue', 'epilogue', 'sleep_interval']
    backend = 'slurm'
    shell = '/bin/bash'
    remote = None
    rootdir = ''
    workdir = ''
    filename = None
    prologue = ''
    epilogue = ''
    sleep_interval = None

    # the following are genuine class attributes:
    _protected_attributes = ['backends', 'debug_cmds', 'cache_folder',
            'cache_prefix', '_cache_counter', '_run_cmd']
    # Trying to create an instance  attribute of the same name will raise an
    # AttributeError.
    backends = {}
    debug_cmds = False
    cache_folder = None
    cache_prefix = 'clusterjob'
    _cache_counter = 0
    _run_cmd = run_cmd # to allow easy mocking

    # the `resources` class attribute is copied into an instance attribute on
    # every instantiation
    resources = OrderedDict()

    @classmethod
    def register_backend(cls, backend):
        """Register a new backend

        `backend` must be a dictionary that follows the same structure as
        `clusterjob.backends.slurm.backend`. If the dictionary is found to have
        the wrong structure, an AssertionError will be raised.
        """
        logger = logging.getLogger(__name__)
        try:
            if check_backend(backend):
                cls.backends[backend['name']] = backend
        except AssertionError as e:
            pp = pprint.PrettyPrinter(indent=4)
            logger.error("Invalid backend:\n%s\n\n%s", pp.pformat(backend), e)

    @classmethod
    def clear_cache_folder(cls):
        """Remove all files in the cache_folder"""
        if cls.cache_folder is not None:
            for file in glob(os.path.join(cls.cache_folder, '*')):
                os.unlink(file)

    def __init__(self, body, jobname, **kwargs):
        """
        Arguments
        ---------

        body: str
            Body (template) for the jobscript as multiline string

        jobname: str
            Name of the job


        Keyword Arguments
        -----------------

        For arguments listed in the "Attributes" section of the JobScript class
        docstring, create an instance attribute with the corresponding vlaue.

        For all other keyword arguments, store a value in the `resources`
        instance attribute. The accepted keywords depend on the backend. At a
        minimum, the following keywords are supported (common to all backends):

        queue: str
            Name of queue/partition to which to submit the job

        time: str
            Maximum runtime

        nodes: int
            Required number of nodes

        threads: int
            Required number of threads (cores)

        mem: int
            Required memory (MB)

        stdout: str
            name of file to which to write the jobs stdout

        stderr: str
            name of file to which to write the jobs stderr

        Some backends may define further options, or even support arbitrary
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
        self.resources = self.__class__.resources.copy()
        self.resources['jobname'] = jobname

        self.body = body

        if len(self.backends) == 0:
            # register all available backends
            for __, module_name, __ \
            in pkgutil.walk_packages(clusterjob.backends.__path__):
                mod = importlib.import_module(
                      'clusterjob.backends.%s' % module_name)
                self.register_backend(mod.backend)
            # perform some consistency checks
            for attr in self.__class__._attributes:
                assert attr in self.__class__.__dict__
            for attr in self.__class__._protected_attributes:
                assert attr in self.__class__.__dict__

        # There is no way to preserve the order of the kwargs, so we sort them
        # to at least guarantee a stable behavior
        for kw in sorted(kwargs):
            if kw in self.__class__._attributes:
                # We define an instance attribute that shadows the underlying
                # class attribute
                self.__setattr__(kw, kwargs[kw])
            else:
                self.resources[kw] = kwargs[kw]

    def __setattr__(self, name, value):
        """Set attributes while preventing shadowing the "genuine" class
        attributes by raising an AttributeError. Perform some checks on the
        value, raising a ValueError if necessary."""
        if name in self.__class__._protected_attributes:
            raise AttributeError("'%s' can only be set as a class attribute"
                                 % name)
        else:
            if name == 'backend':
                if not value in self.backends:
                    raise ValueError("Unknown backend %s" % value)
            elif value in ['rootdir', 'workdir']:
                value = value.strip()
                if value.endswith('/'):
                    value = value[:-1] # strip trailing slash
            self.__dict__[name] = value

    def _default_filename(self):
        """If self.filename is None, attempt to set it from the jobname"""
        if self.filename is None:
            if 'jobname' in self.resources:
                self.filename = "%s.%s" \
                                 % (self.resources['jobname'],
                                    self.backends[self.backend]['extension'])

    def render_script(self, scriptbody, jobscript=False):
        """Render the body of a script. This brings both the main JobScript
        body, as well as the prologue and epilogue scripts, into the final form
        in which they will be executed.

        Rendering proceeds in the following steps:
        * Add a shebang (e.g. "#!/bin/bash", based on the `shell` attribute).
          Any existing shebang will be stripped out
        * If rendering the body of a JobScript (`jobscript=True`), add
          backedn-specific resource headers (based on the `resources`
          attribute)
        * Apply the mappings defined in the `job_vars` entry of the backend,
          replacing environement variables with their proper names. Note that
          the prologue and epilogue will not be run by a scheduler, and thus
          will not have access to the same environment variables as a job
          script.
        * Format each line with known attributes (see
          https://docs.python.org/3.5/library/string.html#formatspec).
          In order of precedence (highest to lowest), the following keys will
          be replaced:
          - keys in the `resources` attribute
          - instance attributes
          - class attributes
        """
        # add a shebang
        rendered_lines = []
        rendered_lines.append("#!%s" % self.shell)
        # add the resource headers
        if jobscript:
            opt_translator = self.backends[self.backend]['translate_resources']
            opt_array = opt_translator(self.resources)
            prefix = self.backends[self.backend]['prefix']
            for option in opt_array:
                rendered_lines.append("%s %s" % (prefix, option))
        # apply environment variable mappings
        var_replacements = self.backends[self.backend]['job_vars']
        for var in var_replacements:
            scriptbody = scriptbody.replace(var, var_replacements[var])
        # apply attribute mappings
        mappings = dict(self.__class__.__dict__)
        mappings.update(self.__dict__)
        mappings.update(self.resources)
        for line in scriptbody.split("\n"):
            if not line.startswith("#!"):
                rendered_lines.append(line.format(**mappings))
        return "\n".join(rendered_lines)

    def __str__(self):
        """String representation of the job, i.e., the fully rendered
        jobscript"""
        return self.render_script(self.body, jobscript=True)

    def write(self, filename=None):
        """Write out the fully rendered jobscript to file. If filename is not
        None, write to the given *local* file. Otherwise, write to the local or
        remote file specified in the filename attribute, in the folder
        specified by the rootdir and workdir attributes. The folder will be
        created if it does not exist already.
        """
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
        self._run_cmd(['mkdir', '-p', filepath], remote,
                      ignore_exit_code=False)

        # Write / Upload
        if remote is None:
            with open(filename, 'w') as run_fh:
                run_fh.write(str(self))
            set_executable(filename)
        else:
            with tempfile.NamedTemporaryFile('w', delete=False) as run_fh:
                run_fh.write(str(self))
                tempfilename = run_fh.name
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
            prologue = self.render_script(self.prologue)
            with tempfile.NamedTemporaryFile('w', delete=False) as prologue_fh:
                prologue_fh.write(prologue)
                tempfilename = prologue_fh.name
            set_executable(tempfilename)
            try:
                sp.check_output( [tempfilename, ], stderr=sp.STDOUT)
            except sp.CalledProcessError as e:
                logger = logging.getLogger(__name__)
                logger.error(r'''
                Prologue script did not exit cleanly.
                CWD: {cwd}
                prologue: ---
                {prologue}
                ---
                response: ---
                {response}
                ---
                '''.format(cwd=os.getcwd(), prologue=self.prologue,
                           response=e.output))
                raise
            finally:
                os.unlink(tempfilename)

    def submit(self, block=False, cache_id=None, force=False, retry=True):
        """Run the prologue script, then submit the job.

        Parameters
        ----------

        block: boolean, optional
            If `block` is True, wait until the job is finished, and return the
            exit status code. Otherwise, return an AsyncResult object.

        cache_id: str or None, optional
            An ID uniquely defining the submission, used as identifier for the
            cached AsyncResult object. If not given, the cache_id is determined
            internally. If an AsyncResult with a matching cache_id is present
            in the cache_folder, nothing is submitted to the scheduler, and the
            cached AsyncResult object is returned. The prologue script is not
            re-run when recovering a cached result.

        force: boolean, optional
            If True, discard any existing cached AsyncResult object, ensuring
            that the job is sent to the schedular.

        retry: boolean, optional
            If True, and the existing cached AsyncResult indicates that the job
            finished with an error (CANCELLED/FAILED), resubmit the job,
            discard the cache and return a fresh AsyncResult object
        """
        logger = logging.getLogger(__name__)
        assert self.filename is not None, 'jobscript must have a filename'
        if self.remote is None:
            logger.info("Submitting job %s locally",
                        self.resources['jobname'])
        else:
            logger.info("Submitting job %s on %s",
                        self.resources['jobname'], self.remote)

        submitted = False
        if cache_id is None:
            Job._cache_counter += 1
            cache_id = str(Job._cache_counter)
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
                    logger.debug("Reloading AsyncResult from %s", cache_file)
                    ar.load(cache_file)
                    submitted = True
                    if ar._status >= CANCELLED:
                        if retry:
                            logger.debug("Cached run %s, resubmitting",
                                         str_status[ar._status])
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
                job_id = id_reader(
                            self._run_cmd(cmd, self.remote, self.rootdir,
                                          self.workdir, ignore_exit_code=True))
                if job_id is None:
                    logger.error("Failed to submit job")
                    status = FAILED
                else:
                    logger.info("Job ID: %s", job_id)
                    status = PENDING
            except sp.CalledProcessError as e:
                logger.error("Failed to submit job: %s", e)
                status = FAILED

            ar.remote = self.remote
            ar.resources = self.resources.copy()
            ar.cache_file = cache_file
            ar.backend = self.backends[self.backend]
            if self.sleep_interval is not None:
                ar.sleep_interval = self.sleep_interval
            else:
                try:
                    ar.sleep_interval \
                    = int(time_to_seconds(self.resources['time']) / 10)
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
    Result of submitting a jobscript

    Attributes
    ----------

    remote: str or None
        The remote host on which the job is running. Passwordless ssh must be
        set up to reach the remote. A value of None indicates that the job is
        running locally

    resources: dict
        copy of the `resources` attribute of the JobScript() instance that
        created the AsyncResult object

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
        (pending/running) to "not running" (completed, canceled, failed).
        The contents of this variable will be written to a temporary file as
        is, and executed as a script in the current working directory.
    """

    debug_cmds = False

    def __init__(self, backend):
        """Create a new AsyncResult instance"""
        self.remote = None
        self.resources = {}
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
        if self._status >= COMPLETED:
            return self._status
        else:
            cmd_status, status_reader = self.backend['cmd_status_running']
            cmd = cmd_status(self.job_id)
            response = self._run_cmd(cmd, self.remote, ignore_exit_code=True)
            status = status_reader(response)
            if status is None:
                cmd_status, status_reader = self.backend['cmd_status_finished']
                cmd = cmd_status(self.job_id)
                response = self._run_cmd(cmd, self.remote,
                                         ignore_exit_code=True)
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
                pickle.dump((self.remote, self.resources, self.sleep_interval,
                             self.job_id, self._status, self.epilogue),
                            pickle_fh)

    def load(self, cache_file):
        """Read dump from file"""
        self.cache_file = cache_file
        with open(cache_file, 'rb') as pickle_fh:
            self.remote, self.resources, self.sleep_interval, self.job_id, \
            self._status, self.epilogue = pickle.load(pickle_fh)


    def wait(self, timeout=None):
        """Wait until the result is available or until roughly timeout seconds
        pass."""
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
        return (self.status >= COMPLETED)

    def successful(self):
        """Return True if the job finished with a COMPLETED status, False if it
        finished with a CANCELLED or FAILED status. Raise an AssertionError if
        the job has not completed"""
        status = self.status
        assert status >= COMPLETED, "status is %s" % status
        return (self.status == COMPLETED)

    def cancel(self):
        """Instruct the cluster to cancel the running job. Has no effect if
        job is not running"""
        if self.status > COMPLETED:
            return
        cmd_cancel = self.backend['cmd_cancel']
        cmd = cmd_cancel(self.job_id)
        self._run_cmd(cmd, self.remote, ignore_exit_code=True)
        self._status = CANCELLED
        self.dump()

    def run_epilogue(self):
        """
        Run the epilogue script in the current working directory.

        Raise sp.CalledProcessError if the script does not finish with
        exit code zero.
        """
        logger = logging.getLogger(__name__)
        if self.epilogue is not None:
            with tempfile.NamedTemporaryFile('w', delete=False) as epilogue_fh:
                epilogue_fh.write(self.epilogue)
                tempfilename = epilogue_fh.name
            set_executable(tempfilename)
            try:
                sp.check_output( [tempfilename, ], stderr=sp.STDOUT)
            except sp.CalledProcessError as e:
                logger.error(dedent(r'''
                Epilogue script did not exit cleanly.
                CWD: {cwd}
                epilogue: ---
                {epilogue}
                ---
                response: ---
                {response}
                ---
                ''').format(cwd=os.getcwd(), epilogue=self.epilogue,
                            response=e.output))
                raise
            finally:
                os.unlink(tempfilename)

