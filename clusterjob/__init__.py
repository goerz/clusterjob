"""Abstraction for job scripts and cluster schedulers, for a variety of
scheduling backends (e.g., SLURM, PBS/TORQUE, ...)

Note:
    To see debug messages, set::

        import logging
        logging.basicConfig(level=logging.DEBUG)
"""
from __future__ import absolute_import

__version__ = "2.0.0-dev"

import os
import tempfile
import re
try:
    import cPickle as pickle
except ImportError:
    import pickle
try:
    from ConfigParser import ConfigParser
    from ConfigParser import Error as ConfigParserError
except ImportError:
    # Python 3
    from configparser import ConfigParser
    from configparser import Error as ConfigParserError
import subprocess as sp
from glob import glob
from textwrap import dedent
from collections import OrderedDict, defaultdict
import logging
import importlib
import pprint
import pkgutil
import time

import clusterjob.backends
from .status import (STATUS_CODES, COMPLETED, FAILED, CANCELLED, PENDING,
        str_status)
from .utils import (set_executable, run_cmd, upload_file, mkdir,
        time_to_seconds)
from .backends import check_backend


def _init_with_read_defaults(cls):
    """Class decorator that calls the read_defaults class method in order to
    set default values for class attributes"""
    cls.read_defaults(filename=None)
    return cls

def _init_default_backends(cls):
    """Register all backends defined in `clusterjob.backends`"""
    for __, module_name, __ \
    in pkgutil.walk_packages(clusterjob.backends.__path__):
        mod = importlib.import_module(
                'clusterjob.backends.%s' % module_name)
        cls.register_backend(mod.backend)
    return cls


@_init_default_backends
@_init_with_read_defaults
class JobScript(object):
    """Encapsulation of a job script

    Arguments:
        body (str): Body template for the jobscript as multiline string.
            Will be stored in the `body` instance attribute, and processed by
            the :meth:`render_script` method before execution.
        jobname (str): Name of the job. Will be stored in the
            `resources['jobname']` instance attribute.
        aux_scripts (dict(str=>str), optional): dictionary of auxiliary
            scripts, to be stored in the `aux_scripts` attribute.

    The _keyword arguments, if given, either set values for keys in the
    `resources` instance attribute, or values for instance attributes directly.
    For the former use of setting `resources`, at a minimum, the following
    _`keyword arguments` are supported:

    Keyword Arguments:
        queue (str):   Name of queue/partition to which to submit the job
        time (str):    Maximum runtime
        nodes (int):   Required number of nodes
        threads (int): Required number of threads (cores)
        mem (int):     Required memory (MB)
        stdout (str):  name of file to which to write the jobs stdout
        stderr (str):  name of file to which to write the jobs stderr

    Some backends may define further options, or even support arbitrary
    additional options. For example, in the default SLURM backend,
    unknown options are passed directly as arguments to ``sbatch``, where
    single-letter argument names are prepended with ``-``, multi-letter
    argument names with ``--``. An argument with boolean values is passed
    without any value iff the value is True::

        contiguous=True          -> --contiguous
        dependency='after:12454' -> --dependency=after:12454
        F='nodefile.txt'         -> -F nodefile.txt

    All backends are encouraged to implement a similar behavior.

    Any of the attributes listed in **Class/Instance Attributes** below may
    also be given as keyword arguments during instantiation, in order to
    initialize the corresponding instance attributes.

    .. rubric:: _`Class Attributes`

    The following class attributes cannot be shadowed by instance attributes
    of the same name (attempting to do so raises an `AttributeError`)

    Class Attributes:
        cache_folder (str or None): Local folder in which to cache the
            :class:`AsyncResult` instances resulting from job submission. If
            None (default), caching is disabled.
        cache_prefix (str): Prefix for cache filenames. If caching is enabled,
            jobs will be stored inside `cachefolder` in a file
            `cache_prefix`.`cache_id`.cache, where `cache_id` is defined in the
            `submit` method.
        debug_cmds (boolean): If set to True, write debug information about all
            external commands (:func:`utils.run_cmd` calls) to stdout.

        resources (OrderedDict): Dictionary of *default* resource requirements.
            Modifying the `resources` class attribute affects the default
            resources for all future instantiations.

    Note:
        The preferred way to set these class attributes is through the
        :meth:`read_defaults` class method.

    .. rubric:: _`Class/Instance Attributes`

    The are class attributes, with the expectation that
    they may be shadowed by instance attributes of the same name.

    Attributes:

        backend (str): Name of backend, must be an element in
            :attr:`JobScript.backends`. That is, `backend` does not refer to
            one of the default backends, the :meth:`register_backend` class
            method must be used to register the backend before any job may use
            it. Defaults to 'slurm'.
        shell (str): Shell that is used to execute runscript.  Defaults to
            ``/bin/bash``.
        remote (str or None): Remote server on which to execute submit
            commands. If None (default), submit locally.
        rootdir (str): Root directory for `workdir`, locally or remote. Defaults
            to ``'.'``, i.e., the current working directory. The `rootdir` is
            guaranteed not to have a trailing slash.
        workdir (str): Work directory (local or remote) in which the job script
            file will be placed, and from which the submission command will be
            called. Relative to `rootdir`. Defaults to ``'.'`` (current working
            directory). The `workdir` is guaranteed not to have a trailing
            slash.
        filename (str or None): Name of file to which the job script will be
            written (inside `rootdir`/`workdir`).  If None (default), the
            filename will be set from the job name (`resources['jobname']`
            attribute) together with a backend-specific file extension
        prologue (str): Multiline shell script that will be executed *locally*
            in the current working directory before submitting the job. Before
            running, the script will be rendered using the
            :meth:`render_script` method.
        epilogue (str): multiline shell script that will be executed *locally*
            in the current working directory the first time that the job is
            known to have finished. It will be rendered using the
            :meth:`render_script` method at the time that the job is
            submitted.  It's execution will be handled by the
            :class:`AsyncResult` object resulting from the job submission. The
            main purpose of the epilogue script is to move data from a remote
            cluster upon completion of the job.
        max_sleep_interval (int): Upper limit for the number of seconds to
            sleep between polling the status of a submitted job.
        ssh (str): The executable to use for ssh. If not a full path, must be
            in the ``$PATH``.
        scp (str): The executable to use for scp. If not a full path, must be
            in the ``$PATH``.

    This allows to define defaults for all jobs by setting the class attribute,
    and overriding them for specific jobs by setting the instance attribute.
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

    Note:

        * The preferred way to set these  attributes as class attributes (i.e.,
          to provide defaults for any instance) is through
          the :meth:`read_defaults` class method. To set them as instance
          attributes, or to set values in the `resources` instance attribute
          defined below, the :meth:`read_settings` method should be used.

        * A common purpose of the `prologue` and `epilogue` scripts is
          to move data to a remote cluster, e.g. via the `prologue` commands::

                ssh {remote} 'mkdir -p {rootdir}/{workdir}'
                rsync -av {workdir}/ {remote}:{rootdir}/{workdir}

    .. rubric:: _`Instance Attributes`

    The following attributes are local to any `JobScript` instance, and are set
    automatically during instantiation.

    Attributes:
        body (str): Multiline string of shell commands. Should not contain
            backend-specific resource headers. Before submission, it will be
            rendered using the :meth:`render_script` method.
        resources (dict): Dictionary of submission options describing resource
            requirements. Set on instantiation, based on the default values in
            the `resources` class attribute and the keyword arguments passed to
            the instantiator.
        aux_scripts (dict(str=>str)): Dictionary mapping filenames to script
            bodies for any auxiliary scripts. As the main job script (`body`)
            is written during submission, any script defined in this
            dictionary will also be rendered using the :meth:`render_script`
            method and will be written in the same folder as the main script.
            While generally not needed, auxiliary scripts may be useful in
            structuring a large job.

    Example:

        >>> body = r'''
        ... echo "####################################################"
        ... echo "Job id: $CLUSTERJOB_ID"
        ... echo "Job name: $CLUSTERJOB_WORKDIR"
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

    Note:
        The fact that arbitrary attributes can be added to an existing
        object can be exploited to define arbitrary template variables in
        the job script:

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
    _attributes = {
        'backend': 'slurm',
        'shell': '/bin/bash',
        'remote': None,
        'rootdir': '.',
        'workdir': '.',
        'filename': None,
        'prologue': '',
        'epilogue': '',
        'max_sleep_interval': 900,
        'ssh': 'ssh',
        'scp': 'scp',
    }

    # the following are genuine class attributes:
    _protected_attributes = {
        '_backends': {},
        'debug_cmds': False,
        'cache_folder': None,
        'cache_prefix': 'clusterjob',
        '_cache_counter': 0,
        '_run_cmd': staticmethod(run_cmd),          # for easy mocking
        '_upload_file': staticmethod(upload_file),  # for easy mocking
    }
    # Trying to create an instance  attribute of the same name will raise an
    # AttributeError.

    # The class attributes listed in _attributes and _protected_attributes are
    # created and initialized by the _init_with_read_defaults class decorator,
    # which runs the `read_defaults` class method with `filename=None` after
    # the class definition was processed.

    # the `resources` class attribute is copied into an instance attribute on
    # every instantiation
    resources = OrderedDict()

    @classmethod
    def register_backend(cls, backend):
        """Register a new backend.

        The `backend` argument must be a dictionary that follows the
        :ref:`structure <backend dictionary>` described in the
        :mod:`clusterjob.backends` documentation. If the dictionary is
        found to have the wrong structure, an `AssertionError`
        will be raised.
        """
        logger = logging.getLogger(__name__)
        try:
            inst = cls(body='', jobname='xxx')
            if check_backend(backend, inst):
                cls._backends[backend['name']] = backend
                logger.debug("Registered backend '%s'" % backend['name'])
        except AssertionError as e:
            pp = pprint.PrettyPrinter(indent=4)
            logger.error("Invalid backend:\n%s\n\n%s", pp.pformat(backend), e)

    @classmethod
    def clear_cache_folder(cls):
        """Remove all files in the :attr:`cache_folder`"""
        if cls.cache_folder is not None:
            for file in glob(os.path.join(cls.cache_folder, '*')):
                os.unlink(file)

    def __init__(self, body, jobname, aux_scripts=None, **kwargs):
        self.resources = self.__class__.resources.copy()
        self.resources['jobname'] = str(jobname)

        self.body = str(body)

        self.aux_scripts = {}
        if aux_scripts is not None:
            self.aux_scripts = {}

        # There is no way to preserve the order of the kwargs, so we sort them
        # to at least guarantee a stable behavior
        for kw in sorted(kwargs):
            if kw in self.__class__._attributes:
                # We define an instance attribute that shadows the underlying
                # class attribute
                self.__setattr__(kw, kwargs[kw])
            else:
                self.resources[kw] = kwargs[kw]

    @property
    def backends(self):
        """List of names of registered backends"""
        return sorted(self._backends.keys())

    def __setattr__(self, name, value):
        """Set attributes while preventing shadowing the "genuine" class
        attributes by raising an AttributeError. Perform some checks on the
        value, raising a ValueError if necessary."""
        if name in self.__class__._protected_attributes:
            raise AttributeError("'%s' can only be set as a class attribute"
                                 % name)
        else:
            self.__dict__[name] = self._sanitize_attr(name, value)

    @classmethod
    def _sanitize_attr(cls, name, value):
        if name == 'backend':
            if not value in cls._backends:
                raise ValueError("Unknown backend %s" % value)
        elif name in ['rootdir', 'workdir']:
            value = value.strip()
            if value.endswith('/'):
                value = value[:-1] # strip trailing slash
        elif name in ['prologue', 'epilogue']:
            if value is None:
                raise ValueError('prologue and epilogue must be strings, '
                                 'not None')
            value = dedent(value).strip()
        return value

    @classmethod
    def read_defaults(cls, filename=None):
        """Set class attributes from the INI file with the given file name

        The file must be in the format specified in
        https://docs.python.org/3.5/library/configparser.html#supported-ini-file-structure
        with the default ConfigParser settings. It must contain exactly one or
        both of the sections "Attributes" and "Resources" (case sensitive). The
        key-value pairs in the Attributes sections are set as class attributes,
        whereas the key-value pairs in the "Resources" section are set as keys
        and values in the `resources` class attribute.

        All keys must be start with a letter, and must consist only of letters,
        numbers, and underscores. Keys are case-insensitive, and are converted
        to lower case. The key names 'resources' and 'backends' may not be
        used. An example for a valid config file is::

            [Attributes]
            remote = login.cluster.edu
            prologue =
                ssh {remote} 'mkdir -p {rootdir}/{workdir}'
                rsync -av {workdir}/ {remote}:{rootdir}/{workdir}
            epilogue = rsync -av {remote}:{rootdir}/{workdir}/ {workdir}
            rootdir = ~/jobs/
            # the following is a new attribute
            text = Hello World

            [Resources]
            queue = exec
            nodes = 1
            threads = 1
            mem = 10

        If no filename is given, reset all class attributes to their initial
        value, and delete any attributes that do not exist by default. This
        restores the JobScript class to a pristine state.
        """
        logger = logging.getLogger(__name__)
        def attr_setter(key, val):
            val = cls._sanitize_attr(key, val)
            logger.debug("Set class attribute %s = %s",  key, val)
            setattr(cls, key, val)
        def rsrc_setter(key, val):
            logger.debug("Set class resources key %s = %s", key, val)
            cls.resources[key] = val
        if filename is None:
            # restore the original class attributes
            known_attrs = set.union(set(cls._attributes.keys()),
                                    set(cls._protected_attributes.keys()),
                                    set(['backends', ]) )
            for attr in list(cls.__dict__.keys()):
                if ((not attr.startswith('_'))
                and (attr not in known_attrs)
                and (not callable(getattr(cls, attr)))):
                    logger.debug("Removing class attribute '%s'", attr)
                    delattr(cls, attr)
            for attr in cls._attributes:
                logger.debug("Set class attribute '%s' to original value '%s'",
                             attr, cls._attributes[attr])
                setattr(cls, attr, cls._attributes[attr])
            for attr in cls._protected_attributes:
                # For the '_backends' attribute, the setattr below sets
                # cls._backends to a *reference* to
                # cls._protected_attributes['_backends'], not a copy. As a
                # consequence, any call to register_backend will modify both
                # locations, and we don't lose registered backends when
                # resetting.
                if attr == '_backends':
                    logger.debug("Keeping known backends: %s", list(
                            cls._protected_attributes['_backends'].keys()))
                else:
                    logger.debug("Set class attribute '%s' to original value "
                                 "'%s'", attr, cls._protected_attributes[attr])
                setattr(cls, attr, cls._protected_attributes[attr])
            cls.resources = OrderedDict()
            logger.debug("Set class attribute 'resources' to original value "
                         "OrderedDict()")
        else:
            cls._read_inifile(filename, attr_setter, rsrc_setter)

    def read_settings(self, filename):
        """Set instance attribute from the INI file with the given file name

        This method behaves exactly like the :meth:`read_defaults` class
        method, but instead of setting class attributes, it sets instance
        attributes ("Attributes" section in the INI file), and instead of
        setting values in :attr:`JobScript.resources`, it sets values in the
        instance's `resources` dictionary ("Resources" section in the INI
        file).
        """
        logger = logging.getLogger(__name__)
        def attr_setter(key, val):
            logger.debug("Set instance attribute %s = %s",  key, val)
            self.__setattr__(key, val)
        def rsrc_setter(key, val):
            logger.debug("Set instance resource key %s = %s",  key, val)
            self.resources[key] = val
        self._read_inifile(filename, attr_setter, rsrc_setter)

    @staticmethod
    def _read_inifile(filename, attr_setter, rsrc_setter):
        logger = logging.getLogger(__name__)
        config = ConfigParser()
        with open(filename) as in_fh:
            config.readfp(in_fh)
        setters = { # section name => where to store keys/values
            'Attributes': attr_setter,
            'Resources':  rsrc_setter,
        }
        readers = {
            # for values that are not strings, be must specify a reader
            'Attributes': defaultdict(lambda:config.get,
                {'debug_cmds': config.getboolean,
                 'max_sleep_interval': config.getint,
                }
            ),
            'Resources': defaultdict(lambda:config.get,
                {'nodes': config.getint,
                 'threads': config.getint,
                 'mem': config.getint,
                }
            ),
        }
        allowed_sections = sorted(setters.keys())
        if len(config.sections()) == 0:
            raise ConfigParserError("Inifile must contain at least one "
            "of the sections "+str(allowed_sections))
        illegal_keys = ['resources', 'backends']
        for section in config.sections():
            logger.debug("Processing section %s in %s", section, filename)
            if section not in allowed_sections:
                raise ConfigParserError("Invalid section '%s' in %s. "
                "Allowed sections are %s" % (section, filename,
                allowed_sections))
            for key, __ in config.items(section=section):
                if not re.match(r'^[a-zA-Z]\w*$', key):
                    raise ConfigParserError(("Key '%s' is invalid. Keys "
                    "must be valid attribute names, i.e., they must match "
                    "the regular expression '^[a-zA-Z]\w*$'") % key)
                if key in illegal_keys:
                    raise ConfigParserError("Keys %s are not allowed"
                                            % str(illegal_keys))
                setters[section](key, readers[section][key](section, key))

    def _default_filename(self):
        """If self.filename is None, attempt to set it from the jobname"""
        if self.filename is None:
            if 'jobname' in self.resources:
                self.filename = "%s.%s" \
                                 % (self.resources['jobname'],
                                    self._backends[self.backend]['extension'])

    def render_script(self, scriptbody, jobscript=False):
        """Render the body of a script. This brings both the main `body`, as
        well as the `prologue`, `epilogue`, and any auxiliary scripts into the
        final form in which they will be executed.

        Rendering proceeds in the following steps:

        * Add a "shbang" (e.g. ``#!/bin/bash``, based on the `shell` attribute).
          Any existing shbang will be stripped out

        * If rendering the body of a JobScript (`jobscript=True`), add
          backend-specific resource headers (based on the `resources`
          attribute)

        * Apply the mappings defined in the `job_vars` entry of the backend,
          replacing environment variables with their proper names. Note that
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
        # add a shbang
        rendered_lines = []
        rendered_lines.append("#!%s" % self.shell)
        # add the resource headers
        backend = self._backends[self.backend]
        if jobscript:
            opt_translator = backend['translate_resources']
            opt_array = opt_translator(self.resources)
            prefix = backend['prefix']
            for option in opt_array:
                rendered_lines.append("%s %s" % (prefix, option))
        # apply environment variable mappings
        var_replacements = backend['job_vars']
        for var in var_replacements:
            scriptbody = scriptbody.replace(var, var_replacements[var])
        # apply attribute mappings
        mappings = dict(self.__class__.__dict__)
        mappings.update(self.__dict__)
        mappings.update(self.resources)
        for line in scriptbody.split("\n"):
            if not line.startswith("#!"):
                try:
                    rendered_lines.append(line.format(**mappings))
                except KeyError as exc:
                    key = str(exc)[1:-1] # stripping out quotes
                    raise KeyError("The scriptbody contains a formatting "
                        "placeholder '{"+key+"}', but there is no matching "
                        "attribute or resource entry")
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
        created if it does not exist already. A '~' in `filename` will be
        expanded to the user's home directory.
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
        if remote is None:
            filename = os.path.expanduser(filename)
        self._write_script(str(self), filename, remote)

    def _write_script(self, scriptbody, filename, remote):
        filepath = os.path.split(filename)[0]
        if len(filepath) > 0:
            self._run_cmd(['mkdir', '-p', filepath], remote,
                        ignore_exit_code=False, ssh=self.ssh)
        if remote is None:
            with open(filename, 'w') as run_fh:
                run_fh.write(scriptbody)
            set_executable(filename)
        else:
            with tempfile.NamedTemporaryFile('w', delete=False) as run_fh:
                run_fh.write(scriptbody)
                tempfilename = run_fh.name
            set_executable(tempfilename)
            try:
                self._upload_file(tempfilename, remote, filename, scp=self.scp)
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
        """Run the :attr:`prologue` script (if defined), then submit the job to
        a local or remote scheduler.

        Parameters
        ----------

        block: boolean, optional
            If `block` is True, wait until the job is finished, and return the
            exit status code (see :mod:`clusterjob.status`). Otherwise, return
            an :class:`AsyncResult` object.

        cache_id: str or None, optional
            An ID uniquely defining the submission, used as identifier for the
            cached :class:`AsyncResult` object. If not given, the `cache_id` is
            determined internally. If an :class:`AsyncResult` with a matching
            `cache_id` is present in the :attr:`cache_folder`, nothing is
            submitted to the scheduler, and the cached :class:`AsyncResult`
            object is returned. The :attr:`prologue` script is not re-run when
            recovering a cached result.

        force: boolean, optional
            If True, discard any existing cached :class:`AsyncResult` object,
            ensuring that the job is sent to the scheduler.

        retry: boolean, optional
            If True, and the existing cached :class:`AsyncResult` indicates
            that the job finished with an error (``CANCELLED``/``FAILED``),
            resubmit the job, discard the cache and return a fresh
            :class:`AsyncResult` object
        """
        logger = logging.getLogger(__name__)
        if self.remote is None:
            logger.info("Submitting job %s locally",
                        self.resources['jobname'])
        else:
            logger.info("Submitting job %s on %s",
                        self.resources['jobname'], self.remote)

        submitted = False
        if cache_id is None:
            JobScript._cache_counter += 1
            cache_id = str(JobScript._cache_counter)
        else:
            cache_id = str(cache_id)
        cache_file = None

        backend = self._backends[self.backend]

        ar = AsyncResult(backend=backend)
        ar.debug_cmds = self.debug_cmds
        ar.ssh = self.ssh
        ar.scp = self.scp

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
                            AsyncResult(backend=backend)
                            ar.debug_cmds = self.debug_cmds
                            ar.ssh = self.ssh
                            ar.scp = self.scp
                            submitted = False

        if not submitted:
            self._run_prologue()
            cmd_submit, id_reader = backend['cmd_submit']
            self.write()
            for filename in self.aux_scripts:
                self._write_script(
                    scriptbody=self.render_script(self.aux_scripts[filename]),
                    filename=os.path.join(self.rootdir, self.workdir,
                                          filename),
                    remote=self.remote)
            job_id = None
            try:
                cmd = cmd_submit(self)
                response = self._run_cmd(cmd, self.remote, self.rootdir,
                                         self.workdir, ignore_exit_code=True,
                                         ssh=self.ssh)
                job_id = id_reader(response)
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
            ar.backend = backend
            try:
                ar.max_sleep_interval \
                = int(time_to_seconds(self.resources['time']) / 10)
                if ar.max_sleep_interval < 10:
                    ar.max_sleep_interval = 10
            except KeyError:
                ar.max_sleep_interval = self.max_sleep_interval
            if self.max_sleep_interval < ar.max_sleep_interval:
                ar.max_sleep_interval = self.max_sleep_interval
            ar._status = status
            ar.job_id = job_id
            if self.epilogue is not None:
                epilogue = self.render_script(self.epilogue)
                ar.epilogue = epilogue

        if block:
            result = ar.get()
        else:
            result = ar

        ar.dump()

        return result


class AsyncResult(object):
    """Result of submitting a jobscript

    Arguments:

        backend (dict): Value for the :attr:`backend` attribute

    Attributes:

        remote (str or None): The remote host on which the job is running.
            Passwordless ssh must be set up to reach the remote. A value of
            None indicates that the job is running locally

        resources (dict): copy of the `resources` attribute of the
            :class:`JobScript()` instance that created the `AsyncResult`
            object

        cache_file (str or None): The full path and name of the file to be used
            to cache the `AsyncResult` object. The cache file will be written
            automatically anytime a change in status is detected

        backend (dict): A reference to the backend options dictionary for the
            backend under which the job is running

        max_sleep_interval (int): Upper limit for the number of seconds to
            sleep between polls to the cluster scheduling systems when waiting
            for the Job to finish

        job_id (str): The Job ID assigned by the cluster scheduler

        epilogue (str): Multiline script to be run once when the status changes
            from "running" (pending/running) to "not running" (completed,
            canceled, failed).  The contents of this variable will be written
            to a temporary file as is, and executed as a script in the current
            working directory.

        ssh (str): The executable to use for ssh. If not a full path, must be
            in the ``$PATH``.

        scp (str): The executable to use for scp. If not a full path, must be
            in the ``$PATH``.
    """

    debug_cmds = False
    _run_cmd = staticmethod(run_cmd)
    # setting the sleep_interval < 1 can have some very problematic
    # consquences, so we build in a safety net.
    _min_sleep_interval = 1
    # For testing, we can still get around this under the assumption that we
    # know exactly what we're doing!

    def __init__(self, backend):
        self.remote = None
        self.resources = {}
        self.cache_file = None
        self.backend = backend
        self.max_sleep_interval = 160
        self.job_id = ''
        self._status = CANCELLED
        self.epilogue = None
        self.ssh = 'ssh'
        self.scp = 'scp'

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
            response = self._run_cmd(cmd, self.remote, ignore_exit_code=True,
                                     ssh=self.ssh)
            status = status_reader(response)
            if status is None:
                cmd_status, status_reader = self.backend['cmd_status_finished']
                cmd = cmd_status(self.job_id)
                response = self._run_cmd(cmd, self.remote,
                                         ignore_exit_code=True, ssh=self.ssh)
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
                pickle.dump((self.remote, self.resources,
                             self.max_sleep_interval, self.job_id,
                             self._status, self.epilogue, self.ssh, self.scp),
                            pickle_fh)

    def load(self, cache_file):
        """Read dump from file"""
        self.cache_file = cache_file
        with open(cache_file, 'rb') as pickle_fh:
            self.remote, self.resources, self.max_sleep_interval, \
            self.job_id, self._status, self.epilogue, self.ssh, self.scp \
            = pickle.load(pickle_fh)


    def wait(self, timeout=None):
        """Wait until the result is available or until roughly timeout seconds
        pass."""
        logger = logging.getLogger(__name__)
        if int(self.max_sleep_interval) < int(self._min_sleep_interval):
            self.max_sleep_interval = int(self._min_sleep_interval)
        t0 = time.time()
        sleep_seconds = min(5, self.max_sleep_interval)
        while self.status < COMPLETED:
            logger.debug("sleep for %d seconds", sleep_seconds)
            time.sleep(sleep_seconds)
            if 2*sleep_seconds <= self.max_sleep_interval:
                sleep_seconds *= 2
            if timeout is not None:
                if int(time.time() - t0) > int(timeout):
                    return

    def ready(self):
        """Return whether the job has completed."""
        return (self.status >= COMPLETED)

    def successful(self):
        """Return True if the job finished with a COMPLETED status, False if it
        finished with a CANCELLED or FAILED status. Raise an `AssertionError`
        if the job has not completed"""
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
        self._run_cmd(cmd, self.remote, ignore_exit_code=True, ssh=self.ssh)
        self._status = CANCELLED
        self.dump()

    def run_epilogue(self):
        """Run the epilogue script in the current working directory.

        raises:
            subprocess.CalledProcessError: if the script does not finish with
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

