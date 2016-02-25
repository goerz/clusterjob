"""Collection of utility functions"""
from __future__ import absolute_import
import os
import stat
import sys
import logging
import subprocess as sp
import pprint
import re
import json
try:
    from shlex import quote
except ImportError:
    from pipes import quote

CMD_RESPONSE_ENCODING = 'utf-8'


def set_executable(filename):
    """Set the exectuable bit on the given filename"""
    st = os.stat(filename)
    os.chmod(filename, st.st_mode | stat.S_IEXEC)


def write_file(filename, data):
    """Write data to the file with the given filename"""
    with open(filename, 'w') as out_fh:
        out_fh.write(data)


def split_seq(seq, n_chunks):
    """Split the given sequence into `n_chunks`. Suitable for distributing an
    array of jobs over a fixed number of workers.

    >>> split_seq([1,2,3,4,5,6], 3)
    [[1, 2], [3, 4], [5, 6]]
    >>> split_seq([1,2,3,4,5,6], 2)
    [[1, 2, 3], [4, 5, 6]]
    >>> split_seq([1,2,3,4,5,6,7], 3)
    [[1, 2], [3, 4, 5], [6, 7]]
    """
    newseq = []
    splitsize = 1.0/n_chunks*len(seq)
    for i in range(n_chunks):
        newseq.append(seq[int(round(i*splitsize)):int(round((i+1)*splitsize))])
    return newseq


def read_file(filename):
    """
    Return the contents of the file with the given filename as a string

    >>> write_file('read_write_file.txt', 'Hello World')
    >>> read_file('read_write_file.txt')
    'Hello World'
    >>> os.unlink('read_write_file.txt')
    """
    with open(filename) as in_fh:
        return in_fh.read()


def upload_file(localfile, remote, remotefile, scp='scp'):
    """Run ``{scp} {localfile} {remote}:{remotefile}``

    Parameters:
        localfile (str): relative or absolute path to a local file
        remote (str): Host on which to put the file
        remotefile (str): remote path where to put the file. May start with '~'
            to indicate the home directory.
        scp (str): the scp executables. If not a full path, the executable must
            be in ``$PATH``.

    Raises:
        subprocess.CalledProcessError: if call to `scp` fails.
    """
    sp.check_output(
        [scp, localfile, remote+':'+remotefile],
        stderr=sp.STDOUT)


def run_cmd(cmd, remote, rootdir='', workdir='', ignore_exit_code=False,
        ssh='ssh'):
    r'''Run the given cmd in the given workdir, either locally or remotely, and
    return the combined stdout/stderr

    Parameters:
        cmd (list of str or str): Command to execute, as list consisting of the
            command, and options.  Alternatively, the command can be given a
            single string, which will then be executed as a shell command. Only
            use shell commands when necessary, e.g. when the command involves a
            pipe.
        remote (None or str): If None, run command locally. Otherwise, run on
            the given host (via SSH)
        rootdir (str, optional): Local or remote root directory. The `workdir`
            variable is taken relative to `rootdir`. If not specified,
            effectively the current working directory is used as the root for
            local commands, and the home directory for remote commands. Note
            that `~` may be used to indicate the home directory locally or
            remotely.
        workdir (str, optional): Local or remote directory from which to run
            the command, relative to `rootdir`. If `rootdir` is empty, `~` may
            be used to indicate the home directory.
        ignore_exit_code (boolean, optional): By default,
            `subprocess.CalledProcessError` will be raised if the call has an
            exit code other than 0. This exception can be supressed by passing
            `ignore_exit_code=False`
        ssh (str, optional): The executable to be used for ssh. If not a full
            path, the executable must be in ``$PATH``

    Example:

        >>> import tempfile, os, shutil
        >>> tempfolder = tempfile.mkdtemp()
        >>> scriptfile = os.path.join(tempfolder, 'test.sh')
        >>> with open(scriptfile, 'w') as script_fh:
        ...     script_fh.writelines(["#!/bin/bash\n", "echo Hello $1\n"])
        >>> set_executable(scriptfile)

        >>> run_cmd(['./test.sh', 'World'], remote=None, workdir=tempfolder)
        'Hello World\n'

        >>> run_cmd("./test.sh World | tr '[:upper:]' '[:lower:]'", remote=None,
        ...         workdir=tempfolder)
        'hello world\n'

        >>> shutil.rmtree(tempfolder)
    '''
    logger = logging.getLogger(__name__)
    workdir = os.path.join(rootdir, workdir)
    if type(cmd) in [list, tuple]:
        use_shell = False
    else:
        cmd = str(cmd)
        use_shell = True
    try:
        if remote is None: # run locally
            workdir = os.path.expanduser(workdir)
            if use_shell:
                logger.debug("COMMAND: %s", cmd)
            else:
                logger.debug("COMMAND: %s",
                             " ".join([quote(part) for part in cmd]))
            if workdir == '':
                response = sp.check_output(cmd, stderr=sp.STDOUT,
                                           shell=use_shell)
            else:
                response = sp.check_output(cmd, stderr=sp.STDOUT, cwd=workdir,
                                           shell=use_shell)
        else: # run remotely
            if not use_shell:
                cmd = " ".join(cmd)
            if workdir == '':
                cmd = [ssh, remote, cmd]
            else:
                cmd = [ssh, remote, 'cd %s && %s' % (workdir, cmd)]
            logger.debug("COMMAND: %s",
                         " ".join([quote(part) for part in cmd]))
            response = sp.check_output(cmd, stderr=sp.STDOUT)
    except sp.CalledProcessError as e:
        if ignore_exit_code:
            response = e.output
        else:
            raise
    if sys.version_info >= (3, 0):
        # For Python 3, we should return a unicode string, so that the backends
        # can safely assume that string operations such as regex matching are
        # possible.
        response = response.decode(CMD_RESPONSE_ENCODING)
    logger.debug("RESPONSE: ---\n%s\n---", response)
    return response


def _wrap_run_cmd(jsonfile, mode='replay'):
    """Wrapper around :func:`run_cmd` for the testing using a record-replay
    model
    """
    records = []
    counter = 0
    json_opts = {'indent': 2, 'separators':(',',': '), 'sort_keys': True}
    def run_cmd_record(*args, **kwargs):
        response = run_cmd(*args, **kwargs)
        records.append({'args': args, 'kwargs': kwargs, 'response': response})
        with open(jsonfile, 'w') as out_fh:
            json.dump(records, out_fh, **json_opts)
        return response
    def run_cmd_replay(*args, **kwargs):
        record = records.pop(0)
        assert list(record['args']) == list(args), \
            "run_cmd call #%d: Obtained args: '%s'; Expected args: '%s'" \
            % (counter+1, str(args), str(record['args']))
        assert record['kwargs'] == kwargs, \
            "run_cmd call #%d: Obtained kwargs: '%s'; Expected kwargs: '%s'" \
            % (counter+1, str(kwargs), str(record['kwargs']))
        return record['response']
    if mode == 'replay':
        with open(jsonfile) as in_fh:
            records = json.load(in_fh)
        return run_cmd_replay
    elif mode == 'record':
        return run_cmd_record
    else:
        raise ValueError("Invalid mode")


def time_to_seconds(time_str):
    """Convert a string describing a time duration into seconds. The supported
    formats are::

        minutes
        minutes:seconds
        hours:minutes:seconds
        days-hours
        days-hours:minutes
        days-hours:minutes:seconds
        days:hours:minutes:seconds

    Raises:
        ValueError: if `time_str` has an invalid format.

    Examples:
        >>> time_to_seconds('10')
        600
        >>> time_to_seconds('10:00')
        600
        >>> time_to_seconds('10:30')
        630
        >>> time_to_seconds('1:10:30')
        4230
        >>> time_to_seconds('1-1:10:30')
        90630
        >>> time_to_seconds('1-0')
        86400
        >>> time_to_seconds('1-10')
        122400
        >>> time_to_seconds('1-1:10')
        90600
        >>> time_to_seconds('1-1:10:30')
        90630
        >>> time_to_seconds('1:1:10:30')
        90630
        >>> time_to_seconds('1 1:10:30')
        Traceback (most recent call last):
        ...
        ValueError: '1 1:10:30' has invalid pattern
    """
    patterns = [
        re.compile(r'^(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)$'),
        re.compile(r'^(?P<days>\d+)-(?P<hours>\d+)$'),
        re.compile(r'^(?P<minutes>\d+)$'),
        re.compile(r'^(?P<minutes>\d+):(?P<seconds>\d+)$'),
        re.compile(r'^(?P<days>\d+)-(?P<hours>\d+):(?P<minutes>\d+)$'),
        re.compile(
          r'^(?P<days>\d+)-(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)$'),
        re.compile(
          r'^(?P<days>\d+):(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)$'),
    ]
    seconds = 0
    for pattern in patterns:
        match = pattern.match(str(time_str).strip())
        if match:
            if 'seconds' in match.groupdict():
                seconds += int(match.group('seconds'))
            if 'minutes' in match.groupdict():
                seconds += 60*int(match.group('minutes'))
            if 'hours' in match.groupdict():
                seconds += 3600*int(match.group('hours'))
            if 'days' in match.groupdict():
                seconds += 86400*int(match.group('days'))
            return seconds
    raise ValueError("'%s' has invalid pattern" % time_str)


def mkdir(name, mode=0o750):
    """Implementation of ``mkdir -p``: Creates folder with the given `name` and
    the given permissions (`mode`)

    * Create missing parents folder
    * Do nothing if the folder with the given `name` already exists
    * Raise `OSError` if there is already a file with the given `name`
    """
    if os.path.isdir(name):
        pass
    elif os.path.isfile(name):
        raise OSError("A file with the same name as the desired " \
                      "dir, '%s', already exists." % name)
    else:
        os.makedirs(name, mode)


def pprint_backend(backend, varname='backend',indent=0):
    """Return a prettyprinted, multiline string representation of the given
    `backend` dictionary.

    Parameters:
        backend (dict): The backend dictionary to pretty-pring (e.g.
            ``clusterjob.backends.slurm.backend``)
        varname (str): Variable name, for the default value of
            ``varname=backend``, the first line of output will start with
            ``backend = {...``
        indent (int): Number of spaces by which to indent each line in the
            output
    """
    pretty = pprint.pformat(backend)
    indent_str = " " * indent
    lines = []
    for line_nr, line in enumerate(pretty.split("\n")):
        if line_nr == 0:
            line = "%s = %s" % (varname, line)
        line = re.sub(" at 0x[a-f0-9]+>", ">", line)
        if len(line) > 0:
            line = indent_str + line
        lines.append(line)
    return "\n".join(lines)
