"""
Collection of utility functions
"""
import os
import stat
import subprocess as sp


def set_executable(filename):
    """Set the execuable bit on the given filename"""
    st = os.stat(filename)
    os.chmod(filename, st.st_mode | stat.S_IEXEC)


def run_cmd(cmd, remote, workdir=None):
    r'''
    Run the given cmd in the given workdir, either locally or remotely, and
    return the combined stdout/stderr

    Parameters
    ----------

    cmd: list of str
        Command to execute, as list of command and options

    remote: None or str
        If None, run command locally. Otherwise, run on the given host (via
        SSH)

    workdir: str, optional
        Local or remote directory from which to run the command.

    Example
    -------

    >>> import tempfile, os, shutil
    >>> tempfolder = tempfile.mkdtemp()
    >>> scriptfile = os.path.join(tempfolder, 'test.sh')
    >>> with open(scriptfile, 'w') as script_fh:
    ...     script_fh.writelines(["#!/bin/bash\n", "echo Hello $1\n"])
    >>> set_executable(scriptfile)

    >>> run_cmd(['./test.sh', 'World'], remote=None, workdir=tempfolder)
    'Hello World\n'

    >>> shutil.rmtree(tempfolder)
    '''
    assert type(cmd) in [list, tuple], "cmd must be given as a list"
    if remote is None: # run locally
        if workdir is None:
            return sp.check_output(cmd, stderr=sp.STDOUT)
        else:
            return sp.check_output(cmd, stderr=sp.STDOUT, cwd=workdir)
    else: # run remotely
        cmd = " ".join(cmd)
        if workdir is None:
            cmd = ['ssh', remote, cmd]
        else:
            cmd = ['ssh', remote, "'cd %s && %s'" % (workdir, cmd)]
        return sp.check_output(cmd, stderr=sp.STDOUT)


def time_to_seconds(time_str):
    """
    Convert a string describing a time duration into seconds. The supported
    formats are: "minutes", "minutes:seconds", "hours:minutes:seconds",
    "days-hours", "days-hours:minutes" and "days-hours:minutes:seconds"

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
    >>> time_to_seconds('1 1:10:30')
    Traceback (most recent call last):
      ...
    ValueError: '1 1:10:30' has invalid pattern
    """
    import re
    patterns = [
        re.compile(r'^(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)$'),
        re.compile(r'^(?P<days>\d+)-(?P<hours>\d+)$'),
        re.compile(r'^(?P<minutes>\d+)$'),
        re.compile(r'^(?P<minutes>\d+):(?P<seconds>\d+)$'),
        re.compile(r'^(?P<days>\d+)-(?P<hours>\d+):(?P<minutes>\d+)$'),
        re.compile(
          r'^(?P<days>\d+)-(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)$'),
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


def mkdir(name, mode=0750):
    """
    Implementation of 'mkdir -p': Creates folder with the given `name` and the
    given permissions (`mode`)

    * Create missing parents folder
    * Do nothing if the folder with the given `name` already exists
    * Raise OSError if there is already a file with the given `name`
    """
    if os.path.isdir(name):
        pass
    elif os.path.isfile(name):
        raise OSError("A file with the same name as the desired " \
                      "dir, '%s', already exists." % name)
    else:
        os.makedirs(name, mode)
