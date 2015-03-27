"""
Collection of utility functions
"""
import os
import stat
import subprocess as sp
try:
    from shlex import quote
except ImportError:
    from pipes import quote


def set_executable(filename):
    """Set the execuable bit on the given filename"""
    st = os.stat(filename)
    os.chmod(filename, st.st_mode | stat.S_IEXEC)


def write_file(filename, data):
    """Write data to the file with the given filename"""
    with open(filename, 'w') as out_fh:
        out_fh.write(data)


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


def run_cmd(cmd, remote, rootdir='', workdir='', ignore_exit_code=False,
    debug=False):
    r'''
    Run the given cmd in the given workdir, either locally or remotely, and
    return the combined stdout/stderr

    Parameters
    ----------

    cmd: list of str or str
        Command to execute, as list consisting of the command, and options.
        Alternatively, the command can be given a single string, which will
        then be executed as a shell command. Only use shell commands when
        necessary, e.g. when the command involves a pipe.

    remote: None or str
        If None, run command locally. Otherwise, run on the given host (via
        SSH)

    rootdir: str, optional
        Local or remote root directory. The `workdir` variable is taken
        relative to `rootdir`. If not specified, effectively the current
        working directory is used as the root for local commands, and the home
        directory for remote commands. Note that `~` may be used to indicate
        the home directory locally or remotely.

    workdir: str, optional
        Local or remote directory from which to run the command, relative to
        `rootdir`. If `rootdir` is empty, `~` may be used to indicate the home
        directory.

    ignore_exit_code: boolean, optional
        By default, subprocess.CalledProcessError will be raised if the call
        has an exit code other than 0. This exception can be supressed by
        passing `ignore_exit_code=False`

    debug: boolean, optional
        If True, print the command that is executed and the response to stdout

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

    >>> run_cmd("./test.sh World | tr '[:upper:]' '[:lower:]'", remote=None,
    ...         workdir=tempfolder)
    'hello world\n'

    >>> shutil.rmtree(tempfolder)
    '''
    workdir = os.path.join(rootdir, workdir)
    if type(cmd) in [list, tuple]:
        use_shell = False
    else:
        cmd = str(cmd)
        use_shell = True
    try:
        if remote is None: # run locally
            workdir = os.path.expanduser(workdir)
            if debug:
                if use_shell:
                    print "COMMAND: %s" % cmd
                else:
                    print "COMMAND: %s" \
                          % " ".join([quote(part) for part in cmd])
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
                cmd = ['ssh', remote, cmd]
            else:
                cmd = ['ssh', remote, 'cd %s && %s' % (workdir, cmd)]
            if debug:
                print "COMMAND: %s" % " ".join([quote(part) for part in cmd])
            response = sp.check_output(cmd, stderr=sp.STDOUT)
    except sp.CalledProcessError as e:
        if ignore_exit_code:
            response = e.output
        else:
            raise
    if debug:
        print "RESPONSE: ---\n%s\n---" % response
    return response


def time_to_seconds(time_str):
    """
    Convert a string describing a time duration into seconds. The supported
    formats are: "minutes", "minutes:seconds", "hours:minutes:seconds",
    "days-hours", "days-hours:minutes", "days-hours:minutes:seconds",
    and "days:hours:minutes:seconds"

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
    import re
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
