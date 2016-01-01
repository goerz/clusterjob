from textwrap import dedent
import inspect
import clusterjob
from collections import OrderedDict
from clusterjob import JobScript
import pytest
import logging
try:
    from ConfigParser import Error as ConfigParserError
except ImportError:
    from configparser import Error as ConfigParserError
# built-in fixtures: tmpdir
# pytest-capturelog fixutres: caplog

def get_methods(obj):
    """Get list of methods of object or class"""
    return sorted([k for (k, v) in inspect.getmembers(obj, inspect.isroutine)])
    # isroutine works in Python 2 and Python 3, while ismethod does not work in
    # Python 3 if obj is a class (since the methods are not *bound*)


def get_attributes(obj, hidden=False):
    """Get list of attributes of object"""
    methods = get_methods(obj)
    attribs = sorted([k for k in obj.__dict__ if k not in methods])
    if hidden:
        return attribs
    else:
        return [attr for attr in attribs if not attr.startswith('_')]


def default_class_attr_val(attr):
    """Return the default value for the given class attribute"""
    defaults = JobScript._attributes.copy()
    defaults.update(JobScript._protected_attributes)
    try:
        return defaults[attr]
    except KeyError:
        if attr == 'resources':
            return OrderedDict()
        else:
            return None


def check_attributes(obj, expected):
    for key in expected:
        assert getattr(obj, key) == expected[key]


def check_resources(obj, expected):
    for key in expected:
        assert obj.resources[key] == expected[key]


def example_inidata():
    inidata = dedent(r'''
    [Attributes]

    remote = login.cluster.edu
    backend = pbs
    shell = /bin/sh
    cache_folder = cache
    prologue =
        ssh {remote} 'mkdir -p {rootdir}/{workdir}'
        rsync -av {workdir}/ {remote}:{rootdir}/{workdir}
    epilogue = rsync -av {remote}:{rootdir}/{workdir}/ {workdir}
    rootdir = ~/jobs/
    workdir = run001
    sleep_interval = 60
    # the following is a new attribute
    text = Hello World

    [Resources]

    queue = exec
    nodes = 1
    threads = 12
    mem = 10000
    ''')

    expected_attribs = {
        'remote': 'login.cluster.edu',
        'backend': 'pbs',
        'shell': '/bin/sh',
        'prologue' : "ssh {remote} 'mkdir -p {rootdir}/{workdir}'\n"
                        "rsync -av {workdir}/ {remote}:{rootdir}/{workdir}",
        'epilogue': "rsync -av {remote}:{rootdir}/{workdir}/ {workdir}",
        'rootdir': '~/jobs',
        'workdir': 'run001',
        'sleep_interval': 60,
        'text': "Hello World"
    }
    expected_resources = {
            'queue': 'exec',
            'nodes': 1,
            'threads': 12,
            'mem':  10000,
    }
    return inidata, expected_attribs, expected_resources


def test_read_inifile(tmpdir):
    p = tmpdir.join("default.ini")
    ini_filename = str(p)
    attribs = {}
    resources = {}
    def attr_setter(k,v):
        attribs[k] = v
    def rsrc_setter(k,v):
        resources[k] = v

    inidata = ''
    p.write(inidata)
    with pytest.raises(ConfigParserError) as exc_info:
        JobScript._read_inifile(ini_filename, attr_setter, rsrc_setter)
    assert "must contain at least one of the sections" in str(exc_info.value)

    inidata = dedent(r'''
    sleep_interval = 60
    ''')
    p.write(inidata)
    with pytest.raises(ConfigParserError) as exc_info:
        JobScript._read_inifile(ini_filename, attr_setter, rsrc_setter)
    assert "File contains no section headers" in str(exc_info.value)

    inidata = dedent(r'''
    [Attributes]
    sleep_interval = 60
    ''')
    p.write(inidata)
    JobScript._read_inifile(ini_filename, attr_setter, rsrc_setter)
    assert attribs['sleep_interval'] == 60

    inidata = dedent(r'''
    [Resources]
    threads = 2
    ''')
    p.write(inidata)
    JobScript._read_inifile(ini_filename, attr_setter, rsrc_setter)
    assert attribs['sleep_interval'] == 60
    assert resources['threads'] == 2

    inidata = dedent(r'''
    [Attributes]
    shell = /bin/bash

    [Resources]
    nodes = 1
    ''')
    p.write(inidata)
    JobScript._read_inifile(ini_filename, attr_setter, rsrc_setter)
    assert attribs['sleep_interval'] == 60
    assert attribs['shell'] == '/bin/bash'
    assert resources['threads'] == 2
    assert resources['nodes'] == 1

    # section headers are case sensitive, keys are not
    inidata = dedent(r'''
    [Attributes]
    Sleep_interval = 120
    Shell = /bin/bash

    [Resources]
    Nodes = 1
    ''')
    p.write(inidata)
    JobScript._read_inifile(ini_filename, attr_setter, rsrc_setter)
    assert attribs['sleep_interval'] == 120
    assert attribs['shell'] == '/bin/bash'
    assert resources['threads'] == 2
    assert resources['nodes'] == 1

    inidata = dedent(r'''
    [Attributes]
    shell = /bin/bash

    [Resources]
    nodes = 1

    [Schedulers]
    cluster = login.cluster.com
    ''')
    p.write(inidata)
    with pytest.raises(ConfigParserError) as exc_info:
        JobScript._read_inifile(ini_filename, attr_setter, rsrc_setter)
    assert "Invalid section 'Schedulers'" in str(exc_info.value)

    inidata = dedent(r'''
    [Attributes]
    _interval = 120
    ''')
    p.write(inidata)
    with pytest.raises(ConfigParserError) as exc_info:
        JobScript._read_inifile(ini_filename, attr_setter, rsrc_setter)
    assert "Key '_interval' is invalid" in str(exc_info.value)

    inidata = dedent(r'''
    [Attributes]
    attribute with spaces = test
    ''')
    p.write(inidata)
    with pytest.raises(ConfigParserError) as exc_info:
        JobScript._read_inifile(ini_filename, attr_setter, rsrc_setter)
    assert "Key 'attribute with spaces' is invalid" in str(exc_info.value)

    inidata = dedent(r'''
    [Attributes]
    resources = {1:2}
    ''')
    p.write(inidata)
    with pytest.raises(ConfigParserError) as exc_info:
        JobScript._read_inifile(ini_filename, attr_setter, rsrc_setter)
    assert "not allowed" in str(exc_info.value)

    # quotes are not stripped out!
    inidata = dedent(r'''
    [Attributes]
    text = "This is a text"
    ''')
    p.write(inidata)
    JobScript._read_inifile(ini_filename, attr_setter, rsrc_setter)
    assert attribs['text'] == '"This is a text"'


def test_read_defaults(caplog, tmpdir):
    JobScript.read_defaults() # reset
    caplog.setLevel(logging.DEBUG, logger='clusterjob')
    jobscript = JobScript(body="echo 'Hello'", jobname="test")
    assert get_attributes(jobscript) == ['aux_scripts', 'body', 'resources']
    assert get_attributes(jobscript.__class__) == ['backend', 'backends',
            'cache_folder', 'cache_prefix', 'debug_cmds', 'epilogue',
            'filename', 'prologue', 'remote', 'resources', 'rootdir', 'scp',
            'shell', 'sleep_interval', 'ssh', 'workdir']
    for attr in get_attributes(jobscript.__class__):
        if attr not in ['resources', 'backends']:
            assert getattr(jobscript, attr) == default_class_attr_val(attr)

    inidata, expected_attribs, expected_resources = example_inidata()
    p = tmpdir.join("default.ini")
    p.write(inidata)
    ini_filename = str(p)

    # Setting class defaults before instantiation sets both the attributes and
    # the resources
    JobScript.read_defaults(ini_filename)
    jobscript = JobScript(body="echo '{text}'", jobname="test")
    assert get_attributes(jobscript) == ['aux_scripts', 'body', 'resources']
    check_attributes(jobscript, expected_attribs)
    check_resources(jobscript, expected_resources)
    assert str(jobscript) == dedent(r'''
    #!/bin/sh
    #PBS -l nodes=1:ppn=12
    #PBS -q exec
    #PBS -l mem=10000m
    #PBS -N test
    echo 'Hello World'
    ''').strip()

    # calling read_defaults without filename argument resets the class, and
    # thus also changes the attributes of an existing instance
    JobScript.read_defaults()
    check_resources(jobscript, expected_resources)
    for attr in get_attributes(jobscript.__class__):
        if attr not in ['resources', 'backends']:
            assert getattr(jobscript, attr) == default_class_attr_val(attr)
    with pytest.raises(KeyError) as exc_info:
        str(jobscript)
    assert "no matching attribute or resource entry" in str(exc_info.value)
    jobscript.text = 'Hello World' # instance attribute
    assert str(jobscript) == dedent(r'''
    #!/bin/bash
    #SBATCH --partition=exec
    #SBATCH --nodes=1
    #SBATCH --cpus-per-task=12
    #SBATCH --mem=10000
    #SBATCH --job-name=test
    echo 'Hello World'
    ''').strip()

    # Setting class defaults after instantiation sets the attributes, but not
    # the resources
    jobscript = JobScript(body="echo '{text}'", jobname="test")
    JobScript.read_defaults(ini_filename)
    assert str(jobscript) == dedent(r'''
    #!/bin/sh
    #PBS -N test
    echo 'Hello World'
    ''').strip()


def test_read_settings(caplog, tmpdir):
    JobScript.read_defaults() # reset
    caplog.setLevel(logging.DEBUG, logger='clusterjob')
    jobscript = JobScript(body="echo '{text}'", jobname="test")
    assert get_attributes(jobscript) == ['aux_scripts', 'body', 'resources']
    jobscript2 = JobScript(body="echo 'Hello'", jobname="test2")
    inidata, expected_attribs, expected_resources = example_inidata()
    p = tmpdir.join("job.ini")
    p.write(inidata)
    ini_filename = str(p)

    with pytest.raises(AttributeError) as excinfo:
        jobscript.read_settings(ini_filename)
    assert "'cache_folder' can only be set as a class attribute" \
           in str(excinfo.value)
    inidata = inidata.replace("cache_folder = cache\n", "")
    p.write(inidata)
    jobscript.read_settings(ini_filename)
    assert get_attributes(jobscript) == ['aux_scripts', 'backend', 'body',
            'epilogue', 'prologue', 'remote', 'resources', 'rootdir', 'shell',
            'sleep_interval', 'text', 'workdir']
    # class attributes remain unaffected
    for attr in get_attributes(JobScript):
        if attr not in ['resources', 'backends']:
            assert getattr(JobScript, attr) == default_class_attr_val(attr)
    assert str(jobscript) == dedent(r'''
    #!/bin/sh
    #PBS -l nodes=1:ppn=12
    #PBS -N test
    #PBS -q exec
    #PBS -l mem=10000m
    echo 'Hello World'
    ''').strip()

    # the second jobscript is unaffected
    assert str(jobscript2) == dedent(r'''
    #!/bin/bash
    #SBATCH --job-name=test2
    echo 'Hello'
    ''').strip()

