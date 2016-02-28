from __future__ import print_function
import os
import logging
from clusterjob import JobScript, AsyncResult
from clusterjob.utils import _wrap_run_cmd
from clusterjob.cli import _run_testing_workflow
import pytest
try:
    # Python 2 compatiblity
    input = raw_input
except NameError:
    pass
# builtin fixtures: tmpdir, monkeypatch

###############################################################################

INI_FILES = ['slurm_ks.ini', 'slurm_local.ini', 'lsf_ds.ini',
             'pbs_copper_local.ini', 'pbs_copper_remote.ini']

# default job name is 'test_clj'. Any other job name must be specified here
JOB_NAMES = {'pbs_copper_local.ini': 'coppertest'}

###############################################################################


@pytest.fixture(params=INI_FILES)
def ini_file(request):
    return request.param


@pytest.fixture()
def workflow_files(request, ini_file):
    test_module = request.module.__file__
    test_dir, _ = os.path.splitext(test_module)
    ini_file = os.path.join(test_dir, ini_file)
    basename = os.path.splitext(ini_file)[0]
    jsonfile     = basename + ".json"
    bodyfile     = basename + ".body.sh"
    renderedfile = basename + ".rendered.sh"
    files = [ini_file, jsonfile, bodyfile, renderedfile]
    for file in files:
        assert os.path.isfile(file)
    return files


def test_workflow(workflow_files, monkeypatch):
    """Given a list of input files (ini_file, jsonfile, bodyfile,
    renderedfile) that are a record for the interaction with a for a particular
    cluster, check that replay of that interaction yields the same
    communication with the cluster (without actually connecting).

    Specifically, the role of the input files is as follows:

    * `ini_file`: Configures the job attributes (including the
      settings for the cluster)
    * `jsonfile`: Contains a record of the expected communication
      with the cluster, and previously recorded responses
    * `bodyfile`: The body of the job script
    * `renderedfile`: The expected rendering of `bodyfile` for the given
      backend and attributes.
    """
    logger = logging.getLogger(__name__)

    (ini_file, jsonfile, bodyfile, renderedfile) = workflow_files

    # Monkeypatch the JobScript class to suppress actual communication in
    # replay mode
    monkeypatch.setattr(JobScript, 'debug_cmds', True)
    monkeypatch.setenv('HOME', '/home/clusterjob_test')
    def dummy_write_script(self, scriptbody, filename, remote):
        filepath = os.path.split(filename)[0]
        if len(filepath) > 0:
            self._run_cmd(['mkdir', '-p', filepath], remote,
                        ignore_exit_code=False, ssh=self.ssh)
    monkeypatch.setattr(JobScript, '_write_script', dummy_write_script)
    # disable file transfer
    monkeypatch.setattr(JobScript, '_upload_file',
                        staticmethod(lambda *args, **kwargs: None))
    # wrap _run_cmd to check communication again jsonfile
    monkeypatch.setattr(JobScript, '_run_cmd',
                        staticmethod(_wrap_run_cmd(jsonfile, 'replay')))
    monkeypatch.setattr(AsyncResult, '_run_cmd',
                        staticmethod(JobScript._run_cmd))
    monkeypatch.setattr(AsyncResult, '_min_sleep_interval', 0)

    # configure job script
    with open(bodyfile) as in_fh:
        body = in_fh.read()
    jobname = 'test_clj'
    for key in JOB_NAMES:
        if ini_file.endswith(key):
            jobname = JOB_NAMES[key]
            break
    job = JobScript(body, jobname=jobname)
    job.read_settings(ini_file)
    stdout = 'clusterjob_test.out'
    job.resources['stdout'] = stdout
    if len(job.prologue) > 0:
        logger.warn("prologue will be disabled")
        job.prologue = ''
    if len(job.epilogue) > 0:
        logger.warn("epilogue will be disabled")
        job.epilogue = ''

    # check that job is rendered as expected
    with open(renderedfile) as in_fh:
        assert str(job) == in_fh.read(), "Unexpected renderedfile"

    # run through the complete workflow
    _run_testing_workflow(job, prompt=False)


