from __future__ import print_function
import os
from clusterjob import JobScript, AsyncResult
from clusterjob.status import str_status
from clusterjob.utils import _wrap_run_cmd
import pytest
try:
    input = raw_input
except NameError:
    pass
# builtin fixtures: tmpdir, monkeypatch


@pytest.fixture(params=['slurm_ks.ini', 'slurm_local.ini', 'lsf_ds.ini'])
def settings_file(request):
    test_module = request.module.__file__
    test_dir, _ = os.path.splitext(test_module)
    return os.path.join(test_dir, request.param)


@pytest.fixture
def mode():
    """Set this to 'replay' for testing, and 'record' for recording a new
    test. When recording, you probably also want to limit the settings_file to
    be limited to only the new test."""
    return 'replay'


def test_workflow(settings_file, monkeypatch, mode):
    """Given an INI file that configures a job for a particular cluster, check
    the communication with the cluster gives when submitting,
    cancelling, resubmitting, checking (while running and after finished) a
    trivial job script. Specifically, check that that clusterjob sends the
    correct commands to the cluster and reacts correctly to responses.

    This test follows a record-replay approach. That is, it replays an
    interactions recorded from an earlier "live" test run. For this to operate
    as a test, `mode` must be 'replay'. To record a new test, `mode` must be
    'record'. This will walk the user through the test interactively, and
    record the communication for later replay.
    """
    monkeypatch.setattr(JobScript, 'debug_cmds', True)
    if mode == 'record':
        prompt = input
        print(("In replay, HOME will be set from %s to "
               "'/home/clusterjob_test'. You may have to edit the recorded "
               "json file to account for this.")
              % os.environ['HOME'])
    elif mode == 'replay':
        monkeypatch.setenv('HOME', '/home/clusterjob_test')
        prompt = print
        def dummy_write_script(self, scriptbody, filename, remote):
            filepath = os.path.split(filename)[0]
            if len(filepath) > 0:
                self._run_cmd(['mkdir', '-p', filepath], remote,
                            ignore_exit_code=False, ssh=self.ssh)
        monkeypatch.setattr(JobScript, '_write_script', dummy_write_script)
    else:
        raise ValueError("Invalid mode")
    body = 'sleep 180'
    job = JobScript(body, jobname='test_clj')
    job.read_settings(settings_file)
    # The sleep interval must be longer than the job duration for the purpose
    # of our test
    job.sleep_interval = 300

    jsonfile = os.path.splitext(settings_file)[0]+".json"
    monkeypatch.setattr(JobScript, '_run_cmd',
                        staticmethod(_wrap_run_cmd(jsonfile, mode)))
    monkeypatch.setattr(AsyncResult, '_run_cmd',
                        staticmethod(JobScript._run_cmd))
    if mode == 'replay':
        monkeypatch.setattr(JobScript, '_upload_file',
                            staticmethod(lambda *args, **kwargs: None))


    print("\n*** Submitting Job ***\n")
    ar = job.submit()
    prompt("Please verify that job has been submitted [Enter]")

    print("\n*** Cancelling Job ***\n")
    ar.cancel()
    prompt("Please verify that job has been cancelled [Enter]")

    print("\n*** Resubmitting Job ***\n")
    ar = job.submit(retry=True)
    prompt("Please verify that job has been resubmitted [Enter]")
    print("\nStatus of running job: "+str_status[ar.status])
    prompt("Please wait for job to finish [Enter]")
    print("\nStatus of finished job: "+str_status[ar.status])


