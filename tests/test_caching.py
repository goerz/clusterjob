import os
import clusterjob
from clusterjob.utils import _wrap_run_cmd
from clusterjob.cli import DEFAULT_TEST_BODY
from clusterjob.status import COMPLETED, CANCELLED
from clusterjob import JobScript, AsyncResult
import pytest
import logging
# builtin fixtures: tmpdir, monkeypatch
#logging.basicConfig(level=logging.DEBUG)


def test_cached_workflow(request, tmpdir, monkeypatch):

    # setup
    test_module = request.module.__file__
    test_dir, _ = os.path.splitext(test_module)
    ini_file = os.path.join(test_dir, 'test_caching.ini')
    jsonfile = os.path.join(test_dir, 'test_caching.json')
    monkeypatch.setattr(JobScript, 'cache_folder', str(tmpdir.join('cache')))
    monkeypatch.setattr(JobScript, '_run_cmd',
                        staticmethod(_wrap_run_cmd(jsonfile, 'replay')))
    monkeypatch.setattr(AsyncResult, '_run_cmd',
                        staticmethod(JobScript._run_cmd))
    job = JobScript(DEFAULT_TEST_BODY, jobname='test_caching')
    job.read_settings(ini_file)
    # To re-create this test, change 'replay' above to 'record', and comment
    # out the remaining lines in the setup section
    monkeypatch.setattr(AsyncResult, '_min_sleep_interval', 0)
    job.max_sleep_interval = 0

    print("*** First Submission (cached) ***")
    ar = job.submit(cache_id='test')
    job_id = ar.job_id
    assert os.path.isfile(str(tmpdir.join('cache', 'clusterjob.test.cache')))

    print("*** Second Submission (cached) ***")
    # submitting again should load from cache
    ar = job.submit(cache_id='test')
    assert (ar.job_id == job_id)

    ar.wait()
    assert ar.status == COMPLETED

    print("*** Submission of completed job (cached) ***")
    # If job finished properly, it should not be submitted again
    ar = job.submit(cache_id='test')
    assert (ar.job_id == job_id)
    # Job should have immediate COMPLETED status, without even querying the
    # cluster
    assert ar.status == COMPLETED

    print("*** Resubmission after clearing cache ***")
    # Submitting after clearing the cache should re-submit
    JobScript.clear_cache_folder()
    assert not os.path.isfile(
            str(tmpdir.join('cache', 'clusterjob.test.cache')))
    ar2 = job.submit(cache_id='test')
    assert (ar2.job_id != job_id)

    print("*** Resubmission (forced) ***")
    ar = job.submit(cache_id='test', force=True)
    assert ar2.status < COMPLETED # should still be running!
    assert (ar.job_id != ar2.job_id != job_id)
    # we're letting ar2 run out without re-visiting it

    print("*** Cancellation ***")
    # We should be able to resubmit after a cancellation, with the 'retry'
    # option
    job_id = ar.job_id
    ar.cancel()
    assert ar.status == CANCELLED
    print("*** Resubmission of cancelled job (no retry) ***")
    ar = job.submit(cache_id='test', retry=False)
    assert ar.job_id == job_id
    assert ar.status == CANCELLED
    print("*** Resubmission of cancelled job (retry) ***")
    status = job.submit(cache_id='test', retry=True, block=True)
    assert status == COMPLETED
