from __future__ import print_function
import os
from clusterjob import JobScript, AsyncResult
from clusterjob.status import str_status
from clusterjob.utils import _wrap_run_cmd
import pytest
from textwrap import dedent
try:
    # Python 2 compatiblity
    input = raw_input
except NameError:
    pass
# builtin fixtures: tmpdir, monkeypatch

###############################################################################

INI_FILES = ['slurm_ks.ini', 'slurm_local.ini', 'lsf_ds.ini']

# Set MODE to 'replay' for testing, and 'record' for recording a new
# test. When recording, you probably also want to limit INI_FILES to include
# only the a single (new) ini file
MODE = 'replay'
#MODE = 'record'

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
    outfile      = basename + ".out"
    bodyfile     = basename + ".body.sh"
    renderedfile = basename + ".rendered.sh"

    files = [jsonfile, outfile, bodyfile, renderedfile]

    if MODE == 'record':
        for file in files:
            try:
                os.unlink(file)
            except OSError:
                pass
        with open(bodyfile, 'w') as out_fh:
            out_fh.write(dedent(r'''
            echo "####################################################"
            echo "Job id         : $CLUSTERJOB_ID"
            echo "Job name       : $CLUSTERJOB_NAME"
            echo "Workdir        : $CLUSTERJOB_WORKDIR"
            echo "Submission Host: $CLUSTERJOB_SUBMIT_HOST"
            echo "Compute Node   : $CLUSTERJOB_NODELIST"
            echo "Job started on" `hostname` `date`
            echo "Current directory:" `pwd`
            echo "####################################################"

            sleep 180

            echo "Job Finished: " `date`
            exit 0
            '''))
    elif MODE == 'replay':
        for file in files:
            assert os.path.isfile(file)
    else:
        raise ValueError("Invalid MODE")

    return (ini_file, jsonfile, outfile, bodyfile, renderedfile)


def test_workflow(workflow_files, monkeypatch):
    """Given a list of input files (ini_file, jsonfile, outfile, bodyfile,
    renderedfile) that are a record for the interaction with a for a particular
    cluster, check that replay of that interaction yields the same
    communication with the cluster (without actually connecting).

    Specifically, the role of the input files is as follows (assuming
    MODE='replay'):

    * `ini_file`: Configures the job attributes (including the
      settings for the cluster)
    * `jsonfile`: Contains a record of the expected communication
      with the cluster, and previously recorded responses
    * `outfile`: Record of the output from actually running the job on the
      cluster. This file is only for reference, and takes no part in the test.
    * `bodyfile`: The body of the job script
    * `renderedfile`: The expected rendering of `bodyfile` for the given
      backend and attributes.

    When MODE='record', `jsonfile`, `outfile`, and `renderedfile` will be
    written based on the actual interaction with the cluster.
    """
    (ini_file, jsonfile, outfile, bodyfile, renderedfile) = workflow_files

    # Monkeypatch the JobScript class to suppress actual communication in
    # replay mode
    monkeypatch.setattr(JobScript, 'debug_cmds', True)
    if MODE == 'record':
        prompt = input
        print(("In replay, HOME will be set from %s to "
               "'/home/clusterjob_test'. You may have to edit the recorded "
               "file %s to account for this.")
              % (os.environ['HOME'], jsonfile))
    elif MODE == 'replay':
        monkeypatch.setenv('HOME', '/home/clusterjob_test')
        prompt = print
        def dummy_write_script(self, scriptbody, filename, remote):
            filepath = os.path.split(filename)[0]
            if len(filepath) > 0:
                self._run_cmd(['mkdir', '-p', filepath], remote,
                            ignore_exit_code=False, ssh=self.ssh)
        monkeypatch.setattr(JobScript, '_write_script', dummy_write_script)
        # disable file transfer
        monkeypatch.setattr(JobScript, '_upload_file',
                            staticmethod(lambda *args, **kwargs: None))
    else:
        raise ValueError("Invalid MODE")
    monkeypatch.setattr(JobScript, '_run_cmd',
                        staticmethod(_wrap_run_cmd(jsonfile, MODE)))
    monkeypatch.setattr(AsyncResult, '_run_cmd',
                        staticmethod(JobScript._run_cmd))

    # configure job script
    with open(bodyfile) as in_fh:
        body = in_fh.read()
    job = JobScript(body, jobname='test_clj')
    job.read_settings(ini_file)
    # The sleep interval must be longer than the job duration, otherwise we may
    # get extra communication with the cluster from the status polls, which
    # will screw up our test
    job.sleep_interval = 300
    stdout = 'clusterjob_test.out'
    job.resources['stdout'] = stdout

    # set up epilogue
    if MODE == 'replay':
        # simply report pre-recorded output
        epilogue = 'echo ""; echo "STDOUT:"; cat ' + outfile
    elif MODE == 'record':
        # set up an epilogue that will overwrite `outfile` with the actual
        # output
        if job.remote is None:
            local_out = '{rootdir}/{workdir}/' + stdout
            epilogue = 'cp %s %s' % (local_out, outfile)
        else:
            remote_out = '{remote}:{rootdir}/{workdir}/' + stdout
            epilogue = 'rsync -av %s %s' % (remote_out, outfile)
        epilogue += "\n" + 'echo ""; echo "STDOUT:"; cat ' + outfile
    job.epilogue = epilogue

    # check that job is rendered as expected
    if MODE == 'replay':
        with open(renderedfile) as in_fh:
            assert str(job) == in_fh.read(), "Unexpected renderedfile"
    elif MODE == 'record':
        with open(renderedfile, 'w') as out_fh:
            out_fh.write(str(job))

    # run through the complete workflow

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
    print("\nStatus of finished job: "+str_status[ar.get()])

    if MODE == 'replay':
        print("\n\nFINISHED WORKFLOW TEST -- REPLAY MODE\n\n")
    elif MODE == 'record':
        print("\n\nFINISHED WORKFLOW TEST -- RECORDING MODE\n\n")
        print("The interaction has been recorded in %s" % jsonfile)
        print("The job output has been recorded in %s -- PLEASE REVIEW"
              % outfile)
        print("The rendered job job has been recorded in %s"
              % renderedfile)
        print("You must add these files to the repository as part of the "
              "test, and switch to REPLAY mode")
        prompt("\nPress ENTER to confirm.")


