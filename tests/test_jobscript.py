from textwrap import dedent
from clusterjob import JobScript
import logging
import os
try:
    from unittest.mock import Mock
except ImportError:
    from mock import Mock

logging.basicConfig(level=logging.DEBUG)

def test_init():
    body = "echo 'Hello World'"
    jobscript = JobScript(body, backend='slurm', jobname='printenv',
                          queue='test', time='00:05:00', nodes=1, threads=1,
                          mem=100, stdout='printenv.out',
                          stderr='printenv.err')
    assert jobscript.backends == ['lpbs', 'lsf', 'pbs', 'sge', 'slurm']
    for key in ['jobname', 'queue', 'time', 'nodes', 'threads', 'mem',
            'stdout', 'stderr']:
        assert key in jobscript.resources
    assert 'backend' in jobscript.__dict__
    assert str(jobscript).strip() == dedent(r'''
    #!/bin/bash
    #SBATCH --job-name=printenv
    #SBATCH --mem=100
    #SBATCH --nodes=1
    #SBATCH --partition=test
    #SBATCH --error=printenv.err
    #SBATCH --output=printenv.out
    #SBATCH --cpus-per-task=1
    #SBATCH --time=00:05:00
    echo 'Hello World'
    ''').strip()


def test_write_expand_tilde(tmpdir, monkeypatch):
    """Check that when writing out a jobscript to file, the filename passed to
    the `_write_script` method has '~' expaned when writing to a local file,
    and *not* expanded when wriging to a remote file (there, the expansion will
    be performed by SSH)
    """
    monkeypatch.setattr(JobScript, '_write_script', Mock())
    body = 'sleep 180'
    job = JobScript(body, jobname='test_clj')
    job.rootdir = '~/jobs/'
    job.workdir = 'job1'
    job.backend = 'slurm'
    job.shell = '/bin/bash'
    job.write()
    job._write_script.assert_called_with(
            '#!/bin/bash\n#SBATCH --job-name=test_clj\nsleep 180',
            os.path.expanduser('~/jobs/job1/test_clj.slr'), None)
    job.remote = 'remote'
    job.write()
    job._write_script.assert_called_with(
            '#!/bin/bash\n#SBATCH --job-name=test_clj\nsleep 180',
            '~/jobs/job1/test_clj.slr', 'remote')

def test_write(tmpdir, monkeypatch):
    body = 'sleep 180'
    job = JobScript(body, jobname='test_clj')
    job.backend = 'slurm'
    filename = str(tmpdir.join('job.slr'))
    assert not os.path.isfile(filename)
    job.write(filename=filename)
    assert os.path.isfile(filename)
    job.remote = 'remote'
    monkeypatch.setattr(JobScript, '_run_cmd', Mock())
    monkeypatch.setattr(JobScript, '_upload_file', Mock())
    job.write()
    assert job._upload_file.call_count == 1
