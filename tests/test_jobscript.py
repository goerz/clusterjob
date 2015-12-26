from textwrap import dedent
from clusterjob import JobScript
import logging

logging.basicConfig(level=logging.DEBUG)

def test_init():
    body = "echo 'Hello World'"
    jobscript = JobScript(body, backend='slurm', jobname='printenv',
    queue='test', time='00:05:00', nodes=1, threads=1, mem=100,
    stdout='printenv.out', stderr='printenv.err')
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

