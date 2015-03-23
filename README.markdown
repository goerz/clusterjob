# clusterjob

Lightweight utilities to manage workflows on traditional HPC cluster
systems such as PBS or slurm.

The library provides the `Job` class that wraps around shell scripts, allowing
them to be submitted to a local or remote HPC cluster scheduling system. The
resource requirements (nodes, cpus, runtime, etc.) for the scheduler are stored
in the attributes of the Job object.

Submitting the Job object to a cluster with the `submit` method immediately
returns an AsyncResult object that is a superset of the interface of
`multiprocessing.pool.AsyncResult`, and is similar to the IPython
`mp.pool.AsyncResult`.

Multiple jobs can be submitted asynchronously before waiting for their
completion with the `AsyncResult` `get` or `wait` methods. The `clusterjob`
library has the option of making the `AsyncResult` objects persistent, such
that if the script is aborted while waiting for the jobs to complete,
restarting it will continue where it left off: the `submit` method of a `Job`
object will then directly restore the previous `AsyncResult` without actually
re-submitting the job the the cluster.


## Installation

    pip install clusterjob

## Usage Example

    import os
    from clusterjob import Job
    from clusterjob.status import str_status

    # persistence is achieved by storing AsyncResult object in a cache
    # directory
    Job.cache_folder = os.path.expanduser('~/.clusterjob_cache')

    # must be set up for passwordless SSH access
    Job.default_remote = 'clusteruser@mycluster'

    # The library knows how to translate options for common HPC schedulers
    Job.default_backend = 'slurm'

    # The actual jobscript can we written using place holders for the usual HPC
    # system environment variables. These will be replaced before submission,
    # e.g. `$XXX_JOB_ID -> $SLURM_JOB_ID`
    script = r'''
    echo "job id: $XXX_JOB_ID"
    echo "job name: $XXX_JOB_NAME"
    sleep 60
    '''

    # we allow prologue and epilogue script to move data to and from the remote
    # before the job is submitted and after the job completes
    epilogue = r'''
        scp {remote}:~/job1.out .
    '''

    job1 = Job(script, jobname='job1', time='00:03:00', nodes=1, threads=1,
               mem=8, stdout='job1.out', filename='job1.slr', epilogue=epilogue)
    job2 = Job(script, jobname='job2', time='00:03:00', nodes=1, threads=1,
               mem=8, stdout='job2.out', filename='job2.slr',
               epilogue=epilogue.replace('job1', 'job2'))

    # Job submission immediately returns an AsyncResult object.
    # Note that these objects are persistent if a `cache_folder` is used:
    # The jobs will only be submitted to the cluster the first time this script
    # runs
    jobs = []
    jobs.append(job1.submit(verbose=True))
    jobs.append(job2.submit(verbose=True))

    # the `get` and `wait` methods of the AsyncResult object block until the
    # job is completed on the cluster
    for job in jobs:
        print "job %s (%s): %s" \
            % (job.options['jobname'], job.job_id, str_status[job.get()])
