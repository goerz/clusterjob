Tutorial
========

Hello World
-----------

A simple "Hello World" example is a simple script that prints the environment
of a job running on a cluster.

.. code-block:: python

    body = r'''
    echo "####################################################"
    echo "Job id: $CLUSTERJOB_ID"
    echo "Job name: $CLUSTERJOB_WORKDIR"
    echo "Job started on" `hostname` `date`
    echo "Current directory:" `pwd`
    echo "####################################################"

    echo "####################################################"
    echo "Full Environment:"
    printenv
    echo "####################################################"

    sleep 90

    echo "Job Finished: " `date`
    exit 0
    '''

Note that this script does not contain a header specifying resource
requirements (lines like ``#SBATCH --mem=100``). Also, it uses some
scheduler-independent
:ref:`environment variables <core environment variables>`.

In order to run this on a local SLURM cluster node, we wrap the above script in
the :class:`~clusterjob.JobScript` class, specifying the required resources.

.. code-block:: python

    jobscript = JobScript(
        body, backend='slurm', jobname='printenv',
        queue='test', time='00:05:00', nodes=1, threads=1, mem=100,
        stdout='printenv.out', stderr='printenv.err')


At this point, the ``jobscript`` turns into a SLURM-specific submission script,
including a resource header, and using SLURM-specific environment variables.
Looking at the string representation of ``jobscript``, we see::

    #!/bin/bash
    #SBATCH --job-name=printenv
    #SBATCH --mem=100
    #SBATCH --nodes=1
    #SBATCH --partition=test
    #SBATCH --error=printenv.err
    #SBATCH --output=printenv.out
    #SBATCH --cpus-per-task=1
    #SBATCH --time=00:05:00

    echo "####################################################"
    echo "Job id: $SLURM_JOB_ID"
    echo "Job name: $SLURM_SUBMIT_DIR"
    echo "Job started on" `hostname` `date`
    echo "Current directory:" `pwd`
    echo "####################################################"

    echo "####################################################"
    echo "Full Environment:"
    printenv
    echo "####################################################"

    sleep 90

    echo "Job Finished: " `date`
    exit 0


To submit ``jobscript`` to the scheduler, we would now simply run

.. code-block:: python

    ar = jobscript.submit()

This immediately returns an :class:`~clusterjob.AsyncResult` instance, allowing
us to monitor and interact with the submitted job. Polling the schedular in
regular intervals until the run ends is achieved by

.. code-block:: python

    ar.wait()
