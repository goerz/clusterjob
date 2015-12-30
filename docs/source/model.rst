.. _model:

Model
=====

The clusterjob package is based on a model that generalizes the concepts
of various HPC scheduling systems.

.. glossary::

    Job Script

        A job script is a shell script with an associated set of properties (resource
        requirements, and backend information).
        In the clusterjob package, job scripts are represented by the
        :class:`clusterjob.JobScript` class.  Each jobscript must have a `name`,
        which may not be unique. See the :class:`clusterjob.JobScript` documentation
        for a list of which properties are available. There is a
        :ref:`core set of keywords <keyword arguments>` for
        resource requirements that is independent of a particular scheduling system.
        Arbitrary additional scheduler-specific resource requirements can be specified.

        Job scripts can have associated prologue and epilogue scripts for pre- and
        post-processing. All of the scripts (the main shell script, as well as the
        prologue and epilogue scripts) may reference any of the Job properties by
        placeholders that are expanded when the job is submitted, see
        :meth:`clusterjob.JobScript.render_script`. Job scripts are
        assumed to operate in a scheduling environment that is defined by a number of
        environment variables. As with the resource requirements keyword, there is a
        :ref:`core set of environment variables <core environment variables>` that are
        independent of the scheduling system. Before submitting the jobscript to a
        scheduler, the environment variables are replaced by a specific equivalent
        environment variable provided by the particular scheduling system.

    Backend

        A backend is the collective information required to submit a job script to
        a *specific* scheduler. This includes information about which commands
        must be used to submit and manage job scripts, how resource
        requirements must be encoded, and what environment variables are
        defined by the scheduler. See the
        :ref:`structure of the backend dictionary <backend dictionary>`
        for details.

    Scheduler

        A scheduler is a software running on a cluster login node that
        accepts job script submissions and runs the job script on some compute
        nodes, taking into account resource constraints. Schedulers that
        clustjob can interact with must meet the following requirements:

        * The scheduler can take all resource requirements from a set of
          comments with a specific prefix at the top of a shell script
        * The scheduler generates a job ID as soon as a job script is
          submitted. The job IDs must be unique within the uptime of the
          scheduler.
        * The scheduler has a command line utilities for submitting jobs,
          querying their status (given a job ID), and cancelling running jobs
        * The scheduler must define environment variables equivalent to the
          :ref:`clusterjob core environment variables <core environment variables>`

    Run

        A Run is the result of submitting a job script to a specific scheduler.
        In the clusterjob package, a run is represented by the
        :class:`clusterjob.AsyncResult` class. This class provides a superset
        of the interface in `multiprocessing.pool.AsyncResult`. It is also
        deliberately similar to the `ipyparallel.client.asyncresult.AsyncResult` class.
        The Run maintains all the required information to communicate with the
        scheduler about the status of the job. It can be cached to hard disc.

