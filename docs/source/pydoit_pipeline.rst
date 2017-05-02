Computational Pipelines with pydoit
===================================

`pydoit`_ is an excellent tool for `describing computational pipelines`_. It is similar to `make`_, but much more flexible.

.. _pydoit: http://pydoit.org/index.html
.. _describing computational pipelines: http://swcarpentry.github.io/bc/intermediate/doit/
.. _make: https://www.gnu.org/software/make/

The following is a real-life example of how to combine ``pydoit`` with a
cluster scheduling system. In this example, we do some quantum optimal control
(the task of steering a quantum system in some desired way)
using the `QDYN Fortran library`_  (without going into any details about the
underlying physics). QDYN provides two tools:

* ``qdyn_prop_traj [options] <rf>`` reads a bunch of input data from files in
  the "runfolder" ``<rf>``, simulates the dynamics of the quantum system, and
  writes new analysis data into the runfolder
* ``qdyn_optimize [options] <rf>`` runs an iterative optimization scheme on the
  controls stored in the runfolder. In each iteration, the evolution of the
  quantum system gets closer to some desired target. The program performs a
  fixed number of these iterations, and then writes the "optimized" controls to
  files in the runfolder

Both of these programs are parallelized using `MPI`_ (see also the section
about :ref:`Parallelization Paradigms <parallelization-model>`).

.. _QDYN Fortran library: https://www.qdyn-library.net
.. _MPI: https://www.open-mpi.org

The entire chain of computations is expressed in a table:

.. code-block:: python

    import pandas as pd
    params_data_str = r'''
    #    T  lambda_a  n_trajs   iter_stop
        10     0.001       10          10
        10    0.0005       10          15
        10    0.0001       10          30
        20     0.001       10          20
        50     0.001       10          30
        70     0.001       10          30
    '''
    params_df = pd.read_fwf(
            StringIO(params_data_str), comment='#', header=1,
            names=['T', 'lambda_a', 'n_trajs', 'iter_stop'])


We want to look at the control problem for different physical durations ``T``
of the process; The parameter ``lambda_a`` is a "step width" of the
optimization algorithm, ``n_trajs`` is how many parallel MPI processes we can
use, and ``iter_stop`` is the number of iterations of the control algorithm.
Our computation pipeline contains four actions:

1. For each duration ``T`` in the table, create a unique runfolder and write the
   appropriate input data to files in this folder
2. For the different values of ``iter_stop`` for any given ``T``, modify the
   data in the runfolder to reflect the corresponding ``lambda_a``
3. Submit a job to the cluster that runs ``qdyn_optimize`` until ``iter_stop`` is reached.
4. Finally, after all optimizations are done, for each runfolder (i.e., each
   unique ``T``), submit a job to the cluster that runs ``qdyn_prop_traj`` in
   order to analyze the result of the optimization

Thus, based on the above table, the following specific steps are required:

* Generate input data for ``T=10`` in the runfolder ``./data/rf10``
* Generate input data for ``T=20`` in the runfolder ``./data/rf20``
* Generate input data for ``T=50`` in the runfolder ``./data/rf50``
* Generate input data for ``T=70`` in the runfolder ``./data/rf70``
* Modify the data in the ``./data/rf10`` to do an optimization with step width 0.001, stopping after 10 iterations
* Modify the data in the ``./data/rf20`` to do an optimization with step width 0.001, stopping after 20 iterations
* Modify the data in the ``./data/rf50`` to do an optimization with step width 0.001, stopping after 30 iterations
* Modify the data in the ``./data/rf70`` to do an optimization with step width 0.001, stopping after 30 iterations
* Submit a job the the cluster that runs ``qdyn_optimize [options] ./data/rf10``
* Submit a job the the cluster that runs ``qdyn_optimize [options] ./data/rf20``
* Submit a job the the cluster that runs ``qdyn_optimize [options] ./data/rf50``
* Submit a job the the cluster that runs ``qdyn_optimize [options] ./data/rf70``
* After submitted job for ``./data/rf10`` finishes, modify the data in that runfolder to continue the optimization with step width 0.0005, stopping after iteration 15
* Submit another job the the cluster that runs ``qdyn_optimize [options] ./data/rf10``
* After that job finishes, modify the data in that runfolder to continue the optimization with step width 0.0001, stopping after iteration 30
* Submit another job the the cluster that runs ``qdyn_optimize [options] ./data/rf10``
* After submitted optimization jobs for ``./data/rf10`` finish, submit a job that runs ``qdyn_prop_traj [options] ./data/rf10``
* After submitted optimization jobs for ``./data/rf20`` finish, submit a job that runs ``qdyn_prop_traj [options] ./data/rf20``
* After submitted optimization jobs for ``./data/rf50`` finish, submit a job that runs ``qdyn_prop_traj [options] ./data/rf50``
* After submitted optimization jobs for ``./data/rf70`` finish, submit a job that runs ``qdyn_prop_traj [options] ./data/rf70``

Actions
-------

The first two of the actions in our pipeline (generating the input data, and
modifying that data for a specific value of ``lambda_a``/``iter_stop``) do not
involve ``clusterjob``. These are handled by two Python routines:

* ``write_model(rf, T, lambda_a, iter_stop)``: write input data to the runfolder ``rf``
*  ``update_config(rf, lambda_a, iter_stop)``: update the config file in the runfolder file new data

For the remaining two actions, we use routines that generate and submit job scripts. We set this up as

.. code-block:: python

    import clusterjob
    clusterjob.JobScript.read_defaults('cluster.ini')

where the configuration file ``cluster.ini`` contains::

    [Attributes]
    backend = slurm
    cache_folder = ./data/cache
    module_load =
        module load intel
        module load mpi

    [Resources]
    nodes = 1
    threads = 1
    mem = 10000

This is for running the pipeline on a single workstation with the SLURM
scheduler installed. With a different configuration file, we could use a
different scheduler or submit to a remote cluster with more compute nodes.

The action routines now are:

.. code-block:: python

    from textwrap import dedent

    def submit_optimization(rf, n_trajs, task):
        body = dedent(r'''
        {module_load}

        cd {rf}
        OMP_NUM_THREADS=1 mpirun -n {n_trajs} qdyn_optimize --n-trajs={n_trajs} \
            --J_T=J_T_sm .
        ''')
        taskname = "oct_%s" % task.name.replace(":", '_')
        jobscript = clusterjob.JobScript(
            body=body, filename=join(rf, 'oct.slr'),
            jobname=taskname, nodes=1, ppn=int(n_trajs), threads=1,
            stdout=join(rf, 'oct.log'))
        jobscript.rf = rf
        jobscript.n_trajs = str(int(n_trajs))
        run = jobscript.submit(cache_id=taskname)
        run.dump(join(rf, 'oct.job.dump'))

.. code-block:: python

    def submit_propagation(rf, n_trajs):
        body = dedent(r'''
        {module_load}

        cd {rf}
        OMP_NUM_THREADS=1 mpirun -n {n_trajs} qdyn_prop_traj --n-trajs={n_trajs} \
            --use-oct-pulses --write-final-state=state_final.dat .
        ''')
        taskname = "prop_" + os.path.split(rf)[-1]
        jobscript = clusterjob.JobScript(
            body=body, filename=join(rf, 'prop.slr'),
            jobname=taskname, nodes=1, ppn=int(n_trajs), threads=1,
            stdout=join(rf, 'prop.log'))
        jobscript.rf = rf
        jobscript.n_trajs = str(int(n_trajs))
        run = jobscript.submit(cache_id=taskname, force=True)
        run.dump(join(rf, 'prop.job.dump'))

Both of these store a dump of the submitted job to a file inside the runfolder.
We then have another action that polls the scheduler, waiting for the job to
finish successfully:


.. code-block:: python

    def wait_for_clusterjob(dumpfile):
        try:
            run = clusterjob.AsyncResult.load(dumpfile)
            run.wait()
            os.unlink(dumpfile)
            return run.successful()
        except OSError:
            # dump file was already removed in earlier execution
            pass

Tasks
-----

We now build the pipeline of ``pydoit`` tasks using the above actions, and the
information in the ``params_df`` table. For convenience, we identify the
appropriate runfolder for each row in ``params_df`` as

.. code-block:: python

    def runfolder(row):
        return './data/rf%d' % row['T']

First, we create a runfolder for each unique value of ``T``:

.. code-block:: python

    def task_create_runfolder():
        jobs = {}
        for ind, row in params_df.iterrows():
            rf = runfolder(row)
            if rf in jobs:
                # only one task per runfolder, not per row!
                continue
            jobs[rf] = {
                'name': str(rf),
                'actions': [
                    (write_model, [], dict(
                        rf=rf, T=row['T'], lambda_a=row['lambda_a'],
                        iter_stop=int(row['iter_stop'])))],
                'targets': [join(rf, 'config')],
                'uptodate': [True, ] # up to date if target exists
            }
        for job in jobs.values():
            yield job

Next, we have a task that updates the config file data as necessary.

.. code-block:: python

    def task_update_runfolder():
        rf_jobs = defaultdict(list)
        for ind, row in params_df.iterrows():
            rf = runfolder(row)
            # we only update the config after any earlier optimization has finished
            task_dep = ['wait_for_optimization:%s' % ind2 for ind2 in rf_jobs[rf]]
            rf_jobs[rf].append(ind)
            yield {
                'name': str(ind),
                'actions': [
                    (update_config, [], dict(
                        rf=rf, lambda_a=row['lambda_a'],
                        iter_stop=int(row['iter_stop'])))],
                'file_dep': [join(rf, 'config')],
                'uptodate': [False, ],  # always run task
                'task_dep': task_dep}

The crucial part of this is the task-dependency: we only update the data after
any *earlier* optimization in the same runfolder has finished (the
``wait_for_optimization`` task will be defined below). There is an implicit
dependence on ``task_create_runfolder`` through the existence of the file
'config' inside the runfolder.

In order to run the optimization, we have one task to run the ``submit_optimization`` action.


.. code-block:: python

    def task_submit_optimization():
        rf_jobs = defaultdict(list)
        for ind, row in params_df.iterrows():
            rf = runfolder(row)
            task_dep = ['wait_for_optimization:%s' % ind2 for ind2 in rf_jobs[rf]]
            task_dep.append('update_runfolder:%s' % ind)
            yield {
                'name': str(ind),
                'actions': [
                    (submit_optimization, [rf, ], dict(n_trajs=row['n_trajs']))],
                    # 'task' keyword arg is added automatically
                'task_dep': task_dep,
                'uptodate': [(pulses_uptodate, [], {'rf': rf}), ],
            }

Again, we only start an optimization, after each earlier optimization in the
same runfolder has finished. This relies on a task that simply waits for the
submitted job to finish.

It is worth noting that we define the pipeline primarily using *task*
dependencies, not *file* dependencies. We have custom routine ``pulses_uptodate``
that checks whether an optimization needs to be run.
Task dependencies are a feature that is unique to ``pydoit``
(in comparison to ``make``, which defines targets entirely through files). That
being said, we use task dependencies here only because we want to dynamically
change the data in the runfolder between tasks. In our example, the reason for
this is that ``write_model`` generate a large amount of data to the runfolder,
whereas ``update_config`` only makes a very small modification. In a situation
where the pipeline can be expressed through file dependencies (if later tasks
do not modify the data from earlier tasks), it is often more straightforward to
do this.

.. code-block:: python

    def task_wait_for_optimization():
        for ind, row in params_df.iterrows():
            rf = runfolder(row)
            yield {
                'name': str(ind),
                'task_dep': ['submit_optimization:%d' % ind],
                'actions': [
                    (wait_for_clusterjob, [join(rf, 'oct.job.dump')], {}),]}

The propagation is handled separately and independent of the optimization.
Again, we have two tasks, one to submit the propagation, and one to wait for
the submitted job to finish.

.. code-block:: python

    def task_submit_propagation():
        jobs = {}
        for ind, row in params_df.iterrows():
            rf = runfolder(row)
            jobs[rf] = {
                'name': str(rf),
                'actions': [
                    (submit_propagation, [rf, ], dict(n_trajs=row['n_trajs']))],
                'file_dep': [join(rf, 'pulse1.oct.dat'),],}
        for job in jobs.values():
            yield job

.. code-block:: python

    def task_wait_for_propagation():
        jobs = {}
        for ind, row in params_df.iterrows():
            rf = runfolder(row)
            jobs[rf] = {
                'name': str(rf),
                'task_dep': ['submit_propagation:%s' % rf],
                'actions': [
                    (wait_for_clusterjob, [join(rf, 'prop.job.dump')], {}),]}
        for job in jobs.values():
            yield job


Running the pipline
-------------------

It is often convenient to use the `Jupyter notebook`_ to define the pipeline; in this case, we use the ``%doit`` magic to run it:

.. code-block:: python

    from doit.tools import register_doit_as_IPython_magic
    register_doit_as_IPython_magic()

Then,

.. code-block:: none

    %doit -n 4 wait_for_optimization

produces::

    .  create_runfolder:./data/rf10
    .  create_runfolder:./data/rf20
    .  create_runfolder:./data/rf50
    .  create_runfolder:./data/rf70
    .  update_runfolder:0
    .  update_runfolder:3
    .  update_runfolder:4
    .  submit_optimization:0
    .  update_runfolder:5
    .  submit_optimization:3
    .  submit_optimization:5
    .  submit_optimization:4
    .  wait_for_optimization:0
    .  wait_for_optimization:3
    .  wait_for_optimization:5
    .  wait_for_optimization:4
    .  update_runfolder:1
    .  submit_optimization:1
    .  wait_for_optimization:1
    .  update_runfolder:2
    .  submit_optimization:2
    .  wait_for_optimization:2

After that, we run the propagation as

.. code-block:: none

    %doit -n 4 wait_for_propagation

resulting in::

    .  submit_propagation:./data/rf10
    .  submit_propagation:./data/rf20
    .  submit_propagation:./data/rf50
    .  submit_propagation:./data/rf70
    .  wait_for_propagation:./data/rf10
    .  wait_for_propagation:./data/rf20
    .  wait_for_propagation:./data/rf50
    .  wait_for_propagation:./data/rf70

Calling ``doit`` with 4 processes (``-n 4``) can provide small speedup: by
having split our tasks into "submission" and "wait", we already largely have an
asynchronous pipeline (``submit_optimizaton`` finishes immediately). However,
with the additional parallelization we create all the runfolder in parallel,
and we also monitor the scheduler for several jobs at the same time
(``wait_for_optimization`` runs in parallel, instead of in series).

If the pipeline is run again after it finishes, only the actions of the
``update_runfolder`` tasks are actually execute; ``pydoit`` recognizes
everything else as "up-to-date". If we add or modify rows in ``params_df``,
re-running the pipeline will only add the missing data.

Moreover, the caching feature of ``clusterjob`` ensures that we could actually
kill ``pydoit``, respectively the notebook containing the pipeline. If we then
were to re-execute it at some later time, ``clusterjob`` would pick up already
submitted jobs.

.. _Jupyter notebook: http://jupyter.org
