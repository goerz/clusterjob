Introduction
============

Workflows for scientific computing center around writing scripts for a
job scheduler, such as `SLURM`_ or `TORQUE/PBS`_, on a
high-performance-computing (HPC) cluster.
The clusterjob package moves this paradigm into the Python ecosystem. It
provides an abstraction of the :ref:`common model <model>` underlying the
various different scheduling systems. From inside a Python script, cluster jobs
can be defined in a way that is agnostic about the specific cluster or
scheduling system that the job will be run under. The job can then be submitted
to a scheduler (either locally or remotely), and the state of the job can be
tracked asynchronously.

The goals of the clusterjob package are *reproducibility*, *robustness*, and
*flexibility*:

* Allow defining a complete computing workflow from within Python. By scripting
  all interactions with the scheduler instead of submitting jobs "manually",
  reproducibility is ensured.
* Keep calculations together with data pre-/post-processing, analysis and
  plotting, leveraging the entire `scientific Python stack`_. The `Jupyter notebook`_
  is a great environment for tying together these different aspects of a
  project.
* Robustness against any kind of crash or network disconnect. By caching
  information about submitted jobs, a workflow script can be aborted and rerun
  at any time, continuing where it left off. The intent is to manage a
  long-running set of calculations on a cluster from e.g. a laptop computer.
* Aid in separating the calculation workflow from the specifics of a particular
  cluster/scheduling system. The clusterjob package can read all backend
  information and resource requirements from :ref:`INI-style text files <inifiles>`.
  This allows to easily port an existing computing workflow to a different
  cluster/scheduling system.

.. _SLURM: http://slurm.schedmd.com
.. _TORQUE/PBS: http://www.adaptivecomputing.com/products/open-source/torque/<F37>
.. _scientific Python stack: http://scipy.org
.. _Jupyter notebook: http://jupyter.org
.. _INI-style text files: https://docs.python.org/3.5/library/configparser.html#supported-ini-file-structure
