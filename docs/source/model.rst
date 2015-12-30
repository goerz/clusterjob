.. _model:

Model
=====

Job Script
----------

A Jobscript is a shell script with an associated set of properties and resource
requirements.

In the clusterjob package, job scripts are represented by the
:class:`clusterjob.JobScript` class.

Backend
-------

A backend is something that can submit a Job to a Scheduler

Scheduler
---------

A scheduler is a software running on a cluster that runs a jobscript under the
resource constraints.

Run
---

A Run is the result of submitting 
In the clusterjob package, a run is represented by the :class:`clusterjob.AsyncResult` class.
