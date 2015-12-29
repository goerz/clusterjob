The clusterjob API
==================

The :mod:`clusterjob` package provides the following
two classes:

* :class:`JobScript <clusterjob.JobScript>`
    Encapsulation of a Jobscript
* :class:`AsyncResult <clusterjob.AsyncResult>`
    Encapsulation of a Run, i.e., a submitted Jobscript

The package contains two sub-modules:

* :mod:`clusterjob.utils`
    Collection of utility function

* :mod:`clusterjob.status`
    Definition of status codes

The default backends are defined in the
:mod:`clusterjob.backends` sub-package
