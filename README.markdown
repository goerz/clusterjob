# clusterjob

[![Build Status](https://travis-ci.org/goerz/clusterjob.svg?branch=develop)](https://travis-ci.org/goerz/clusterjob)
[![Documentation Status](https://readthedocs.org/projects/clusterjob/badge/?version=latest)](http://clusterjob.readthedocs.org/en/latest/?badge=latest)
[![Coverage Status](https://coveralls.io/repos/goerz/clusterjob/badge.svg?branch=develop&service=github)](https://coveralls.io/github/goerz/clusterjob?branch=develop)

Python library to manage workflows on traditional HPC cluster systems.

The library provides the `JobScript` class that wraps around shell scripts,
allowing them to be submitted to a local or remote HPC cluster scheduling
system. The resource requirements (nodes, CPUs, runtime, etc.) for the scheduler
are stored in the attributes of the JobScript object.

[Read the full documentation on ReadTheDocs](http://clusterjob.readthedocs.org/en/latest/)

At present, the following schedulers are supported:

*   [SLURM](https://computing.llnl.gov/linux/slurm/) (default)
*   [Torque/PBS](http://www.adaptivecomputing.com/products/open-source/torque/)
*   [PBS Pro](http://www.pbsworks.com/PBSProduct.aspx?n=PBS-Professional&c=Overview-and-Capabilities)
*   [LPBS](https://github.com/goerz/LPBS)
*   [LSF](http://www.platform.com/Products/platform-lsf)
*   [Sun Grid Engine (SGE)](http://en.wikipedia.org/wiki/Oracle_Grid_Engine) (deprecated)

Support for the
[Univa Grid Engine (UGE)](http://www.univa.com/products/grid-engine.php)
is planned.

## Installation

    pip install clusterjob
