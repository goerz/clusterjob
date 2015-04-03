#!/usr/bin/env python
"""
Run all tests

Options:

-v            Be verbose about tests
--nocapture   Print stdout in nose tests
"""
from __future__ import print_function, division, absolute_import, \
                       unicode_literals
import sys
import doctest
try:
    import importlib
except ImportError:
    print("You must install the importlib module", file=sys.stderr)
    sys.exit(1)
try:
    import nose
except ImportError:
    print("You must install the nose module", file=sys.stderr)
    sys.exit(1)


###############################################################################


doctest_modules = [
'clusterjob',
'clusterjob.utils',
]
def run_doctests(modules):
    print("*****************")
    print("* Running Doctest")
    print("*****************")
    for module in modules:
        module = importlib.import_module(module)
        print("*** Running doctests for %s" % str(module.__name__))
        doctest.testmod(module)
    print("\n\n")


###############################################################################


nosetest_modules = [
'tests.test_submit',
]
def run_nosetests(modules):
    print("*******************")
    print("* Running Nosetests")
    print("*******************")
    nose.main(defaultTest=modules)
    print("\n\n")


###############################################################################


def main(argv=None):
    if argv is None:
        argv = sys.argv
    if '-h' in argv or '--help' in argv:
        print(__doc__)
        return 0
    run_doctests(doctest_modules)
    run_nosetests(nosetest_modules)

if __name__ == "__main__":
    sys.exit(main())
