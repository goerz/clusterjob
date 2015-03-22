#!/usr/bin/env python
from distutils.core import setup
from clusterjob import __version__


setup(name='clusterjob',
      version=__version__,
      description='Manage traditional HPC cluster workflows in Python',
      author='Michael Goerz',
      author_email='mail@michaelgoerz.net',
      url='https://github.com/goerz/clusterjob',
      license='GPL',
      packages=['clusterjob', 'clusterjob.backends'],
      scripts=[],
     )
