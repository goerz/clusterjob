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
      classifiers=[
          'Development Status :: 4 - Beta',
          'Environment :: Console',
          'Environment :: Web Environment',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: MIT License',
          'Natural Language :: English',
          'Topic :: Scientific/Engineering',
          'Topic :: System :: Clustering',
          'Topic :: Utilities',
          'Operating System :: POSIX',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.3',
          'Programming Language :: Python :: 3.4'
      ]
     )
