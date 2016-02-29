#!/usr/bin/env python
from distutils.core import setup


def get_version(filename):
    with open(filename) as in_fh:
        for line in in_fh:
            if line.startswith('__version__'):
                return line.split('=')[1].strip()[1:-1]
    raise ValueError("Cannot extract version from %s" % filename)


try:
    # In Python >3.3, 'mock' is part of the standard library
    import unittest.mock
    mock_package = []
except ImportError:
    # In other versions, it has be to be installed as an exernal package
    mock_package = ['mock', ]

setup(name='clusterjob',
      version=get_version('clusterjob/__init__.py'),
      description='Manage traditional HPC cluster workflows in Python',
      author='Michael Goerz',
      author_email='goerz@stanford.edu',
      url='https://github.com/goerz/clusterjob',
      license='MIT',
      install_requires=['six', 'click',],
      extras_require={'dev': ['pytest', 'pytest-capturelog', 'sphinx',
                              'sphinx-autobuild', 'sphinx_rtd_theme',
                              'coverage', 'pytest-cov'] + mock_package},
      packages=['clusterjob', 'clusterjob.backends'],
      entry_points='''
          [console_scripts]
          clusterjob-test-backend=clusterjob.cli:test_backend
      ''',
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
