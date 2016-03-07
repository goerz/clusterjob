"""
Package for default backends
"""
from __future__ import absolute_import
from abc import ABCMeta, abstractmethod
import six

@six.add_metaclass(ABCMeta)
class ClusterjobBackend(object):
    """Abstract base class for all clusterjob backends. All backends must
    inherit from this and implement the interface specified below.

    Attributes:
        name (str): (default) name of the backend
        extension (str): extension to be used for job scripts
    """
    common_keys = ['name', 'queue', 'time', 'nodes', 'ppn', 'threads', 'mem',
                   'stdout', 'stderr']

    @abstractmethod
    def cmd_submit(self, jobscript):
        """Given a :class:`~clusterjob.JobScript` instance, return a command
        that submits the job to the scheduler. The returned command must be
        be a sequence of program arguments or a string, see `args` argument of
        :class:`subprocess.Popen`.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_job_id(self, response):
        """Given the stdout from the command returned by :meth:`cmd_submit`,
        return a job ID as a str, or None if the job ID cannot be determined"""
        raise NotImplementedError()

    @abstractmethod
    def cmd_status(self, run, finished=False):
        """Given a :class:`~clusterjob.AsyncResult` instance, return a command
        (cf. :meth:`cmd_submit`) that queries the scheduler for the job status.
        If ``finished=True``, the command should be appropriate for a run that
        has already finished."""
        raise NotImplementedError()

    @abstractmethod
    def get_status(self, respone, finished=False):
        """Given the stdout from the command returned by :meth:`cmd_status`,
        return one of the status code defined in :mod:`clusterjob.status`, or
        None if  the status cannot be determined."""
        raise NotImplementedError()

    @abstractmethod
    def cmd_cancel(self, run):
        """Given a :class:`~clusterjob.AsyncResult` instance, return a command
        (cf. :meth:`cmd_submit`) that cancels the run."""
        raise NotImplementedError()

    @abstractmethod
    def resource_headers(self, jobscript):
        """Given a :class:`~clusterjob.JobScript` instance, return a list of
        lines (no trailing newlines) that encode the resource requirements, to
        be added at the top of the rendered job script, between the shbang and
        the script body. At the very least, keys in the `jobscript` resources
        dict that are in the list of :attr:`common_keys` must be handled, or a
        :exc:`ResourcesNotSupportedError` must be raised.
        """
        raise NotImplementedError()

    @abstractmethod
    def replace_body_vars(self, body):
        """Given a multiline string that is the body of the job script, replace
        the placeholders for environment variables with backend-specific
        realizations, and return the modified body

        At a minimum the following environment variables should be handled:

        .. rubric:: _`Core Environment Variables`

        .. glossary::

        ``$CLUSTERJOB_ID``
            The job ID assigned by the scheduler after submission

        ``$CLUSTERJOB_WORKDIR``
            The directory on the cluster from which the job script was
            submitted.

        ``$CLUSTERJOB_SUBMIT_HOST``
                The hostname on which the job script was submitted.

        ``$CLUSTERJOB_NAME``
                The name of the job.

        ``$CLUSTERJOB_NODELIST``
                The hostname(s) on which the job script is running.
        """
        raise NotImplementedError()

#   def submission_scripts(self, jobscript):
#       """Given a :class:`~clusterjob.JobScript` instance, return a dictionary
#       of submission scripts. This is for situations where the backend
#       requires a separate script for submission. Most backends will not need
#       to implement this method. The result must be a dictionary that maps
#       filenames to file contents (as multiline strings). Before submission,
#       each file in the dictionary will be written to the same folder as the
#       job script. The :meth:`cmd_submit` method is assumed to take into
#       account the submission scripts
#       """
#       return {}


class ResourcesNotSupportedError(Exception):
    """Exception to indicate that a backend is unable to encode a resource
    requirement"""
    pass
