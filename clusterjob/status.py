"""Generalized (integer) status codes for submitted jobs::

    PENDING < RUNNING < COMPLETED < CANCELLED < FAILED

``COMPLETED`` corresponds to the value 0, such that prior to completion, the
status code is negative, and on cancellation/failure, the status code is
positive.

The ``str_status`` dictionary allows to obtain a string representation of a
status code.

>>> from clusterjob.status import str_status, COMPLETED
>>> print(str_status[COMPLETED])
COMPLETED
"""

PENDING   = -2
RUNNING   = -1
COMPLETED =  0
CANCELLED =  1
FAILED    =  2

STATUS_CODES = [PENDING, RUNNING, COMPLETED, CANCELLED, FAILED]

str_status = {
 PENDING   : 'PENDING',
 RUNNING   : 'RUNNING',
 COMPLETED : 'COMPLETED',
 CANCELLED : 'CANCELLED',
 FAILED    : 'FAILED',
}

