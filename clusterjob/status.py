"""
Generalized status codes for submitted jobs

The status code are ordered such that

    pending/running < completed successfully < completed with error

An status code of 0 indicates succesful completion
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

