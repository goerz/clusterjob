"""
LPBS backend
"""
from __future__ import absolute_import

from . pbs import translate_resources, get_job_id, get_job_status
from ..utils import pprint_backend

backend = {
    'name': 'lpbs',
    'prefix': '#PBS',
    'extension' : 'pbs',
    'cmd_submit'         : (lambda job_script: ['lqsub', job_script],
                            get_job_id),
    'cmd_status_running' : (lambda job_id: ['lqstat', str(job_id)],
                            get_job_status),
    'cmd_status_finished': (lambda job_id: ['lqstat', str(job_id)],
                            get_job_status),
    'cmd_cancel'         : lambda job_id: ['lqdel', str(job_id) ],
    'translate_resources': translate_resources,
    'job_vars': {
        '$XXX_JOB_ID'     : '$PBS_JOB_ID',
        '$XXX_WORKDIR'    : '$PBS_O_WORKDIR',
        '$XXX_HOST'       : '$PBS_O_HOST',
        '$XXX_JOB_NAME'   : '$PBS_JOBNAME',
        '$XXX_ARRAY_INDEX': '$PBS_ARRAYID',
        '$XXX_NODELIST'   : '`cat $PBS_NODEFILE`',
        '${XXX_JOB_ID}'     : '${PBS_JOB_ID}',
        '${XXX_WORKDIR}'    : '${PBS_O_WORKDIR}',
        '${XXX_HOST}'       : '${PBS_O_HOST}',
        '${XXX_JOB_NAME}'   : '${PBS_JOBNAME}',
        '${XXX_ARRAY_INDEX}': '${PBS_ARRAYID}',
        '${XXX_NODELIST}'   : '`cat $PBS_NODEFILE`',
    },
}

__doc__ += "\n\n::\n\n" + pprint_backend(backend, indent=4)
