"""
LPBS backend
"""
from . pbs import translate_options, get_job_id, get_job_status

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
    'translate_options': translate_options,
    'default_opts': {
        'nodes'  : 1,
        'threads': 1,
        '-V': True,    # export all environment variables
        '-j oe': True, # combine output
    },
    'job_vars': {
        '$XXX_JOB_ID'     : '$PBS_JOB_ID',
        '$XXX_WORKDIR'    : '$PBS_O_WORKDIR',
        '$XXX_HOST'       : '$PBS_O_HOST',
        '$XXX_JOB_NAME'   : '$PBS_JOBNAME',
        '$XXX_ARRAY_INDEX': '$PBS_ARRAYID',
        '$XXX_NODELIST'   : '`cat $PBS_NODEFILE`',
    },
}
