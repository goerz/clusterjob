"""
SLURM backend
"""
from ..status import PENDING, RUNNING, COMPLETED, CANCELLED, FAILED

opt_replacements = {
    'jobname': ('--job-name',      lambda s: str(s).strip() ),
    'queue'  : ('--partition',     lambda s: str(s).strip() ),
    'time'   : ('--time',          lambda s: str(s).strip() ),
    'nodes'  : ('--nodes',         lambda s: str(s).strip() ),
    'threads': ('--cpus-per-task', lambda s: str(s).strip() ),
    'mem'    : ('--mem',           lambda s: str(s).strip() ),
    'stdout' : ('--output',        lambda s: str(s).strip() ),
    'stderr' : ('--error',         lambda s: str(s).strip() ),
}


def translate_options(options_dict):
    """Translate dictionary of options into array of options for SLURM"""
    opt_array = []
    for (key, val) in options_dict.items():
        if key in opt_replacements:
            slurm_key, converter = opt_replacements[key]
            val = converter(val)
        else:
            slurm_key = key
        if not slurm_key.startswith('-'):
            if len(slurm_key) == 1:
                slurm_key = '-%s' % slurm_key
            else:
                slurm_key = '--%s' % slurm_key
        if val is None:
            continue
        if type(val) is bool:
            if val:
                opt_array.append(slurm_key)
        else:
            if slurm_key.startswith('--'):
                opt_array.append('%s=%s' % (slurm_key, str(val)))
            else:
                opt_array.append('%s %s' % (slurm_key, str(val)))
    return opt_array


def get_job_id(response):
    """Return the job id from the response of the sbatch command"""
    import re
    match = re.search('Submitted batch job (\d+)\s*$', response)
    if match:
        return match.group(1)
    else:
        return None


def get_job_status(response):
    """Return job job status code from the response of the sacct/squeue
    command"""
    slurm_status_mapping = {
            'RUNNING'    : RUNNING,
            'CANCELLED'  : CANCELLED,
            'COMPLETED'  : COMPLETED,
            'CONFIGURING': PENDING,
            'COMPLETING' : RUNNING,
            'FAILED'     : FAILED,
            'NODE_FAIL'  : FAILED,
            'PENDING'    : PENDING,
            'PREEMPTED'  : FAILED,
            'SUSPENDED'  : PENDING,
            'TIMEOUT'    : FAILED,
    }
    for line in response.split("\n"):
        if line.strip() in slurm_status_mapping:
            return slurm_status_mapping[line.strip()]
    return None

backend = {
    'name': 'slurm',
    'prefix': '#SBATCH',
    'extension' : 'slr',
    'cmd_submit'         : (lambda job_script: ['sbatch', job_script],
                            get_job_id),
    'cmd_status_running' : (lambda job_id: \
                            ['squeue', '-h', '-o %T', '-j %s' % job_id],
                            get_job_status),
    'cmd_status_finished': (lambda job_id: \
                           ['sacct', '--format=state', '-n', '-j %s' % job_id],
                            get_job_status),
    'cmd_cancel'         : lambda job_id: ['scancel', str(job_id)],
    'translate_options': translate_options,
    'default_opts': {
        'nodes'  : 1,
        'threads': 1,
    },
    'job_vars': {
        '$XXX_JOB_ID'     : '$SLURM_JOB_ID',
        '$XXX_WORKDIR'    : '$SLURM_SUBMIT_DIR',
        '$XXX_HOST'       : '$SLURM_SUBMIT_HOST',
        '$XXX_JOB_NAME'   : '$SLURM_JOB_NAME',
        '$XXX_ARRAY_INDEX': '$SLURM_ARRAY_TASK_ID',
        '$XXX_NODELIST'   : '$SLURM_JOB_NODELIST',
    },
}
