"""
PBS/TORQUE backend
"""
from ..status import PENDING, RUNNING, COMPLETED, CANCELLED, FAILED

opt_replacements = {
    'jobname': ('-N',           lambda s: str(s).strip() ),
    'queue'  : ('-q',           lambda s: str(s).strip() ),
    'time'   : ('-l walltime=', lambda s: str(s).strip() ),
    'mem'    : ('-l mem=',      lambda s: "%sm" % str(s).strip() ),
    'stdout' : ('-o',           lambda s: str(s).strip() ),
    'stderr' : ('-e',           lambda s: str(s).strip() ),
    # nodes and threads are handled separately, in translate_options
}


def translate_options(options_dict):
    """Translate dictionary of options into array of options for PBS"""
    options_dict = options_dict.copy()
    opt_array = []
    if 'nodes' in options_dict:
        if 'threads' in options_dict:
            opt_array.append('-l nodes=%s:ppn=%s'
                            % (options_dict['nodes'], options_dict['threads']))
            del options_dict['threads']
        else:
            opt_array.append('-l nodes=%s' % (options_dict['nodes'], ))
        del options_dict['nodes']
    for (key, val) in options_dict.items():
        if key in opt_replacements:
            pbs_key, converter = opt_replacements[key]
            val = converter(val)
        else:
            pbs_key = key
        if val is None:
            continue
        if type(val) is bool:
            if val:
                if not pbs_key.startswith('-'):
                    pbs_key = '-' + pbs_key
                opt_array.append(pbs_key)
        else:
            if not pbs_key.startswith('-'):
                pbs_key = '-l %s=' % pbs_key
            if pbs_key.endswith('='):
                opt_array.append('%s%s' % (pbs_key, str(val)))
            else:
                opt_array.append('%s %s' % (pbs_key, str(val)))
    return opt_array


def get_job_id(response):
    """Return the job id from the response of the qsub command"""
    import re
    lines = [line.strip() for line in response.split("\n")
             if line.strip() != '']
    last_line = lines[-1]
    match = re.match('(\d+)\.[\w.-]+$', last_line)
    if match:
        return match.group(1)
    else:
        return None


def get_job_status(response):
    """Return job job status code from the response of the qstat
    command"""
    pbs_status_mapping = {
            'C' : COMPLETED,
            'E' : RUNNING,
            'H' : PENDING,
            'Q' : PENDING,
            'R' : RUNNING,
            'T' : PENDING,
            'W' : PENDING,
            'S' : PENDING,
    }
    lines = [line.strip() for line in response.split("\n")
             if line.strip() != '']
    last_line = lines[-1]
    if last_line.startswith('qstat: Unknown Job'):
        return COMPLETED
    else:
        try:
            status = lines[-1].split()[4]
            return pbs_status_mapping[status]
        except IndexError:
            return None


backend = {
    'name': 'pbs',
    'prefix': '#PBS',
    'extension' : 'pbs',
    'cmd_submit'         : (lambda job_script: ['qsub', job_script],
                            get_job_id),
    'cmd_status_running' : (lambda job_id: ['qstat', str(job_id)],
                            get_job_status),
    'cmd_status_finished': (lambda job_id: ['qstat', str(job_id)],
                            get_job_status),
    'cmd_cancel'         : lambda job_id: ['qdel', str(job_id) ],
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
        '${XXX_JOB_ID}'     : '${PBS_JOB_ID}',
        '${XXX_WORKDIR}'    : '${PBS_O_WORKDIR}',
        '${XXX_HOST}'       : '${PBS_O_HOST}',
        '${XXX_JOB_NAME}'   : '${PBS_JOBNAME}',
        '${XXX_ARRAY_INDEX}': '${PBS_ARRAYID}',
        '${XXX_NODELIST}'   : '`cat $PBS_NODEFILE`',
    },
}
