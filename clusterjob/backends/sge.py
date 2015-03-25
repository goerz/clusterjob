"""
SGE (Sun Grid Engine) backend
"""
from ..status import RUNNING, COMPLETED

opt_replacements = {
    'jobname': ('-N',           lambda s: str(s).strip() ),
    'nodes'  : (None,           lambda s: None),
    'threads': (None,           lambda s: None),
    'queue'  : ('-q',           lambda s: str(s).strip() ),
    'time'   : ('-l h_rt=',     lambda s: str(s).strip() ),
    'mem'    : ('-l h_vmem=',   lambda s: str(s).strip() ),
    'stdout' : ('-o',           lambda s: str(s).strip() ),
    'stderr' : ('-e',           lambda s: str(s).strip() ),
}
# Note: nodes and threads are not directly supported on SGE, but must be set up
# using "parallel environments". The configuration is set by the administrator
# so you have to check what they've called the parallel environments
#
# %> qconf -spl
# pe1
# omp
#
# look for one with $pe_slots in the config
#
# %> qconf -sp pe1
# %> qconf -sp omp
# qsub with that environment and number of cores you want to use
#
# qsub -pe omp 8 -cwd ./myscript
#
# Depending on how the cluster is set up, it may be necessary to pass the shell
# as e.g. '-S /bin/bash'. If this definition is missing, the run can crash with
# some very unclear error messages

def translate_options(options_dict):
    """Translate dictionary of options into array of options for SGE"""
    options_dict = options_dict.copy()
    opt_array = []
    for (key, val) in options_dict.items():
        if key in opt_replacements:
            sge_key, converter = opt_replacements[key]
            val = converter(val)
        else:
            sge_key = key
        if val is None:
            continue
        if type(val) is bool:
            if val:
                if not sge_key.startswith('-'):
                    sge_key = '-' + sge_key
                opt_array.append(sge_key)
        else:
            if not sge_key.startswith('-'):
                sge_key = '-l %s=' % sge_key
            if sge_key.endswith('='):
                opt_array.append('%s%s' % (sge_key, str(val)))
            else:
                opt_array.append('%s %s' % (sge_key, str(val)))
    return opt_array


def get_job_id(response):
    """Return the job id from the response of the qsub command"""
    import re
    lines = [line.strip() for line in response.split("\n")
             if line.strip() != '']
    last_line = lines[-1]
    match = re.match(r'Your job (\d+) .* has been submitted$', last_line)
    if match:
        return match.group(1)
    else:
        return None


def get_job_status(response):
    """Return job job status code from the response of the qstat
    command"""
    # Sadly, qstat -j doesn't give the state, and just 'qstat' doesn't allow to
    # filter for a specific job id
    if "Following jobs do not exist" in response:
        return COMPLETED
    else:
        return RUNNING



backend = {
    'name': 'sge',
    'prefix': '#$',
    'extension' : 'sge',
    'cmd_submit'         : (lambda job_script: ['qsub', job_script],
                            get_job_id),
    'cmd_status_running' : (lambda job_id: ['qstat', '-j %s' % job_id],
                            get_job_status),
    'cmd_status_finished': (lambda job_id: ['qstat', '-j %s' % job_id],
                            get_job_status),
    'cmd_cancel'         : lambda job_id: ['qdel', str(job_id)],
    'translate_options': translate_options,
    'default_opts': {
        'nodes'  : 1,
        'threads': 1,
        '-V': True,    # export all environment variables
        '-j ': 'y',    # combine output
        '-cwd': True   # Work in submission directory
    },
    'job_vars': {
        '$XXX_JOB_ID'     : '$JOB_ID',
        '$XXX_WORKDIR'    : '$SGE_O_WORKDIR',
        '$XXX_HOST'       : '$SGE_O_HOST',
        '$XXX_JOB_NAME'   : '$JOBNAME',
        '$XXX_ARRAY_INDEX': '$SGE_TASK_ID',
        '$XXX_NODELIST'   : '$HOSTNAME',
    },
}
