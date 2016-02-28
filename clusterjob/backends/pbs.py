"""
PBS/TORQUE backend
"""
from __future__ import absolute_import

from ..status import PENDING, RUNNING, COMPLETED
from ..utils import pprint_backend

resource_replacements = {
    'jobname': ('-N',           lambda s: str(s).strip() ),
    'queue'  : ('-q',           lambda s: str(s).strip() ),
    'time'   : ('-l walltime=', lambda s: str(s).strip() ),
    'mem'    : ('-l mem=',      lambda s: "%sm" % str(s).strip() ),
    'stdout' : ('-o',           lambda s: str(s).strip() ),
    'stderr' : ('-e',           lambda s: str(s).strip() ),
    # nodes and threads are handled separately, in translate_resources
}


def translate_resources(resources_dict):
    """Translate dictionary of resources into array of options for PBS"""
    resources_dict = resources_dict.copy()
    opt_array = []
    if 'nodes' in resources_dict:
        if 'threads' in resources_dict:
            opt_array.append('-l nodes=%s:ppn=%s'
                            % (resources_dict['nodes'],
                               resources_dict['threads']))
            del resources_dict['threads']
        else:
            opt_array.append('-l nodes=%s' % (resources_dict['nodes'], ))
        del resources_dict['nodes']
    for (key, val) in resources_dict.items():
        if key in resource_replacements:
            pbs_key, converter = resource_replacements[key]
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
            'B' : RUNNING,
            'E' : RUNNING,
            'H' : PENDING,
            'M' : PENDING,
            'Q' : PENDING,
            'R' : RUNNING,
            'T' : PENDING,
            'W' : PENDING,
            'U' : PENDING,
            'S' : PENDING,
            'F' : COMPLETED,
            'X' : COMPLETED,
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
        except (IndexError, KeyError):
            return None


backend = {
    'name': 'pbs',
    'prefix': '#PBS',
    'extension' : 'pbs',
    'cmd_submit'         : (lambda jobscript: ['qsub', jobscript.filename],
                            get_job_id),
    'cmd_status_running' : (lambda job_id: ['qstat', '-x', str(job_id)],
                            get_job_status),
    'cmd_status_finished': (lambda job_id: ['qstat', '-x', str(job_id)],
                            get_job_status),
    'cmd_cancel'         : lambda job_id: ['qdel', str(job_id) ],
    'translate_resources': translate_resources,
    'job_vars': {
        '$CLUSTERJOB_ID'         : '$PBS_JOB_ID',
        '$CLUSTERJOB_WORKDIR'    : '$PBS_O_WORKDIR',
        '$CLUSTERJOB_SUBMIT_HOST': '$PBS_O_HOST',
        '$CLUSTERJOB_NAME'       : '$PBS_JOBNAME',
        '$CLUSTERJOB_ARRAY_INDEX': '$PBS_ARRAYID',
        '$CLUSTERJOB_NODELIST'   : '`cat $PBS_NODEFILE`',
        '${CLUSTERJOB_ID}'         : '${PBS_JOB_ID}',
        '${CLUSTERJOB_WORKDIR}'    : '${PBS_O_WORKDIR}',
        '${CLUSTERJOB_SUBMIT_HOST}': '${PBS_O_HOST}',
        '${CLUSTERJOB_NAME}'       : '${PBS_JOBNAME}',
        '${CLUSTERJOB_ARRAY_INDEX}': '${PBS_ARRAYID}',
        '${CLUSTERJOB_NODELIST}'   : '`cat $PBS_NODEFILE`',
    },
}

__doc__ += "\n\n::\n\n" + pprint_backend(backend, indent=4)
