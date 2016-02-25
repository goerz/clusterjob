"""
LSF backend
"""
from __future__ import absolute_import

from ..status import PENDING, RUNNING, COMPLETED, CANCELLED, FAILED
from ..utils import pprint_backend, time_to_seconds

def time_to_minutes(val):
    return str(int(time_to_seconds(val) / 60))

resource_replacements = {
    'jobname': ('-J',      lambda s: str(s).strip() ),
    'queue'  : ('-q',      lambda s: str(s).strip() ),
    'time'   : ('-W',      time_to_minutes),
    'mem'    : ('-M',      lambda s: str(s).strip() ),
    'stdout' : ('-o',      lambda s: str(s).strip() ),
    'stderr' : ('-e',      lambda s: str(s).strip() ),
    # nodes and threads are handled separately, in translate_resources
}


def translate_resources(resources_dict):
    """Translate dictionary of resources into array of options for LSF"""
    resources_dict = resources_dict.copy()
    opt_array = []
    if 'threads' in resources_dict:
        if 'nodes' in resources_dict:
            opt_array.append('-n %d' % (resources_dict['nodes']
                                        * resources_dict['threads']))
            del resources_dict['nodes']
        else:
            opt_array.append('-n %d' % (resources_dict['threads']))
        del resources_dict['threads']
    for (key, val) in resources_dict.items():
        if key in resource_replacements:
            lsf_key, converter = resource_replacements[key]
            val = converter(val)
        else:
            lsf_key = key
        if not lsf_key.startswith('-'):
            lsf_key = '-%s' % lsf_key
        if val is None:
            continue
        if type(val) is bool:
            if val:
                opt_array.append(lsf_key)
        else:
            opt_array.append('%s %s' % (lsf_key, str(val)))
    return opt_array


def get_job_id(response):
    """Return the job id from the response of the bsub command"""
    import re
    match = re.search('Job <([^>]+)> is submitted', response)
    if match:
        return match.group(1)
    else:
        return None


def get_job_status(response):
    """Return job job status code from the response of the bjobs command
    command"""
    lsf_status_mapping = {
            'PEND'  : PENDING,
            'PSUSP' : PENDING,
            'RUN'   : RUNNING,
            'USUSP' : PENDING,
            'SSUSP' : PENDING,
            'DONE'  : COMPLETED,
            'EXIT'  : FAILED,
            'UNKWN' : PENDING,
            'WAIT'  : PENDING,
            'ZOMBI' : FAILED,
    }
    status_pos = 0
    for line in response.split("\n"):
        if line.startswith('JOBID'):
            try:
                status_pos = line.find('STAT')
            except ValueError:
                return None
        else:
            status = line[status_pos:].split()[0]
            if status in lsf_status_mapping:
                return lsf_status_mapping[status]
    return None

backend = {
    'name': 'lsf',
    'prefix': '#BSUB',
    'extension' : 'lsf',
    'cmd_submit'         : (lambda jobscript:
                            'bsub < "%s"' % jobscript.filename,
                            get_job_id),
    'cmd_status_running' : (lambda job_id: \
                            ['bjobs', '-a', job_id],
                            get_job_status),
    'cmd_status_finished': (lambda job_id: \
                            ['bjobs', '-a', job_id],
                            get_job_status),
    'cmd_cancel'         : lambda job_id: ['bkill', str(job_id)],
    'translate_resources': translate_resources,
    'job_vars': {
        '$XXX_JOB_ID'     : '$LSB_JOBID',
        '$XXX_WORKDIR'    : '$LS_SUBCWD',
        '$XXX_HOST'       : '`hostname`', # Not available in LSF
        '$XXX_JOB_NAME'   : '$LSB_JOBNAME',
        '$XXX_ARRAY_INDEX': '$LSB_JOBINDEX',
        '$XXX_NODELIST'   : '$LSB_HOSTS',
        '${XXX_JOB_ID}'     : '${LSB_JOBID}',
        '${XXX_WORKDIR}'    : '${LS_SUBCWD}',
        '${XXX_HOST}'       : '`hostname`',
        '${XXX_JOB_NAME}'   : '${LSB_JOBNAME}',
        '${XXX_ARRAY_INDEX}': '${LSB_JOBINDEX}',
        '${XXX_NODELIST}'   : '${LSB_HOSTS}',
    },
}

__doc__ += "\n\n::\n\n" + pprint_backend(backend, indent=4)
