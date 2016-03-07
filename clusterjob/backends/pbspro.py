"""
PBS Pro backend
"""
from __future__ import absolute_import
from .pbs import PbsBackend

class PbsProBackend(PbsBackend):
    """PBS Pro Backend"""

    name = 'pbspro'
    extension = 'pbs'
    prefix = '#PBS'

    def resource_headers(self, jobscript):
        """Given a :class:`~clusterjob.JobScript` instance, return a list of
        lines that encode the resource requirements, to be added at the top of
        the rendered job script
        """
        resources = jobscript.resources
        lines = []
        cores_per_node = 1
        nodes = 1
        ppn = 1
        threads = 1
        if 'ppn' in resources:
            ppn = resources['ppn']
            cores_per_node *= ppn
        if 'threads' in resources:
            threads = resources['threads']
            cores_per_node *= threads
        if 'nodes' in resources:
            nodes = resources['nodes']
        if len(set(['ppn', 'threads', 'nodes']).intersection(resources)) > 0:
            lines.append("%s -l select=%d:ncpus=%d:mpiprocs=%d:ompthreads=%d"
                         % (self.prefix, int(nodes), int(cores_per_node),
                            int(ppn), int(threads)))
        for (key, val) in resources.items():
            if key in ['nodes', 'threads', 'ppn']:
                continue
            if key in self.resource_replacements:
                pbs_key = self.resource_replacements[key]
                if key == 'mem':
                    val = str(val) + "m"
            else:
                pbs_key = key
            if val is None:
                continue
            if type(val) is bool:
                if val:
                    if not pbs_key.startswith('-'):
                        pbs_key = '-' + pbs_key
                    lines.append("%s %s" % (self.prefix, pbs_key))
            else:
                if not pbs_key.startswith('-'):
                    pbs_key = '-l %s=' % pbs_key
                if pbs_key.endswith('='):
                    lines.append('%s %s%s' % (self.prefix, pbs_key, str(val)))
                else:
                    lines.append('%s %s %s' % (self.prefix, pbs_key, str(val)))
        return lines


