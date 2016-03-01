from clusterjob.backends import ClusterjobBackend


class Backend1(object):
    """Backend that has the right interface but does not derive from
    ClustterjobBackend"""
    name = 'backend1'
    extension = 'slr'
    def cmd_submit(self, jobscript):
        return 'sbatch 1'
    def get_job_id(self, response):
        return '1'
    def cmd_status(self, run, finished=False):
        return 'squeue 1'
    def get_status(self, response, finished=False):
        return 0
    def cmd_cancel(self, run):
        return 'scancel 1'
    def resource_headers(self, jobscript):
        return []
    def replace_body_vars(self, body):
        return body


class Backend2(ClusterjobBackend):
    """Backend that has a missing get_status method"""
    name = 'backend2'
    extension = 'slr'
    def cmd_submit(self, jobscript):
        return 'sbatch 1'
    def get_job_id(self, response):
        return '1'
    def cmd_status(self, run, finished=False):
        return 'squeue 1'
    def cmd_cancel(self, run):
        return 'scancel 1'
    def resource_headers(self, jobscript):
        return []
    def replace_body_vars(self, body):
        return body


class Backend3(ClusterjobBackend):
    """Backend that missed 'name' and 'extension' attributes"""
    def cmd_submit(self, jobscript):
        return 'sbatch 1'
    def get_job_id(self, response):
        return '1'
    def cmd_status(self, run, finished=False):
        return 'squeue 1'
    def get_status(self, response, finished=False):
        return 0
    def cmd_cancel(self, run):
        return 'scancel 1'
    def resource_headers(self, jobscript):
        return []
    def replace_body_vars(self, body):
        return body


class Backend4(ClusterjobBackend):
    """Backend that matches the correct interface"""
    name = 'backend4'
    extension = 'slr'
    def cmd_submit(self, jobscript):
        return 'sbatch 1'
    def get_job_id(self, response):
        return '1'
    def cmd_status(self, run, finished=False):
        return 'squeue 1'
    def get_status(self, response, finished=False):
        return 0
    def cmd_cancel(self, run):
        return 'scancel 1'
    def resource_headers(self, jobscript):
        return []
    def replace_body_vars(self, body):
        return body


