
class JobInfo:
    def __init__(self, _jobid, _task, _owner):
        self.jobid = _jobid
        self.task = _task
        self.owner = _owner

    def getOwner(self):
        return self.owner

    def getJobID(self):
        return self.jobid

    def getTaskName(self):
        return self.task
