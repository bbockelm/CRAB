# -*- coding: iso-8859-1 -*-
import time
import logging

historyLogger = logging.getLogger("history")
historyLogger.setLevel(logging.INFO)
hl = logging.StreamHandler()
hl.setLevel(logging.INFO)
hl.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
historyLogger.addHandler(hl)

class Job:
    def __init__(self, jobId):
        historyLogger.debug("Creato Job con id "+str(jobId))
        self.jobId = int(jobId) # id of the Job
        self.host = None # CE where the job will run
        self.exitcode = None # exitcode of the job
        self.exitstatus = None # exitstatus of the grid job
        self.history = [(None, None, time.time())] # local status, remote status, time in wich the status changed
        self.wasInState = set()
        self.changed = True # Does the job status changed since last check?

    def setLocalStatus(self, status):
        historyLogger.debug("Imposto lo stato locale di "+str(self.jobId)+" a "+str(status))
        local,remote,lastTime = self.history[-1]
        if not local == status:
            self.changed = True
            self.history.append((status, remote, time.time()))

    def setRemoteStatus(self, status):
        historyLogger.debug("Imposto lo stato remoto di "+str(self.jobId)+" a "+str(status))
        local,remote,lastTime = self.history[-1]
        self.wasInState.add(status)
        if not remote == status:
            self.changed = True
            self.history.append((local, status, time.time()))

    def getLastTime(self):
        """ Returns the last time in which the job changed status. """
        local, remote, ret = self.history[-1]
        return ret

    def setAttrs(self, host, exitcode, exitstatus):
        if not(self.host == host and self.exitcode == exitcode and self.exitstatus == exitstatus):
            self.changed = True
            self.exitcode = exitcode
            self.exitstatus = exitstatus
            self.host = host

    def getAttrs(self):
        return self.host, self.exitcode, self.exitstatus

    def getStatus(self):
        local, remote, lastTime = self.history[-1]
        return local, remote

    def isChanged(self):
        ret = self.changed
        self.changed = False
        return ret

class History:
    def __init__(self, jobN = -1):
        """ Construct a history of jobN jobs. """
        historyLogger.debug("History initialization with "+str(jobN)+" jobs")
        self.jobsList = []
        for i in range(1, int(jobN)+1):
            self.jobsList.append(Job(i))

    def setJobsNumber(self, jobN):
        """ If necessary enlarges a history to jobN jobs. """
        historyLogger.debug("History enlargement with "+str(jobN)+" jobs")
        for i in range(len(self.jobsList)+1, int(jobN)+1):
            self.jobsList.append(Job(i))
            
    def setLocalJobStatus(self, jobId, status):
        historyLogger.debug("Setting the local status of job "+str(jobId)+" to "+str(status))
        jobId = int(jobId)
        assert(jobId <= len(self.jobsList) and jobId > 0)
        self.jobsList[jobId-1].setLocalStatus(status)

    def setRemoteJobStatus(self, jobId, status):
        historyLogger.debug("Setting the remote status of job "+str(jobId)+" to "+str(status))
        jobId = int(jobId)
        assert(jobId <= len(self.jobsList) and jobId > 0)
        self.jobsList[jobId-1].setRemoteStatus(status)

    def setJobAttrs(self, jobId, host, exitcode, exitstatus):
        jobId = int(jobId)
        assert(jobId <= len(self.jobsList) and jobId > 0)
        self.jobsList[jobId-1].setAttrs(host, exitcode, exitstatus)
    
    def getJobAttrs(self, jobId):
        jobId = int(jobId)
        assert(jobId <= len(self.jobsList) and jobId > 0)
        return self.jobsList[jobId-1].getAttrs()

    def wasJobInRemoteStatus(self, jobId, status):
        jobId=int(jobId)
        assert(jobId <= len(self.jobsList) and jobId > 0)
        return status in self.jobsList[jobId-1].wasInState

    def isStatusTimeout(self, jobId, status, timeout):
        """ Check if the job was ever in the status before a timeout. """
        jobId = int(jobId)
        assert(jobId <= len(self.jobsList) and jobId > 0)
        if status in self.jobsList[jobId-1].wasInState: 
            return False # No timeout because was once already in that status 
        else:
            if time.time() - self.jobsList[jobId-1].getLastTime() > timeout:
                return True # Time exhausted
            else:
                return False
            
    
    def getJobsInLocalStatus(self, status):
        """ Returns the set of jobsIds in a particular local status. """
        
        ret = []
        for i in range(1,len(self.jobsList)+1):
            local, remote = self.getJobStatus(i)
            if local == status:
                ret.append(i)
        return set(ret)

    def getJobsInRemoteStatus(self, status):
        """ Returns the set of jobsIds in a particular remote status. """
        
        ret = []
        for i in range(1,len(self.jobsList)+1):
            local, remote = self.getJobStatus(i)
            if remote in status:
                ret.append(i)
        return set(ret)

    def getJobStatus(self, jobId):
        """ Returns a tuple local status, remote status, for the current status of jobId. """
        historyLogger.debug("getJobStatus: jobId->"+str(jobId))
        jobId = int(jobId)
        assert(jobId <= len(self.jobsList) and jobId > 0)
        return self.jobsList[jobId-1].getStatus()

    def isChanged(self):
        ret = False
        for job in self.jobsList:
            if job.isChanged():
                ret = True
        return ret

    def __repr__(self):
        ret = ""
        for job in self.jobsList:
            ret += "History for job "+str(job.jobId)+" (HOST= "+str(job.host)+", EXITCODE="+str(job.exitcode)+", EXITSTATUS="+str(job.exitstatus)+")\n"
            lstTime = None
            for evt in job.history:
                if lstTime:
                    ret += "\t"+str(evt[0])+", "+str(evt[1])+", "+str(time.ctime(evt[2]))+", "+str(evt[2]-lstTime)+"\n"
                else:
                    ret += "\t"+str(evt[0])+", "+str(evt[1])+", "+str(time.ctime(evt[2]))+"\n"
                lstTime=evt[2]
            ret += "\n"
        return ret

    def jobIds2str(self, jobIds):
        """ Translates a list of jobs into a string. "all" means a complete list of jobs built using self.totJobs.

        >>> jobIds2str([3,1,2])
        '3,1,2'
        """
        if jobIds:
            jobIds = list(jobIds)
            jobs = str(jobIds[0])
            for jobId in jobIds[1:]:
                jobs += ","+str(jobId)
            return jobs
        else:
            return ""


    def __str__(self):
        statusList = set()
        ret = ""
        for job in self.jobsList:
            local, remote = job.getStatus()
            statusList.add(remote)
        for status in statusList:
            ret += str(status)+"->"+self.jobIds2str(self.getJobsInRemoteStatus([status]))+", "
        return ret[:-2]