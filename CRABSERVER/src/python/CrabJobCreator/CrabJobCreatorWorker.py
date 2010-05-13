#!/usr/bin/env python
#pylint: disable-msg=W0613,W0703,E1101
"""
Update the status of wmbs job when receiving
event from GetOutput component 
"""
__revision__ = "$Id: CrabJobCreatorWorker.py, \
  v 1.1 2009/10/13 15:23:06 riahi Exp $"
__version__ = "$Revision: 1.1 $"

import time
import threading

from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI

# WMCORE
from WMCore.DAOFactory import DAOFactory
from WMCore.WMBS.Job import Job

class CrabJobCreatorWorker:
    """
    _CrabJobCreatorWorker_
    """ 
    def __init__(self, logger, workerAttributes):

        # Worker Properties
        self.tInit = time.time()
        self.log = logger
        self.configs = workerAttributes

        # derived attributes
        self.blDBsession = BossLiteAPI('MySQL', \
             pool=self.configs['blWorkerPool'])

        # Load DB queries
        myThread = threading.currentThread()
        self.daoFactory = DAOFactory(package = "WMCore.WMBS", \
                                     logger = myThread.logger, \
                                     dbinterface = myThread.dbi)
        self.queries = self.daoFactory(classname = "Jobs.ChangeState")

    def updateState(self, payload, status):
        """
        Udpate the status of job in payload to status
        """ 
        self.log.info("CrabJobCreatorWorker initialized with payload \
                   %s"%payload)

        import re
        r = re.compile("BossJob_(\d+)_(\d+)/")
        m = r.search(payload)
        if (m):
            taskId, jobId = m.groups()
        else:
            self.log.info("CrabJobCreatorWorkerFailed to parse %s \
              and update job status to %s" %(payload,status))
            return

        # Parse payload to obtain taskId and jobId 
        self.log.info("--->>> taskId = " + str(taskId))
        self.log.info("--->>> jobId = " + str(jobId))


        #task = self.bliteSession.load(taskId, jobId)
        task = self.blDBsession.load(taskId, jobId)

        self.log.info("--->>> wmbs job id %s" %task.jobs[0]["wmbsJobId"])

        if not task.jobs[0]["wmbsJobId"] :
            self.log.info("--->>> jobId %s doesn't have wmbsJobId %s" \
            %(str(jobId),task.jobs[0]["wmbsJobId"])) 
            return

        # Changment state work
        jobObj = Job(id = task.jobs[0]["wmbsJobId"])

        if jobObj.exists() == False:

            self.log.info("--->>> wmbs job %s doesn't exists" %task.jobs[0]["wmbsJobId"])

        else:
 
            jobObj.load()
            jobObj.changeState(status)
            self.queries.execute(jobs = [jobObj])

            self.log.info("CrabJobCreatorWorker update state to %s of wmbsJob \
        %s bl_job %s task %s" %(status, task.jobs[0]["wmbsJobId"], jobId, taskId)) 

        self.log.info("CrabJobCreatorWorker finished")
        return


