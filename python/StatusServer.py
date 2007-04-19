from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import Statistic
import time
from ProgressBar import ProgressBar
from TerminalController import TerminalController

import xml.dom.minidom
import xml.dom.ext
import TaskDB

class StatusServer(Actor):
 
    def __init__(self, cfg_params,):
        self.cfg_params = cfg_params
        return
    
    def run(self):
        """
        The main method of the class: check the status of the task
        """
        common.logger.debug(5, "status server::run() called")
        start = time.time()

        totalCreatedJobs = 0
        flagSubmit = 1
        for nj in range(common.jobDB.nJobs()):
            if (common.jobDB.status(nj)!='S'):
                totalCreatedJobs +=1
                flagSubmit = 0

        if not flagSubmit:
            common.logger.message("Not all jobs are submitted: before checking the status submit all the jobs.")
            return

        server_name = self.cfg_params['CRAB.server_name'] # gsiftp://pcpg01.cern.ch/data/SEDir/

        common.scheduler.checkProxy()

        common.taskDB.load()
        WorkDirName =os.path.basename(os.path.split(common.work_space.topDir())[0])
        projectUniqName = 'crab_'+str(WorkDirName)+'_'+common.taskDB.dict('TasKUUID')     
        try: 
            common.logger.message ("Checking the status...\n")
            cmd = 'lcg-cp --vo cms  gsiftp://' + str(server_name) + str(projectUniqName)+'/res/xmlReportFile.xml file://'+common.work_space.resDir()+'xmlReportFile.xml'
            os.system(cmd +' >& /dev/null')  

        except: 
            msg = ("task status not yet available")
            raise CrabException(msg)

        try:     
            file = open(common.work_space.resDir()+"xmlReportFile.xml", "r")
            doc = xml.dom.minidom.parse(common.work_space.resDir()+ "xmlReportFile.xml" )
        
        except: 
            msg = ("problems reading report file")
            raise CrabException(msg)

        task     = doc.childNodes[0].childNodes[1].getAttribute("taskName")
        success   =str(doc.childNodes[0].childNodes[3].getAttribute("count"))
        failed   = str(doc.childNodes[0].childNodes[5].getAttribute("count"))
        progress = str(doc.childNodes[0].childNodes[7].getAttribute("count"))
        totJob =-1 
        try:
            totJob = int(success)+ int(failed) +int(progress)
        except:
            pass

        common.logger.message('This is a preliminary command line implementation for the status check\n')
        if (totJob>=0): 
            msg = '****    Total number of jobs  =  '+str(totJob)
            msg +='\n\n      ****    Jobs finished with success   =  '+str(success)
            msg +='\n      ****    Jobs failed  =   '+str(failed)
            msg +='\n      ****    Jobs running =   '+str(progress)+'\n'
            common.logger.message ( msg )
        elif progress == "all":
            common.logger.message ('****    The task  '+str(task)+'  is under management on the server.\n' )
        elif failed == "all":
            common.logger.message ('****    The task  '+str(task)+'  is failed.\n' )
        elif success == "all":
            common.logger.message ('****    The task  '+str(task)+'  is ended.\n' )
        else:
            common.logger.message ('****    The status of the task  '+str(task)+'  is not known.\n' )
 
        return

