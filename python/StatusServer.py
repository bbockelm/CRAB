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

        self.countNotSubmit = 0
        self.countSubmit = 0
        self.countSubmitting = 0
        self.countDone = 0
        self.countReady = 0
        self.countSched = 0
        self.countRun = 0
        self.countAbort = 0
        self.countCancel = 0
        self.countKilled = 0
        self.countCleared = 0
        self.countToTjob = 0

        try:  
            self.server_name = self.cfg_params['CRAB.server_name'] # gsiftp://pcpg01.cern.ch/data/SEDir/
        except KeyError:
            msg = 'No server selected ...' 
            msg = msg + 'Please specify a server in the crab cfg file' 
            raise CrabException(msg) 

        return

    def translateStatus(self, status):
        """
        simmetric as server
        """

        stateConverting = {'Running': 'R', 'Aborted': 'A', 'Done': 'D', 'Done (Failed)': 'D',\
                           'Cleared': 'D', 'Cancelled': 'K', 'Killed': 'K', 'NotSubmitted': 'C'}

        if status in stateConverting:
            return stateConverting[status]
        return None


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
        #        flagSubmit = 0

        if not flagSubmit:
            common.logger.message("Not all jobs are submitted: before checking the status submit all the jobs.")
            return

        common.scheduler.checkProxy()

        common.taskDB.load()
        WorkDirName =os.path.basename(os.path.split(common.work_space.topDir())[0])
        projectUniqName = 'crab_'+str(WorkDirName)+'_'+common.taskDB.dict('TasKUUID')     
        try: 
            common.logger.message ("Checking the status...\n")
            cmd = 'lcg-cp --vo cms  gsiftp://' + str(self.server_name) + str(projectUniqName)+'/res/xmlReportFile.xml file://'+common.work_space.resDir()+'xmlReportFile.xml'
            common.logger.debug(6, cmd)
            os.system(cmd +' >& /dev/null')  

        except: 
            #msg = ("task status not yet available")
            msg = "The server is managing your task."
            msg += "\n      A detailed report will be ready soon.\n"
            raise CrabException(msg)

        try:     
            file = open(common.work_space.resDir()+"xmlReportFile.xml", "r")
            doc = xml.dom.minidom.parse(common.work_space.resDir()+ "xmlReportFile.xml" )
        
        except: 
            #msg = ("problems reading report file")
            msg = "The server is managing your task."
            msg += "\n      A detailed report will be ready soon.\n"
            raise CrabException(msg)

        ###  <Job status='Submitted' job_exit='NULL' id='1' exe_exit='NULL'/>

        task     = doc.childNodes[0].childNodes[1].getAttribute("taskName")
        self.countToTjob = int(doc.childNodes[0].childNodes[1].getAttribute("totJob") )
       
        addTree = 3

        common.jobDB.load()

        if doc.childNodes[0].childNodes[3].getAttribute("id") == "all":
            if doc.childNodes[0].childNodes[3].getAttribute("status") == "Submitted":
                self.countSubmitting = common.jobDB.nJobs()
                for nj in range(common.jobDB.nJobs()):
                    common.jobDB.setStatus(nj, 'S')
            elif doc.childNodes[0].childNodes[3].getAttribute("status") == "Killed":
                self.countKilled = common.jobDB.nJobs()
                for nj in range(common.jobDB.nJobs()):
                    common.jobDB.setStatus(nj, 'K')
            elif doc.childNodes[0].childNodes[3].getAttribute("status") == "NotSubmitted":
                self.countNotSubmit = common.jobDB.nJobs()
                for nj in range(common.jobDB.nJobs()):
                    common.jobDB.setStatus(nj, 'C')
            self.countToTjob = common.jobDB.nJobs()
        else:
            printline = ''
            printline+= "%-10s %-20s %-20s %-25s" % ('JOBID','STATUS','EXE_EXIT_CODE','JOB_EXIT_STATUS')
            print printline
            print '-------------------------------------------------------------------------------------'

            for job in range( self.countToTjob ):
                idJob = doc.childNodes[0].childNodes[job+addTree].getAttribute("id")
                stato = doc.childNodes[0].childNodes[job+addTree].getAttribute("status")
                exe_exit_code = doc.childNodes[0].childNodes[job+addTree].getAttribute("job_exit")
                job_exit_status = doc.childNodes[0].childNodes[job+addTree].getAttribute("exe_exit")
                cleared = doc.childNodes[0].childNodes[job+addTree].getAttribute("cleared")
                jobDbStatus = self.translateStatus(stato)
 
                if jobDbStatus != None:
                    common.logger.debug(5, '*** Updating jobdb for job %s ***' %idJob)
                    if common.jobDB.status( str(int(idJob)-1) ) != "Y":
                        if jobDbStatus == 'D' and int(cleared) != 1:#exe_exit_code =='' and job_exit_status=='':
                            ## 'Done' but not yet cleared (server side) still showing 'Running'
                            stato = 'Running'
                            jobDbStatus = 'R'
                        common.jobDB.setStatus( str(int(idJob)-1), self.translateStatus(stato) )
                    else:
                        stato = "Cleared"
                    common.jobDB.setExitStatus(  str(int(idJob)-1), job_exit_status )
                if stato != "Done" and stato != "Cleared" and stato != "Aborted" and stato != "Done (Failed)":
                    print "%-10s %-20s" % (idJob,stato)
                else:
                    print "%-10s %-20s %-20s %-25s" % (idJob,stato,exe_exit_code,job_exit_status)

                if stato == 'Running':
                    self.countRun += 1
                elif stato == 'Aborted':
                    self.countAbort += 1
                elif stato == 'Done':
                    self.countDone += 1
                elif stato == 'Cancelled':
                    self.countCancel += 1
                elif stato == 'Submitted':
                    self.countSubmit += 1
                elif stato == 'Submitting':
                    self.countSubmitting += 1
                elif stato == 'Ready':
                    self.countReady += 1
                elif stato == 'Scheduled':
                    self.countSched += 1
                elif stato == 'Cleared':
                    self.countCleared += 1
                elif stato == 'NotSubmitted':
                    self.countSubmitting += 1

                addTree += 1
        common.jobDB.save()

        self.PrintReport_()


    def PrintReport_(self) :

        """ Report #jobs for each status  """


        print ''
        print ">>>>>>>>> %i Total Jobs " % (self.countToTjob)
        print ''

        if (self.countSubmitting != 0) :
            print ">>>>>>>>> %i Jobs Submitting by the server" % (self.countSubmitting)
        if (self.countNotSubmit != 0):
            print ">>>>>>>>> %i Jobs Not Submitted to the grid" % (self.countNotSubmit)
        if (self.countSubmit != 0):
            print ">>>>>>>>> %i Jobs Submitted" % (self.countSubmit)
        if (self.countReady != 0):
            print ">>>>>>>>> %i Jobs Ready" % (self.countReady)
        if (self.countSched != 0):
            print ">>>>>>>>> %i Jobs Scheduled" % (self.countSched)
        if (self.countRun != 0):
            print ">>>>>>>>> %i Jobs Running" % (self.countRun)
        if (self.countDone != 0):
            print ">>>>>>>>> %i Jobs Done" % (self.countDone)
            print "          Retrieve them with: crab -getoutput -continue"
        if (self.countKilled != 0):
            print ">>>>>>>>> %i Jobs Killed" % (self.countKilled)
            print "          Retrieve more information with: crab -postMortem -continue"
        if (self.countAbort != 0):
            print ">>>>>>>>> %i Jobs Aborted" % (self.countAbort)
        if (self.countCleared != 0):
            print ">>>>>>>>> %i Jobs Cleared" % (self.countCleared)

        countUnderMngmt = self.countToTjob - (self.countSubmitting+ self.countNotSubmit + self.countSubmit)
        countUnderMngmt -= (self.countReady + self.countSched + self.countRun + self.countDone) 
        countUnderMngmt -= (self.countKilled + self.countAbort + self.countCleared)
        if (countUnderMngmt != 0):
            print ">>>>>>>>> %i Jobs Waiting or Under Server Management" % (countUnderMngmt)

        print ''
        pass

