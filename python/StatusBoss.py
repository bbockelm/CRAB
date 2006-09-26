from Actor import *
import common, crab_util
import string, os, sys, time
import Statistic
from SchedulerBoss import *

class StatusBoss(Actor):
    def __init__(self, *args):
        self.cfg_params = args[0]
        self.countToTjob = 0
        self.countCreated = 0
        self.countDone = 0
        self.countRun = 0
        self.countSched = 0
        self.countReady = 0
        self.countCancel = 0
        self.countAbort = 0
        self.countCleared = 0
        return

    def run(self):
        """
        The main method of the class: compute the status and print a report
        """
        common.logger.debug(5, "Status::run() called")

        self.compute()
        self.PrintReport_()
        pass
        print ''

    def splitbyoffset_(self,line,fields):
        ret_val=[]
        nn=fields.split()
        offs=0
        for i in range(len(nn)):
            ret_val.append(line[offs:offs+int(nn[i])])
            offs = offs+int(nn[i])+2
        return ret_val

    def compute(self):
        """
        compute the status
        """
        # moved loading of jobDB before boss status check to enable condor_g scheduler to query jobdb for efficient access to destination
        common.jobDB.load()

        bossTaskId=common.taskDB.dict('BossTaskId')
        cmd = 'boss RTupdate -taskid '+bossTaskId
        runBossCommand(cmd)
        add2tablelist=''
        addjoincondition = ''
        nodeattr='JOB.E_HOST'

        ##BOSS4
        cmd = 'bossAdmin SQL -fieldsLen -query "select JOB.CHAIN_ID,JOB.SCHED_ID,crabjob.EXE_EXIT_CODE,JOB.EXEC_HOST,crabjob.JOB_EXIT_STATUS  from JOB,crabjob'+add2tablelist+' where crabjob.CHAIN_ID=JOB.CHAIN_ID '+addjoincondition+' and JOB.TASK_ID=\''+bossTaskId+'\' ORDER BY crabjob.CHAIN_ID"' 
        cmd_out = runBossCommand(cmd)
        jobAttributes={}
        CoupJobsID={}
        nline=0
        header=''
        fielddesc=()
        for line in cmd_out.splitlines():
            if nline==1:
                fielddesc=line
            else:
                if nline==2:
                    header = self.splitbyoffset_(line,fielddesc)
                elif nline > 2:
                    js = line.split(None,2)
                    jobAttributes[int(js[0])]=self.splitbyoffset_(line,fielddesc)
                    CoupJobsID[int(js[0])]=int(js[0])
            nline = nline+1

        # query also the ended table to get job status of jobs already retrieved
        cmd = 'bossAdmin SQL -fieldsLen -query "select ENDED_JOB.CHAIN_ID,ENDED_JOB.SCHED_ID,ENDED_crabjob.EXE_EXIT_CODE,ENDED_JOB.EXEC_HOST,ENDED_crabjob.JOB_EXIT_STATUS  from ENDED_JOB,ENDED_crabjob'+add2tablelist+' where ENDED_crabjob.CHAIN_ID=ENDED_JOB.CHAIN_ID '+addjoincondition+' and ENDED_JOB.TASK_ID=\''+bossTaskId+'\' ORDER BY ENDED_crabjob.CHAIN_ID"' 
        cmd_out = runBossCommand(cmd)
        nline=0
        for line in cmd_out.splitlines():
            if nline==1:
                fielddesc=line
            else:
                if nline==2:
                    header = self.splitbyoffset_(line,fielddesc)
                elif nline > 2:
                    js = line.split(None,2)
                    jobAttributes[int(js[0])]=self.splitbyoffset_(line,fielddesc)
                    CoupJobsID[int(js[0])]=int(js[0])
            nline = nline+1

        printline = ''
        printline+=header[0]
        printline+='   STATUS          E_HOST            EXE_EXIT_CODE        JOB_EXIT_STATUS'
        print printline
        for_summary = []
        orderdBossID = CoupJobsID.values()
        for bossid in orderdBossID:
            printline=''
            jobStatus=''
            jobStatus = common.scheduler.queryStatus(bossTaskId, bossid)
            # debug
            msg = 'jobStatus' + jobStatus
            common.logger.debug(4,msg)
            ###
            for_summary.append(jobStatus)
            exe_code =jobAttributes[bossid][2]   ##BOSS4 EXE_EXIT_CODE
   
        ###########------> This info must be come from BOSS4      DS.
        ###########------> For the moment BOSS know only WN, but then it will know also CE   DS.
            try:
                if common.scheduler.boss_scheduler_name == "condor_g" :
                    ldest = common.scheduler.queryDest(string.strip(jobAttributes[bossid][0]))  ##BOSS4 CHAIN_ID
                else :
                    ldest = common.scheduler.queryDest(string.strip(jobAttributes[bossid][1]))  ##BOSS4 SCHED_ID 
                if ( ldest.find(":") != -1 ) :
                    dest = ldest.split(":")[0]
                else :
                    dest = ldest
            except: 
                dest = ''  
                pass
            ############# -----> For the moment is WN but it will became CE....    DS.
 
            job_exit_status = jobAttributes[bossid][4]   ##BOSS4 JOB_EXIT_STATUS
            
            if jobStatus == 'Done (Success)' or jobStatus == 'Cleared(BOSS)':
                printline+=jobAttributes[bossid][0]+' '+jobStatus+'   '+dest+'      '+exe_code+'       '+job_exit_status
            elif jobStatus == 'Created(BOSS)':
                pass
                #self.countCreated = self.countCreated + 1
                #printline+=' '+jobStatus+'   '+dest+'      '+exe_code+'       '+job_exit_status
            else:
                printline+=jobAttributes[bossid][0]+' '+jobStatus+'   '+dest
            resFlag = 0
            if jobStatus != 'Created(BOSS)'  and jobStatus != 'Unknown(BOSS)':
                jid1 = string.strip(jobAttributes[bossid][2])

        ##########--------> for the moment this is out, when BOSS will know also the ce we reimplement it  DS. 
   ##             if jobStatus == 'Aborted':
   ##                 Statistic.Monitor('checkstatus',resFlag,jid1,'abort')
   ##             else:
   ##                 Statistic.Monitor('checkstatus',resFlag,jid1,exe_code)   

                if int(self.cfg_params['USER.activate_monalisa']) == 1:
                    params = {'taskId': self.cfg_params['taskId'], 'jobId': str(bossid) + '_' + string.strip(jobAttributes[bossid][2]), \
                    'sid': string.strip(jobAttributes[bossid][2]), 'StatusValueReason': common.scheduler.getAttribute(string.strip(jobAttributes[bossid][2]), 'reason'), \
                    'StatusValue': jobStatus, 'StatusEnterTime': common.scheduler.getAttribute(string.strip(jobAttributes[bossid][2]), 'stateEnterTime'), 'StatusDestination': dest}
                    self.cfg_params['apmon'].sendToML(params)
            if printline != '': 
                print printline

        self.update_(for_summary)
        return

    def status(self) :
        """ Return #jobs for each status as a tuple """
        return (self.countToTjob,self.countCreated,self.countReady,self.countSched,self.countRun,self.countCleared,self.countAbort,self.countCancel,self.countDone)

    def update_(self,statusList) :
        """ update the status of the jobs """

        # moved loading of jobDB before boss status check to enable condor_g scheduler to query jobdb for efficient access to destination
        # common.jobDB.load()
        nj = 0
        for status in statusList:
            if status == 'Created(BOSS)':
                self.countCreated = self.countCreated + 1
            if status == 'Done (Success)' or status == 'Done (Aborted)':
                self.countDone = self.countDone + 1
	        common.jobDB.setStatus(nj, 'D')
            elif status == 'Running' :
                self.countRun = self.countRun + 1
            elif status == 'Scheduled' :
                self.countSched = self.countSched + 1
            elif status == 'Ready' :
                self.countReady =  self.countReady + 1
            elif status == 'Cancelled' or status == 'Killed(BOSS)':
                self.countCancel =  self.countCancel + 1
                common.jobDB.setStatus(nj, 'K')
            elif status == 'Aborted':
                self.countAbort =  self.countAbort + 1
                common.jobDB.setStatus(nj, 'A')
            elif status == 'Cleared(BOSS)':
                self.countCleared = self.countCleared + 1
                pass 
            nj = nj + 1   

        common.jobDB.save()
        common.logger.debug(5,'done loop StatusBoss::report')
 
        self.countToTjob = (len(statusList)) 
        return

    def PrintReport_(self):
        print ''
        print ">>>>>>>>> %i Total Jobs " % (self.countToTjob)

        if (self.countCreated != 0):
            print ''
            print ">>>>>>>>> %i Jobs Created" % (self.countCreated)
        if (self.countReady != 0):
            print ''
            print ">>>>>>>>> %i Jobs Ready" % (self.countReady)
        if (self.countSched != 0):
            print ''
            print ">>>>>>>>> %i Jobs Scheduled" % (self.countSched)
        if (self.countRun != 0):
            print ''
            print ">>>>>>>>> %i Jobs Running" % (self.countRun)
        if (self.countCancel != 0) or (self.countAbort != 0) or (self.countCleared != 0):
            print ''
            tot = int(self.countAbort) + int(self.countCancel) + int(self.countCleared)
            print ">>>>>>>>> %i Jobs killed or Aborted or Cleared" % (tot)
            print "          You can resubmit them specifying JOB numbers: crab -resubmit JOB_number (or range of JOB)" 
            print "          (i.e -resubmit 1-3 => 1 and 2 and 3 or -resubmit 1,3 => 1 and 3)"       
        if (self.countDone != 0):
            print ">>>>>>>>> %i Jobs Done" % (self.countDone)
            print "          Retrieve them with: crab -getoutput to retrieve all" 
            print "          or specifying JOB numbers (i.e -getoutput 1-3 => 1 and 2 and 3 or -getoutput 1,3 => 1 and 3)"
            print('\n')  
        pass
         

