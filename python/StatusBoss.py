from Actor import *
import common, crab_util
import string, os, sys, time
import Statistic
from SchedulerBoss import *

class StatusBoss(Actor):
    def __init__(self, *args):
        self.cfg_params = args[0]
        self.countToTjob = 0
        self.countCreated = []
        self.countDone = []
        self.countRun = []
        self.countSched = []
        self.countReady = []
        self.countCancel = []
        self.countAbort = []
        self.countCorrupt = []
        self.countCleared = []
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

        # format output
        # header: field length 8
        # status: field length 18
        # E_HOST: field length 40
        # EXE_EXIT_CODE: field lenght 13
        # JOB_EXIT_CODE: field lenght 15

        printline = ''
        printline+= "%-8s " % header[0]
        printline+= "%-18s %-40s %-13s %-15s" % ('STATUS','E_HOST','EXE_EXIT_CODE','JOB_EXIT_STATUS')
        print printline
        print '---------------------------------------------------------------------------------------------------'
        for_summary = {}
        orderdBossID = CoupJobsID.values()
        counter = 0
        for bossid in orderdBossID:
            # every 10 jobs, print a line for orientation
            if counter != 0 and counter%10 == 0 :
                print '---------------------------------------------------------------------------------------------------'
            counter += 1
            printline=''
            jobStatus=''

            # if JobDB status is 'Z', corrupted output tarball, don't check status
            if common.jobDB.status(int(jobAttributes[bossid][0].strip())-1) == 'Z' :
                jobStatus = 'Cleared (Corrupt)'
            else :
                jobStatus = common.scheduler.queryStatus(bossTaskId, bossid)
            # debug
            msg = 'jobStatus' + jobStatus
            common.logger.debug(4,msg)
            ###
            for_summary[int(jobAttributes[bossid][0].strip())] = jobStatus
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
            
            if jobStatus == 'Done (Success)' or jobStatus == 'Cleared':
                printline+="%-8s %-18s %-40s %-13s %-15s" % (jobAttributes[bossid][0],jobStatus,dest,exe_code,job_exit_status)
            elif jobStatus == 'Created':
                printline+="%-8s %-18s %-40s %-13s %-15s" % (jobAttributes[bossid][0],'Created',dest,'','')
                pass
            else:
                printline+="%-8s %-18s %-40s %-13s %-15s" % (jobAttributes[bossid][0],jobStatus,dest,'','')
            resFlag = 0
            if jobStatus != 'Created'  and jobStatus != 'Unknown':
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
        for id in statusList.keys():
            if statusList[id] == 'Created':
                self.countCreated.append(id)
            elif statusList[id] == 'Done (Success)' or statusList[id] == 'Done (Aborted)':
                self.countDone.append(id)
	        common.jobDB.setStatus(int(id)-1, 'D')
            elif statusList[id] == 'Running' :
                self.countRun.append(id)
            elif statusList[id] == 'Scheduled' :
                self.countSched.append(id)
            elif statusList[id] == 'Ready' :
                self.countReady.append(id)
            elif statusList[id] == 'Cancelled' or statusList[id] == 'Killed':
                self.countCancel.append(id)
                common.jobDB.setStatus(int(id)-1, 'K')
            elif statusList[id] == 'Aborted':
                self.countAbort.append(id)
                common.jobDB.setStatus(int(id)-1, 'A')
            elif statusList[id] == 'Cleared (Corrupt)':
                self.countCorrupt.append(id)
                common.jobDB.setStatus(int(id)-1, 'Z')
            elif statusList[id] == 'Cleared':
                self.countCleared.append(id)
                pass 

        common.jobDB.save()
        common.logger.debug(5,'done loop StatusBoss::report')
 
        self.countToTjob = (len(statusList.keys())) 
        return

    def PrintReport_(self):
        print ''
        print ">>>>>>>>> %i Total Jobs " % (self.countToTjob)

        if (len(self.countCreated) != 0):
            print ''
            print ">>>>>>>>> %i Jobs Created" % len(self.countCreated)
            print "          List of jobs: %s" % self.joinIntArray_(self.countCreated)
        if (len(self.countReady) != 0):
            print ''
            print ">>>>>>>>> %i Jobs Ready" % len(self.countReady)
            print "          List of jobs: %s" % self.joinIntArray_(self.countReady)
        if (len(self.countSched) != 0):
            print ''
            print ">>>>>>>>> %i Jobs Scheduled" % len(self.countSched)
            print "          List of jobs: %s" % self.joinIntArray_(self.countSched)
        if (len(self.countRun) != 0):
            print ''
            print ">>>>>>>>> %i Jobs Running" % len(self.countRun)
            print "          List of jobs: %s" % self.joinIntArray_(self.countRun)
        if (len(self.countCancel) != 0):
            print ''
            print ">>>>>>>>> %i Jobs canceled/killed" % len(self.countCancel)
            print "          List of jobs: %s" % self.joinIntArray_(self.countCancel)
            print "          You can resubmit them specifying JOB numbers: crab -resubmit JOB_number (or range of JOB)" 
            print "          (i.e -resubmit 1-3 => 1 and 2 and 3 or -resubmit 1,3 => 1 and 3)"       
        if (len(self.countAbort) != 0):
            self.countAbort.sort()
            print ''
            print ">>>>>>>>> %i Jobs aborted" % len(self.countAbort)
            print "          List of jobs: %s" % self.joinIntArray_(self.countAbort)
            print "          You can resubmit them specifying JOB numbers: crab -resubmit JOB_number (or range of JOB)" 
            print "          (i.e -resubmit 1-3 => 1 and 2 and 3 or -resubmit 1,3 => 1 and 3)"       
        if (len(self.countDone) != 0):
            self.countDone.sort()
            print ''
            print ">>>>>>>>> %i Jobs Done" % len(self.countDone)
            print "          List of jobs: %s" % self.joinIntArray_(self.countDone)
            print "          Retrieve them with: crab -getoutput to retrieve all" 
            print "          or specifying JOB numbers (i.e -getoutput 1-3 => 1 and 2 and 3 or -getoutput 1,3 => 1 and 3)"
        if (len(self.countCorrupt) != 0):
            self.countCorrupt.sort()
            print ''
            print ">>>>>>>>> %i Jobs cleared with corrupt output" % len(self.countCorrupt)
            print "          List of jobs: %s" % self.joinIntArray_(self.countCorrupt)
            print "          You can resubmit them specifying JOB numbers: crab -resubmit JOB_number (or range of JOB)" 
            print "          (i.e -resubmit 1-3 => 1 and 2 and 3 or -resubmit 1,3 => 1 and 3)"       
        if (len(self.countCleared) != 0):
            print ''
            print ">>>>>>>>> %i Jobs cleared" % len(self.countCleared)
            print "          List of jobs: %s" % self.joinIntArray_(self.countCleared)

         

    def joinIntArray_(self,array) :
        output = ''
        for item in array :
            output += str(item)+','
        if output[-1] == ',' :
            output = output[:-1]
        return output
