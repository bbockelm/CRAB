from Actor import *
import common, crab_util
import string, os, sys
import Statistic
from SchedulerBoss import *



class StatusBoss(Actor):
    def __init__(self, cfg_params, nj_list):
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        self.countDone = 0
        self.countReady = 0
        self.countSched = 0
        self.countRun = 0
        self.countCleared = 0
        self.countToTjob = 0
        

        Status = crab_util.importName('edg_wl_userinterface_common_LbWrapper', 'Status')
        # Bypass edg-job-status interfacing directly to C++ API
        # Job attribute vector to retrieve status without edg-job-status
        self.level = 0
        # Instance of the Status class provided by LB API
        self.jobStat = Status()

        self.states = [ "Acl", "cancelReason", "cancelling","ce_node","children", \
          "children_hist","children_num","children_states","condorId","condor_jdl", \
          "cpuTime","destination", "done_code","exit_code","expectFrom", \
          "expectUpdate","globusId","jdl","jobId","jobtype", \
          "lastUpdateTime","localId","location", "matched_jdl","network_server", \
          "owner","parent_job", "reason","resubmitted","rsl","seed",\
          "stateEnterTime","stateEnterTimes","subjob_failed", \
          "user tags" , "status" , "status_code","hierarchy"]
        self.hstates = {}
        for key in self.states:
            self.hstates[key]=''

        return

    def splitbyoffset(self,line,fields):
        ret_val=[]
        nn=fields.split(',')
        nfields=int(nn[0])
        nn[0]=0
        offs=0
        for i in range(1,nfields+1):
            offs = offs+int(nn[i-1])
            ret_val.append(line[offs:offs+int(nn[i])-1])
        return ret_val

    def run(self):

        """
        The main method of the class.
        """
        common.logger.debug(5, "Status::run() called")
        dir = string.split(common.work_space.topDir(), '/')
        group = dir[len(dir)-2]
        cmd = 'boss RTupdate -jobid all '
        runBossCommand(cmd)
        add2tablelist=''
        addjoincondition = ''
        nodeattr='JOB.E_HOST'
        cmd = 'boss SQL -query "select JOB.ID,crabjob.INTERNAL_ID,JOB.SID,crabjob.EXE_EXIT_CODE,JOB.E_HOST,crabjob.JOB_EXIT_STATUS  from JOB,crabjob'+add2tablelist+' where crabjob.JOBID=JOB.ID '+addjoincondition+' and JOB.GROUP_N=\''+group+'\' ORDER BY crabjob.INTERNAL_ID" '
        cmd_out = runBossCommand(cmd)
        jobAttributes={}
        nline=0
        header=''
        fielddesc=()
        for line in cmd_out.splitlines():
            if nline==0:
                fielddesc=line
            else:
                if nline==1:
                    header = self.splitbyoffset(line,fielddesc)
                else:
                    js = line.split(None,2)
                    #print "js = ", js
                    jobAttributes[int(js[0])]=self.splitbyoffset(line,fielddesc)
                    #print "jobAttributes = ",jobAttributes
            nline = nline+1
        printline = ''
        printline+=header[1]
        printline+='   STATUS          E_HOST            EXE_EXIT_CODE        JOB_EXIT_STATUS'
        print printline
        for_summary = []
        orderdBossID = jobAttributes.keys()
        orderdBossID.sort()
        for bossid in orderdBossID:
            printline=''
            jobStatus = common.scheduler.queryStatus(bossid)
            for_summary.append(jobStatus)
            exe_code =jobAttributes[bossid][3]
            try:
                dest = common.scheduler.queryDest(string.strip(jobAttributes[bossid][2])).split(":")[0]
            except: 
                dest = ''  
                pass 
            job_exit_status = jobAttributes[bossid][5]
            printline+=jobAttributes[bossid][1]
            
            if jobStatus == 'Done (Success)' or jobStatus == 'Cleared(BOSS)':
                printline+=' '+jobStatus+'   '+dest+'      '+exe_code+'       '+job_exit_status
            else:
                printline+=' '+jobStatus+'   '+dest
            resFlag = 0
            if jobStatus != 'Created(BOSS)'  and jobStatus != 'Unknown(BOSS)':
                jid1 = string.strip(jobAttributes[bossid][2])
                if jobStatus == 'Aborted':
                    Statistic.Monitor('checkstatus',resFlag,jid1,'abort')
                else:
                    Statistic.Monitor('checkstatus',resFlag,jid1,exe_code)   
            print printline
        self.Report_(for_summary)
        pass
        print ''


    def Report_(self,statusList) :
        
        """ Report #jobs for each status """

        common.logger.debug(5,'starting StatusBoss::report')
         
        countSche = 0
        countDone = 0
        countRun = 0
        countSche = 0
        countReady = 0
        countCancel = 0
        countAbort = 0
        countCleared = 0

       
        for status in statusList:
            if status == 'Done (Success)' or status == 'Done (Aborted)':
                countDone = countDone + 1
            elif status == 'Running' :
                countRun = countRun + 1
            elif status == 'Scheduled' :
                countSche = countSche + 1
            elif status == 'Ready' :
                countReady =  countReady + 1
            elif status == 'Cancelled' or status == 'Killed(BOSS)':
                countCancel =  countCancel + 1
            elif status == 'Aborted':
                countAbort =  countAbort + 1
            elif status == 'Cleared(BOSS)':
                countCleared = countCleared + 1


        common.logger.debug(5,'done loop StatusBoss::report')
        #job_stat = common.job_list.loadStatus()
 
        ToTjob = (len(statusList)) 
        print ''
        print ">>>>>>>>> %i Total Jobs " % (ToTjob)

        if (countReady != 0):
            print ''
            print ">>>>>>>>> %i Jobs Ready" % (countReady)
        if (countSche != 0):
            print ''
            print ">>>>>>>>> %i Jobs Scheduled" % (countSche)
        if (countRun != 0):
            print ''
            print ">>>>>>>>> %i Jobs Running" % (countRun)
        if (countCleared != 0):
            print ''
            print ">>>>>>>>> %i Jobs Retrieved (=Cleared)" % (countCleared)
        if (countCancel != 0) or (countAbort != 0):
            print ''
            tot = int(countAbort) + int(countCancel)
            print ">>>>>>>>> %i Jobs killed or Aborted" % (tot)
         #   print "          You can resubmit them specifying JOB numbers: crab.py -resubmit JOB_number (or range of JOB) -continue" 
         #   print "          (i.e -resubmit 1-3 => 1 and 2 and 3 or -resubmit 1,3 => 1 and 3)"       
        # if job_stat[6] or job_stat[7]:
        #     print ''
        #     print ">>>>>>>>> %i Jobs aborted or killed(=cancelled by user)" % (job_stat[6] + job_stat[7])
        #     print "          Resubmit them with: crab.py -resubmit -continue to resubmit all" 
        #     print "          or specifying JOB numbers (i.e -resubmit 1-3 => 1 and 2 and 3 or -resubmit 1,3 => 1 and 3)"       
        #     print "           "       
        if (countDone != 0):
            print ">>>>>>>>> %i Jobs Done" % (countDone)
            print "          Retrieve them with: crab.py -getoutput to retrieve all" 
            print "          or specifying JOB numbers (i.e -getoutput 1-3 => 1 and 2 and 3 or -getoutput 1,3 => 1 and 3)"
            print('\n')  
        pass
         

