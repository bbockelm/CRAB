from Actor import *
import common, crab_util
import string, os, sys
import Statistic
from crab_util import *
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
        
        fileCODE1 = open(common.work_space.logDir()+"/.code","r")
        array = fileCODE1.read().split('::')
        self.ID1 = array[0]
        self.NJC = array[1]
        self.dataset = array[2]
        self.owner = array[3]
        fileCODE1.close()
        #######################################################################################



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

        EDGstatus={
            'W':'Created(BOSS)',
            'R':'Running',
            'SC':'Checkpointed',
            'SS':'Scheduled',
            'SR':'Ready',
            'RE':'Ready',
            'SW':'Waiting',
            'SU':'Submitted',
            'UN':'Undefined',
            'SK':'Cancelled',
            'SD':'Done (Success)',
            'SA':'Aborted',
            'DA':'Done (Aborted)',
            'SE':'Cleared',
            'OR':'Done (Success)',
            'A?':'Aborted(BOSS)',
            'K':'Killed(BOSS)',
            'E':'Cleared(BOSS)',
            'NA':'Unknown(BOSS)',
            'I?':'Idle(BOSS)',
            'O?':'Done(BOSS)',
            'R?':'Running(BOSS)'             
            }
        """
        The main method of the class.
        """
        common.logger.debug(5, "Status::run() called")
#        common.jobDB.load()
        dir = string.split(common.work_space.topDir(), '/')
        group = dir[len(dir)-2]
        cmd = 'boss RTupdate -jobid all '
        runBossCommand(cmd)
        add2tablelist=''
        addjoincondition = ''
        nodeattr='JOB.E_HOST'
#        boss_scheduler_name = string.lower(self.boss_scheduler.name())
#        if boss_scheduler_name == 'edg' :
#            add2tablelist+=',edg'
#            addjoincondition=' and edg.JOBID=JOB.ID'
#            nodeattr='edg.CE'
        cmd = 'boss SQL -query "select JOB.ID,crabjob.INTERNAL_ID,JOB.SID,crabjob.EXE_EXIT_CODE,JOB.E_HOST  from JOB,crabjob'+add2tablelist+' where crabjob.JOBID=JOB.ID '+addjoincondition+' and JOB.GROUP_N=\''+group+'\' ORDER BY crabjob.INTERNAL_ID" '
        cmd_out = runBossCommand(cmd)
        #print "cmd_out = ", cmd_out
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
                    jobAttributes[int(js[0])]=self.splitbyoffset(line,fielddesc)
            nline = nline+1
        cmd = 'boss q -all -statusOnly -group '+group
        cmd_out = runBossCommand(cmd)
        jobStatus={}
        for line in cmd_out.splitlines():
            js = line.split(None,2)               
            jobStatus[int(js[0])]=EDGstatus[js[1]]
        #printfields = [1,2,3,4]
        printfields = [1,2,4]
        printfields1 = [3]
        printline = ''
        for i in printfields:
            printline+=header[i]
        printline+=' STATUS       EXIT_CODE'
        print printline
        for bossid in jobAttributes.keys():
            printline=''
            for i in printfields1:
                exe_code =jobAttributes[bossid][i]
            for i in printfields:
                printline+=jobAttributes[bossid][i]
                if jobStatus[bossid] != 'Created(BOSS)'  and jobStatus[bossid] != 'Unknown(BOSS)':
                    destination = "---" #jobAttributes[bossid][4]
                    broker = jobAttributes[bossid][2].split("/")[2].split(":")[0]
                    ID3 = jobAttributes[bossid][2].split("/")[3]
            if jobStatus[bossid] == 'Done (Success)' or jobStatus[bossid] == 'Cleared(BOSS)':
                printline+=' '+jobStatus[bossid]+' '+exe_code
            else:
                printline+=' '+jobStatus[bossid]
            resFlag = 0
            if jobStatus[bossid] != 'Created(BOSS)'  and jobStatus[bossid] != 'Unknown(BOSS)':
                Statistic.notify('checkstatus',resFlag,exe_code,self.dataset,self.owner,destination,broker,ID3,self.ID1,self.NJC)
            print printline
        self.Report_()
        pass
        print ''


    def Report_(self) :

        """ Report #jobs for each status  """  
        common.logger.debug(5,'starting StatusBoss::report')
        countSche = 0
        countDone = 0
        countRun = 0
        countSche = 0
        countReady = 0
        countCancel = 0
        countAbort = 0
        countCleared = 0  
        listBoss=common.scheduler.listBoss()
        countToTjob = len(listBoss)
        dirGroup = string.split(common.work_space.topDir(), '/') 
        group = dirGroup[len(dirGroup)-2]
        for id in listBoss: 
            boss_id =  common.scheduler.boss_ID((id +1),group)
            status = common.scheduler.queryStatus(boss_id)
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
            elif status == 'Cleared':            
                countCleared = countCleared + 1




        common.logger.debug(5,'done loop StatusBoss::report')
        #job_stat = common.job_list.loadStatus()

        print ''
        print ">>>>>>>>> %i Total Jobs " % (countToTjob)

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
        if (countCancel != 0) :
            print ''
            print ">>>>>>>>> %i Jobs killed " % (countCancel)
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


