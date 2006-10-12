from Actor import *
import common, crab_util
import string, os, sys, time
import Statistic
from SchedulerBoss import *
import time

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
        self.countCleared = {}
        return

    def run(self):
        """
        The main method of the class: compute the status and print a report
        """
        common.logger.debug(5, "Status::run() called")

        start = time.time()
        self.compute()
        self.PrintReport_()
        stop = time.time()
        common.logger.debug(1, "Status Time: "+str(stop - start))
        common.logger.write("Status Time: "+str(stop - start))
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

        # load jobdb
        common.jobDB.load()

        # query TaskId from taskdb
        bossTaskId=common.taskDB.dict('BossTaskId')

        common.logger.message("Checking the status of all jobs: please wait")

        # update RT information
        cmd = 'boss RTupdate -taskid '+bossTaskId
        runBossCommand(cmd)
        
        # update status in Boss database
        results = common.scheduler.queryEveryStatus(bossTaskId)

        # query Boss database
        add2tablelist=''
        addjoincondition = ''

        # SQL query format
        # JOB.CHAIN_ID
        # JOB.SCHED_ID
        # JOB.EXEC_HOST
        # JOB.SUB_T
        # JOB.START_T
        # JOB.STOP_T
        # JOB.GETOUT_T
        # JOB.LAST_T
        # crabjob.EXE_EXIT_CODE
        # crabjob.JOB_EXIT_STATUS

        # query unfinished jobs
        #cmd = 'bossAdmin SQL -fieldsLen -query "select JOB.CHAIN_ID,JOB.SCHED_ID,crabjob.EXE_EXIT_CODE,JOB.EXEC_HOST,crabjob.JOB_EXIT_STATUS  from JOB,crabjob'+add2tablelist+' where crabjob.CHAIN_ID=JOB.CHAIN_ID '+addjoincondition+' and JOB.TASK_ID=\''+bossTaskId+'\' and JOB.SCHED_ID!=\'\' ORDER BY crabjob.CHAIN_ID"' 

        
        #SL SCHED_ table is not present for condor_g, so must change the SQL query
        if common.scheduler.boss_scheduler_name != "condor_g" :
            schedTableName = 'SCHED_'+string.lower(common.scheduler.boss_scheduler_name)
            schedTable1 = ','+schedTableName+'.DEST_CE,'+schedTableName+'.STATUS_REASON,'+schedTableName+'.LAST_T'
            schedTable2 = ','+schedTableName
            schedTable3 = ' and '+schedTableName+'.CHAIN_ID=JOB.CHAIN_ID '
            schedTable3Ended = ' and '+schedTableName+'.CHAIN_ID=ENDED_JOB.CHAIN_ID '
        else:
            schedTable1 = ''
            schedTable2 = ''
            schedTable3 = ''
            schedTable3Ended = ''

        cmd = 'bossAdmin SQL -fieldsLen -query "select JOB.CHAIN_ID,JOB.SCHED_ID,crabjob.EXE_EXIT_CODE,JOB.EXEC_HOST,crabjob.JOB_EXIT_STATUS'+schedTable1+' from JOB,crabjob'+add2tablelist+schedTable2+' where crabjob.CHAIN_ID=JOB.CHAIN_ID '+addjoincondition+' and JOB.TASK_ID=\''+bossTaskId+'\' and JOB.SCHED_ID!=\'\' '+schedTable3+'  ORDER BY crabjob.CHAIN_ID"' 

        cmd_out = runBossCommand(cmd)
        jobAttributes={}
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
                    array = self.splitbyoffset_(line,fielddesc)
                    jobAttributes[int(array[0])]= array
            nline = nline+1

        # SQL query format
        # ENDED_ JOB.CHAIN_ID
        # ENDED_ JOB.SCHED_ID
        # ENDED_ JOB.EXEC_HOST
        # ENDED_ JOB.SUB_T
        # ENDED_ JOB.START_T
        # ENDED_ JOB.STOP_T
        # ENDED_ JOB.GETOUT_T
        # ENDED_ JOB.LAST_T
        # ENDED_ crabjob.EXE_EXIT_CODE
        # ENDED_ crabjob.JOB_EXIT_STATUS
        # ENDED_ crabjob.DEST_CE
        # ENDED_ crabjob.STATUS_REASON
        # ENDED_ crabjob.LAST_T

        # query also the ended table to get job status of jobs already retrieved
        #cmd = 'bossAdmin SQL -fieldsLen -query "select ENDED_JOB.CHAIN_ID,ENDED_JOB.SCHED_ID,ENDED_crabjob.EXE_EXIT_CODE,ENDED_JOB.EXEC_HOST,ENDED_crabjob.JOB_EXIT_STATUS,'+schedTableName+'.DEST_CE,'+schedTableName+'.STATUS_REASON,'+schedTableName+'.LAST_T  from ENDED_JOB,ENDED_crabjob'+add2tablelist+','+schedTableName+' where ENDED_crabjob.CHAIN_ID=ENDED_JOB.CHAIN_ID '+addjoincondition+' and ENDED_JOB.TASK_ID=\''+bossTaskId+'\'  and ENDED_JOB.SCHED_ID!=\'\' and '+schedTableName+'.CHAIN_ID=ENDED_JOB.CHAIN_ID ORDER BY ENDED_crabjob.CHAIN_ID"' 
        cmd = 'bossAdmin SQL -fieldsLen -query "select ENDED_JOB.CHAIN_ID,ENDED_JOB.SCHED_ID,ENDED_crabjob.EXE_EXIT_CODE,ENDED_JOB.EXEC_HOST,ENDED_crabjob.JOB_EXIT_STATUS'+schedTable1+' from ENDED_JOB,ENDED_crabjob'+add2tablelist+schedTable2+' where ENDED_crabjob.CHAIN_ID=ENDED_JOB.CHAIN_ID '+addjoincondition+' and ENDED_JOB.TASK_ID=\''+bossTaskId+'\' and ENDED_JOB.SCHED_ID!=\'\' '+schedTable3Ended+'  ORDER BY ENDED_crabjob.CHAIN_ID"' 
        cmd_out = runBossCommand(cmd)
        nline=0
        for line in cmd_out.splitlines():
            if nline==1:
                fielddesc=line
            else:
                if nline==2:
                    header = self.splitbyoffset_(line,fielddesc)
                elif nline > 2:
                    array = self.splitbyoffset_(line,fielddesc)
                    jobAttributes[int(array[0])]= array
            nline = nline+1

        # format output
        # header: field length 8
        # status: field length 18
        # E_HOST: field length 40
        # EXE_EXIT_CODE: field lenght 13
        # JOB_EXIT_CODE: field lenght 15

        # secure header if BossDB does not give any answer back (jobs created but not submitted)
        if header == '' :
            header = ['Chain']

        # pritn header
        printline = ''
        printline+= "%-8s " % header[0]
        printline+= "%-18s %-40s %-13s %-15s" % ('STATUS','E_HOST','EXE_EXIT_CODE','JOB_EXIT_STATUS')
        print printline
        print '---------------------------------------------------------------------------------------------------'
        for_summary = {}
        orderedBossID = jobAttributes.keys()
        orderedBossID.sort()
        counter = 0
        now = time.time()
        for bossid in orderedBossID:
            # every 10 jobs, print a line for orientation
            if counter != 0 and counter%10 == 0 :
                print '---------------------------------------------------------------------------------------------------'
            counter += 1
            printline=''

            # if JobDB status is 'Z', corrupted output tarball, don't check status
            if common.jobDB.status(int(jobAttributes[bossid][0].strip())-1) == 'Z' :
                jobStatus = 'Cleared (Corrupt)'
            else :
                try: jobStatus = results[bossid]
                except: jobStatus = 'Unknown'

            # debug
            msg = 'jobStatus' + jobStatus
            common.logger.debug(4,msg)
            ###
            exe_code =jobAttributes[bossid][2]   ##BOSS4 EXE_EXIT_CODE
            for_summary[int(jobAttributes[bossid][0].strip())] = jobStatus + '_' + str(exe_code)
   
            ###########------> This info must be come from BOSS4      DS.
            ###########------> For the moment BOSS know only WN, but then it will know also CE   DS.
            # try:
            #     if common.scheduler.boss_scheduler_name == "condor_g" :
            #         ldest = common.scheduler.queryDest(string.strip(jobAttributes[bossid][0]))  ##BOSS4 CHAIN_ID
            #     else :
            #         ldest = common.scheduler.queryDest(string.strip(jobAttributes[bossid][1]))  ##BOSS4 SCHED_ID 
            #     if ( ldest.find(":") != -1 ) :
            #         dest = ldest.split(":")[0]
            #     else :
            #         dest = ldest
            # except: 
            #     dest = ''  
            #     pass
            ############# -----> For the moment is WN but it will became CE....    DS.

 
            job_exit_status = jobAttributes[bossid][4]   ##BOSS4 JOB_EXIT_STATUS

            ##SL For condor_g need to get some info from scheduler
            if common.scheduler.boss_scheduler_name == "condor_g" :
                try:
                    ldest = common.scheduler.queryDest(string.strip(jobAttributes[bossid][0]))  ##BOSS4 CHAIN_ID
                    if ( ldest.find(":") != -1 ) :
                        dest = ldest.split(":")[0]
                    else :
                        dest = ldest
                        job_status_reason = ''
                        job_last_time = ''
                except: 
                    dest = ''  
                    job_status_reason = common.scheduler.getAttribute(string.strip(jobAttributes[bossid][1]), 'reason')
                    job_last_time = common.scheduler.getAttribute(string.strip(jobAttributes[bossid][1]), 'stateEnterTime')
                    pass
            else :
                dest = jobAttributes[bossid][5]
                job_status_reason = jobAttributes[bossid][6]   ##BOSS4 STATUS_REASON
                job_last_time = jobAttributes[bossid][7]   ##BOSS4 LAST_T
            
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
                    common.logger.debug(7,"sending info to ML")
                    params = {'taskId': self.cfg_params['taskId'], \
                    'jobId': str(bossid) + '_' + string.strip(jobAttributes[bossid][1]), \
                    'sid': string.strip(jobAttributes[bossid][2]), \
                    'StatusValueReason': job_status_reason, \
                    'StatusValue': jobStatus, \
                    'StatusEnterTime': job_last_time, \
                    'StatusDestination': dest}
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
            if statusList[id].split('_')[0] == 'Created':
                self.countCreated.append(id)
            elif statusList[id].split('_')[0] == 'Done (Success)' or statusList[id].split('_')[0] == 'Done (Aborted)':
                self.countDone.append(id)
	        common.jobDB.setStatus(int(id)-1, 'D')
            elif statusList[id].split('_')[0] == 'Running' :
                self.countRun.append(id)
            elif statusList[id].split('_')[0] == 'Scheduled' :
                self.countSched.append(id)
            elif statusList[id].split('_')[0] == 'Ready' :
                self.countReady.append(id)
            elif statusList[id].split('_')[0] == 'Cancelled' or statusList[id].split('_')[0] == 'Killed':
                self.countCancel.append(id)
                common.jobDB.setStatus(int(id)-1, 'K')
            elif statusList[id].split('_')[0] == 'Aborted':
                self.countAbort.append(id)
                common.jobDB.setStatus(int(id)-1, 'A')
            elif statusList[id].split('_')[0] == 'Cleared (Corrupt)':
                self.countCorrupt.append(id)
                common.jobDB.setStatus(int(id)-1, 'Z')
            elif statusList[id].split('_')[0] == 'Cleared':
                if statusList[id].split('_')[-1] in self.countCleared.keys() :
                    self.countCleared[statusList[id].split('_')[-1]].append(id)
                else :
                    self.countCleared[statusList[id].split('_')[-1]] = [id]
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
            print "          You can resubmit them specifying JOB numbers: crab -resubmit JOB_number <Jobs list>" 
        if (len(self.countAbort) != 0):
            self.countAbort.sort()
            print ''
            print ">>>>>>>>> %i Jobs aborted" % len(self.countAbort)
            print "          List of jobs: %s" % self.joinIntArray_(self.countAbort)
            print "          You can resubmit them specifying JOB numbers: crab -resubmit JOB_number <Jobs list>" 
        if (len(self.countDone) != 0):
            self.countDone.sort()
            print ''
            print ">>>>>>>>> %i Jobs Done" % len(self.countDone)
            print "          List of jobs: %s" % self.joinIntArray_(self.countDone)
            print "          Retrieve them with: crab -getoutput <Jobs list>"
        if (len(self.countCorrupt) != 0):
            self.countCorrupt.sort()
            print ''
            print ">>>>>>>>> %i Jobs cleared with corrupt output" % len(self.countCorrupt)
            print "          List of jobs: %s" % self.joinIntArray_(self.countCorrupt)
            print "          You can resubmit them specifying JOB numbers: crab -resubmit JOB_number <Jobs list>" 
        if (len(self.countCleared.keys()) != 0):
            total_size = 0
            for key in self.countCleared.keys() :
                total_size += len(self.countCleared[key])
            print ''
            print ">>>>>>>>> %i Jobs cleared" % total_size
            for key in self.countCleared.keys() :
                print "          %i Jobs with EXE_EXIT_CODE: %s" % (len(self.countCleared[key]),key)
                print "          List of jobs: %s" % self.joinIntArray_(self.countCleared[key])

         

    def joinIntArray_(self,array) :
        output = ''
        for item in array :
            output += str(item)+','
        if output[-1] == ',' :
            output = output[:-1]
        return output
