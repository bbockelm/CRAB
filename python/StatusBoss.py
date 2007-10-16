from Actor import *
import common
import string, os, time
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
        self.countCleared = {}

        if common.scheduler.boss_scheduler_name == 'condor_g':
            # create hash of cfg file
            self.hash = makeCksum(common.work_space.cfgFileName())
        else:
            self.hash = ''

        # check rt flag
        try:
            self.doRT = int(self.cfg_params["USER.use_boss_rt"])
        except:
            self.doRT = 0


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
        if self.doRT :
            try:
                common.scheduler.bossUser.RTupdate(bossTaskId)
            except:
                common.logger.message("Problem in contacting RealTime server")
        
        # update status in Boss database
        jobAttributes = common.scheduler.queryEverything(bossTaskId)

        # query Boss database

        # format output
        # header: field length 8
        # status: field length 18
        # E_HOST: field length 40
        # EXE_EXIT_CODE: field lenght 13
        # JOB_EXIT_CODE: field lenght 15

        # secure header if BossDB does not give any answer back (jobs created but not submitted)
        # header = ['Chain', 'SCHED_ID', 'STATUS', 'EXEC_HOST', 'STATUS_REASON','LAST_T','DEST_CE','EXE_EXIT_CODE','JOB_EXIT_STATUS']

        # pritn header
#        printline = ''
#        printline+= "%-8s " % header[0]
#        printline+= "%-18s %-40s %-13s %-15s" % ('STATUS','E_HOST','EXE_EXIT_CODE','JOB_EXIT_STATUS')
#        print printline
#        print '---------------------------------------------------------------------------------------------------'
        for_summary = {}
        orderedBossID = jobAttributes.keys()
        orderedBossID.sort()
        # counter = 0
        toPrint = []
        # now = time.time()
        for bossid in orderedBossID:
            # every 10 jobs, print a line for orientation
#            if counter != 0 and counter%10 == 0 :
#                print '---------------------------------------------------------------------------------------------------'
#            counter += 1
            printline=''

            # if JobDB status is 'Z', corrupted output tarball, don't check status
            if common.jobDB.status(int(bossid)-1) == 'Z' :
                jobStatus = 'Cleared (Corrupt)'
            else :
                try: jobStatus = jobAttributes[bossid]['STATUS']
                except: jobStatus = 'Unknown'
            #print "RB = " + str(jobAttributes[bossid]['RB'])
            RB = None
            try:
                RB = str(jobAttributes[bossid]['RB'])
            except:
                RB = None
            # debug
            msg = 'jobStatus' + jobStatus
            common.logger.debug(4,msg)
            ###
            exe_code =jobAttributes[bossid]['EXE_EXIT_CODE']   ##BOSS4 EXE_EXIT_CODE
            for_summary[int(bossid)] = jobStatus + '_' + str(exe_code)
   
            job_exit_status = jobAttributes[bossid]['JOB_EXIT_STATUS']   ##BOSS4 JOB_EXIT_STATUS

            ##SL For condor_g need to get some info from scheduler
            if common.scheduler.boss_scheduler_name == "condor_g" :
                try:
                    ldest = common.scheduler.queryDest(bossid) ##BOSS4 CHAIN_ID
                    if ( ldest.find(":") != -1 ) :
                        dest = ldest.split(":")[0]
                    else :
                        dest = ldest
                    job_status_reason = common.scheduler.getAttribute(bossid, 'reason')
                    job_last_time = common.scheduler.getAttribute(bossid, 'stateEnterTime')
                except: 
                    dest = ''  
                    job_status_reason = common.scheduler.getAttribute(bossid, 'reason')
                    job_last_time = common.scheduler.getAttribute(bossid, 'stateEnterTime')
                    pass
            else :
                dest = ''
                job_status_reason = ''
                job_last_time = ''
                if jobAttributes[bossid].has_key('DEST_CE') :
                    dest = jobAttributes[bossid]['DEST_CE']
                if jobAttributes[bossid].has_key('STATUS_REASON') :
                    job_status_reason = jobAttributes[bossid]['STATUS_REASON']   ##BOSS4 STATUS_REASON
#                if jobAttributes.has_key('LAST_T') :
#                job_last_time = jobAttributes[bossid]['LAST_T']   ##BOSS4 LAST_T
                if jobAttributes[bossid].has_key('LB_TIMESTAMP') :
                    job_last_time = jobAttributes[bossid]['LB_TIMESTAMP']   ##BOSS4 LAST_T

                    
            if jobStatus == 'Done (Success)' or jobStatus == 'Cleared' or jobStatus == 'Done (Aborted)':
                if exe_code.find('NULL') != -1 :
                    exe_code_string = ''
                else :
                    exe_code_string = exe_code
                if job_exit_status.find('NULL') != -1 :
                    job_exit_status_string = ''
                else :
                    job_exit_status_string = job_exit_status
                printline+="%-8s %-18s %-40s %-13s %-15s" % (bossid,jobStatus,dest,exe_code_string,job_exit_status_string)
                toPrint.append(printline)
            elif jobStatus == 'Created':
#                printline+="%-8s %-18s %-40s %-13s %-15s" % (bossid,'Created',dest,'','')
                pass
            else:
                printline+="%-8s %-18s %-40s %-13s %-15s" % (bossid,jobStatus,dest,'','')
                toPrint.append(printline)
            resFlag = 0
            if jobStatus != 'Created'  and jobStatus != 'Unknown':
                jid1 = string.strip(jobAttributes[bossid]['SCHED_ID'])

                # CrabMon  
                if jobStatus == 'Aborted':
                    Statistic.Monitor('checkstatus',resFlag,jid1,'abort',dest)
                else:
                    Statistic.Monitor('checkstatus',resFlag,jid1,exe_code,dest)   

                jobId = ''
                if common.scheduler.boss_scheduler_name == 'condor_g':
                    jobId = str(bossid) + '_' + self.hash + '_' + string.strip(jobAttributes[bossid]['SCHED_ID'])
                    common.logger.debug(5,'JobID for ML monitoring is created for CONDOR_G scheduler:'+jobId)
                else:
                    jobId = str(bossid) + '_' + string.strip(jobAttributes[bossid]['SCHED_ID'])
                    common.logger.debug(5,'JobID for ML monitoring is created for EDG scheduler'+jobId)

                if int(self.cfg_params['USER.activate_monalisa']) == 1:
                    common.logger.debug(7,"sending info to ML")
                    params = {}
                    if RB != None:
                        params = {'taskId': self.cfg_params['taskId'], \
                        'jobId': jobId,\
                        'sid': string.strip(jobAttributes[bossid]['SCHED_ID']), \
                        'StatusValueReason': job_status_reason, \
                        'StatusValue': jobStatus, \
                        'StatusEnterTime': job_last_time, \
                        'StatusDestination': dest, \
                        'RBname': RB }
                    else:
                        params = {'taskId': self.cfg_params['taskId'], \
                        'jobId': jobId,\
                        'sid': string.strip(jobAttributes[bossid]['SCHED_ID']), \
                        'StatusValueReason': job_status_reason, \
                        'StatusValue': jobStatus, \
                        'StatusEnterTime': job_last_time, \
                        'StatusDestination': dest }
                    common.logger.debug(5,str(params))

                    self.cfg_params['apmon'].sendToML(params)
#            if printline != '': 
#                print printline

        self.detailedReport(toPrint)
        self.update_(for_summary)
        return

    def detailedReport(self, lines):

        header = ['Chain', 'SCHED_ID', 'STATUS', 'EXEC_HOST', 'STATUS_REASON','LAST_T','DEST_CE','EXE_EXIT_CODE','JOB_EXIT_STATUS']
        counter = 0
        printline = ''
        printline+= "%-8s " % header[0]
        printline+= "%-18s %-40s %-13s %-15s" % ('STATUS','E_HOST','EXE_EXIT_CODE','JOB_EXIT_STATUS')
        print printline
        print '---------------------------------------------------------------------------------------------------'

        for i in range(len(lines)):
            if counter != 0 and counter%10 == 0 :
                print '---------------------------------------------------------------------------------------------------'
            print lines[i]
            counter += 1
    
    def status(self) :
        """ Return #jobs for each status as a tuple """
        return (self.countToTjob,self.countCreated,self.countReady,self.countSched,self.countRun,self.countCleared,self.countAbort,self.countCancel,self.countDone)

    def update_(self,statusList) :
        """ update the status of the jobs """

        # moved loading of jobDB before boss status check to enable condor_g scheduler to query jobdb for efficient access to destination
        # common.jobDB.load()
        for iid in statusList.keys():
            if statusList[iid].split('_')[0] == 'Created':
                self.countCreated.append(iid)
            elif statusList[iid].split('_')[0] == 'Done (Success)' :
                self.countDone.append(iid)
	        common.jobDB.setStatus(int(iid)-1, 'D')
            elif statusList[iid].split('_')[0] == 'Running' :
                self.countRun.append(iid)
	        common.jobDB.setStatus(int(iid)-1, 'R')
            elif statusList[iid].split('_')[0] == 'Scheduled' :
                self.countSched.append(iid)
            elif statusList[iid].split('_')[0] == 'Ready' :
                self.countReady.append(iid)
            elif statusList[iid].split('_')[0] == 'Cancelled' or statusList[iid].split('_')[0] == 'Killed':
                self.countCancel.append(iid)
                common.jobDB.setStatus(int(iid)-1, 'K')
            elif statusList[iid].split('_')[0] == 'Aborted' or statusList[iid].split('_')[0] == 'Done (Aborted)':
                self.countAbort.append(iid)
                common.jobDB.setStatus(int(iid)-1, 'A')
            elif statusList[iid].split('_')[0] == 'Cleared (Corrupt)':
                self.countCorrupt.append(iid)
                common.jobDB.setStatus(int(iid)-1, 'Z')
            elif statusList[iid].split('_')[0] == 'Cleared':
                if statusList[iid].split('_')[-1] in self.countCleared.keys() :
                    self.countCleared[statusList[iid].split('_')[-1]].append(iid)
                else :
                    self.countCleared[statusList[iid].split('_')[-1]] = [iid]
                pass 
	        common.jobDB.setStatus(int(iid)-1, 'Y')

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

