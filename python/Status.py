from Actor import *
import common
import string, os, time
from crab_util import makeCksum

class Status(Actor):
    def __init__(self, *args):
        self.cfg_params = args[0]

        if common.scheduler.name().upper() == 'CONDOR_G':
            # create hash of cfg file
            self.hash = makeCksum(common.work_space.cfgFileName())
        else:
            self.hash = ''

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

    def compute(self):
        """
        compute the status
        """

        common.logger.message("Checking the status of all jobs: please wait")
        task = common._db.getTask()
        up_task = common.scheduler.queryEverything(task['id']) ## NeW BL--DS
        toPrint=[]
        for job in up_task.jobs :
            #print job['id']
            #id = str(job.runningJob['id'])
            id = str(job['id'])
            jobStatus =  str(job.runningJob['statusScheduler'])
            dest = str(job.runningJob['destination']).split(':')[0]
            exe_exit_code = str(job.runningJob['applicationReturnCode'])
            job_exit_code = str(job.runningJob['wrapperReturnCode']) 
            printline=''
        
#            if jobStatus == 'Done (Success)' or jobStatus == 'Cleared' or jobStatus == 'Done (Aborted)':
            if dest == 'None' :  dest = ''
            if exe_exit_code == 'None' :  exe_exit_code = ''
            if job_exit_code == 'None' :  job_exit_code = ''
            printline+="%-8s %-18s %-40s %-13s %-15s" % (id,jobStatus,dest,exe_exit_code,job_exit_code)
            toPrint.append(printline)
#           elif jobStatus == 'Created':
#               printline+="%-8s %-18s %-40s %-13s %-15s" % (bossid,'Created',dest,'','')
#               pass
#           else:
#               printline+="%-8s %-18s %-40s %-13s %-15s" % (bossid,jobStatus,dest,'','')
#               toPrint.append(printline)
            resFlag = 0

## Here to be implemented.. maybe putting stuff in a dedicated funcion.... better if not needed
#"""     
#            if jobStatus != 'Created'  and jobStatus != 'Unknown':
#                jid1 = string.strip(jobAttributes[bossid]['SCHED_ID'])
#
#                jobId = ''
#                if common.scheduler.name().upper() == 'CONDOR_G':
#                    jobId = str(bossid) + '_' + self.hash + '_' + string.strip(jobAttributes[bossid]['SCHED_ID'])
#                    common.logger.debug(5,'JobID for ML monitoring is created for CONDOR_G scheduler:'+jobId)
#                else:
#                    jobId = str(bossid) + '_' + string.strip(jobAttributes[bossid]['SCHED_ID'])
#                    if common.scheduler.name() == 'lsf' or common.scheduler.name() == 'caf':
#                        jobId=str(bossid)+"_https://"+common.scheduler.name()+":/"+string.strip(jobAttributes[bossid]['SCHED_ID'])+"-"+string.replace(common.taskDB.dict('taskId'),"_","-")
#                        common.logger.debug(5,'JobID for ML monitoring is created for LSF scheduler:'+jobId)
#                    pass
#                pass
#
#                common.logger.debug(5,"sending info to ML")
#                params = {}
#                if RB != None:
#                    params = {'taskId': common.taskDB.dict('taskId'), \
#                    'jobId': jobId,\
#                    'sid': string.strip(jobAttributes[bossid]['SCHED_ID']), \
#                    'StatusValueReason': job_status_reason, \
#                    'StatusValue': jobStatus, \
#                    'StatusEnterTime': job_last_time, \
#                    'StatusDestination': dest, \
#                    'RBname': RB }
#                else:
#                    params = {'taskId': common.taskDB.dict('taskId'), \
#                    'jobId': jobId,\
#                    'sid': string.strip(jobAttributes[bossid]['SCHED_ID']), \
#                    'StatusValueReason': job_status_reason, \
#                    'StatusValue': jobStatus, \
#                    'StatusEnterTime': job_last_time, \
#                    'StatusDestination': dest }
#                common.logger.debug(5,str(params))
#
#                common.apmon.sendToML(params)
##            if printline != '':
##                print printline

        self.detailedReport(toPrint)
  #      self.update_(for_summary)
        return

    def detailedReport(self, lines):

        counter = 0
        printline = ''
        printline+= "%-8s %-18s %-40s %-13s %-15s" % ('ID','STATUS','E_HOST','EXE_EXIT_CODE','JOB_EXIT_STATUS')
        print printline
        print '---------------------------------------------------------------------------------------------------'

        for i in range(len(lines)):
            if counter != 0 and counter%10 == 0 :
                print '---------------------------------------------------------------------------------------------------'
            print lines[i]
            counter += 1

    def PrintReport_(self):

        # query sui distinct statusScheduler
        #distinct_status =  common._db.queryDistJob('dlsDestination')
        possible_status = [
                         'Undefined', 
                         'Submitted',
                         'Waiting',
                         'Ready', 
                         'Scheduled',
                         'Running',
                         'Done',
                         'Cancelled',
                         'Aborted',
                         'Unknown',
                         'Done(failed)'
                         'Cleared'
                          ]

        print ''
        print ">>>>>>>>> %i Total Jobs " % (common._db.nJobs())
        print ''
        list_ID=[] 
        for st in possible_status:
            list_ID = common._db.queryAttrRunJob({'statusScheduler':st},'jobId')
            if len(list_ID)>0:
                print ">>>>>>>>> %i Jobs  %s " % (len(list_ID), str(st))#,len(list_ID)
                if st == 'killed' or st == 'Aborted': print "          You can resubmit them specifying JOB numbers: crab -resubmit JOB_number <Jobs list>"
                if st == 'Done'   : print "          Retrieve them with: crab -getoutput <Jobs list>"
		if st == 'Cleared': print "          %i Jobs with EXE_EXIT_CODE: %s" % (len(common._db.queryDistJob('wrapperReturnCode')))
                print "          List of jobs: %s" % str(common._db.queryAttrRunJob({'statusScheduler':st},'jobId'))
                print " "

#        if (len(self.countCorrupt) != 0):
#            self.countCorrupt.sort()
#            print ''
#            print ">>>>>>>>> %i Jobs cleared with corrupt output" % len(self.countCorrupt)
#            print "          List of jobs: %s" % self.joinIntArray_(self.countCorrupt)
#            print "          You can resubmit them specifying JOB numbers: crab -resubmit JOB_number <Jobs list>"
#        if (len(self.countCleared.keys()) != 0):
#            total_size = 0
#            for key in self.countCleared.keys() :
#                total_size += len(self.countCleared[key])
#            print ''



    def joinIntArray_(self,array) :
        output = ''
        for item in array :
            output += str(item)+','
        if output[-1] == ',' :
            output = output[:-1]
        return output

