from Actor import *
import common
import string, os, time
import sha
from crab_util import *


class Status(Actor):
    def __init__(self, *args):
        self.cfg_params = args[0]
        self.xml = self.cfg_params.get("USER.xml_report",'')

        return

    def run(self):
        """
        The main method of the class: compute the status and print a report
        """
        common.logger.debug(5, "Status::run() called")

        start = time.time()

        self.query()
        self.PrintReport_()
        self.showWebMon()

        stop = time.time()
        common.logger.debug(1, "Status Time: "+str(stop - start))
        common.logger.write("Status Time: "+str(stop - start))
        pass

    def query(self,display=True):
        """
        compute the status
        """
        common.logger.message("Checking the status of all jobs: please wait")
        task = common._db.getTask()
        upTask = common.scheduler.queryEverything(task['id'])
        self.compute(upTask,display)

    def compute(self, up_task, display=True ):

        toPrint=[]
        taskId = uniqueTaskName(up_task['name'])
        task_unique_name = up_task['name']

        self.wrapErrorList = []
        for job in up_task.jobs :
            id = str(job.runningJob['jobId'])
            jobStatus =  str(job.runningJob['statusScheduler'])
            dest = str(job.runningJob['destination']).split(':')[0]
            exe_exit_code = str(job.runningJob['applicationReturnCode'])
            job_exit_code = str(job.runningJob['wrapperReturnCode'])
            self.wrapErrorList.append(job_exit_code)
            ended = str(job['standardInput'])  
            printline=''
            if dest == 'None' :  dest = ''
            if exe_exit_code == 'None' :  exe_exit_code = ''
            if job_exit_code == 'None' :  job_exit_code = ''
            printline+="%-6s %-18s %-36s %-13s %-16s %-4s" % (id,jobStatus,dest,exe_exit_code,job_exit_code,ended)
            toPrint.append(printline)

            if jobStatus is not None:
                self.dataToDash(job,id,taskId,task_unique_name,dest,jobStatus)
        header = ''
        if len(ended) > 0:
            header+= "%-6s %-18s %-36s %-13s %-16s %-4s" % ('ID','STATUS','E_HOST','EXE_EXIT_CODE','JOB_EXIT_STATUS','ENDED')
        else:
            header+= "%-6s %-18s %-36s %-13s %-16s" % ('ID','STATUS','E_HOST','EXE_EXIT_CODE','JOB_EXIT_STATUS')

        if display: displayReport(self,header,toPrint,self.xml)

        return

    def PrintReport_(self):


        possible_status = [
                         'Created',
                         'Undefined',
                         'Submitting',
                         'Submitted',
                         'Waiting',
                         'Ready',
                         'Scheduled',
                         'Running',
                         'Done',
                         'Killing',
                         'Killed',
                         'Aborted',
                         'Unknown',
                         'Done (Failed)',
                         'Cleared',
                         'retrieved'
                          ]

        jobs = common._db.nJobs('list')
        WrapExitCode = list(set(self.wrapErrorList))
        print ''
        print ">>>>>>>>> %i Total Jobs " % (len(jobs))
        print ''
        list_ID=[]
        for c in WrapExitCode:
            if c != 'None':
                self.reportCodes(c)
            else:
                for st in possible_status:
                    list_ID = common._db.queryAttrRunJob({'statusScheduler':st},'jobId')
                    if len(list_ID)>0:
                        if st == 'killed':
                            print ">>>>>>>>> %i Jobs %s  " % (len(list_ID), str(st))
                            print "          You can resubmit them specifying JOB numbers: crab -resubmit <List of jobs>"
                            print "          List of jobs: %s \n" % readableList(self,list_ID)
                        elif st == 'Aborted':
                            print ">>>>>>>>> %i Jobs %s  " % (len(list_ID), str(st))
                            print "          You can resubmit them specifying JOB numbers: crab -resubmit <List of jobs>"
                            print "          List of jobs: %s \n" % readableList(self,list_ID)
                        elif st == 'Done' or st == 'Done (Failed)' :
                            print ">>>>>>>>> %i Jobs %s  " % (len(list_ID), str(st))
                            print "          Retrieve them with: crab -getoutput <List of jobs>"
                            print "          List of jobs: %s \n" % readableList(self,list_ID)
                        else   :
                            print ">>>>>>>>> %i Jobs %s \n " % (len(list_ID), str(st))

    def reportCodes(self,code): 
        """
        """
        list_ID = common._db.queryAttrRunJob({'wrapperReturnCode':code},'jobId')
        if len(list_ID)>0:
            print ">>>>>>>>> %i Jobs with Wrapper Exit Code : %s " % (len(list_ID), str(code))
            print "          List of jobs: %s" % readableList(self,list_ID)
            print " "

        return
 
    def dataToDash(self,job,id,taskId,task_unique_name,dest,jobStatus):
        jid = job.runningJob['schedulerId']
        job_status_reason = str(job.runningJob['statusReason'])
        job_last_time = str(job.runningJob['startTime'])
        if common.scheduler.name().upper() in ['CONDOR_G','GLIDEIN']:
            WMS = 'OSG'
            taskHash = sha.new(common._db.queryTask('name')).hexdigest()
            jobId = str(id) + '_https://' + common.scheduler.name() + '/' + taskHash + '/' + str(id)
            common.logger.debug(5,'JobID for ML monitoring is created for CONDOR_G scheduler:'+jobId)
        elif common.scheduler.name().upper() in ['LSF','CAF']:
            WMS = common.scheduler.name()
            jobId=str(id)+"_https://"+common.scheduler.name()+":/"+str(jid)+"-"+string.replace(task_unique_name,"_","-")
            common.logger.debug(5,'JobID for ML monitoring is created for Local scheduler:'+jobId)
        else:
            jobId = str(id) + '_' + str(jid)
            WMS = job.runningJob['service']
            common.logger.debug(5,'JobID for ML monitoring is created for gLite scheduler:'+jobId)
        pass

        common.logger.debug(5,"sending info to ML")
        params = {}
        if WMS != None:
            params = {'taskId': taskId, \
            'jobId': jobId,\
            'sid': str(jid), \
            'StatusValueReason': job_status_reason, \
            'StatusValue': jobStatus, \
            'StatusEnterTime': job_last_time, \
            'StatusDestination': dest, \
            'RBname': WMS }
        else:
            params = {'taskId': taskId, \
            'jobId': jobId,\
            'sid': str(jid), \
            'StatusValueReason': job_status_reason, \
            'StatusValue': jobStatus, \
            'StatusEnterTime': job_last_time, \
            'StatusDestination': dest }
        common.logger.debug(5,str(params))
        common.apmon.sendToML(params)

        return

    def joinIntArray_(self,array) :
        output = ''
        for item in array :
            output += str(item)+','
        if output[-1] == ',' :
            output = output[:-1]
        return output


    def showWebMon(self):
        pass
