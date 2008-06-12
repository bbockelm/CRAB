from Actor import *
import common
import string, os, time
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
        stop = time.time()
        common.logger.debug(1, "Status Time: "+str(stop - start))
        common.logger.write("Status Time: "+str(stop - start))
        pass

    def query(self):
        """
        compute the status
        """
        common.logger.message("Checking the status of all jobs: please wait")
        task = common._db.getTask()
        upTask = common.scheduler.queryEverything(task['id'])
        self.compute(upTask)

    def compute(self, up_task):

        toPrint=[]
        taskId= str("_".join(str(up_task['name']).split('_')[:-1]))
        self.wrapErrorList = []
        for job in up_task.jobs :
            id = str(job.runningJob['jobId'])
            jobStatus =  str(job.runningJob['statusScheduler'])
            dest = str(job.runningJob['destination']).split(':')[0]
            exe_exit_code = str(job.runningJob['applicationReturnCode'])
            job_exit_code = str(job.runningJob['wrapperReturnCode'])
            self.wrapErrorList.append(job_exit_code)
            printline=''
            header = ''
            if dest == 'None' :  dest = ''
            if exe_exit_code == 'None' :  exe_exit_code = ''
            if job_exit_code == 'None' :  job_exit_code = ''
            printline+="%-8s %-18s %-40s %-13s %-15s" % (id,jobStatus,dest,exe_exit_code,job_exit_code)
            toPrint.append(printline)

            if jobStatus is not None:
                self.dataToDash(job,id,taskId,dest,jobStatus)

        header = ''
        header+= "%-8s %-18s %-40s %-13s %-15s" % ('ID','STATUS','E_HOST','EXE_EXIT_CODE','JOB_EXIT_STATUS')

        displayReport(self,header,toPrint,self.xml)

        return

    def PrintReport_(self):

        jobs = common._db.nJobs('list')

        WrapExitCode = list(set(self.wrapErrorList))

        print ''
        print ">>>>>>>>> %i Total Jobs " % (len(jobs))
        print ''
        list_ID=[]
        for c in WrapExitCode:
            if c != 'None':
                list_ID = common._db.queryAttrRunJob({'wrapperReturnCode':c},'jobId')
                if len(list_ID)>0:
                    print ">>>>>>>>> %i Jobs with Wrapper Exit Code : %s " % (len(list_ID), str(c))#,len(list_ID)
         #           if st == 'killed' or st == 'Aborted': print "          You can resubmit them specifying JOB numbers: crab -resubmit JOB_number <Jobs list>"
         #           if st == 'Done'   : print "          Retrieve them with: crab -getoutput <Jobs list>"
         #           if st == 'Cleared': print "          %i Jobs with EXE_EXIT_CODE: %s" % (len(common._db.queryDistJob('wrapperReturnCode')))
                    print "          List of jobs: %s" % self.readableList(list_ID)
                    print " "

    def readableList(self,rawList):
      listString = str(rawList[0])
      endRange = ''
      for i in range(1,len(rawList)):
        if rawList[i] == rawList[i-1]+1:
          endRange = str(rawList[i])
        else:
          if endRange:
            listString += '-' + endRange + ',' + str(rawList[i])
            endRange = ''
          else:
            listString += ',' + str(rawList[i])
      if endRange:
        listString += '-' + endRange
        endRange = ''

      return listString


    def dataToDash(self,job,id,taskId,dest,jobStatus):


        jid = job.runningJob['schedulerId']
        job_status_reason = str(job.runningJob['statusReason'])
        job_last_time = str(job.runningJob['startTime'])
        if common.scheduler.name().upper() == 'CONDOR_G':
            WMS = 'OSG'
            self.hash = makeCksum(common.work_space.cfgFileName())
            jobId = str(id) + '_' + self.hash + '_' + str(jid)
            common.logger.debug(5,'JobID for ML monitoring is created for CONDOR_G scheduler:'+jobId)
        else:
            if common.scheduler.name() in ['lsf','caf']:
                WMS = common.scheduler.name()
                jobId=str(id)+"_https://"+common.scheduler.name()+":/"+str(jid)+"-"+string.replace(taskId,"_","-")
                common.logger.debug(5,'JobID for ML monitoring is created for Local scheduler:'+jobId)
            else:
                jobId = str(id) + '_' + str(jid)
                WMS = job.runningJob['service']
                common.logger.debug(5,'JobID for ML monitoring is created for gLite scheduler:'+jobId)
            pass
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

