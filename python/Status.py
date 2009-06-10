from Actor import *
import common
import string, os, time
import sha
from crab_util import *


class Status(Actor):
    def __init__(self, *args):
        self.cfg_params = args[0]
        self.verbose = (str(self.cfg_params.get("CRAB.status",'')) in ('verbose','v'))
        self.xml = self.cfg_params.get("USER.xml_report",'')
        self.server_name = ''
 
        return

    def run(self):
        """
        The main method of the class: compute the status and print a report
        """
        common.logger.debug( "Status::run() called")

        start = time.time()

        self.query()
        self.PrintReport_()
        ## TEMPORARY FIXME Ds
        msg = showWebMon(self.server_name)
        common.logger.info(msg)

        stop = time.time()
        common.logger.debug( "Status Time: "+str(stop - start))
        pass

    def query(self,display=True):
        """
        compute the status
        """
        common.logger.info("Checking the status of all jobs: please wait")
        task = common._db.getTask()
        upTask = common.scheduler.queryEverything(task['id'])
        self.compute(upTask,display)

    def compute(self, up_task, display=True ):

        toPrint=[]
        taskId = str(up_task['name'])
        task_unique_name = str(up_task['name'])
        ended = None

        run_jobToSave = {'state' :'Terminated'}
        listId=[]
        listRunField=[]

        self.wrapErrorList = []
        msg='\n'
        for job in up_task.jobs :
            id = str(job.runningJob['jobId'])
            jobStatus =  str(job.runningJob['statusScheduler'])
            jobState =  str(job.runningJob['state'])
            dest = str(job.runningJob['destination']).split(':')[0]
            exe_exit_code = str(job.runningJob['applicationReturnCode'])
            job_exit_code = str(job.runningJob['wrapperReturnCode'])
            self.wrapErrorList.append(job_exit_code)
            ended = str(job['standardInput'])  
            printline=''
            if dest == 'None' :  dest = ''
            if exe_exit_code == 'None' :  exe_exit_code = ''
            if job_exit_code == 'None' :  job_exit_code = ''
            if job.runningJob['state'] == 'Terminated' : jobStatus = 'Done'
            if job.runningJob['state'] == 'SubRequested' : jobStatus = 'Submitting'
            #TODO 09-Jun-2009 SL Not sure this is needed at all...
            if job.runningJob['status'] in ['SD','DA'] :
                listId.append(id)
                listRunField.append(run_jobToSave)
            if (self.verbose) :printline+="%-6s %-18s %-14s %-36s %-13s %-16s %-4s" % (id,jobStatus,jobState,dest,exe_exit_code,job_exit_code,ended)
            else: printline+="%-6s %-18s %-36s %-13s %-16s %-4s" % (id,jobStatus,dest,exe_exit_code,job_exit_code,ended)
            toPrint.append(printline)

            if jobStatus is not None:
                msg += self.dataToDash(job,id,taskId,task_unique_name,dest,jobStatus)
        common.logger.log(10-1,msg)
        #TODO 09-Jun-2009 SL Not sure this is needed at all...
        if len(listId) > 0 : common._db.updateRunJob_(listId, listRunField)
        header = ''
        if ended != None and len(ended) > 0:
            if (self.verbose): header+= "%-6s %-18s %-14s %-36s %-13s %-16s %-4s" % ('ID','STATUS','LAST_ACTION','E_HOST','EXE_EXIT_CODE','JOB_EXIT_STATUS','ENDED')
            else: header+= "%-6s %-18s %-36s %-13s %-16s %-4s" % ('ID','STATUS','E_HOST','EXE_EXIT_CODE','JOB_EXIT_STATUS','ENDED')
        else:
            if (self.verbose): header+= "%-6s %-18s %-14s %-36s %-13s %-16s" % ('ID','STATUS','LAST_ACTION','E_HOST','EXE_EXIT_CODE','JOB_EXIT_STATUS')
            else: header+= "%-6s %-18s %-36s %-13s %-16s" % ('ID','STATUS','E_HOST','EXE_EXIT_CODE','JOB_EXIT_STATUS')

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
                         'Retrieved'
                          ]

        jobs = common._db.nJobs('list')
        WrapExitCode = list(set(self.wrapErrorList))
        msg=  " %i Total Jobs \n" % (len(jobs))
        list_ID=[]
        for c in WrapExitCode:
            if c != 'None':
                self.reportCodes(c)
            else:
                for st in possible_status:
                    list_ID = common._db.queryAttrRunJob({'statusScheduler':st},'jobId')
                    if len(list_ID)>0:
                        if st == 'killed':
                            msg+=  ">>>>>>>>> %i Jobs %s \n" % (len(list_ID), str(st))
                            msg+=  "\tYou can resubmit them specifying JOB numbers: crab -resubmit <List of jobs>\n"
                            msg+=  "\tList of jobs: %s \n" % readableList(self,list_ID)
                        elif st == 'Aborted':
                            msg+=  ">>>>>>>>> %i Jobs %s\n " % (len(list_ID), str(st))
                            msg+=  "\tYou can resubmit them specifying JOB numbers: crab -resubmit <List of jobs>\n"
                            msg+=  "\tList of jobs: %s \n" % readableList(self,list_ID)
                        elif st == 'Done' or st == 'Done (Failed)' :
                            msg+=  ">>>>>>>>> %i Jobs %s\n " % (len(list_ID), str(st))
                            msg+=  "\tRetrieve them with: crab -getoutput <List of jobs>\n"
                            msg+=  "\tList of jobs: %s \n" % readableList(self,list_ID)
                        else   :
                            msg+=  ">>>>>>>>> %i Jobs %s \n " % (len(list_ID), str(st))
                            msg+=  "\tList of jobs %s: %s \n" % (str(st),readableList(self,list_ID))
        common.logger.info(msg)
        return

    def reportCodes(self,code): 
        """
        """
        list_ID = common._db.queryAttrRunJob({'wrapperReturnCode':code},'jobId')
        if len(list_ID)>0:
            msg = 'ExitCodes Summary\n'
            msg +=  ">>>>>>>>> %i Jobs with Wrapper Exit Code : %s \n " % (len(list_ID), str(code))
            msg +=  "\t List of jobs: %s \n" % readableList(self,list_ID)
            if (code!=0):
                msg +=  "\tSee https://twiki.cern.ch/twiki/bin/view/CMS/JobExitCodes for Exit Code meaning\n"

        common.logger.info(msg)
        return
 
    def dataToDash(self,job,id,taskId,task_unique_name,dest,jobStatus):
        jid = job.runningJob['schedulerId']
        job_status_reason = str(job.runningJob['statusReason'])
        job_last_time = str(job.runningJob['startTime'])
        msg = '' 
        if common.scheduler.name().upper() in ['CONDOR_G','GLIDEIN']:
            WMS = 'OSG'
            taskHash = sha.new(common._db.queryTask('name')).hexdigest()
            jobId = str(id) + '_https://' + common.scheduler.name() + '/' + taskHash + '/' + str(id)
            msg += ('JobID for ML monitoring is created for CONDOR_G scheduler: %s\n'%jobId)
        elif common.scheduler.name().upper() in ['LSF','CAF']:
            WMS = common.scheduler.name()
            jobId=str(id)+"_https://"+common.scheduler.name()+":/"+str(jid)+"-"+string.replace(task_unique_name,"_","-")
            msg += ('JobID for ML monitoring is created for Local scheduler: %s\n'%jobId)
        elif common.scheduler.name().upper() in ['ARC']:
            taskHash = sha.new(common._db.queryTask('name')).hexdigest()
            jobId = str(id) + '_https://' + common.scheduler.name() + '/' + taskHash + '/' + str(id)
            msg += ('JobID for ML monitoring is created for ARC scheduler: %s\n'%jobId)
            WMS = 'ARC'
        else:
            jobId = str(id) + '_' + str(jid)
            WMS = job.runningJob['service']
            msg += ('JobID for ML monitoring is created for gLite scheduler: %s'%jobId)
        pass

        msg += ("sending info to ML\n")
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
        msg += ('%s\n'%str(params))
        common.apmon.sendToML(params)

        return msg

    def joinIntArray_(self,array) :
        output = ''
        for item in array :
            output += str(item)+','
        if output[-1] == ',' :
            output = output[:-1]
        return output
