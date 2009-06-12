import os, common, string
from Actor import *
from crab_util import *

class Reporter(Actor):
    """ A class to report a short summary of the info of a task, including what
    is needed for user analysis, such as #events requestes/done, integrated
    lumi and so one.
    """
    def __init__(self, cfg_params):
        self.cfg_params = cfg_params
        return

    def run(self):
        """
        The main method of the class: report status of a task
        """
        common.logger.debug( "Reporter::run() called")
        task = common._db.getTask()

        msg= "--------------------\n"
        msg +=  "Dataset: %s\n"%str(task['dataset'])
        if self.cfg_params.has_key('USER.copy_data') and int(self.cfg_params['USER.copy_data'])==1:
            msg+=  "Remote output :\n"
            ## TODO: SL should come from jobDB!
            from PhEDExDatasvcInfo import PhEDExDatasvcInfo

            stageout = PhEDExDatasvcInfo(self.cfg_params)
            endpoint, lfn, SE, SE_PATH, user = stageout.getEndpoint()
            #print endpoint, lfn, SE, SE_PATH, user

            msg+=  "SE: %s %s  srmPath: %s\n"%(self.cfg_params['USER.storage_element'],SE,endpoint)
            
        else:
            msg+=  "Local output: %s"%task['outputDirectory']
        #print task
        from ProdCommon.FwkJobRep.ReportParser import readJobReport
        possible_status = [ 'Created',
                            'Undefined',
                            'Submitting',
                            'Submitted',
                            'NotSubmitted',
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
        eventsRead=0
        eventsRequired=0
        filesRead=0
        filesRequired=0
        for job in task.getJobs():
            if (job.runningJob['applicationReturnCode']>0 or job.runningJob['wrapperReturnCode']>0): continue
            # get FJR filename
            fjr=task['outputDirectory']+job['outputFiles'][-1]
            jobReport = readJobReport(fjr)
            if len(jobReport)>0:
                inputFiles=jobReport[0].inputFiles
                for inputFile in inputFiles:
                    runs=inputFile.runs
                    #print [inputFile[it] for it in ['LFN','EventsRead']]
                    # print "FileIn :",inputFile['LFN'],": Events",inputFile['EventsRead']
                    # for run in runs.keys():
                    #     print "Run",run,": lumi sections",runs[run]
                    filesRead+=1
                    eventsRead+=int(inputFile['EventsRead'])
                #print jobReport[0].inputFiles,'\n'
            else:
                pass
                #print 'no FJR avaialble for job #%s'%job['jobId']
            #print "--------------------------"
        msg+=  "Total Events read: %s required: %s\n"%(eventsRead,eventsRequired)
        msg+=  "Total Files read: %s required: %s\n"%(filesRead,filesRequired)
        msg+=  "Total Jobs : %s \n"%len(task.getJobs())
        list_ID={}
        task = common.scheduler.queryEverything(task['id'])
        for st in possible_status:
            list_ID = common._db.queryAttrRunJob({'statusScheduler':st},'jobId')
            if (len(list_ID)>0):
                msg+=  "   # Jobs: %s:%s\n"%(str(st),len(list_ID))
            pass
        msg+=  "\n----------------------------\n"
        common.logger.info(msg)   
        return      

                   
