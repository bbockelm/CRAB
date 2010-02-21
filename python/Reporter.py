import os, common, string
from Actor import *
from crab_util import *
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from LumiList import LumiList

try: # FUTURE: Python 2.6, prior to 2.6 requires simplejson
    import json
except:
    import simplejson as json


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
        lumis = []
        for job in task.getJobs():
            if (job.runningJob['applicationReturnCode']>0 or job.runningJob['wrapperReturnCode']>0): continue
            # get FJR filename
            fjr=task['outputDirectory']+job['outputFiles'][-1]
            jobReport = readJobReport(fjr)
            if len(jobReport) > 0:
                inputFiles = jobReport[0].inputFiles
                for inputFile in inputFiles:
                    # Accumulate the list of lum sections run over
                    for run in inputFile.runs.keys():
                        for lumi in inputFile.runs[run]:
                            lumis.append((run, lumi))
                    filesRead+=1
                    eventsRead+=int(inputFile['EventsRead'])
                #print jobReport[0].inputFiles,'\n'
            else:
                pass
                #print 'no FJR avaialble for job #%s'%job['jobId']
            #print "--------------------------"

        # Compact and write the list of successful lumis

        lumiList = LumiList(lumis = lumis)
        compactList = lumiList.getCompactList()

        lumiFilename = task['outputDirectory'] + 'lumiSummary.json'
        lumiSummary = open(lumiFilename, 'w')
        json.dump(compactList, lumiSummary)
        lumiSummary.write('\n')
        lumiSummary.close()

        msg += "Total Events read: %s\n" % eventsRead
        msg += "Total Files read: %s\n" % filesRead
        msg += "Total Jobs : %s\n" % len(task.getJobs())
        msg += "Luminosity section summary file: %s\n" % lumiFilename
        list_ID={}

        # TEMPORARY by Fabio, to be removed 
        # avoid clashes between glite_slc5 and glite schedulers when a server is used
        # otherwise, -report with a server requires a local scheduler 
        if self.cfg_params.get('CRAB.server_name', None) is None:
            common.logger.debug( "Reporter updating task status")
            task = common.scheduler.queryEverything(task['id'])

        for st in possible_status:
            list_ID = common._db.queryAttrRunJob({'statusScheduler':st},'jobId')
            if (len(list_ID)>0):
                msg+=  "   # Jobs: %s:%s\n"%(str(st),len(list_ID))
            pass
        msg+=  "\n----------------------------\n"
        common.logger.info(msg)
        return


