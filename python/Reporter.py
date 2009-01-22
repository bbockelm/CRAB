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
        common.logger.debug(5, "Reporter::run() called")
        task = common._db.getTask()
        #print task
        print "\n----------------------------\n"
        print "Dataset: ",task['dataset']
        from ProdCommon.FwkJobRep.ReportParser import readJobReport
        possible_status = [ 'Created',
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
        eventsRead=0
        filesRead=0
        for job in task.getJobs():
            # get FJR filename
            fjr=task['outputDirectory']+job['outputFiles'][-1]
            #print fjr
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
                print 'no FJR avaialble'
            #print "--------------------------"
        print "Total Events read: ",eventsRead     
        print "Total Files read: ",filesRead     
        print "Total Jobs : ",len(task.getJobs())
        list_ID={}
        for st in possible_status:
            list_ID = common._db.queryAttrRunJob({'statusScheduler':st},'jobId')
            if (len(list_ID)>0):
                print "   # Jobs:",str(st),":",len(list_ID)
            pass
        print "\n----------------------------\n"
    
        return      
