#!/usr/bin/env python

import sys, getopt, string

from ProdCommon.FwkJobRep.ReportParser import readJobReport
from DashboardAPI import apmonSend, apmonFree

class parseFjr:
    def __init__(self, argv):
        """
        parseCrabFjr
 
        - parse CRAB FrameworkJobReport on WN: { 'protocol' : { 'action' : [attempted,succedeed,total-size,total-time,min-time,max-time] , ... } , ... }
        - report parameters to DashBoard using DashBoardApi.py: for all 'read' actions of all protocols, report MBPS
        - return ExitStatus and dump of DashBoard report separated by semi-colon to WN wrapper script
        """
        # defaults
        self.input = ''
        self.MonitorID = ''
        self.MonitorJobID = ''
        self.info2dash = False 
        self.exitCode = False 
        self.lfnList = False  
        self.debug = 0
        try:
            opts, args = getopt.getopt(argv, "", ["input=", "dashboard=", "exitcode", "lfn" , "debug", "help"])
        except getopt.GetoptError:
            print self.usage()
            sys.exit(2)
        self.check(opts)  

        return

    def check(self,opts): 
        # check command line parameter
        for opt, arg in opts :
            if opt  == "--help" :
                print self.usage()
                sys.exit()
            elif opt == "--input" :
                self.input = arg
            elif opt == "--exitcode":
                self.exitCode = True 
            elif opt == "--lfn":
                self.lfnList = True 
            elif opt == "--dashboard":
                self.info2dash = True 
                try: 
                   self.MonitorID = arg.split(",")[0]
                   self.MonitorJobID = arg.split(",")[1]
                except:
                   self.MonitorID = ''
                   self.MonitorJobID = ''
            elif opt == "--debug" :
                self.debug = 1
                
        if self.input == '' or (not self.info2dash and not self.lfnList and not self.exitCode)  :
            print self.usage()
            sys.exit()
         
        if self.info2dash: 
            if self.MonitorID == '' or self.MonitorJobID == '':
                print self.usage()
                sys.exit()
        return

    def run(self): 

        # load FwkJobRep
        jobReport = readJobReport(self.input)[0]
        if self.exitCode : 
            self.exitCodes(jobReport)
        if self.lfnList : 
           self.lfn_List(jobReport)
        if self.info2dash : 
           self.reportDash(jobReport)
        return

    def exitCodes(self, jobReport):

        exit_status = ''
        ##### temporary fix for FJR incomplete ####
        fjr = open (self.input)
        len_fjr = len(fjr.readlines())
        if (len_fjr <= 6):
           ### 50115 - cmsRun did not produce a valid/readable job report at runtime
           exit_status = str(50115)
        else: 
            # get ExitStatus of last error
            if len(jobReport.errors) != 0 :
                exit_status = str(jobReport.errors[-1]['ExitStatus'])
            else :
                exit_status = str(0)
        #check exit code 
        if string.strip(exit_status) == '': exit_status = -999
        print exit_status   
  
        return

    def lfn_List(self,jobReport):
        ''' 
        get list of analyzed files 
        '''
        lfnlist = [x['LFN'] for x in jobReport.inputFiles]
        for file in lfnlist: print file
        return 

    def storageStat(self,jobReport):
        ''' 
        get i/o statistics 
        '''
        storageStatistics = str(jobReport.storageStatistics)
        storage_report = {}
        # check if storageStatistics is valid
        if storageStatistics.find('Storage statistics:') != -1 :
            # report form: { 'protocol' : { 'action' : [attempted,succedeed,total-size,total-time,min-time,max-time] , ... } , ... }
            data = storageStatistics.split('Storage statistics:')[1]
            data_fields = data.split(';')
            for data_field in data_fields:
                # parse: format protocol/action = attepted/succedeed/total-size/total-time/min-time/max-time
                if data_field == ' ' or not data_field or data_field == '':
                   continue
                key = data_field.split('=')[0].strip()
                item = data_field.split('=')[1].strip()
                protocol = str(key.split('/')[0].strip())
                action = str(key.split('/')[1].strip())
                item_array = item.split('/')
                attempted = str(item_array[0].strip())
                succeeded = str(item_array[1].strip())
                total_size = str(item_array[2].strip().split('MB')[0])
                total_time = str(item_array[3].strip().split('ms')[0])
                min_time = str(item_array[4].strip().split('ms')[0])
                max_time = str(item_array[5].strip().split('ms')[0])
                # add to report
                if protocol in storage_report.keys() :
                    if action in storage_report[protocol].keys() :
                        print 'protocol/action:',protocol,'/',action,'listed twice in report, taking the first'
                    else :
                        storage_report[protocol][action] = [attempted,succeeded,total_size,total_time,min_time,max_time]
                else :
                    storage_report[protocol] = {action : [attempted,succeeded,total_size,total_time,min_time,max_time] }
 
            if self.debug :
                for protocol in storage_report.keys() :
                    print 'protocol:',protocol
                    for action in storage_report[protocol].keys() :
                        print 'action:',action,'measurement:',storage_report[protocol][action]
 
        if self.debug == 1 : print storage_report
        return storage_report

    def n_of_events(self,jobReport):
        '''
        #Brian's patch to sent number of events procedded to the Dashboard 
        # Add NoEventsPerRun to the Dashboard report
        '''   
        event_report = {}
        eventsPerRun = 0
        for inputFile in jobReport.inputFiles:
            try:
                eventsRead = str(inputFile.get('EventsRead', 0))
                eventsRead = int(eventsRead.strip())
            except:
                continue
            eventsPerRun += eventsRead
        event_report['NoEventsPerRun'] = eventsPerRun
        event_report['NbEvPerRun'] = eventsPerRun
        event_report['NEventsProcessed'] = eventsPerRun

        if self.debug == 1 : print event_report

        return event_report
       
    def reportDash(self,jobReport):
        '''
        dashboard report dictionary 
        '''
        event_report = self.n_of_events(jobReport)
        storage_report = self.storageStat(jobReport)
        dashboard_report = {}
        #
        for k,v in event_report.iteritems() :
            dashboard_report[k]=v

        # extract information to be sent to DashBoard
        # per protocol and for action=read, calculate MBPS
        # dashboard key is io_action
        dashboard_report['MonitorID'] = self.MonitorID
        dashboard_report['MonitorJobID'] = self.MonitorJobID
        for protocol in storage_report.keys() :
            for action in storage_report[protocol].keys() :
                try: size = float(storage_report[protocol][action][2])
                except: size = 'NULL'
                try: time = float(storage_report[protocol][action][3])/1000
                except: time = 'NULL'
                dashboard_report['io_'+protocol+'_'+action] = str(size)+'_'+str(time)
        if self.debug :
            ordered = dashboard_report.keys()
            ordered.sort()
            for key in ordered:
                print key,'=',dashboard_report[key]
 
        # send to DashBoard
        apmonSend(self.MonitorID, self.MonitorJobID, dashboard_report)
        apmonFree()

        if self.debug == 1 : print dashboard_report

        return

    def usage(self):
        
        msg=""" 
        required parameters:
        --input            :       input FJR xml file
 
        optional parameters:
        --dashboard        :       send info to the dashboard. require following args: "MonitorID,MonitorJobID"
            MonitorID        :       DashBoard MonitorID
            MonitorJobID     :       DashBoard MonitorJobID
        --exitcode         :       print executable exit code 
        --lfn              :       report list of files really analyzed
        --help             :       help
        --debug            :       debug statements
        """
        return msg 


if __name__ == '__main__' :
    try: 
        parseFjr_ = parseFjr(sys.argv[1:]) 
        parseFjr_.run()  
    except:
        pass
