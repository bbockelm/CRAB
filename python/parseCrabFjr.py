#!/usr/bin/env python

import sys, getopt, string

from FwkJobRep.ReportParser import readJobReport
from DashboardAPI import apmonSend, apmonFree


def main(argv) :
    """
    parseCrabFjr

    - parse CRAB FrameworkJobReport on WN: { 'protocol' : { 'action' : [attempted,succedeed,total-size,total-time,min-time,max-time] , ... } , ... }
    - report parameters to DashBoard using DashBoardApi.py: for all 'read' actions of all protocols, report MBPS
    - return ExitStatus and dump of DashBoard report separated by semi-colon to WN wrapper script

    required parameters:
    --input            :       input FJR xml file
    --MonitorID        :       DashBoard MonitorID
    --MonitorJobID     :       DashBoard MonitorJobID

    optional parameters:
    --help             :       help
    --debug            :       debug statements
    
    """

    # defaults
    input = ''
    MonitorID = ''
    MonitorJobID = ''
    debug = 0

    try:
        opts, args = getopt.getopt(argv, "", ["input=", "MonitorID=", "MonitorJobID=", "debug", "help"])
    except getopt.GetoptError:
        print main.__doc__
        sys.exit(2)

    # check command line parameter
    for opt, arg in opts :
        if opt  == "--help" :
            print main.__doc__
            sys.exit()
        elif opt == "--input" :
            input = arg
        elif opt == "--MonitorID" :
            MonitorID = arg
        elif opt == "--MonitorJobID" :
            MonitorJobID = arg
        elif opt == "--debug" :
            debug = 1
            
    if input == '' or MonitorID == '' or MonitorJobID == '':
        print main.__doc__
        sys.exit()

    # load FwkJobRep
    jobReport = readJobReport(input)[0]

    exit_satus = ''
    
    # get ExitStatus of last error
    if len(jobReport.errors) != 0 :
        exit_status = str(jobReport.errors[-1]['ExitStatus'])
    else :
        exit_status = str(0)

    # get i/o statistics
    storageStatistics = str(jobReport.storageStatistics)

    # dashboard report dictionary
    dashboard_report = {}

    # check if storageStatistics is valid
    if storageStatistics.find('Storage statistics:') != -1 :
        # report form: { 'protocol' : { 'action' : [attempted,succedeed,total-size,total-time,min-time,max-time] , ... } , ... }
        report = {}
        data = storageStatistics.split('Storage statistics:')[1]
        data_fields = data.split(';')
        for data_field in data_fields:
            # parse: format protocol/action = attepted/succedeed/total-size/total-time/min-time/max-time
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
            if protocol in report.keys() :
                if action in report[protocol].keys() :
                    print 'protocol/action:',protocol,'/',action,'listed twice in report, taking the first'
                else :
                    report[protocol][action] = [attempted,succeeded,total_size,total_time,min_time,max_time]
            else :
                report[protocol] = {action : [attempted,succeeded,total_size,total_time,min_time,max_time] }

        if debug :
            for protocol in report.keys() :
                print 'protocol:',protocol
                for action in report[protocol].keys() :
                    print 'action:',action,'measurement:',report[protocol][action]

        # extract information to be sent to DashBoard
        # per protocol and for action=read, calculate MBPS
        # dashboard key is io_action
        dashboard_report['MonitorID'] = MonitorID
        dashboard_report['MonitorJobID'] = MonitorJobID
        for protocol in report.keys() :
            for action in report[protocol].keys() :
                try: size = float(report[protocol][action][2])
                except: size = 'NULL'
                try: time = float(report[protocol][action][3])*1000
                except: time = 'NULL'
                dashboard_report['io_'+protocol+'_'+action] = str(size)+'_'+str(time)

        if debug :
            ordered = dashboard_report.keys()
            ordered.sort()
            for key in ordered:
                print key,'=',dashboard_report[key]

        # send to DashBoard
        apmonSend(MonitorID, MonitorJobID, dashboard_report)
        apmonFree()

    # prepare exit string
    exit_string = str(exit_status)
    for key in dashboard_report.keys() :
        exit_string += ';' + str(key) + '=' + str(dashboard_report[key])

    return exit_string


if __name__ == '__main__' :
    exit_status = main(sys.argv[1:])
    # output for wrapper script
    print exit_status

    
