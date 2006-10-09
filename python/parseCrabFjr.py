#!/usr/bin/env python

import sys, getopt, string

from FwkJobRep.ReportParser import readJobReport

def main(argv) :
    """
    parseCrabFjr

    parse CRAB FrameworkJobReport on WN and return parameters to WN wrapper script

    prints information separated by semi-colon in fixed order:

    1. ExitStatus (0 or ExitStatus from CMSSW)

    required parameters:
    --input            :       input FJR xml file

    optional parameters:
    --help             :       help
    --debug            :       debug statements
    
    """

    # defaults
    input = ''
    debug = 0

    try:
        opts, args = getopt.getopt(argv, "", ["input=", "debug", "help"])
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
        elif opt == "--debug" :
            debug = 1
            
    if input == '':
        print main.__doc__
        sys.exit()

    # load FwkJobRep
    jobReport = readJobReport(input)[0]

    report = []
    
    # get ExitStatus of last error
    if len(jobReport.errors) != 0 :
        report.append(str(jobReport.errors[-1]['ExitStatus']))
    else :
        report.append(str(0))

    # get i/o statistics
    storageStatistics = str(jobReport.storageStatistics)

    print ';'.join(report)
    


if __name__ == '__main__' :
    main(sys.argv[1:])

    
