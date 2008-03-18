#!/usr/bin/env python
"""
_JobReportErrorCode.py

Adds to the FJR the WrapperExitCode and the ExeExitCode


"""
import os, string
import sys
import popen2

from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport


if __name__ == '__main__':
    try:
        reportFileName = sys.argv[1]
        wrapperExitCode = sys.argv[2]
    except:
        print "it is necessary to specify the fjr name"
        sys.exit(2)
    try:
        exeExitCode = sys.argv[3] 
    except:
        exeExitCode=''
    pass
                                    
    if not os.path.exists(reportFileName):
        fwjr = FwkJobReport()
        fwjr.addError(wrapperExitCode, "WrapperExitCode")
        if (exeExitCode != ""):
            fwjr.addError(exeExitCode, "ExeExitCode")
        fwjr.write(reportFileName)
    else:
        jobReport = readJobReport(reportFileName)[0]
        if (len(jobReport.errors) > 0):
            for err in jobReport.errors:
                if err['Type'] == "WrapperExitCode" :
                    err['ExitStatus'] = wrapperExitCode 
                    jobReport.write(reportFileName)
                if (exeExitCode != ""):
                    if err['Type'] == "ExeExitCode" :
                        err['ExitStatus'] = exeExitCode 
                        jobReport.write(reportFileName)
        else:
            jobReport.addError(wrapperExitCode, "WrapperExitCode")
            if (exeExitCode != ""):
                jobReport.addError(exeExitCode, "ExeExitCode")
            jobReport.write(reportFileName) 
