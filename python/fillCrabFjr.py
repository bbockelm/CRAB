#!/usr/bin/env python
"""
_fillCrabFjr.py

Adds to the FJR the WrapperExitCode and the ExeExitCode


"""
import os, string
import sys
import popen2

from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport

class fjrParser:
    def __init__(self, argv):
        try:
            self.reportFileName = argv[1]
            self.wrapperExitCode = argv[2]
        except:
            print "it is necessary to specify the fjr name"
            sys.exit(2)
        try:
            self.exeExitCode = argv[3] 
        except:
            self.exeExitCode=''
        pass

        return

    def run(self): 

        if not os.path.exists(self.reportFileName):
            self.writeFJR()
        else:
            self.fillFJR()
        self.setStatus() 

        return

    def setStatus(self):
        """
        """    
        if (self.wrapperExitCode == '0') and (self.exeExitCode == '0'):
           status = 'Success'
        else:
           status = 'Failed'

        jobReport = readJobReport(self.reportFileName)[0]
        jobReport.status = status
        jobReport.write(self.reportFileName)

        return 

    def writeFJR(self):
        """
        """                                         
        fwjr = FwkJobReport()
        fwjr.addError(self.wrapperExitCode, "WrapperExitCode")
        if (self.exeExitCode != ""):
            fwjr.addError(self.exeExitCode, "ExeExitCode")
        fwjr.write(self.reportFileName)

        return

    def checkValidFJR(self): 
        """
        """ 
        valid = 0
        fjr=open(self.reportFileName,'r')
        lines = fjr.readlines()
        if len(lines) > 0: valid = 1

        return valid

    def fillFJR(self): 
        """
        """ 
        valid = self.checkValidFJR()
        if valid == 1:
            jobReport = readJobReport(self.reportFileName)[0]
            if (len(jobReport.errors) > 0):
                error = 0
                for err in jobReport.errors:
                    if err['Type'] == "WrapperExitCode" :
                        err['ExitStatus'] = self.wrapperExitCode 
                        jobReport.write(self.reportFileName)
                        error = 1
                    if (self.exeExitCode != ""):
                        if err['Type'] == "ExeExitCode" :
                            err['ExitStatus'] = self.exeExitCode 
                            jobReport.write(self.reportFileName)
                            error = 1
                if (error == 0):
                    jobReport.addError(self.wrapperExitCode, "WrapperExitCode")
                    if (self.exeExitCode != ""):
                        jobReport.addError(self.exeExitCode, "ExeExitCode")
                    jobReport.write(self.reportFileName) 
            else:
                jobReport.addError(self.wrapperExitCode, "WrapperExitCode")
                if (self.exeExitCode != ""):
                    jobReport.addError(self.exeExitCode, "ExeExitCode")
                jobReport.write(self.reportFileName)
        else: 
            self.writeFJR()

if __name__ == '__main__':
    try: 
        FjrParser_ = fjrParser(sys.argv) 
        FjrParser_.run()  
    except:
        pass

