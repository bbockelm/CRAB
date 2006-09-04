import time, os, string
from os.path import abspath

class SessionManager:

    currTime = 'default'
    stratCyclesLow = 1
    #stratCyclesHigh = 21

    def createDir (self, dirName):

        os.mkdir(dirName)

        return

    def pathName(self, roboLogDir):

        if self.currTime == 'default':
            self.currTime = time.strftime( '%y%m%d_%H%M%S', time.localtime() )
        #currDir = string.join( os.getcwd() + '/' + 'crab_datalog_' + self.currTime, "" )
        currDir = string.join( roboLogDir + '/' + 'crab_0_' + self.currTime, "" )
        self.createDir( currDir )

        return currDir

    def pathRoboName(self, cwd, cfgName):

        if self.currTime == 'default':
            self.currTime = time.strftime( '%y%m%d_%H%M%S', time.localtime() )
        currDir = abspath(cfgName + "_" +self.currTime)
        self.createDir( currDir )

        return currDir

    def incrKill( self, statusId, jobs, jobIndex ):
        """
        - increment the field nResubmit
        - increment the "killed" array
        """
        jobs.incrResubmit( jobIndex )
        stt = self.indexStatus(statusId)
        if stt != -1:
            jobs.incrResLocal( jobIndex, stt )
        return

    def resetRules( self, fl, jobs, jobIndex ):
        """
        resetta il numero di cicli e lo stato
        """
        if fl:
            #print "reset to ", self.stratCyclesLow
            jobs.resetCycles( jobIndex, self.stratCyclesLow )
        elif fl == 0:
            #print "reset to ", self.stratCyclesHigh
            jobs.resetCycles( jobIndex, self.stratCyclesHigh )

        return

    def indexStatus( self, statusId ):
        """
        returns the index of the "killed" array corresponding to the status
        """
        if statusId == 1:
            return 0
        elif statusId == 2:
            return 1
        elif statusId == 3:
            return 2
        elif statusId == 6:
            return 3
        elif statusId == 7:
            return 4
        elif statusId == 4 or statusId == 5:
            return 5
        return -1

    def rules( self, statusId, jobs, jobIndex, scan ):
        """
         if not running =>
           wait 30 min =>
             se é running => 
               aspetta altri 10 min =>
                 if Done =>
                   getoutput
                 else =>
                   kill
             else (non é running) => 
               kill
               check status = failed (+ info)
         else
           aspetta altri 10 min =>
             if Done =>
               getoutput
             else =>
               kill
        """
        # Code cases:
        #  -1 -> Unknown
        #   0 -> Done
        #   1 -> Scheduled
        #   2 -> Waiting
        #   3 -> Ready
        #   4 -> Aborted
        #   5 -> Killed
        #   6 -> Running
        #   7 -> Idle
        statoNow = jobs.getStatus(jobIndex)
        statoPre = jobs.getStatusPre(jobIndex)
        statusId =  scan.strCodeStatus( statoNow )

        if statoPre != 102 and statoPre != 101 and statoNow != "Created(BOSS)" and statoNow != "Cleared(BOSS)" and statoNow != "Killed" and statoNow != "Killed(BOSS)" and statoNow != "Aborted":
            
            #print jobs.getStep(jobIndex), statoNow
            if jobs.getStep(jobIndex) != 1: 
                if statoNow == "Running":
                    ##print "status running and increasing to 30"
                    jobs.changeStep(jobIndex)
                    while jobs.getCycles(jobIndex) < 30:
                        #print jobIndex, "-", jobs.getCycles(jobIndex), "=> incrementing"
                        jobs.incrCycles(jobIndex)
                elif jobs.getStep(jobIndex) == 0:
                    if jobs.getCycles(jobIndex) > 29:
                        #print jobIndex, "-", jobs.getCycles(jobIndex)
                        #if scan.strCodeStatus( statoNow ) != "Running":
##                        print "yet not running: killing not running jobs"
                        return 1
                    else:
                        #print jobIndex, "-", jobs.getCycles(jobIndex), "=> incrementing"
                        jobs.incrCycles(jobIndex)
            elif jobs.getStep(jobIndex) == 1:
                if jobs.getCycles(jobIndex) > 39:
                    #print jobIndex, "-", jobs.getCycles(jobIndex)
                    print "too long execution: killing running jobs"
                    return 1
                else:
                    #print jobIndex, "-", jobs.getCycles(jobIndex), "=> incrementing"
                    jobs.incrCycles(jobIndex)
                
        return 0

