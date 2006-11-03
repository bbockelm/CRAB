from Job import *

class JobsManager:
    """
       class manager and interfacer with the job's class
    """

    def __init__(self):  ## Matt.
        self.jobsN = 0
        self.jobList = []
        #pass

    def createList(self, nCreate, nSubmit):
        """
           creates and inits the jobs' list
        """
        i = 1
        while i <= int(nCreate):
            j = Job(i,100)              ##100 => job still needs to be created
                                        ##101 => job was created but needs to be submitted
                                        ##102 => job created and submitted - needs to be checked the status
                                        ##103 => job to be killed
            self.jobList.insert( len(self.jobList) , j )

            if i <= int(nSubmit):          ##M.: for the table coloumn "Test Status"
                self.jobList[ len(self.jobList)-1 ].toSubmit = 1

            i = i + 1

        self.jobsN = len(self.jobList)

    def getToSubmit(self, index):
        return self.jobList[index].toSubmit

    def getStep(self, index):
        return self.jobList[index].step

    def changeStep(self, index):
        if self.jobList[index].step == 0:
            self.jobList[index].step = 1

    def getStatus(self, index):
        return self.jobList[index].getStatus()

    def getStatusPre(self, index):
        return self.jobList[index].getStatusPre()

    def cngStatus(self, jobId, status):
        self.jobList[jobId-1].changeSt(status)
        if status == 101 and self.jobList[jobId - 1].toSubmit == 0:  ##M.: check 4 the table coloumn "Test Status"
            self.jobList[jobId-1].completed = 1

    def created(self, idJob, flag):
        self.jobList[ idJob-1 ].created = flag

    def submitted(self, idJob, flag):
        self.jobList[ idJob-1 ].submitted = flag

    def done(self, idJob, flag):
        if flag != -1:
            ##print "change done from", self.jobList[ idJob-1 ].done, "to", flag
            #print " --> flag = ",flag
            self.jobList[ idJob-1 ].done = flag
        if flag == 0 or flag == 1 or flag == -1:
            self.jobList[ idJob-1 ].completed = flag ##M.: 4 the table coloumn "Test Status"

    def getCSD(self, indexJob):
        return [ self.jobList[indexJob].created, self.jobList[indexJob].submitted, self.jobList[indexJob].done ]

    def getCompleted(self, indexJob):
        ##print "returning",self.jobList[ indexJob ].completed
        return self.jobList[ indexJob ].completed

    def setCompleted(self):
        i = 0
        while i < self.jobsN:
            if self.jobList[ i ].done != 1 and self.jobList[ i ].done != 2:
                ##print "job", i, "Incompleted"
                self.jobList[ i ].completed = -1
            else:
                self.jobList[ i ].completed = 1
                ##print "job", i, "Completed"
            i = i + 1

    def setIncompleted(self):
        i = 0
        ##print "Called setIncompleted"
        while i < self.jobsN:
            if self.jobList[i].toSubmit != 0:
                if self.getStatus(i) != "Done" and self.getStatus(i) != "Created": # Sk nomore (BOSS)":
                    ##print "incompleted job",i+1
                    self.done( i + 1, -1 )                
            i = i + 1

    def printStatusAll(self):
        """
           print infos about all the jos in the main list
        """
        i = 0
        while i < len(self.jobList):
            print "id - stato - cicli - resubmit"
            print self.jobList[i].getId(), "  " , self.jobList[i].getStatus(), "  " , self.getCycles(i), "  " , self.getResubmit(i)
            print ""
            i = i + 1

    def getJobId(self, index):
        return self.jobList[index].getId()

    def nJobs(self):
        return self.jobsN

    def getFailed(self):
        """
           get failed jobs
        """
        i = 0
        str = ""
        while i < len(self.jobList):
            if self.jobList[i].getStatus() == 103:
                temp = str( self.jobList[i].getId() )
                if len(str) > 0:
                   str = str + "," + temp
                else:
                   str = temp
            i = i + 1
        #print str
        return str
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

    def allDone(self):
        """
           checks if all the jobs are at the status "Done" ore "Cleared(BOSS)"
        """
        i = 0
        stts = 1
        while i < self.nJobs():
            if self.jobList[i].getStatus() != "Done" and self.jobList[i].getStatus() != "Cleared": # Sk nomore (BOSS)":
                if self.jobList[i].getStatus() != "Killed" and self.jobList[i].getStatus() != "Killed": # Sk nomore (BOSS)":
                    if self.jobList[i].toSubmit != 0:
                        stts = 0
                else:
                    stts = -1
            i = i + 1

        return stts

    def allFinished(self):
        """
           checks if all the jobs are at the end
        """
        i = 0
        stts = 1
        while i < self.nJobs():
            #print i+1, self.jobList[i].completed
            if self.jobList[i].completed == 0:
                ##print "job ", i+1, self.jobList[i].completed
                stts = 0
            i = i + 1

        return stts

    def allNotDone(self):
        """
           returns all the jobs not "Done" or "Cleared(BOSS)"
        """
        i = 0
        stringa = ""
        while i < len(self.jobList):
            if self.jobList[i].getStatus() != "Done" or self.jobList[i].getStatus() != "Cleared": #  Sk. nomore (BOSS)":
                temp = str( self.jobList[i].getId() )
                if len(stringa) > 0:
                   stringa = stringa + "," + temp
                else:
                   stringa = temp
            i = i + 1
        return stringa

    def incrResubmit(self, jobIndex):
        return self.jobList[jobIndex].incrResubmit()

    def incrCycles(self, jobIndex):
        return self.jobList[jobIndex].incrCycles()

    def resetCycles(self, jobIndex, value):
        self.jobList[jobIndex].resetCycles( value )
    
    def incrResLocal(self, jobIndex, index):
        self.jobList[jobIndex].incrResLocal(index)

    def getResLocal(self, jobIndex, index):
        return self.jobList[jobIndex].readResLocal(index)

    def getCycles(self, jobIndex):
        return self.jobList[jobIndex].readCycles()

    def getResubmit(self, jobIndex):
        return self.jobList[jobIndex].readResubmit()

