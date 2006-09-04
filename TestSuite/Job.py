class Job:

    id = 0
    status = -2                          ##100 => job still needs to be created
                                        ##101 => job was created but needs to be submitted
                                        ##102 => job created and submitted - needs to be checked the status
                                        ##103 => job to be killed
    statusPre = -2
    nResubmitted = 0
    nCycles = 1#28
    nResLocal = []
    # number kill/resubmit 4: "Scheduled", "Waiting", "Ready", "Running", "Idle", "Aborted"
    tot = 6                                ## number of state 4 nResLocal 4 each job
    
    created = 0                                ## 5 fields 4 the final table
    submitted = 0
    done = 0
    completed = 0
    toSubmit = 0
    step = 0

    def __init__(self, id, status):
        self.id = id
        self.status = status
        h = 0
        while h < 6:
            self.nResLocal.insert( len( self.nResLocal ), 0 )
            h = h + 1

    def changeSt(self, status):
        self.statusPre = self.status
        self.status = status
        if status == "Done" or status == "Cleared(BOSS)":
            completed = 1

    def getId(self):
        return self.id

    def getStatus(self):
        return self.status

    def getStatusPre(self):
        return self.statusPre

    def incrCycles(self):
        self.nCycles = self.nCycles + 1

    def resetCycles(self, value):
        self.nCycles = value

    def incrResubmit(self):
        self.nResubmitted = self.nResubmitted + 1

    def incrResLocal(self, index):
        ii = index + self.tot * (self.id -1)
        self.nResLocal[ii] = self.nResLocal[ii] + 1

    def readResLocal(self, index):
        return self.nResLocal[ index + self.tot * (self.id - 1) ]

    def readCycles(self):
        return self.nCycles

    def readResubmit(self):
        return self.nResubmitted
