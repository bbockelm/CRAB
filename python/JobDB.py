from WorkSpace import WorkSpace
from crab_exceptions import *
import common

import os, string

class dbEntry:
    def __init__(self):
        self.status = 'X'       # job status
        self.exitStatus = ''    # job exit status
        self.jid = ''           # scheduler job id
        self.bossid = ''        # BOSS job id
        self.collections = []   # EvCollection to be analyzed in this job
        self.inputSandbox = []  # InputSandbox
        self.outputSandbox = [] # OutputSandbox
        self.taskId = ''        # Task job belongs to
        self.arguments = []    ### Fabio: abstract job_type parameters
        return

    def __str__(self):
        txt  = 'Status <' + self.status + '>; '
        if self.exitStatus!='':
            txt += 'exitStatus <' + str(self.exitStatus) + '>\n'
        txt += 'Job Id <' + self.jid + '>\n'
        if self.arguments:
            txt += 'Job Type Arguments <' + str(self.arguments) + '>\n'
        if self.collections:
            txt += 'Collections <' + str(self.collections) + '>\n'

        return txt

class JobDB:
    def __init__(self):
        self._dir = common.work_space.shareDir() + 'db/'
        self._db_fname = 'jobs'
        self._jobs = []        # list of dbEntry's
        return

    def __str__(self):
        njobs = self.nJobs()
        if njobs == 1: plural = ''
        else:          plural = 's'
        txt = 'Total of %d job%s:\n' % (njobs, plural)
        for i in range(njobs):
            txt += ('Job %03d' % (i+1)) + ': '
            txt += str(self._jobs[i])
            pass
        return txt

    def dump(self, jobs):
        njobs = len(jobs)
        if njobs == 1: plural = ''
        else:          plural = 's'
        print 'Listing %d job%s:\n' % (njobs, plural)
        for job in jobs:
            print ('Job %03d' % (job)) + ': ' + str(self._jobs[job-1])
            pass

    def nJobs(self):
        return len(self._jobs)

    def create(self, njobs):

        if os.path.exists(self._dir):
            msg = 'Cannot create Job DB: already exists.'
            raise CrabException(msg)

        os.mkdir(self._dir)

        for i in range(njobs):
            self._jobs.append(dbEntry())
            pass

        common.logger.debug(5,'Created DB for '+str(njobs)+' jobs')

        self.save()
        return

    def save(self):
        db_file = open(self._dir+self._db_fname, 'w')
        for i in range(len(self._jobs)):
            db_file.write(`(i+1)`+';')
            db_file.write(self._jobs[i].status+';')
            db_file.write(self._jobs[i].exitStatus+';')
            db_file.write(self._jobs[i].jid+';')
            db_file.write(self._jobs[i].bossid+';')
            db_file.write(string.join(self._jobs[i].collections)+';')
            db_file.write(string.join(self._jobs[i].inputSandbox)+';')
            db_file.write(string.join(self._jobs[i].outputSandbox)+';')
            db_file.write(str(self._jobs[i].taskId)+';')
            db_file.write(string.join(self._jobs[i].arguments)+';')
            db_file.write('\n')
            pass
        db_file.close()
        return

    def load(self):
        self._jobs = []
        try:
            db_file = open(self._dir+self._db_fname, 'r')
        except IOError:
            raise DBException("Something really serious! no JobDB is present!!!")

        for line in db_file:
            db_entry = dbEntry()
            (n, db_entry.status, db_entry.exitStatus, db_entry.jid, db_entry.bossid, collectionsTMP,  inputSandboxTMP , outputSandboxTMP , db_entry.taskId, argumentsTMP, rest) = string.split(line, ';')
            db_entry.collections = string.split(collectionsTMP)
            db_entry.inputSandbox = string.split(inputSandboxTMP)
            db_entry.outputSandbox = string.split(outputSandboxTMP)
            db_entry.arguments = string.split(argumentsTMP)
            self._jobs.append(db_entry)
            pass
        db_file.close()
        return
    
    
    def setStatus(self, nj, status):
        self._jobs[int(nj)].status = status
        return
    
    def status(self, nj):
        return self._jobs[int(nj)].status
    
    def setExitStatus(self, nj, exitStatus):
        self._jobs[int(nj)].exitStatus = exitStatus
        return
    
    def exitStatus(self, nj):
        return self._jobs[int(nj)].exitStatus
    
    def setJobId(self, nj, jid):
        self._jobs[int(nj)].jid = jid
        return
    
    def jobId(self, nj):
        return self._jobs[int(nj)].jid

    def setBossId(self, nj, bossid):
        self._jobs[int(nj)].bossid = bossid
        return
    
    def bossId(self, nj):
        return self._jobs[int(nj)].bossid

    def setArguments(self, nj, args):
        self._jobs[int(nj)].arguments = args
        return
    
    def arguments(self, nj):
        return self._jobs[int(nj)].arguments

    def setCollections(self, nj, Collections):
        self._jobs[int(nj)].Collections = Collections
        return
    
    def collections(self, nj):
        return self._jobs[int(nj)].collections

    def setInputSandbox(self, nj, InputSandbox):
        self._jobs[int(nj)].inputSandbox = InputSandbox
        return
    
    def inputSandbox(self, nj):
        return self._jobs[int(nj)].inputSandbox

    def setOutputSandbox(self, nj, OutputSandbox):
        self._jobs[int(nj)].outputSandbox = OutputSandbox
        return
    
    def outputSandbox(self, nj):
        return self._jobs[int(nj)].outputSandbox
        
    def setTaskId(self, nj, taskId):
        self._jobs[int(nj)].taskId = taskId
