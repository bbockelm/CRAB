from WorkSpace import WorkSpace
from crab_exceptions import *
import common

import os, string

class dbEntry:
    def __init__(self):
        self.status = 'X'       # job status
        self.jid = ''           # scheduler job id
        self.firstEvent = 0     # first event for this job
        self.maxEvents = 0      # last event for this job
        self.collections = []   # EvCollection to be analyzed in this job
        self.inputSandbox = []  # InputSandbox
        self.outputSandbox = [] # OutputSandbox
        return

    def __str__(self):
        txt  = 'Status <' + self.status + '>; '
        txt += 'Job Id <' + self.jid + '>\n'
        if self.maxEvents!=0:
            txt += 'FirstEvent <' + str(self.firstEvent) + '>\n'
            txt += 'MaxEvents <' + str(self.maxEvents) + '>\n'
        if len(self.collections)>0:
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
            print string.strip(('Job %03d' % (job+1)) + ': ' + str(self._jobs[job]))
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
            db_file.write(self._jobs[i].jid+';')
            db_file.write(str(self._jobs[i].firstEvent)+';')
            db_file.write(str(self._jobs[i].maxEvents)+';')
            db_file.write(str(self._jobs[i].collections)+';')
            db_file.write(str(self._jobs[i].inputSandbox)+';')
            db_file.write(str(self._jobs[i].outputSandbox)+';')
            db_file.write('\n')
            pass
        db_file.close()
        return

    def load(self):
        self._jobs = []
        try:
            db_file = open(self._dir+self._db_fname, 'r')
        except IOError:
            raise CrabException("Something really serious! no JobDB is present!!!")

        for line in db_file:
            db_entry = dbEntry()
            (n, db_entry.status, db_entry.jid, db_entry.firstEvent, db_entry.maxEvents, collectionsTMP,  inputSandboxTMP , outputSandboxTMP , rest) = string.split(line, ';')
            db_entry.collections = self.strToList_(collectionsTMP)
            db_entry.inputSandbox = self.strToList_(inputSandboxTMP)
            db_entry.outputSandbox = self.strToList_(outputSandboxTMP)
            self._jobs.append(db_entry)
            pass
        db_file.close()
        return
    
    def strToList_(self, list):
        return string.split(string.replace(list[1:-1],"'",""),',')  
    
    def setStatus(self, nj, status):
        self._jobs[nj].status = status
        return
    
    def status(self, nj):
        return self._jobs[nj].status
    
    def setJobId(self, nj, jid):
        self._jobs[nj].jid = jid
        return
    
    def jobId(self, nj):
        return self._jobs[nj].jid

    def setFirstEvent(self, nj, firstEvent):
        self._jobs[nj].firstEvent = firstEvent
        return
    
    def firstEvent(self, nj):
        return self._jobs[nj].firstEvent

    def setMaxEvents(self, nj, MaxEvents):
        self._jobs[nj].maxEvents = MaxEvents
        return
    
    def maxEvents(self, nj):
        return self._jobs[nj].maxEvents

    def setCollections(self, nj, Collections):
        self._jobs[nj].Collections = Collections
        return
    
    def collections(self, nj):
        return self._jobs[nj].collections

    def setInputSandbox(self, nj, InputSandbox):
        self._jobs[nj].inputSandbox = InputSandbox
        return
    
    def inputSandbox(self, nj):
        return self._jobs[nj].inputSandbox

    def setOutputSandbox(self, nj, OutputSandbox):
        self._jobs[nj].outputSandbox = OutputSandbox
        return
    
    def outputSandbox(self, nj):
        return self._jobs[nj].outputSandbox
