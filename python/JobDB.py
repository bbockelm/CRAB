from WorkSpace import WorkSpace
from crab_exceptions import *
import common

import os

class dbEntry:
    def __init__(self):
        self.status = 'X'     # job status
        self.jid = ''         # scheduler job id
        return

    def __repr__(self):
        txt  = 'Status ' + self.status + '\n'
        txt += 'Job Id ' + self.jid + '\n'
        return txt

class JobDB:
    def __init__(self):
        self._dir = common.work_space.shareDir() + 'db/'
        self._db_fname = 'jobs'
        self._jobs = []        # list of dbEntry's
        return

    def __repr__(self):
        njobs = self.nJobs()
        if njobs == 1: plural = ''
        else:          plural = 's'
        txt = 'Total of %d job%s:\n' % (njobs, plural)
        for i in range(njobs):
            txt += ('Job %03d' % i) + ':\n'
            txt += str(self._jobs[i])
            pass
        return txt

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

        self.save()
        return

    def save(self):
        db_file = open(self._dir+self._db_fname, 'w')
        for i in range(len(self._jobs)):
            db_file.write(`(i+1)`+';')
            db_file.write(self._jobs[i].status+';')
            db_file.write(self._jobs[i].jid+';')
            db_file.write('\n')
            pass
        db_file.close()
        return

    def load(self):
        self._jobs = []
        db_file = open(self._dir+self._db_fname, 'r')
        db_entry = dbEntry()
        for line in db_file:
            (n, db_entry.status, db_entry_jid, rest) = string.split(line, ';')
            self._jobs.append(db_entry)
            pass
        db_file.close()
        return
    
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
    
