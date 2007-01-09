# -*- coding: iso-8859-1 -*-
import re
import logging
from Session import *

############# Keep these RE synchronized with CRAB! #############
#                                                               #
# crab. Total of 3 jobs created. -> created = 3
createdRE = re.compile("crab\. Total of (?P<created>[\\d]*) jobs created\.")
# crab. Total of 3 jobs submitted. -> 
submittedRE = re.compile("crab\. Total of (?P<submitted>[\\d]*) jobs submitted\.")
# crab. Results of job # 3 are in /afs/cern.ch/user/s/skaplun/scratch0/prova/crab_xxx/res
getoutputRE = re.compile("crab\. Results of Job # (?P<jobid>[\\d]*) are in .*")
# >>>>>>>>> 3 Total Jobs
totRE = re.compile(">* (?P<tot>[\\d]*) Total Jobs")
# >>>>>>>>> 3 Jobs cleared.
statusRE = re.compile(">* (?P<jobn>[\\d]*) Jobs (?P<status>.*)")
# List of jobs: 1,2,3
jobListRE = re.compile("List of jobs: (?P<jobList>[\\d]*(,[\\d]*)*)")
# working directory   /afs/cern.ch/user/s/skaplun/scratch0/prova/crab_xxx/
wdRE = re.compile("working directory[\\s]*(?P<wd>.*)")
#                                                               #
#################################################################

parserLogger = logging.getLogger("parser")
parserLogger.setLevel(logging.INFO)

def parseCreate(text):
    """ Parses crab -create returning the number of created jobs. """
    for line in text.split("\n"):
        g = createdRE.search(line)
        if g:
            return int(g.group("created"))
    raise TestException, "Possible troubles in parsing crab -create"

def parseSubmit(text):
    """ Parses crab -submit returning the number of submitted jobs. """
    for line in text.split("\n"):
        g = submittedRE.search(line)
        if g:
            return int(g.group("submitted"))
    raise TestException, "Possible troubles in parsing crab -submit"

def parseGetOutput(text):
    """ Parses crab -getoutput returning the set of jobIds retrieved. """
    jobIds = []
    for line in text.split("\n"):
        g = getoutputRE.search(line)
        if g:
            jobIds.append(int(g.group("jobid")))
    if not jobIds:
        raise TestException, "Possible troubles in parsing crab -getoutput"
    return set(jobIds)

def parseStatus1(text):
    """ Parses the first part of crab -status returning a list of jobs infos.

    >>> parseStatus2(open("status.log").read())
    [['1', 'done (success)', 'ce106.cern.ch'], ['2', 'cleared', 'ce.polgrid.pl', '0', '0'], ['3', 'done (success)', 'ce105.cern.ch']]
    """

    tabFound = False
    jobs = []
    for line in text.split("\n"):
        if not tabFound:
            if "--------" in line:
                tabFound = True
                continue
        elif line:
            jobs.append([s.strip().lower() for s in line.split("  ") if s.strip()])
        else:
            break;
    if not tabFound:
        raise TestException, "Possible troubles in parsing crab -status 1st part"
    return jobs
                
def parseStatus2(text):
    """ Parses the second part of crab -status returning a list of pairs: a jobId and a literal status.

    >>> parseStatus(open('status.log').read())
    [(1, 'done'), (2, 'cleared'), (3, 'done')]
    """
    tot = None
    totFound = False
    nextLineJobsList = False
    jobs = []
    currentStatus = None
    for line in text.split("\n"):
        if not totFound:
            # Searching total job number
            g=totRE.search(line)
            if g:
                tot = int(g.group("tot"))
                totFound = True
                # Preparing the jobList
                for i in range(1, tot+1):
                    jobs.append((i, None))
                continue
        if totFound:
            if not nextLineJobsList:
                # Searching a status
                g = statusRE.search(line)
                if g:
                    currentStatus = g.group("status")
                    nextLineJobsList = True
            else:
                # Searching the list of jobs in this status
                g = jobListRE.search(line)
                if g:
                    jobList = g.group("jobList").split(",")
                    for i in jobList:
                        jobs[int(i)-1] = (int(i), currentStatus.strip().lower())
                    nextLineJobsList = False
                    currentStatus = None
    if not totFound:
        raise TestException, "Possible troubles in parsing crab -status 2nd part"
    return jobs

def parseEntireStatus(text):
    """ Parses crab -status returning a list of jobs infos. """
    jobs1 = parseStatus1(text) # 1st part
    jobs2 = parseStatus2(text) # 2nd part
    parserLogger.debug("parseEntireStatus -> jobs1="+str(jobs1))
    parserLogger.debug("parseEntireStatus -> jobs2="+str(jobs2))
    
    ret = [[i+1, None, None, None, None] for i in range(len(jobs2))]

    for job in jobs1:
        exitcode = None
        exitstatus = None
        host = None
        logging.debug(str(job))
        # a job in the 1st part of -status may contain 2 to 5 different infos
        if len(job) == 5:
            jobid, status, host, exitcode, exitstatus = job
        elif len(job) == 4:
            jobid, status, host, exitstatus = job
        elif len(job) == 3:
            jobid, status, host = job
        elif len(job) == 2:
            jobid, status = job
        else:
            raise ValueError, "Job infos length not expected!"
        ret[int(jobid)-1] = [jobid, status, host, exitcode, exitstatus]

    # We merge the infos contained in the 2nd part of -status
    for jobId, status in jobs2:
        # Workaround when 1st part is "Done (Aborted)", 2nd is only "Done"!!!
        if not ret[int(jobId-1)][1] and status:
            ret[int(jobId)-1][1] = status
    parserLogger.debug("parseEntireStatus -> ret="+str(ret))

    return ret

def findCrabWD(text):
    """ Parses crab -create/-status seeking the working dir """
    for line in text.split("\n"):
        g = wdRE.search(line)
        if g:
            parserLogger.debug("findCrabWD -> "+str(g.group("wd")))
            return g.group("wd")
    raise TestException, "findCrabWD"

def parseCrabCfg(text, section, var, default):
    """ Returns the value of a variable belonging to a section in crab.cfg.

    Returns the value of a variable belonging to a section in crab.cfg. It returns default if the variable isn't found.
    
    >>> parseCrabCfg(open("crab.cfg").read(), "CRAB", "jobtype", "patata")
    'cmssw'
    >>> parseCrabCfg(open("crab.cfg").read(), "CRAB", "jobtypes", "patata")
    'patata'
    """
    sectionFound = False
    for line in text.split("\n"):
        line = line.strip()
        comment = line.find("#")
        if comment > -1:
            line = line[:comment]
        if line:
            if not sectionFound:
                if line == "["+section+"]":
                    sectionFound = True
                    continue
            else:
                if line[0] == "[" and line[-1] == "]":
                    sectionFound = False # Found a new section. Current correct section finished.
                else:
                    line = line.split("=") # Splitting variable name from its value.
                    if len(line) == 2 and line[0].strip() == var:
                        return line[1].strip()
    return default
                
def parseOutNames(text):
    """ Returns the list of output file names CRAB creates in the UI """
    useUI = bool(parseCrabCfg(text, "USER", "return_data", "1")) # We use the UI to returns output
    if useUI:
        names = parseCrabCfg(text, "CMSSW", "output_file", "")
        return [name.strip() for name in names.split(",")]
    else:
        return []
    