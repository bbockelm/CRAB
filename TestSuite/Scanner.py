import string, os, commands
from LockerFile import *
from JobsManager import *

class Scanner:

    nameCfg = "crab.cfg"
    locks = LockerFile()
    nameOutput = ""
    created = None
    jobType = "CMSSW"

    def __init__(self, cfgName):
        self.nameCfg = cfgName

    def checkDim(self, path):

	p = os.path.getsize(path)
	if p > 0:
	    return p
	return 0

    def findInside(self, text, str):
        return text.find(str) != -1

    def add2JobList(self, nJob, status):
        self.created 

    def findNJob(self, text, nJobs, opt, jobs):
        statusJob = 101
        count = 0
        if opt == 1:
            s1 = "Submitting"
            s2 = "submitted"
            statusJob = 102
            ## if not submitted, crab will return: "crab. Job # 4 not submitted: status None"
            ##                                     "crab. Total of 3 jobs submitted (from 4 requested)."
        elif opt == 0:
            s1 = "Creating"
            s2 = "created"
        n = text.count("\n")
        str = text.split("\n", n)
        i = 0
        jobC = 0
        while i < len(str):
            if jobC < nJobs:
                xJob = 1
                while xJob < nJobs+1:
                    if str[i] == "crab. " + s1 + " job # %(#)d"%{'#' : xJob} :
                        jobs.cngStatus( xJob, statusJob )
                        jobC += 1
                        if opt == 1:
                            jobs.submitted( xJob, 1 )
                        elif opt == 0:
                            jobs.created( xJob, 1 )
                    elif opt == 1 and str[i] == "crab. Job # %(#)d"%{'#' : xJob} + " not submitted: status None":
                        #print str[i], "crab. Job # %(#)d"%{'#' : xJob} + " not submitted: status None"
                        count = count + 1
                    xJob += 1
            if jobC > 0:
                if str[i] == "crab. Total of %(#)d jobs "%{'#' : jobC} + s2 + ".":
                    return -1
                #elif opt == 1:
                  #  tt = jobC - count
                    ## "crab. Total of 3 jobs submitted"
                  #  print tt, jobC, count
                  #  print "crab. Total of %(#)d"%{'#' : tt} + " jobs submitted"
                  #  if str[i] == "crab. Total of %(#)d"%{'#' : tt} + " jobs submitted":
			#(from %(#d)"%{'#' : jobC } + " requested).":
                  #      return count
            i += 1
        if opt == 1:
            tt = nJobs - count
            ## "crab. Total of 3 jobs submitted"
            #print tt, nJobs, count
            #  print "crab. Total of %(#)d"%{'#' : tt} + " jobs submitted"
            if str[i-3] == "crab. Total of %(#)d"%{'#' : tt} + " jobs submitted (from %(#)d"%{'#' : nJobs } + " requested).":
                return count

        return -2

    def scanCreate(self, text, n, jobs):
        if self.findNJob( text, int(n), 0, jobs ):
	   #self.findInside (text, "crab. Total of %(#)d jobs created"%{'#' : int(n)} ):
            return 0
        else:
           n = text.count("\n")
        return 1

    def scanKill(self, text, n):
        #if self.findInside (text, "crab. Killing job # %(#)d"%{'#' : n} ):
        if self.findInside (text, "crab. Killing job # " + n):
            return 0
        return 1

    def scanSubmit(self, text, n, jobs):
        ret = self.findNJob( text, int(n), 1, jobs )
        #print ret
        if ret == -1:
            return 0
        elif ret >= 0: 
            return ret
        else:
            print "\n\n"
        return -1

    def scanGetOutput(self, text, n):
	#if self.findInside(text, "crab. Results of Job # %(#)d are in"%{'#' : n} ):
        if self.findInside(text, "crab. Results of Job # " + n  + " are in"):
	    return 0
	return 1

    def exeExitStatus(self, text):

        str1 = text.split(" ", 1)
        flag = 0
        while flag == 0:
        # line to be parsed
        #' '+jobStatus+'   '+dest+'      '+exe_code+'       '+job_exit_status
            if str1[0] == "(Success)":
                str1 = str1[1].split("   ", 5)
                flag = 1
            else:
                str1 = str1[1].split(" ", 1)

        return str1[2]

    def splitExtension(self, nameExt):

        nDot = nameExt.count(".")
        name = nameExt.split(".", nDot)
        stringa = ""
        for i in name:
            if i != "cfg":
                if len(stringa) > 0:
                    stringa = stringa + "." + i
                else:
                    stringa = i
##        print stringa     
        return stringa

    def getExtension(self, name):

        l = len(name)
        i = 0
        nP = 0
        n = name
        while (i < l):
            c = n[i]
            if c == ".":
                nP = nP +1
                e = n.split(c, nP)
                n = e[nP]
                i = 0
                l = len(n)
                nP = 0
            i = i + 1

        return e

    def getPreName(self, name, ext):

        ln = len (name)
        le = len (ext)
        st = name[0:(ln-le)]

        return st

    def getNameFile(self, path):
        """
        
        """

        jb = self.loadField(path, "jobtype")
        jobtype = ""
        for charact in jb:
            if ord(charact) != 32 and ord(charact) != 10:   
                jobtype = jobtype + charact.upper()

        self.jobType = jobtype
        
        fOut = open( path + '/' + self.nameCfg )
        self.locks.lock_F( fOut, 1 )
        extens = 'aida'       # default

        flag = 0

        while True:
	    line = fOut.readline()
	    if len(line) == 0: # lunghezza zero indica l'EOF 
	                       # (NdT. End Of File, fine del file)
		break
            else:
                str1 = line.split("\n")
                str2 = line.split(" ", 1)
                try:
                    if str1[0] == "[" + jobtype + "]" and flag == 0:
                        flag = 1
                except AttributeError:
                    print "\n"
                if str2[0] == "output_file" and flag:
                    str2 = line.split(" ", 3)
                    extens = self.getExtension(str2[2])
                    self.nameOutput = str2[2]
                    break

        self.locks.unlock_F( fOut )
        fOut.close()

        return extens[1]

    def checkStd(self, path, strIdJob):
        i = 0
        nZeri = ""
        while i < ( 6 - len(strIdJob) ):
            nZeri = nZeri + "0"
            i = i + 1
        exists = 0
        #print 'ls -go ' + path + ' | grep -cE ' +self.jobType+ '_' +nZeri+strIdJob+ '\.stdout'
        if int( commands.getoutput('ls -go ' + path + ' | grep -cE ' +self.jobType+ '_' +nZeri+strIdJob+ '\.stdout') ) > 0:
            if int( commands.getoutput('ls -go '+path+' | grep -cE ' +self.jobType+ '_' +nZeri+strIdJob+ '\.stderr') ) > 0:
                exists = 1
        return exists

    def existsFile(self, path, cwd, strIdJob, flagLast):

	exists = 0
        flagLast = 1

	try:
            extension = self.getNameFile(cwd)
            temp = self.nameOutput.split("\n",1)
            self.nameOutput = temp[0]
            preExt = self.getPreName( self.nameOutput, extension )
            temp1 = extension.split("\n",1)
            extension = temp1[0]
            #print 'ls -go ' + path + ' | grep -cE ' + preExt + '_' + strIdJob + '\.' + extension
	    if int( commands.getoutput('ls -go ' + path + ' | grep -cE ' + preExt + '_' + strIdJob + '\.'+extension) ) > 0:
                exists = 4
                dimOut = self.checkDim(path + '/' + preExt + '_' + strIdJob + '.' + extension)
                if dimOut > 0:
                    exists = exists + 2
            if flagLast:
                exists = self.checkStd(path, strIdJob) + exists
                return exists
            else:
                return exists
        except ValueError:
	    exists = 0

	return exists

    def compareAscii(self, str1, str2):
        strT = str2.split(" ", 1)
        #print strT
        str2 = strT[0]
        i = 0
        ret = 0
        if len(str1) == len (str2):
            i = 0
            #print str1, str2
            while i < len(str1):
                if  ord(str1[i]) == ord(str2[i]):
                    ret = ret + 1
                else:
                    return 0
                i = i + 1
        return i

    def jobsStatus(self, text, idJob):
        n = text.count("\n")
        stringa = text.split("\n", n)
        i = 0
        stratus = ""
        flag = 0
        j = 0
        while i < len(stringa):
            l = len(stringa[i])
            if stringa[i] != "":
                #print idJob, stringa[i][0], stringa[i]
                #print ord(idJob), ord(stringa[i][0])
                h2 = self.compareAscii( str(idJob), stringa[i] )
                if h2 > 0:
                    #print "OK: trovato ",idJob, stringa[i][0], stringa[i]    
                #if ord(idJob) == ord(stringa[i][0]):
                    while j < l and flag == 0:
                        #h = j + 1
                        if h2 > j + 1:
                            h = h2
                        else:
                            h = j + 1
                        if stringa[i][h] != 32:
                            #print stringa[i][h]
                            while ord(stringa[i][h]) != 32:
                                stratus = stratus + stringa[i][h]
                                h = h + 1
                                j = h
                                flag = 1
                            else:
                                if flag == 1:
                                   #print "X job", idJob,"ritorno status == ", stratus
                                   return stratus
                        j = j + 1
            i = i + 1

        return stratus

    def scanStatus(self, text, jobs):
        """
        Method that checks the status of job[s] submitted
	(This method could result a little bit redundant
	but it is just for the moment, because the parsing
	is quite poor - it is just looking to the state)
	"""
	codeStatus = -1
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
        #   8 -> Cleared(BOSS)
        i = 0
        z = int( jobs.nJobs() )
        while i < z:
            if jobs.getStatus(i) != 100:
                nJ = jobs.getJobId(i)
                stratus = self.jobsStatus( text, str(nJ) )
                jobs.cngStatus( nJ, stratus )
                i = i + 1
        #jobs.printStatusAll()
        return

    def strCodeStatus(self, stratus):

        if self.findInside(stratus, 'Done'):
            codeStatus = 0
        elif self.findInside(stratus, 'Scheduled'):
            codeStatus = 1
        elif self.findInside(stratus, 'Waiting'):
            codeStatus = 2
        elif self.findInside(stratus, 'Ready'):
            codeStatus = 3
        elif self.findInside(stratus, 'Aborted'):
            codeStatus = 4
        elif self.findInside(stratus, 'Killed') or self.findInside(stratus, 'Killed(BOSS)'):
            codeStatus = 5
        elif self.findInside(stratus, 'Running'):
            codeStatus = 6
        elif self.findInside(stratus, 'Idle'):
            codeStatus = 7
        elif self.findInside(stratus, 'Created(BOSS)'):
            codeStatus = 101
        elif self.findInside(stratus, 'Cleared(BOSS)'):
            codeStatus = 99
        elif self.findInside(stratus, 'Submitted'):
            codeStatus = 8
        else:
            codeStatus = -1

        return codeStatus

    def scanVomsProxy(self, txt):
        """
        WARNING: Unable to verify signature! Server certificate possibly not installed.
        Error: VOMS extension not found!
        xxxxx
        """
        n = 0
        n = txt.count("\n")
        stringa = txt.split("\n", n)
        for line in stringa:
            #print "line: ", line
            if ord(line[0]) >= 48 and ord(line[0]) <= 57:
                print line
                if int(line) > 180:
                    return line
                else:
                    break
        return 0

    def scanMyProxy(self, txt):
        """
        username: /C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Mattia Cinquilli
        owner: /C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Mattia Cinquilli
          timeleft: 167:50:27  (7.0 days)
        """
        flag = 0
        n = 0
        n = txt.count("\n")
        stringa = txt.split("\n", n)
        i = 0
        h = ""
        m = ""
        s = ""
        while i < n:
            #print stringa[i]
            if self.findInside(stringa[i], "timeleft"):
                flag = 1
                stringa2 = stringa[i].split(":", 1)
                character = stringa2[1][0]
                count = 0
                j = 1
                h = ""
                m = ""
                s = ""
                tt = len(stringa2[1])
                while character != "\n" and j < tt:
                    if ord(character) >= 48 and ord(character) <= 57:
                        if count == 0:
                            h = h + character
                        elif count == 1:
                            m = m + character
                        elif count == 2:
                            s = s + character
                    elif ord(character) == 58: # ascii(":") = 58
                        count += 1
                    elif ord(character) == 32 and count > 0: # ascii(" ") = 32
                        i = n
                        break
                    character = stringa2[1][j]
                    j = j + 1
            i = i + 1
        if flag:
            s = s + stringa2[1][tt-1]
        ##print h,m,s
            hours = int(h)
            mins = int(m)
        ##secs = int(s)
            if hours > 0:
                return 1
            elif mins > 45:
                return 1
            else:
                return 0

    def loadField (self, path, field):
## jobtype
        fOut = open( path + '/' + self.nameCfg )
        self.locks.lock_F( fOut, 1 )

        while True:
            line = fOut.readline()
            if len(line) == 0: # lunghezza zero indica l'EOF
                               # (NdT. End Of File, fine del file)
                break
            else:
                stri = line.split(" ",1)
               ## if stri[0] == "proxy_server":
                if stri[0] == field:
                    stri = line.split(" ", 3)
                    proxyStr = stri[2]
                    break

        self.locks.unlock_F( fOut )
        fOut.close()

        return proxyStr
