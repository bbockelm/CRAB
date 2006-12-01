import string, os, commands
from LockerFile import *
from JobsManager import *
import re

class Scanner:
    def __init__(self, cfgName = 'crab.cfg'): # Rebuilt Sk.
        self.nameCfg = str(cfgName)
        self.locks = LockerFile()
        self.nameOutput = ''
        self.created = None
        self.jobType = 'CMSSW'

#    def scanCreatePolished(self, lines):
#        """
#        searches through lines looking for "crab. Total of # jobs created." and return #
#        if a row like this doesn't exist it returns -1
#        """
#        r = re.compile("crab\\. Total of ([\\d]*) jobs created\\.")
#        for line in lines:
#            if res = r.match(line):
#                return res.group(1) # Restutuisce il numero dei job matchati
#        return -1
#
#    def scanStatusPolished(lines):
#        """
#        search the jobs status table and parse it
#        """
#        foundTable = False
#        jobs = []
#        # Matcha una riga di -status senza EXE_EXIT_CODE e JOB_EXIT_STATUS
#        r1 = re.compile("([\\d]*)[\\s]* ([\\w]*(\\([\\w]\\))?)[\\s]*(.*)")
#        # Matcha una intera riga di -status
#        r2 = re.compile("([\\d]*)[\\s]*([\\w]*(\\([\\w]\\))?)[\\s]*(.*)[\\s]*([\\d]*)[\\s]*([\\d]*)")
#        for line in lines:
#            if "-----------" in line:
#                foundTable = True
#            if foundTable:
#                if res = r2.match(line):
#                    jobs.append(res.group(1,2,3,4,5)
#                elif res = r1.match(line):
#                    jobs.append(res.group(1,2,3)
#        return jobs

    def checkDim(self, path):
        """
           return the dim. of the specified path
        """
        p = os.path.getsize(path)
        if p > 0:
            return p
        return 0

    def findInside(self, text, str):
        return text.find(str) != -1

    #def add2JobList(self, nJob, status): # Sk.
        #self.created

    def findNJob(self, text, nJobs, opt, jobs):
        """
           checks nJobs inside the crab's output (creation and submition only)
           if opt = 1 check for submitted jobs, otherwise for created ones
           jobs is ignored
           It returns -1 if the total number of job in the chosen state is non negative and we find
           a row in the output asserting this
           If we checks for submitted jobs and all is right it returns the total number of jobs not submitted
           Otherwise it returns -2
        """
        statusJob = 101 # created black magic code Sk.
        count = 0 # Count how many jobs are in the state not submitted Sk.
        if opt == 1:
            #s1 = "Submitting"
            s1 = "Submitted"
            s2 = "submitted"
            statusJob = 102 # submitted black magic code Sk.
            ## if not submitted, crab will return: "crab. Job # 4 not submitted: status None"
            ##                                     "crab. Total of 3 jobs submitted (from 4 requested)."
        elif opt == 0:
            #s1 = "Creating"
            s1 = "Created"
            s2 = "created"
        if type(text) != type(''): # Detect if the text is empty, which is a clear signal of troubles! Sk.
            return -2
        n = text.count("\n")
        rows = text.split("\n", n)
        jobC = 0 # Count how many jobs are in the state chosen by opt Sk.
        for row in rows:
            if jobC < nJobs:
                for xJob in range(1, nJobs+1):
                    if row == "crab. " + s1 + " job # %(#)d"%{'#' : xJob} :
                        jobs.cngStatus( xJob, statusJob )
                        jobC += 1
                        if opt == 1:
                            jobs.submitted( xJob, 1 )
                        elif opt == 0:
                            jobs.created( xJob, 1 )
                    elif opt == 1 and row == "crab. Job # %(#)d"%{'#' : xJob} + " not submitted: status None":
                        #print str[i], "crab. Job # %(#)d"%{'#' : xJob} + " not submitted: status None"
                        count = count + 1
            if jobC > 0: # Check if we have counted all the jobs Sk.
                if row == "crab. Total of %(#)d jobs "%{'#' : jobC} + s2 + ".":
                    return -1 # Is this a bad thing?? Sk.
                #elif opt == 1:
                  #  tt = jobC - count
                    ## "crab. Total of 3 jobs submitted"
                  #  print tt, jobC, count
                  #  print "crab. Total of %(#)d"%{'#' : tt} + " jobs submitted"
                  #  if str[i] == "crab. Total of %(#)d"%{'#' : tt} + " jobs submitted":
                        #(from %(#d)"%{'#' : jobC } + " requested).":
                  #      return count
        if opt == 1:
            tt = nJobs - count # Jobs submitted Sk.
            ## "crab. Total of 3 jobs submitted"
            #print tt, nJobs, count
            #  print "crab. Total of %(#)d"%{'#' : tt} + " jobs submitted"
            if rows[-3] == "crab. Total of %(#)d"%{'#' : tt} + " jobs submitted (from %(#)d"%{'#' : nJobs } + " requested).":
                return count # Jobs not submitted! Sk.

        return -2 # What does this mean? Is this a bad thing?? Sk.

    def scanCreate(self, text, n, jobs):
        """
            checks the crab's output for the option "-create"
            text is a list of string representing the output to be parsed
            n is the total number of jobs
            jobs is the number of
        """
        if self.findNJob( text, int(n), 0, jobs ):
           #self.findInside (text, "crab. Total of %(#)d jobs created"%{'#' : int(n)} ):
            return 0
        #else:
           #n = text.count("\n")
        return 1

    def scanKill(self, text, n):
        """
            checks the crab's output for the option "-kill"
        """
        #if self.findInside (text, "crab. Killing job # %(#)d"%{'#' : n} ):
        if self.findInside (text, "crab. Killing job # " + n):
            return 0
        return 1

    def scanSubmit(self, text, n, jobs):
        """
            checks the crab's output for the option "-submit"
        """
        ret = self.findNJob( text, int(n), 1, jobs )
        #print ret
        if ret == -1:
            return 0
        elif ret >= 0:
            return ret
        else:
            print "\n"
        return -1

    def scanGetOutput(self, text, n):
        """
            checks the crab's output for the option "-submit"
        """
        #if self.findInside(text, "crab. Results of Job # %(#)d are in"%{'#' : n} ):
        if self.findInside(text, "crab. Results of Job # " + n  + " are in"):
            return 0
        return 1

    def exeExitStatus(self, text):
        """
            scan the crab's output for the option "-status"
             -> looks for the exit status of the job
        """
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

    #def splitExtension(self, nameExt): # Alredy in Python Sk.
        #nDot = nameExt.count(".")
        #name = nameExt.split(".", nDot)
        #stringa = ""
        #for i in name:
            #if i != "cfg":
                #if len(stringa) > 0:
                    #stringa = stringa + "." + i
                #else:
                    #stringa = i
###        print stringa
        #return stringa


    def getExtension(self, name):
        """
           get the extension of the file named "name"
        """
        return string.split(name,'.')[-1]
        # e = ""
        # l = len(name)
        # i = 0
        # nP = 0
        # n = name
        # while (i < l):
        #     c = n[i]
        #     if c == ".":
        #         nP = nP +1
        #         e = n.split(c, nP)
        #         n = e[nP]
        #         i = 0
        #         l = len(n)
        #         nP = 0
        #     i = i + 1

        # return e

    def getPreName(self, name, ext):
        """
           get the name of the file named "name" without the extension
        """
        ln = len (name)
        le = len (ext)
        st = name[0:(ln-le)]

        return st

    def getNameFile(self, path):
        """
           returns the extension of the "output_file" option in crab's config file
        """
        jb = self.loadField(path, "jobtype")
        jobtype = ""
        for charact in jb:
            if ord(charact) != 32 and ord(charact) != 10:
                jobtype = jobtype + charact.upper()

        self.jobType = jobtype

        fOut = open( self.nameCfg )
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
                    print " "
                if str2[0] == "output_file" and flag and len(str2) == 3:
                    str2 = line.split(" ", 3)
                    #print str2
                    extens = self.getExtension(str2[2])
                    #print extens
                    self.nameOutput = str2[2]
                    break

        self.locks.unlock_F( fOut )
        fOut.close()

        return extens[1]

    def checkStd(self, path, strIdJob):
        """
           checks the if the standard output and error files are inside the "res" dir
        """
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
        """
           checks all the output files
        """
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
        """
           compares, char by char, the str1 and str2 strings
        """
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
        """
           get the status string of the job idJob from the output
           of the crab's option "-status"
        """
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
        z = int( jobs.nJobs() )
        for i in range(z):
            if jobs.getStatus(i) != 100:
                nJ = jobs.getJobId(i)
                stratus = self.jobsStatus( text, str(nJ) )
                jobs.cngStatus( nJ, stratus )
        #jobs.printStatusAll()
        return

    def strCodeStatus(self, stratus):
        """
           converts a status string on the corresponding integer code
        """
        stratus = str(stratus)
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
        elif self.findInside(stratus, 'Killed'): # or self.findInside(stratus, 'Killed(BOSS)'): Sk.
            codeStatus = 5
        elif self.findInside(stratus, 'Running'):
            codeStatus = 6
        elif self.findInside(stratus, 'Idle'):
            codeStatus = 7
        elif self.findInside(stratus, 'Created'): # (BOSS)'): Sk.
            codeStatus = 101
        elif self.findInside(stratus, 'Cleared'): # (BOSS)'): Sk.
            codeStatus = 99
        elif self.findInside(stratus, 'Submitted'):
            codeStatus = 8
        else:
            codeStatus = -1

        return codeStatus

    def scanVomsProxy(self, txt):
        """
           scans the output of the command 'voms-proxy-info -timeleft'
        _ _ _ _ _ _ _ _
        error case:
        WARNING: Unable to verify signature! Server certificate possibly not installed.
        Error: VOMS extension not found!
        xxxxx
        _ _ _ _ _ _ _ _
        """
        n = 0
        n = txt.count("\n")
        stringa = txt.split("\n", n)
        for line in stringa:
            #print "line: ", line
            if ord(line[0]) >= 48 and ord(line[0]) <= 57:
                #print line
                if int(line) > 180:
                    return line
                else:
                    break
        return 0

    def scanMyProxy(self, txt):
        """
           scans the output of the command 'myproxy-info -d -s proxy"
        _ _ _ _ _ _ _ _
        correct case:
        username: /C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Mattia Cinquilli
        owner: /C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Mattia Cinquilli
          timeleft: 167:50:27  (7.0 days)
        _ _ _ _ _ _ _ _
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
        """
           load a field value from the crab's config file
             -> fields: "proxy_server", "jobtype"
        """
## jobtype
        fOut = open( self.nameCfg )
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
