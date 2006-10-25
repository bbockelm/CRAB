# -*- coding: iso-8859-15 -*-
from Scanner import *
from Outputter import *
from SessionManager import *
from JobsManager import *
import popen2, os, fcntl, select, time, sys, commands
from tempfile import mkstemp
from subprocess import *
from os import path
import string

class InteractCrab:

    def __init__(self, nC, nS, dbg, wait, cfgName, wd):
        self.nCreate = str(nC)
        self.nSubmit = str(nS)
        self.jobs = JobsManager()
        if (int(self.nCreate > 0)):
            self.jobs.createList(self.nCreate, self.nSubmit)
        self.debug = dbg
        self.shorTSleep = wait
        self.longTSleep = 60
        self.cfgFileName = cfgName
        self.sess = SessionManager()
        self.cwd = str(wd)
        logging.debug('Inizializzato InteractCRAB nella cartella '+self.cwd)
        self.roboLogDir = path.abspath(self.sess.pathRoboName(self.cwd, cfgName))
        self.workingDir = path.abspath(self.sess.pathName(self.roboLogDir))
        self.creation = 0

    def getRoboLogDir (self):
        return self.roboLogDir

    def getWorkingDir (self):
        return self.workingDir

    def checkWorkingDir(self):
        """
        function that sets the working dir
          ---> Needs to implement the checking
        """
        
#        cwd = os.getcwd() + '/WorkSpace'  # Daniele
        cwd = os.getcwd()    # Must be more general  D.    
        os.chdir( cwd )
        
        return cwd

    def makeNonBlocking(self,fd):
        fl = fcntl.fcntl(fd,fcntl.F_GETFL)
        try:
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
        except AttributeError:
                fcntl.fcntl(fd,fcntl.F_SETFL, fl | os.FNDELAY)

    def run( self, cmd, putOut ):
        """
        Run command 'cmd'.
        Returns command stdoutput+stderror string on success,
        or None if an error occurred.
        """

        timeout = -1
        #putOut.debugger( "TestSuite: executing \"" + cmd + "\" in "+self.cwd, self.debug ) # prints the debug level 
        #putOut.debugger( "TestSuite: executing \"" + cmd + "\"", self.debug ) # prints the debug level
        putOut.debugger( "Executing \"" + cmd + "\" in "+self.cwd, self.debug ) # prints the debug level # Sk.
        child = popen2.Popen3(cmd, 1) # capture stdout and stderr from command
        child.tochild.close()             # don't need to talk to child
        outfile = child.fromchild 
        outfd = outfile.fileno()
        errfile = child.childerr
        errfd = errfile.fileno()
        self.makeNonBlocking(outfd)            # don't deadlock!
        self.makeNonBlocking(errfd)
        outdata = []
        errdata = []
        outeof = erreof = 0
 
        if timeout > 0 :
            maxwaittime = time.time() + timeout

        err = -1
        while (timeout == -1 or time.time() < maxwaittime):
            ready = select.select([outfd,errfd],[],[]) # wait for input
            if outfd in ready[0]:
                outchunk = outfile.read()
                if outchunk == '': outeof = 1
                outdata.append(outchunk)
            if errfd in ready[0]:
                errchunk = errfile.read()
                if errchunk == '': erreof = 1
                errdata.append(errchunk)
            if outeof and erreof:
                err = child.wait()
                break
            select.select([],[],[],.1) # give a little time for buffers to fill
        if err == -1:
            # kill the pid
            logging.debug('killing process '+(cmd)+' with timeout '+str(timeout))
            os.kill (child.pid, 9)
            err = child.wait()

        cmd_out = string.join(outdata,"")
        cmd_err = string.join(errdata,"")

        if err:
            logging.warning('`'+cmd+'`\n   failed with exit code '
                                  +`err`+'='+`(err&0xff)`+'(signal)+'
                                  +`(err>>8)`+'(status)')
            logging.info   ("\n--- CRAB STDOUT ---\n" + cmd_out + "\n------------------\n")
            logging.warning("\n--- CRAB STDERR ---\n" + cmd_err + "\n------------------\n")
            return None

        cmd_out = cmd_out + cmd_err

        return cmd_out

    def discoverJobsNumber(self, workingDir): ## Sk.
        cnt=0
        try:
            for rows in open(workingDir+"/share/db/jobs"):
                cnt += 1
        except OSError, str:
            logging.error("Can't discover how many jobs were created: "+str)
            sys.exit(1)
        return cnt

    def createNotSubmit(self, putOut, workingDir, parser, flag ):   ## Matt.
        """
        Executes the command for create the job[s]
        """
        # Messages must be somethings like CrabRob: ....  D.
        #putOut.printStep('TestSuite: CRAB is creating '+self.nCreate+' JOBS')
        putOut.printStep('CRAB is creating '+self.nCreate+' JOBS') # Sk.


        if flag > 0:
            creatioNumber = str(flag)
        else:
           creatioNumber = str (self.nCreate)
        
        if flag > 0:
            cmd = "crab -create " + creatioNumber + " -c " + workingDir
        elif int(creatioNumber) < 1:
            cmd = "crab -create -debug 10 -cfg " + self.cfgFileName + " -USER.ui_working_dir " + workingDir
        else:
            cmd ="crab -create " + creatioNumber + " -debug 10 -cfg " + self.cfgFileName + " -USER.ui_working_dir " + workingDir

        # "USER.ui_working_dir" is a CRAB's option that allow to change the CRAB's workingDir

        strOut = self.run( cmd, putOut ) ## Selfdiscovering of jobs' number... Sk.
        if int(creatioNumber) < 1:
            creatioNumber = self.discoverJobsNumber (workingDir)
            self.nCreate = str(creatioNumber)
            if int(self.nSubmit) < 1 or int(self.nSubmit) > int(self.nCreate):
                self.nSubmit = str(self.nCreate)
            self.jobs.createList(self.nCreate, self.nSubmit)

        
        if ( parser.scanCreate( strOut, self.nCreate, self.jobs ) == 0 ):
            putOut.writeOut( cmd, strOut, 0 )
            putOut.printStep('          Creation:  Ok  ')   # D.
            self.creation = 1
            return 1
        else:
            putOut.writeOut( cmd, strOut, 0 )
            putOut.printStep('          Creation:  Failed  ')
            
        return 0


    #def run( self, cmd, putOut ):
        """
        Run command 'cmd'.
        Returns command stdoutput+stderror string on success,
        or None if an error occurred.
        Following recipe on http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/52296
        """

        #timeout = -1

        #putOut.debugger( "TestSuite: executing \"" + cmd + "\"", self.debug ) # prints the debug level

        #child = popen2.Popen3(cmd, 1) # capture stdout and stderr from command
        #child.tochild.close()             # don't need to talk to child
        #outfile = child.fromchild
        #outfd = outfile.fileno()
        #errfile = child.childerr
        #errfd = errfile.fileno()
        #self.makeNonBlocking(outfd)            # don't deadlock!
        #self.makeNonBlocking(errfd)
        #outdata = []
        #errdata = []
        #outeof = erreof = 0

        #if timeout > 0 :
            #maxwaittime = time.time() + timeout

        #err = -1
        #while (timeout == -1 or time.time() < maxwaittime):
            #ready = select.select([outfd,errfd],[],[]) # wait for input
            #if outfd in ready[0]:
                #outchunk = outfile.read()
                #if outchunk == '': outeof = 1
                #outdata.append(outchunk)
            #if errfd in ready[0]:
                #errchunk = errfile.read()
                #if errchunk == '': erreof = 1
                #errdata.append(errchunk)
            #if outeof and erreof:
                #err = child.wait()
                #break
            #select.select([],[],[],.1) # give a little time for buffers to fill
        #if err == -1:
            # kill the pid
            #print('killing process '+(cmd)+' with timeout '+str(timeout))
            #os.kill (child.pid, 9)
            #err = child.wait()

        #cmd_out = string.join(outdata,"")
        #cmd_err = string.join(errdata,"")

        #if err:
            #print('`'+cmd+'`\n   failed with exit code '
                  #+`err`+'='+`(err&0xff)`+'(signal)+'
                              #+`(err>>8)`+'(status)')
            #print(cmd_out)
            #print(cmd_err)
            #return None

        #cmd_out = cmd_out + cmd_err

        #return cmd_out



    def justSubmit(self, putOut, workingDir, parser, sess, cwd, flag): ## Matt.
        """
        Executes the command for submit the job[s]
        """
        #putOut.printStep('TestSuite: CRAB is submitting '+self.nSubmit+' JOBS')
        putOut.printStep('CRAB is submitting '+self.nSubmit+' JOBS') # Sk.
        
        submissioNumber = self.nSubmit
        if flag > 0:
            submissioNumber = str(flag)

        cmd = "crab -submit " + submissioNumber + " -debug 10 -c " + workingDir
        strOut = self.run( cmd, putOut )

        ##print(strOut)

        result = parser.scanSubmit(strOut, int(submissioNumber), self.jobs )

        if result == 0:
            putOut.writeOut( cmd, strOut, 0 )
            putOut.printStep("         Submission: Ok ")    #D.
            self.creation = 1
            return 1
        elif result > 0:
            self.creation = 1
            putOut.writeOut( cmd, strOut, 0 )
            putOut.printStep("         Submission: Ok ")
            stringa = str(self.nSubmit)
            result = result - 1
            flag = 0
            while result > 0:
                tStr = str( int(self.nSubmit) - result )
                stringa = stringa + ", " + tStr
                result = result - 1
                flag = 1
            if flag:
                putOut.printStep("         Warning: jobs " + stringa + " are not submitted")
            else:
                putOut.printStep("         Warning: job " + stringa + " is not submitted")
            return 1
        else:
            putOut.writeOut( cmd, strOut, 0 )
            putOut.printStep("         Submission: Failed ")
            self.creation = 0
            #self.killManager( putOut, workingDir, parser, sess, "all", cwd )
#            print strOut         # Aggiungere in caso di verbositÃ
        #putOut.writeOut( cmd, strOut, 0 )      

        return 0

    def checkStatus(self, putOut, workingDir, parser, sess, flag, cwd):
        """
        Executes the commands for get the status of the job[s]
        """

        #putOut.printStep('TestSuite: CRAB start Checking JOBS\' status')
        putOut.printStep('CRAB start Checking JOBS\' status') # Sk.
        
        i = 0
        #resultStatus = 0
        toDo = 0

        while i < 30:
            #if toDo == -1:
                #i = 0

            #print " -- i = ", i, " -- "
            all = self.jobs.allDone()
            if self.jobs.allFinished() or all:
                return 0
            elif all == -1:
                return 0
            elif i == 29:
                logging.debug(self.jobs.allDone())
                logging.debug(self.jobs.allFinished())
                i = 0
            
            #else:
            #    resulStatus = -2
            # goody RoboCrab sleep
            if flag:
                if i == 1:
                    putOut.printStep('Sleeping '+str(self.longTSleep))
                    time.sleep(self.longTSleep)
#                   for conta in range(1,80):
#                       sys.stdout.write('.')
#                       sys.stdout.flush()
#                       time.sleep( 2.25 )
                else:
                    putOut.printStep('Sleeping '+str(self.shorTSleep))
                    time.sleep(self.shorTSleep)
#                   for conta in range(1,40):
#                       sys.stdout.write('.')
#                       sys.stdout.flush()
#                       time.sleep( 1.5 )
            else:
                i = 29

            cmd = "crab -status -c " + workingDir
            strOut =  self.run( cmd, putOut )
            putOut.writeOut( cmd, strOut, 0 )
           
            putOut.printStep('Parsing output')
            st = parser.scanStatus(strOut, self.jobs)
            ## why doesn't Python have a switch construct ?-)
            j = 0
            z = self.jobs.nJobs()
            while j < z:
                xT = self.jobs.getStatus(j)
                x = parser.strCodeStatus(xT)
                if x == -1:
                    h = str(j+1)
                    #putOut.printStep('TestSuite: JOBS\' status: \'Unknown\'')  ##M.: added idJob and status
                    putOut.printStep('JOBS\' status: \'Unknown\'')  ##M.: added idJob and status # Sk.
                    mesg = '          jobId: ' + h
                    putOut.printStep(mesg)
                    mesg = '          status: ' + str(xT)
                    putOut.printStep(mesg)
                elif x == 8:
                    h = str(j+1)
                    #putOut.printStep('TestSuite: JOBS\' status: \'Submitted\'')  ##M.: added idJob and status
                    putOut.printStep('JOBS\' status: \'Submitted\'')  ##M.: added idJob and status # Sk.
                    mesg = '          jobId: ' + h
                    putOut.printStep(mesg)
                elif x == 6:
                    pass
                elif x == 0:
                    h = str(j+1)
                    #mesg = 'TestSuite: JOB N° ' + h + '- status: \'Done (Success)\'' ##M.: added idJob
                    mesg = 'JOB N° ' + h + '- status: \'Done (Success)\'' ##M.: added idJob # Sk.
                    putOut.printStep( mesg )
                    #mesg = 'TestSuite: JOB N° ' + h + '- EXE_EXIT_STATUS: ' + parser.exeExitStatus(strOut) ##M.: added idJob
                    mesg = 'JOB N° ' + h + '- EXE_EXIT_STATUS: ' + parser.exeExitStatus(strOut) ##M.: added idJob # Sk.
                    putOut.printStep( mesg ) 
                    self.getResultOutput( putOut, workingDir, parser, cwd, j + 1 )
                    putOut.printTable(self.jobs)
                    #return 0
                elif x == 1:
                    pass
                elif x == 2:
                    pass
                elif x == 3:
                    pass
                elif x == 4:
               #     h = str(j+1)
               #     mesg = 'CrabRob: JOB NÂ° ' + h + ' - status: \'Aborted\'' ##M.: added idJob
               #     putOut.printStep( mesg )
               #     statoNow = self.jobs.getStatus(j)#jobIndex)
               #     statoPre = self.jobs.getStatusPre(j)#jobIndex)
                    statoNow = self.jobs.getStatus(j)#jobIndex)
                    statoPre = self.jobs.getStatusPre(j)#jobIndex)
                    if statoNow != statoPre:
                        h = str(j+1)
                        #mesg = 'TestSuite: JOB N° ' + h + ' - status: \'Aborted\'' ##M.: added idJob
                        mesg = 'JOB N° ' + h + ' - status: \'Aborted\'' ##M.: added idJob # Sk.
                        putOut.printStep( mesg )
                        self.getMortemOutput( putOut, workingDir, parser, j)
                    #return x
                elif x == 5:
                    h = str(j+1)
                    #mesg =  'TestSuite: JOB N° ' + h + ' - status: \'Killed\'' ##M.: added idJob
                    mesg =  'JOB N° ' + h + ' - status: \'Killed\'' ##M.: added idJob # Sk.
                    #putOut.printStep( mesg )
                    #if ( self.resubmitJob( outStream, workingDir, parser, jobId ) ):
                    #    print "resubmitted job " + jobId
                    #return -2
                    #else:
                    #    self.getMortemOutput( putOut, workingDir, parser )
                    #return x
                elif x == 7:
                    pass

                
                toDo = sess.rules( x, self.jobs, j, parser )
                if toDo == 2:
                    return -3
                elif toDo == 1:
                    strJob = str( j+1 )
                    #putOut.printStep("TestSuite: too long execution for job " + strJob + " => Killing!")
                    putOut.printStep("too long execution for job " + strJob + " => Killing!") # Sk.
                    if self.killManager( putOut, workingDir, parser, sess, j + 1, cwd ):
                        #print "Ok Killing ---> restart the Robot"
                        putOut.printTable(self.jobs)
                    else:
                        h = str(j+1)
                        #mesg = "TestSuite: ERROR on JOB N° " + h
                        mesg = "ERROR on JOB N° " + h # Sk.
                        putOut.printStep( mesg )

                j = j + 1

            i = i + 1

        return# resultStatus


    def workFlowKRP ( self, putOut, workingDir, parser, sess, cwd ):   ## Matt.
        """
        Executes the crab's command:
           -create -submit
           -kill -resuBmit -kill -getoutput
        """

        #mesg = "TestSuite: verifying others Crab's functionality: kill, resubmit and postMortem"
        mesg = "Verifying others Crab's functionality: kill, resubmit and postMortem" # Sk.
        putOut.printStep(mesg)

        if self.creation:
            idJobTest = int(self.nCreate) + 1
        else:
            idJobTest = 1
        if ( self.createNotSubmit ( putOut, workingDir, parser, 1 ) ):
            if ( self.justSubmit ( putOut, workingDir, parser, sess, cwd, 1 ) ):
                ##tot = int(self.nSubmit)
                ##for i in range (1, tot):
                ##    if self.jobs.getToSubmit(i-1):
                if not self.killManager( putOut, workingDir, parser, sess, idJobTest, cwd ):
                    self.getMortemOutput( putOut, workingDir, parser, idJobTest-1 )
        return 0



    def killManager(self, outStream, workingDir, parser, sess, jobId, cwd):
        #jobId = self.jobs.getFailed()
        jobId = str(jobId)
        ##print "...I will kill " + jobId  #incasodiverbositÃ 
        if ( self.killJob( outStream, workingDir, parser, jobId) ):
            return 1
            ##resultStatus = self.checkStatus ( outStream, workingDir, parser, sess, 0, cwd)
            ##if (resultStatus == 5 or resultStatus == 4):
                ##return 1
                #if ( self.resubmitJob( outStream, workingDir, parser, jobId ) ):
                    #return 1
        return 0

    def getResultOutput( self, putOut, workingDir, parser, cwd, idJob ):
        """
        Gets the output of the job[s]
        """

        # executing the commmand
        strJob = str(idJob)
        #mesg = 'TestSuite: JOB N° ' + strJob + '- Getting succeded(Done) output' ##M.: added idJob
        mesg = 'JOB N° ' + strJob + '- Getting succeded(Done) output' ##M.: added idJob # Sk.
        putOut.printStep(mesg)
        cmd = "crab -getoutput " + strJob + " -debug 10 -c " + workingDir
        strOut = self.run( cmd, putOut )

        # checking if the output files are in the subdir "/res"
        flagFiles = parser.existsFile(workingDir + '/res', cwd, strJob, 1)

        # printing output to the files and to the console
        if ( flagFiles > 1 ):
            if ( parser.scanGetOutput(strOut, strJob) == 0 ):
                #mesg = 'TestSuite: JOB NÂ° ' + strJob + ' - output was got correctly'  ##M.: added idJob
                mesg = 'JOB N° ' + strJob + ' - output was got correctly'  ##M.: added idJob #Sk.
                putOut.printStep( mesg )
                self.jobs.done( idJob, 1 )
            else:
                #mesg = 'TestSuite: JOB NÂ° ' + strJob + ' - output wasn\'t got correctly'  ##M.: added idJob
                mesg = 'JOB N° ' + strJob + ' - output wasn\'t got correctly'  ##M.: added idJob # Sk.
                putOut.printStep( mesg )
                self.jobs.done( idJob, -2 )
            if ( flagFiles == 4 ):
                putOut.printStep("\"WARNING: emply output file in " + workingDir + "/res/\"") # Sk.
                putOut.printStep("\"WARNING: missing stdout/stderr file in " + workingDir + "/res/\"") # Sk.
            elif ( flagFiles == 6 ):
                putOut.printStep("\"WARNING: missing stdout/stderr file in " + workingDir + "/res/\"") # Sk.
            elif ( flagFiles == 5 ):
                putOut.printStep("\"WARNING: emply output file in " + workingDir + "/res/\"") # Sk.
        else:
            putOut.printStep("ERROR - missing output file[s] in dir " + workingDir + "/res") # Sk.
            self.jobs.done( idJob, 2 )
            if ( flagFiles == 0 ):
                putOut.printStep("\"WARNING: missing stdout/stderr file in " + workingDir + "/res/\"") # Sk.

        putOut.writeOut( cmd, strOut, 0 )

        return 

    def getMortemOutput( self, putOut, workingDir, parser, flag ):
        """
        Executes the command for verify why a job (may be more then one) is dead
        """

        if flag < 0:
            jobsRange = self.jobs.allNotDone()
        else:
            jobsRange = str(flag + 1)
        mesg = 'JOB N° ' + jobsRange + ' - Getting postMortem informations' # Sk.
        putOut.printStep(mesg)   ##M.: added idJob
        cmd = "crab -postMortem " + jobsRange + " -debug 10 -c " + workingDir
        strOut = self.run( cmd, putOut )
        putOut.writeOut( cmd, strOut, 0 )

        return

    def killJob( self, putOut, workingDir, parser, jobId ):
        """
        kills the job with ID = jobId
        """

        # executing the commmand
        #putOut.printStep('Killing JOB %(#)d'%{'#' : jobId})
        putOut.printStep('Killing JOB N° ' + jobId) # Sk.
        #cmd = "crab -kill  %(#)d"%{'#' : jobId} + " -c " + workingDir
        cmd = "crab -kill " + jobId + " -c " + workingDir
        strOut = self.run( cmd, putOut )

        if ( parser.scanKill(strOut, jobId) == 0 ):
            putOut.writeOut( cmd, strOut, 0 )
            putOut.printStep("         killing:  Ok  ")
            return 1
        else:
            putOut.writeOut( cmd, strOut, 0 )
            putOut.printStep("         killing:  Failed  ")

        return 0

    def resubmitJob( self, putOut, workingDir, parser, jobId ):
        """
        resubmits the job with ID = jobId
        """

        # executing the commmand
        #putOut.printStep('Resubmitting JOB %(#)d'%{'#' : jobId})
        putOut.printStep('Resubmitting JOB N° ' + jobId) # Sk.
        #cmd = "crab -resubmit  %(#)d"%{'#' : jobId} + " -c " + workingDir
        cmd = "crab -resubmit " + jobId + " -c " + workingDir
        strOut = self.run( cmd, putOut )

        if ( parser.scanSubmit(strOut, int(self.nSubmit), self.jobs ) == 0 ):
            putOut.writeOut( cmd, strOut, 0 )
            putOut.printStep("         resubmitting:  Ok  ")
            return 1
        else:
            putOut.writeOut( cmd, strOut, 0 )
            putOut.printStep("         resubmitting:  Failed  ")

        return 0

    def outputChooser( self, putOut, workingDir, parser, choise, cwd ):
        """
        Calls Crab's getoutput or postMortem
        """

        # modificato /home/cinquilli/CRAB_1_0_7/python/SchedulerBoss.py linea 469
        if choise == 0:
            #jobDA prendere l'output OK
            self.getResultOutput( putOut, workingDir, parser, cwd )
        else:
            #jobDA prendere postmortem
            self.getMortemOutput( putOut, workingDir, parser)

        return

    #def checkProxy(self, outStream, parser, cwd): # Sk.
        #"""
        #"""
        #ok = 0
        #timeLeft = 0
      ##  try:
        #timeLeftLocal = self.run('voms-proxy-info -timeleft',outStream)
        
        ##timeLeft = parser.scanVomsProxy(timeLeftLocal)
        ##print "timeLeftLocal = ", timeLeftLocal

        #if timeLeftLocal == None:
            #timeLeft = 0
        #else:
            #tL = parser.scanVomsProxy(timeLeftLocal)
            #timeLeft = int(tL)
        #try:
            ##print "timeLeft = ", timeLeft
            #if timeLeft <= 0: #or not timeLeft:
                #cmd = 'voms-proxy-init -valid 24:00 -voms cms'
###            try:
                #out = os.system(cmd)
                #if out > 0:
                    #msg = "Unable to create a valid proxy!\n"
                    #outStream.printStep('TestSuite: ' + msg)
                #else:
                    #ok = 1
  ###          except:
                ##msg = "Unable to create a valid proxy!\n"
                ##outStream.printStep('TestSuite: ' + msg)
                ##print msg
            #elif int(timeLeft) > 0:
                #ok = 1

            #proxy = parser.loadField(cwd, "proxy_server")
###            proxy = "myproxy.cern.ch"
        ###    print proxy
            #if ok == 1:
                #controlMyProxy = self.run('myproxy-info -d -s '+ proxy, outStream)
                #flagError = 0
                #if controlMyProxy == None:
                    #flagError = 1
                    #controlMyProxy = "timeleft: 0"
                #if not parser.scanMyProxy(controlMyProxy) or flagError:
                    #ok = 0
                    #outStream.printStep('TestSuite: No credential delegated to myproxy server '+proxy+' will do now')
                    #print ("Enter pass phrase:")
                    #setMyProxy = self.run('myproxy-init -d -n -s '+proxy, outStream)
                    #controlMyProxy = self.run('myproxy-info -d -s '+proxy, outStream)
                    #if not parser.scanMyProxy(controlMyProxy):
                        #ok = 0
                    #else:
                        #ok = 1
        #except AttributeError:
            #outStream.printStep("TestSuite: Unable to create a valid proxy!\n")

        #return ok

    def crabRunner(self):
        """
        Main class for the interaction with Crab
        """

##        self.cfgFileName = self.cwd + "/" + self.cfgFileName

        parser = Scanner(self.cfgFileName)
#        print parser.nameCfg
        cfgName, ext = os.path.splitext(self.cfgFileName) # Using library function Sk.
        #print parser.getNameFile(self.cwd)

        outStream = Outputter(self.roboLogDir)
        outStream.printStep('CRAB Working_dir: ' + self.workingDir) # Sk.
        if self.nCreate < self.nSubmit:
            outStream.printStep('"WARNING jobs to submit are more then the jobs to create"') # Sk.

        resultStatus = -1

        if ( self.createNotSubmit ( outStream, self.workingDir, parser, 0 ) ):     ## Matt.
            outStream.printTable(self.jobs)
            if ( self.justSubmit ( outStream, self.workingDir, parser, self.sess, self.cwd, 0 ) ):   ## Matt.
                outStream.printTable(self.jobs)
                resultStatus = self.checkStatus ( outStream, self.workingDir, parser, self.sess, 1, self.cwd )
                outStream.printTable(self.jobs)
                #while resultStatus == -2:
                #    print "I will kill..."
                #    jobId = self.jobs.getFailed() 
                #    print "...I will kill " + jobId
            #    if ( self.killJob( outStream, self.workingDir, parser, jobId) ):
            #        resultStatus = self.checkStatus ( outStream, self.workingDir, parser, self.sess, 0, self.cwd )
                #        if (resultStatus == 5 or resultStatus == 4):
            #            if ( self.resubmitJob( outStream, self.workingDir, parser, jobId ) ):
            #                resultStatus = self.checkStatus ( outStream, self.workingDir, parser, self.sess, 1, self.cwd )

        #if (resultStatus == 0) or 
        #if (resultStatus == 4) or (resultStatus == 5):
            #self.outputChooser( outStream, self.workingDir, parser, resultStatus, self.cwd )
            #outStream.printTable(self.jobs)
        #elif resultStatus == -3:
        #    self.killJob(outStream, self.workingDir, parser, "all")
        #    outStream.printStep('CrabRob: ERROR\n\tINCOMPLETE RUNNING OF ROBOCRAB\n\tToo much time!!')
        #    #outStream.printStep('CrabRob: ERROR\n\tINCOMPLETE RUNNING OF ROBOCRAB\n\tToo many Resubmits')
        #    self.jobs.setIncompleted()
        #    outStream.printTable(self.jobs)

        self.jobs.setCompleted()
        outStream.printTable(self.jobs)
        outStream.printBadTable(self.jobs)
        outStream.printPathTable() ##M.: prints a message that shows the path of the table

        #self.workFlowKRP ( outStream, self.workingDir, parser, self.sess, self.cwd )   ## Matt.
            
        return
