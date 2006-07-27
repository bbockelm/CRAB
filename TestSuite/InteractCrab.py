from Scanner import *
from Outputter import *
from SessionManager import *
from JobsManager import *
import popen2, os, fcntl, select, time, sys, commands

class InteractCrab:

    nCreate = 0
    nSubmit = 0
    debug = 0
    set = -1
    jobs = JobsManager()
    longTSleep = 180
    shorTSleep = 60
    cfgFileName = ""

    def __init__(self, s, nC, nS, dbg, wait, cfgName):
        self.set = s
        self.nCreate = str(nC)
        self.nSubmit = str(nS)
        self.jobs.createList(self.nCreate, self.nSubmit)
        self.debug = dbg
        self.shorTSleep = wait
        self.cfgFileName = cfgName

    def checkWorkingDir(self):
        """
	function that sets the working dir 
	  ---> Needs to implement the checking
	"""
        
#	cwd = os.getcwd() + '/WorkSpace'  # Daniele
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
        Following recipe on http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/52296
        """

        timeout = -1

        putOut.debugger( "TestSuite: executing \"" + cmd + "\"", self.debug ) # prints the debug level

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
            print('killing process '+(cmd)+' with timeout '+str(timeout))
            os.kill (child.pid, 9)
            err = child.wait()

        cmd_out = string.join(outdata,"")
        cmd_err = string.join(errdata,"")

        if err:
            print('`'+cmd+'`\n   failed with exit code '
				  +`err`+'='+`(err&0xff)`+'(signal)+'
	                          +`(err>>8)`+'(status)')
            print(cmd_out)
            print(cmd_err)
            return None

        cmd_out = cmd_out + cmd_err

        return cmd_out

    def createNotSubmit(self, putOut, workingDir, parser ):
        """
        Executes the command for create the job[s]
        """
        # Messages must be somethings like CrabRob: ....  D.
        putOut.printStep('TestSuite: CRAB is creating '+self.nCreate+' JOBS')

	# "USER.ui_working_dir" is a CRAB's option that allow to change the CRAB's workingdir
	cmd = "crab -create " + self.nCreate + " -debug 10 -cfg " + self.cfgFileName + " -USER.ui_working_dir " + workingDir
        strOut = self.run( cmd, putOut )

	if ( parser.scanCreate( strOut, self.nCreate, self.jobs ) == 0 ):
            putOut.writeOut( cmd, strOut, 0 )
#            putOut.printStep(' => JOBS created correctly')
            putOut.printStep('          Creation:  Ok  ')   # D.
            return 1
        else:
	    putOut.writeOut( cmd, strOut, 0 )
    #        putOut.printStep(' => JOBS not created correctly')
            putOut.printStep('          Creation:  Failed  ')
            
            #print strOut      # Questo lo mostrerei solo in caso di Verbosità  D.
	#putOut.writeOut( cmd, strOut, 0 )

        return 0

    def justSubmit(self, putOut, workingDir, parser, sess, cwd):
        """
	Executes the command for submit the job[s]
	"""
	putOut.printStep('TestSuite: CRAB is submitting '+self.nSubmit+' JOBS')
	cmd = "crab -submit " + self.nSubmit + " -debug 10 -c " + workingDir
	strOut = self.run( cmd, putOut )

        ##print(strOut)

        result = parser.scanSubmit(strOut, int(self.nSubmit), self.jobs )

	if result == 0:
            putOut.writeOut( cmd, strOut, 0 )
            putOut.printStep("         Submission: Ok ")    #D.
            return 1
        elif result > 0:
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
            #self.killManager( putOut, workingDir, parser, sess, "all", cwd )
#            print strOut         # Aggiungere in caso di verbosità
	#putOut.writeOut( cmd, strOut, 0 )	

	return 0

    def checkStatus(self, putOut, workingDir, parser, sess, flag, cwd):
        """
        Executes the commands for get the status of the job[s]
        """

        putOut.printStep('TestSuite: CRAB start Checking JOBS\' status')
	
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
                print self.jobs.allDone()
                print self.jobs.allFinished()
                i = 0
            
            #else:
            #    resulStatus = -2
	    # goody RoboCrab sleep
            if flag:
                if i == 1:
		    time.sleep(self.longTSleep)
#                   for conta in range(1,80):
#                       sys.stdout.write('.')
#                       sys.stdout.flush()
#                       time.sleep( 2.25 )
                else:
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
           
	    st = parser.scanStatus(strOut, self.jobs)
	    ## why doesn't Python have a switch construct ?-)
            j = 0
            z = self.jobs.nJobs()
            while j < z:
                xT = self.jobs.getStatus(j)
                x = parser.strCodeStatus(xT)
                if x == -1:
                    h = str(j+1)
		    putOut.printStep('TestSuite: JOBS\' status: \'Unknown\'')  ##M.: added idJob and status
                    mesg = '          jobId: ' + h
                    putOut.printStep(mesg)
                    mesg = '          status: ' + xT
                    putOut.printStep(mesg)
                elif x == 8:
                    h = str(j+1)
                    putOut.printStep('TestSuite: JOBS\' status: \'Submitted\'')  ##M.: added idJob and status
                    mesg = '          jobId: ' + h
                    putOut.printStep(mesg)
                elif x == 6:
                    pass
                elif x == 0:
                    h = str(j+1)
                    mesg = 'TestSuite: JOB N° ' + h + '- status: \'Done (Success)\'' ##M.: added idJob 
                    putOut.printStep( mesg )
                    mesg = 'TestSuite: JOB N° ' + h + '- EXE_EXIT_STATUS: ' + parser.exeExitStatus(strOut) ##M.: added idJob 
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
               #     mesg = 'CrabRob: JOB N° ' + h + ' - status: \'Aborted\'' ##M.: added idJob
               #     putOut.printStep( mesg )
               #     statoNow = self.jobs.getStatus(j)#jobIndex)
               #     statoPre = self.jobs.getStatusPre(j)#jobIndex)
                    if statoNow != statoPre:
                        h = str(j+1)
                        mesg = 'TestSuite: JOB N° ' + h + ' - status: \'Aborted\'' ##M.: added idJob
                        putOut.printStep( mesg )
                        statoNow = self.jobs.getStatus(j)#jobIndex)
                        statoPre = self.jobs.getStatusPre(j)#jobIndex)
                        self.getMortemOutput( putOut, workingDir, parser, j)
                    #return x
                elif x == 5:
                    h = str(j+1)
                    mesg =  'TestSuite: JOB N° ' + h + ' - status: \'Killed\'' ##M.: added idJob
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
                    putOut.printStep("TestSuite: too long execution for job " + strJob + " => Killing!")
                    if self.killManager( putOut, workingDir, parser, sess, j + 1, cwd ):
                        #print "Ok Killing ---> restart the Robot"
                        putOut.printTable(self.jobs)
                    else:
                        h = str(j+1)
                        mesg = "TestSuite: ERROR on JOB N° " + h
                        putOut.printStep( mesg )

                j = j + 1

            i = i + 1

        return# resultStatus

    def killManager(self, outStream, workingDir, parser, sess, jobId, cwd):
        #jobId = self.jobs.getFailed()
        jobId = str(jobId)
        ##print "...I will kill " + jobId  #incasodiverbosità
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
        mesg = 'TestSuite: JOB N° ' + strJob + '- Getting succeded(Done) output' ##M.: added idJob
	putOut.printStep(mesg)
	cmd = "crab -getoutput " + strJob + " -debug 10 -c " + workingDir
        strOut = self.run( cmd, putOut )

	# checking if the output files are in the subdir "/res"
	flagFiles = parser.existsFile(workingDir + '/res', cwd, strJob, 1)

	# printing output to the files and to the console
	if ( flagFiles > 1 ):
	    if ( parser.scanGetOutput(strOut, strJob) == 0 ):
                 mesg = 'TestSuite: JOB N° ' + strJob + ' - output was got correctly'  ##M.: added idJob
                 putOut.printStep( mesg )
                 self.jobs.done( idJob, 1 )
	    else:
                mesg = 'TestSuite: JOB N° ' + strJob + ' - output wasn\'t got correctly'  ##M.: added idJob
                putOut.printStep( mesg )
                self.jobs.done( idJob, -2 )
            if ( flagFiles == 4 ):
                putOut.printStep("TestSuite: \"WARNING: emply output file in " + workingDir + "/res/\"")
                putOut.printStep("TestSuite: \"WARNING: missing stdout/stderr file in " + workingDir + "/res/\"")
            elif ( flagFiles == 6 ):
                putOut.printStep("TestSuite: \"WARNING: missing stdout/stderr file in " + workingDir + "/res/\"")
            elif ( flagFiles == 5 ):
                putOut.printStep("TestSuite: \"WARNING: emply output file in " + workingDir + "/res/\"")
	else:
	    putOut.printStep("TestSuite: ERROR - missing output file[s] in dir " + workingDir + "/res")
            self.jobs.done( idJob, 2 )
            if ( flagFiles == 0 ):
                putOut.printStep("TestSuite: \"WARNING: missing stdout/stderr file in " + workingDir + "/res/\"")

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
        mesg = 'TestSuite: JOB N° ' + jobsRange + ' - Getting postMortem informations'
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
        putOut.printStep('TestSuite: Killing JOB N° ' + jobId)
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
        putOut.printStep('TestSuite: Resubmitting JOB N° ' + jobId)
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

    def checkProxy(self, outStream, parser, cwd):
        """
        """
        ok = 0
        timeLeft = 0
      #  try:
        timeLeftLocal = self.run('voms-proxy-info -timeleft',outStream)
        
        #timeLeft = parser.scanVomsProxy(timeLeftLocal)
        #print "timeLeftLocal = ", timeLeftLocal

        if timeLeftLocal == None:
            timeLeft = 0
        else:
            tL = parser.scanVomsProxy(timeLeftLocal)
            timeLeft = int(tL)
        try:
            #print "timeLeft = ", timeLeft
            if timeLeft <= 0: #or not timeLeft:
                cmd = 'voms-proxy-init -valid 24:00 -voms cms'
##            try:
                out = os.system(cmd)
                if out > 0:
                    msg = "Unable to create a valid proxy!\n"
                    outStream.printStep('TestSuite: ' + msg)
                else:
                    ok = 1
  ##          except:
                #msg = "Unable to create a valid proxy!\n"
                #outStream.printStep('TestSuite: ' + msg)
                #print msg
            elif int(timeLeft) > 0:
                ok = 1

            proxy = parser.loadField(cwd, "proxy_server")
##            proxy = "myproxy.cern.ch"
        ##    print proxy
            if ok == 1:
                controlMyProxy = self.run('myproxy-info -d -s '+ proxy, outStream)
                flagError = 0
                if controlMyProxy == None:
                    flagError = 1
                    controlMyProxy = "timeleft: 0"
                if not parser.scanMyProxy(controlMyProxy) or flagError:
                    ok = 0
                    outStream.printStep('TestSuite: No credential delegated to myproxy server '+proxy+' will do now')
                    print ("Enter pass phrase:")
                    setMyProxy = self.run('myproxy-init -d -n -s '+proxy, outStream)
                    controlMyProxy = self.run('myproxy-info -d -s '+proxy, outStream)
                    if not parser.scanMyProxy(controlMyProxy):
                        ok = 0
                    else:
                        ok = 1
        except AttributeError:
            outStream.printStep("TestSuite: Unable to create a valid proxy!\n")

        return ok

    def crabRunner(self):
        """
        Main class for the interaction with Crab
        """

	cwd = self.checkWorkingDir()
##        self.cfgFileName = cwd + "/" + self.cfgFileName

	sess = SessionManager()
	parser = Scanner(self.cfgFileName)
#        print parser.nameCfg
        cfgName = parser.splitExtension(self.cfgFileName)
        #print parser.getNameFile(cwd)
        roboLogDir = sess.pathRoboName(cwd, cfgName)
        workingDir = sess.pathName(roboLogDir)

	outStream = Outputter(roboLogDir)
        outStream.printStep('TestSuite: CRAB Working_dir: ' + workingDir)
        if self.nCreate < self.nSubmit:
            outStream.printStep('TestSuite: "WARNING jobs to submit are more then the jobs to create"')

	resultStatus = -1

        if self.checkProxy(outStream, parser, cwd):
            ## A valid proxy certificate exists
            if ( self.createNotSubmit ( outStream, workingDir, parser ) ):
                outStream.printTable(self.jobs)
                if ( self.justSubmit ( outStream, workingDir, parser, sess, cwd ) ):
                    outStream.printTable(self.jobs)
                    resultStatus = self.checkStatus ( outStream, workingDir, parser, sess, 1, cwd )
                    outStream.printTable(self.jobs)
                #while resultStatus == -2:
                #    print "I will kill..."
                #    jobId = self.jobs.getFailed() 
                #    print "...I will kill " + jobId
                #    if ( self.killJob( outStream, workingDir, parser, jobId) ):
                #        resultStatus = self.checkStatus ( outStream, workingDir, parser, sess, 0, cwd )
                #        if (resultStatus == 5 or resultStatus == 4):
                #            if ( self.resubmitJob( outStream, workingDir, parser, jobId ) ):
                #                resultStatus = self.checkStatus ( outStream, workingDir, parser, sess, 1, cwd )

	#if (resultStatus == 0) or 
        #if (resultStatus == 4) or (resultStatus == 5):
	    #self.outputChooser( outStream, workingDir, parser, resultStatus, cwd )
            #outStream.printTable(self.jobs)
        #elif resultStatus == -3:
        #    self.killJob(outStream, workingDir, parser, "all")
        #    outStream.printStep('CrabRob: ERROR\n\tINCOMPLETE RUNNING OF ROBOCRAB\n\tToo much time!!')
        #    #outStream.printStep('CrabRob: ERROR\n\tINCOMPLETE RUNNING OF ROBOCRAB\n\tToo many Resubmits')
        #    self.jobs.setIncompleted()
        #    outStream.printTable(self.jobs)

            self.jobs.setCompleted()
            outStream.printTable(self.jobs)
            outStream.printPathTable() ##M.: prints a message that shows the path of the table

        else:
            ## A valid proxy certificate doesn't exist
            outStream.printStep('TestSuite: Unable to create a valid proxy')
            
        return
