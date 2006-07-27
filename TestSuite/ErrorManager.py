#InteractCrab.py                                                                                     0100644 0001052 0002567 00000016714 10416735222 013344  0                                                                                                    ustar   cinquilli                       cms                                                                                                                                                                                                                    from Scanner import *
from Outputter import *
from SessionManager import *
import popen2, os, fcntl, select, time, sys, commands

class InteractCrab:

    def __init__(self):
        return

    def checkWorkingDir(self):
        """
	function that sets the working dir 
	Needs to implement the checking
	"""
        
	cwd = os.getcwd() + '/WorkSpace'
        os.chdir( cwd )
        
        return cwd

    def makeNonBlocking(self,fd):
        fl = fcntl.fcntl(fd,fcntl.F_GETFL)
        try:
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
        except AttributeError:
                fcntl.fcntl(fd,fcntl.F_SETFL, fl | os.FNDELAY)

    def run(self,cmd):
        """
        Run command 'cmd'.
        Returns command stdoutput+stderror string on success,
        or None if an error occurred.
        Following recipe on http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/52296
        """

        timeout = -1

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

    def createNotSubmit(self, putOut, workingDir, parser):
        """
        Executes the command for create the job[s]
        """
        putOut.printStep('Creating JOBS')

	# "USER.ui_working_dir" is a CRAB's option that allow to change the CRAB's workingdir
	cmd = "crab -create 1 -USER.ui_working_dir " + workingDir
        strOut = self.run( cmd )

	if ( parser.scanCreate( strOut, 1 ) == 0 ):
            putOut.printStep(' => JOBS created correctly')
        else:
            putOut.printStep(' => JOBS not created correctly')

	putOut.writeOut( cmd, strOut, 0 )

        return

    def justSubmit(self, putOut, workingDir, parser):
        """
	Executes the command for submit the job[s]
	"""
	putOut.printStep('Submitting JOBS')
	cmd = "crab -submit 1 -c " + workingDir
	strOut = self.run(cmd)

	if ( parser.scanSubmit(strOut, 1) == 0 ):
            putOut.printStep(" => JOBS submitted correctly")
        else:
            putOut.printStep(" => ERROR:  JOBS not submitted correctly")

	putOut.writeOut( cmd, strOut, 0 )	

	return

    def checkStatus(self, putOut, workingDir, parser):
        """
        Executes the commands for get the status of the job[s]
        """

        putOut.printStep('Checking JOBS\' status')
	
        i = 0

        while i < 30:

	    # goody RoboCrab sleep
	    if i != 0:
		time.sleep(60)
#		for conta in range(1,40):
#		    sys.stdout.write('.')
#		    sys.stdout.flush()
#	            time.sleep( 1.5 )
	    else:
		time.sleep(180)
#		for conta in range(1,80):
#                    sys.stdout.write('.')
#		    sys.stdout.flush()
#                    time.sleep( 2.25 )

            #print'\n  -> Running check number: ', i

	    cmd = "crab -status -c " + workingDir
            strOut =  self.run(cmd)
	    putOut.writeOut( cmd, strOut, 0 )
           
	    x = parser.scanStatus(strOut)
	    ## why doesn't Python have a switch construct ?-)
	    if x == -1:
		putOut.printStep(' => JOBS\' status: \'Unknown\'')
	    #elif x == 6:
            #    putOut.printStep('JOBS\' status: \'Running\'')
	    elif x == 0:
		putOut.printStep(' => JOBS\' status: \'Done (Success)\'')
		putOut.printStep(' => EXE_EXIT_STATUS: ' + parser.exeExitStatus(strOut) )
		return 0
	    #elif x == 1:
		#putOut.printStep('JOBS\' status: \'Scheduled\'')
	    #elif x == 2:
		#putOut.printStep('JOBS\' status: \'Waiting\'')
	    #elif x == 3:
		#putOut.printStep('JOBS\' status: \'Ready\'')
	    elif x == 4:
		putOut.printStep(' => JOBS\' status: \'Aborted\'')
		return 4
	    elif x == 5:
		putOut.printStep(' => JOBS\' status: \'Killed\'')
		return 5

	    i = i + 1

        return

    def getResultOutput( self, putOut, workingDir, parser ):
        """
        Gets the output of the job[s]
        """

	# executing the commmand
	putOut.printStep('Getting succeded(Done) JOBS\' output')
	cmd = "crab -getoutput 1 -c " + workingDir
        strOut = self.run(cmd)

	# checking if the output files are in the subdir "/res"
	flagFiles = parser.existsFile(workingDir + '/res')

	# printing output to the files and to the console
	if ( flagFiles != 0 ):
	    if ( parser.scanGetOutput(strOut, 1) == 0 ):
                putOut.printStep(" => JOBS' output was get correctly")
	    else:
            	putOut.printStep(" => ERROR: JOBS' output wasn't got correctly")
	    if ( flagFiles == 2 ):
	        putOut.printStep(" => ERROR: not emply file *.stderr in " + workingDir + "/res/")
	else:
	    putOut.printStep(" => ERROR: JOBS' output wasn't got correctly - missing file[s] in dir " + workingDir + "/res")

        putOut.writeOut( cmd, strOut, 0 )

        return

    def getMortemOutput( self, putOut, workingDir, parser ):
        """
        Executes the command for verify why a job (may be more then one) is dead
	"""

	putOut.printStep('Getting postMortem JOBS\' informations')
	cmd = "crab -postMortem 1 -c " + workingDir
        strOut = self.run(cmd)
        putOut.writeOut( cmd, strOut, 0 )

        return

    def outputChooser( self, putOut, workingDir, parser, choise ):
	"""
	Calls Crab's getoutput or postMortem
	"""

	# modificato /home/cinquilli/CRAB_1_0_7/python/SchedulerBoss.py linea 469
	if choise == 0:
	    self.getResultOutput( putOut, workingDir, parser )
	else:
	    self.getMortemOutput( putOut, workingDir, parser )

	return 

    def crabRunner(self):
        """
        Main class for the interaction with Crab
        """
	cwd = self.checkWorkingDir()

	sess = SessionManager()
	parser = Scanner()

        workingDir = sess.pathName()
	roboLogDir = sess.pathRoboName(cwd)

	outStream = Outputter(roboLogDir)
        outStream.printStep('Working on directory: ' + workingDir)

        self.createNotSubmit ( outStream, workingDir, parser )
        self.justSubmit ( outStream, workingDir, parser )
        resultStatus = self.checkStatus ( outStream, workingDir, parser )
	
	self.outputChooser( outStream, workingDir, parser, resultStatus )

        return
                                                    LockerFile.py                                                                                       0100644 0001052 0002567 00000000773 10416703103 013011  0                                                                                                    ustar   cinquilli                       cms                                                                                                                                                                                                                    import os, fcntl

class LockerFile:

    LOCK_EX = fcntl.LOCK_EX  # exclusive lock of default
    LOCK_SH = fcntl.LOCK_SH  # shared lock specified with the parameter "flags"
    LOCK_NB = fcntl.LOCK_NB  # don't block when blocking
    LOCK_UN = fcntl.LOCK_UN

    def __init__(self):
	return

    def lock_F( self, file, flags ):
	
	LOCK = self.LOCK_EX

	if flags == 1:
	    LOCK = self.LOCK_SH
	 
	fcntl.flock(file.fileno(), LOCK)

    def unlock_F( self, file ):
	fcntl.flock(file.fileno(), self.LOCK_UN)
     Outputter.py                                                                                        0100644 0001052 0002567 00000003006 10416735234 013007  0                                                                                                    ustar   cinquilli                       cms                                                                                                                                                                                                                    import os
from LockerFile import *

class Outputter:

    # global path of robot's dir
    roboLogDir = 'default'
    # file name for the crab's output bypassed by the robot
    nameCrabLog = 'crab.robot.out'
    # file name for the robot's output
    nameRoboLog = 'robo.out'
    # Locker
    locks = LockerFile()
    
    def __init__( self, dir ):

	str = dir + '/Robolog'
	#print( str )
	os.mkdir( str )
	self.roboLogDir = str
        
	self._createFile_( self.nameCrabLog )
	self._createFile_( self.nameRoboLog )

	return

    def _createFile_( self, name ):
	
	dir = self.roboLogDir + '/' + name

	if not os.path.exists( dir ):
	    file = open( dir, 'w' )
	    self.locks.lock_F( file, 0 )
	    file.write(" |-> LOG FILE STARTS... \n\n")
	    self.locks.unlock_F( file )
	    file.close()

	return

    def printStep( self, str ):
	"""
	Prints the output "str"
	"""
        print('')
	print '**** ****  ', str, '  **** ****'
	self.writeOut( '', str, 1 )

	return


    def writeOut( self, cmd, text, opt ):
	"""
	Writes on the file corresponding to the option "opt"
	"""

	if opt == 0:
	    fName = self.roboLogDir + '/' + self.nameCrabLog
	elif opt == 1:
	    fName = self.roboLogDir + '/' + self.nameRoboLog

	fOut = open( fName, "a" )
	self.locks.lock_F( fOut, 0 )
	if opt == 0:
	    fOut.write('\n|-> COMMAND:  ' + cmd + '\n\n|-> OUTPUT:\n\n')
	fOut.write( text )
	fOut.write('\n-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-\n')
	self.locks.unlock_F( fOut )
	fOut.close()

	return
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          portalocker.py                                                                                      0100700 0001052 0002567 00000002003 10416701352 013300  0                                                                                                    ustar   cinquilli                       cms                                                                                                                                                                                                                    ##!/usr/bin/env python2.2
"""
Synopsis:

   import portalocker
   file = open("somefile", "r+")
   portalocker.lock(file, portalocker.LOCK_EX)
   file.seek(12)
   file.write("foo")
   file.close()

If you know what you're doing, you may choose to

   portalocker.unlock(file)

before closing the file, but why?

Methods:

   lock( file, flags )
   unlock( file )

Constants:

   LOCK_EX
   LOCK_SH
   LOCK_NB
"""
import os
import fcntl

LOCK_EX = fcntl.LOCK_EX
LOCK_SH = fcntl.LOCK_SH
LOCK_NB = fcntl.LOCK_NB

def lock(file, flags):
    fcntl.flock(file.fileno(), flags)

def unlock(file):
    fcntl.flock(file.fileno(), fcntl.LOCK_UN)

if __name__ == '__main__':
    from time import time, strftime, localtime
    import sys
    import portalocker

    log = open('log.txt', "a+")
    portalocker.lock(log, portalocker.LOCK_EX)

    timestamp = strftime("%m/%d/%Y %H:%M:%S\n", localtime(time()))
    log.write( timestamp )

    print "Wrote lines. Hit enter to release lock."
    dummy = sys.stdin.readline()

    log.close()
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             RoboCrab.py                                                                                         0100700 0001052 0002567 00000000373 10414736527 012466  0                                                                                                    ustar   cinquilli                       cms                                                                                                                                                                                                                    #!/usr/bin/env python2.2
from InteractCrab import InteractCrab

class RoboCrab:

    def __init__(self):
        return

if __name__=='__main__':
    
    roboStart = RoboCrab()
    
    crabber = InteractCrab()
    crabber.crabRunner()
    
    pass
                                                                                                                                                                                                                                                                     Scanner.py                                                                                          0100644 0001052 0002567 00000005073 10416732274 012374  0                                                                                                    ustar   cinquilli                       cms                                                                                                                                                                                                                    import string, os, commands

class Scanner:

    def __init__(self):
        return

    def checkDim(self, path):

	p = os.path.getsize(path)
	#print "size of ", path, " = ", p
	if p > 0:
	    return 1
	return 0

    def findInside(self, text, str):
        return text.find(str) != -1

    def scanCreate(self, text, n):
        if self.findInside (text, "crab. Total of %(#)d jobs created"%{'#' : n} ):
            return 0
        return 1

    def scanSubmit(self, text, n):
        if self.findInside(text, "crab. Total of %(#)d jobs submitted"%{'#' : n} ):
            return 0
        return 1

    def scanGetOutput(self, text, n):
	if self.findInside(text, "crab. Results of Job # %(#)d are in"%{'#' : n} ):
	    return 0
	return 1

    def exeExitStatus(self, text):

        str = text.split(" ", 1)
        flag = 0
        while flag == 0:
        # line to be parsed
        #' '+jobStatus+'   '+dest+'      '+exe_code+'       '+job_exit_status
            if str[0] == "(Success)":
                str = str[1].split("   ", 5)
                flag = 1
            else:
                str = str[1].split(" ", 1)

        return str[2]

    def existsFile(self, path):

	exists = 0

	try:
	    if int( commands.getoutput('ls -go ' + path + ' | grep -cE *.aida ') ) > 0:
	        if int( commands.getoutput('ls -go ' + path + ' | grep -cE *.stdout') ) > 0:
		    if int( commands.getoutput('ls -go ' + path + ' | grep -cE *.stderr') ) > 0:
		        exists = 1
		        nameErr = commands.getoutput('ls -1 ' + path + ' | grep -E *.stderr')
		        if self.checkDim(path + '/' + nameErr):
			    exists = 2
        except ValueError:
	    exists = 0

	return exists

    def scanStatus(self, text):
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
        if self.findInside(text, 'Done'):
            codeStatus = 0
        elif self.findInside(text, 'Scheduled'):
            codeStatus = 1
	elif self.findInside(text, 'Waiting'):
            codeStatus = 2
	elif self.findInside(text, 'Ready'):
            codeStatus = 3
	elif self.findInside(text, 'Aborted'):
            codeStatus = 4
	elif self.findInside(text, 'Killed'):
            codeStatus = 5
	elif self.findInside(text, 'Running'):
            codeStatus = 6

        return codeStatus
                                                                                                                                                                                                                                                                                                                                                                                                                                                                     SessionManager.py                                                                                   0100644 0001052 0002567 00000001032 10415424377 013711  0                                                                                                    ustar   cinquilli                       cms                                                                                                                                                                                                                    import time, os, string

class SessionManager:

    currTime = 'default'

    def createDir (self, dirName):

        os.mkdir(dirName)

	return

    def pathName(self):

	self.currTime = time.strftime( '%y%m%d_%H%M%S', time.localtime() )
        currDir = string.join( os.getcwd() + '/' + 'crab_datalog_' + self.currTime, "" )
        self.createDir( currDir )

        return currDir

    def pathRoboName(self, cwd):

	currDir = string.join( cwd + '/' + 'robo_datalog_' +self.currTime, "" )
	self.createDir( currDir )

	return currDir
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      