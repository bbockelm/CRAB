import os, time, logging
from JobsManager import *
from LockerFile import *
from os import path

class Outputter:

    # Times: string, float, float
    startTime = ""
    tStart = 0
    tFinish = 0
    # global path of robot's dir
    roboLogDir = 'default'
    # file name for the crab's output bypassed by the robot
    nameCrabLog = 'crab.robot.out'
    # file name for the robot's output
    nameRoboLog = 'robo.out'
    # file name for the "Juice Table"
    nameTableLog = 'juice.table.out'
    # file name for the bad table!
    nameBadTableLog = 'bad.table.out'
    # Locker
    locks = LockerFile()
    
    def __init__( self, wd ):

        self.startTime = self.getTime()
        self.tStart = time.time()

        str = wd + '/Robolog'
        #print( str )
        os.mkdir( str )
        self.roboLogDir = str
        
        self._createFile_( self.nameCrabLog, 1 )
        self._createFile_( self.nameRoboLog, 1 )
        self._createFile_( self.nameTableLog, 0 )

        return

    def calcTime( self ):
        t = 60

        self.tFinish = time.time()
        tot = self.tFinish-self.tStart
        sec = int ( tot % t )
        min = int ( tot / t )
        hou = int ( min / t )
        min = int ( min % t )

        return [ str(hou) ,str(min), str(sec) ]

    def getTime(self):
        return time.strftime( '---> time: %H:%M:%S- %d/%m/%y <---', time.localtime() )

    def _createFile_( self, name, flagWrite ):
        
        dir = self.roboLogDir + '/' + name

        if not os.path.exists( dir ):
            file = open( dir, 'w' )
            self.locks.lock_F( file, 0 )
            if flagWrite:
                file.write(self.getTime())
                file.write(" |-> LOG FILE STARTS... \n\n")
            self.locks.unlock_F( file )
            file.close()

        return

    def printStep( self, str ):
        """
        Prints the output "str"
        """
#        print('')
#        print ' ****  ', str, '  ****'
        logging.info(self.roboLogDir.rsplit('/',3)[-2]+': '+str) # Sk.
        self.writeOut( '', str, 1 )

        return


    def writeOut( self, cmd, text, opt ):
        """
        Writes on the file corresponding to the option "opt"
        """

        if opt == 0:
            fName = path.join(self.roboLogDir, self.nameCrabLog)
        elif opt == 1:
            fName = path.join(self.roboLogDir, self.nameRoboLog)

        fOut = open( fName, "a" )
        self.locks.lock_F( fOut, 0 )
        if opt == 0:
            fOut.write('\n|-> COMMAND:  ' + cmd + '\n\n|-> OUTPUT:\n\n')
        else:
            fOut.write( self.getTime() )
        fOut.write( " " + str(text) )
        fOut.write('\n-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-\n')
        self.locks.unlock_F( fOut )
        fOut.close()

        return

    def printTable(self, jobs):  ## M.: added the coloumn
        fOut = open( self.roboLogDir + '/' + self.nameTableLog, "w" )
        self.locks.lock_F( fOut, 0 )
        fOut.write("     JOBS' JUICE TABLE\n")
        fOut.write("\n\n")
        fOut.write(" - robot started at: " + self.startTime + "\n")
        fOut.write(" - table created at: " + self.getTime() + "\n")
        howLong = self.calcTime()
        fOut.write(" - table created after "\
                    + howLong[0] + " hours "\
                    + howLong[1] + " mins "\
                    + howLong[2] + " secs "\
                    "of TestSuite's runnning")
        fOut.write("\n\n")
        fOut.write("    _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _\n")
        fOut.write("   |       |          |             |                |             |             |             |\n")
        fOut.write("   | JobID | Creation |  Submission |  Check Status  | Get Output  | Out File[s] | Test Status |\n")
        fOut.write("   |_ _ _ _|_ _ _ _ _ |_ _ _ _ _ _ _|_ _ _ _ _ _ _ _ |_ _ _ _ _ _ _|_ _ _ _ _ _ _|_ _ _ _ _ _ _|\n")
        i = 0
        while i < jobs.nJobs():
            listCSD = jobs.getCSD(i)
            iStr = str(i+1)
            complTemp = jobs.getCompleted(i)
            complete = self.getCompletedConvert( complTemp )
            jj = 0
            listCSDConv = [self.getStepConvert(listCSD[0]),self.getStepConvert(listCSD[1]),self.getOutputConvert(listCSD[2])]
            outFiles = self.outFilesConvert(listCSD[2])
            #if complete == "incompleted":
            #    space0 = " "
            #else:
            space0 = "  "
            if listCSDConv[0] == "Ok":
                space1 = "    "
            else:
                space1 = "  "
            if listCSDConv[1] == "Ok":
                space2 = "     "
            else:
                space2 = "   "
            if listCSDConv[2] == "Ok":
                space3 = "     "
            elif listCSDConv[2] == "":
                space3 = "      "
            else:
                space3 = "   "
            if outFiles == "Ok ":
                space4 = "     "
            else:
                space4 = "   "            
            if len(iStr) == 1:
                spaceId1 = "   "
                spaceId2 = "   "
            elif len(iStr):
                spaceId1 = "  "
                spaceId2 = "   "
            #print listCSDConv[0], jobs.getToSubmit(i), complTemp,
            #print(listCSDConv[0] != "Ok" and jobs.getToSubmit(i) == 0) or complTemp == -1
            #if listCSDConv[0] != "Ok" and jobs.getToSubmit(i) != 0:
            #    pass
            #if (listCSDConv[0] != "Ok" and jobs.getToSubmit(i) != 0) or complTemp == -1:
            if complTemp == -1 and listCSDConv[1] == "Ok":
                space5 = "     "
                checkStatus = "Failed"
                fOut.write("   |       |          |             |" + space5 + checkStatus + space5 + "|             |             |             |\n")
                stringa2 = "   |" + spaceId1  + iStr + spaceId2 + "|" \
                           + space1 + listCSDConv[0] + space1 + "|" \
                           + space2 + listCSDConv[1] + space2 + " |" \
                           + "** killed for **|" \
                           + space3 + listCSDConv[2] + space3 + " |" \
                           + space4 + outFiles + space4 + "|" \
                           + space0 + complete + space0 + "|\n"
                fOut.write(stringa2)
                fOut.write("   |       |          |             |"+"extratime  exec."+"|             |             |             |\n")
                fOut.write("   |_ _ _ _|_ _ _ _ _ |_ _ _ _ _ _ _|_ _ _ _ _ _ _ _ |_ _ _ _ _ _ _|_ _ _ _ _ _ _|_ _ _ _ _ _ _|\n")
            elif complete == "Incompleted":
                checkStatus = "Failed"
                space5 = "     "
                fOut.write("   |       |          |             |                |             |             |             |\n")
                fOut.write("   |" + spaceId1  + iStr + spaceId2 + "|"\
                           + space1 + listCSDConv[0] + space1 + "|"\
                           + space2 + listCSDConv[1] + space2 + " |"\
                           + space5 + checkStatus + space5 + "|"\
                           + space3 + listCSDConv[2] + space3 + " |"\
                           + space4 + outFiles + space4 + "|"\
                           + space0 + complete + space0 + "|\n")
                fOut.write("   |_ _ _ _|_ _ _ _ _ |_ _ _ _ _ _ _|_ _ _ _ _ _ _ _ |_ _ _ _ _ _ _|_ _ _ _ _ _ _|_ _ _ _ _ _ _|\n")
            else:
                if listCSDConv[1] == "Ok":
                    checkStatus = "Ok"
                else:
                    checkStatus = "  "
                space5 = "       "
                fOut.write("   |       |          |             |                |             |             |             |\n")
                fOut.write("   |" + spaceId1  + iStr + spaceId2 + "|"\
                           + space1 + listCSDConv[0] + space1 + "|"\
                           + space2 + listCSDConv[1] + space2 + " |"\
                           + space5 + checkStatus + space5 + "|"\
                           + space3 + listCSDConv[2] + space3 + " |"\
                           + space4 + outFiles + space4 + "|"\
                           + space0 + complete + space0 + "|\n")
                fOut.write("   |_ _ _ _|_ _ _ _ _ |_ _ _ _ _ _ _|_ _ _ _ _ _ _ _ |_ _ _ _ _ _ _|_ _ _ _ _ _ _|_ _ _ _ _ _ _|\n")
            i = i + 1
        self.locks.unlock_F( fOut )
        fOut.close()

    def printBadTable(self, jobs):  ## M.: added the coloumn
        fOut = open( self.roboLogDir + '/' + self.nameBadTableLog, "w" )
        fOut.write ('# JobID\tCreated\tSubmitted\tDone\tStatus\tOutFiles\tCompleted\n')
        for i in range(jobs.nJobs()):
            listCSD = jobs.getCSD(i) # Created, submitted, done
            listCSDConv = [self.getStepConvert(listCSD[0]), self.getStepConvert(listCSD[1]), self.getOutputConvert(listCSD[2])]
            complTemp = jobs.getCompleted(i)
            complete = self.getCompletedConvert( complTemp )
            if complTemp == -1 and listCSDConv[1] == "Ok":
                checkStatus = "Failed"
            elif complete == "Incompleted":
                checkStatus = "Failed"
            elif listCSDConv[1] == "Ok":
                checkStatus = "Ok"
            else:
                checkStatus = "  "
            outFiles = self.outFilesConvert(listCSD[2])
            fOut.write(str(i+1)+'\t'+listCSDConv[0]+'\t'+listCSDConv[1]+'\t'+checkStatus+'\t'+outFiles+'\t'+complete+'\n')


    def printPathTable(self):  ##M.: prints the and "log message"
        self.printStep("TestSuite: Log file is in:")
        self.printStep(self.roboLogDir + '/' + self.nameTableLog) 

    def getCompletedConvert(self, value):
        ##print "complete on outputter.py is",value
        if value == 1 or value == 2 or value == -1:
            return "finished "
        #elif value == -1:
         #   return "incompleted"
        return "on going "

    def getStepConvert(self, value):
        if value == 1 or value == 2:
            return "Ok"
        return "Failed"

    def getOutputConvert(self, value):
        if value == -2:
            return "Failed"
        elif value == 0:
            return ""
        return "Ok"

    def outFilesConvert(self, value):
        if value == 1:
            return "Ok "
        else:
            return "Missing"

    def debugger(self, stringa, value):
        if value > 0:
             self.printStep(stringa)
