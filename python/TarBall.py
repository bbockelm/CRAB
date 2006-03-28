import common
from crab_util import *
import Scram
import os, string, re

class TarBall:
    def __init__(self, exe, scram):
        self.tgz_name = 'default.tgz'
        self.executable = exe
        self.scram = scram
        pass

    def prepareTarBall(self):
        """
        Prepare tar ball with packed user code
        """
        
        # if it exist, just return it
        self.tgzNameWithPath = common.work_space.shareDir()+self.tgz_name
        if os.path.exists(self.tgzNameWithPath):
            return self.tgzNameWithPath

        # Prepare a tar gzipped file with user binaries.
        self.prepareTgz_()

        return string.strip(self.tgzNameWithPath)

    def prepareTgz_(self):
        # First of all declare the user Scram area
        swArea = self.scram.getSWArea_()
        #print "swArea = ", swArea
        swVersion = self.scram.getSWVersion()
        #print "swVersion = ", swVersion
        swReleaseTop = self.scram.getReleaseTop_()
        #print "swReleaseTop = ", swReleaseTop

        ## First find the executable
        exeWithPath = self.scram.findFile_(self.executable)
        if ( not exeWithPath ): raise CrabException('User executable '+self.executable+' not found')

        ## check if working area is release top
        if swReleaseTop == '' or swArea == swReleaseTop:
            return

        filesToBeTarred = []
        ## then check if it's private or not
        if exeWithPath.find(swReleaseTop) == -1:
            # the exe is private, so we must ship
            common.logger.debug(5,"Exe "+exeWithPath+" to be tarred")
            path = swArea+'/'
            exe = string.replace(exeWithPath, path,'')
            filesToBeTarred.append(exe)
            pass
        else:
            # the exe is from release, we'll find it on WN
            pass

        ## Now get the libraries: only those in local working area
        cmd = 'ldd ' + exeWithPath + ' | grep ' + swArea
        myCmd = os.popen(cmd)
        for line in (myCmd):
            libWithFullPath = string.split(string.strip(line))[2]
            path = swArea+'/'
            lib = string.replace(libWithFullPath, path,'')
            common.logger.debug(5,"lib "+lib+" to be tarred")
            filesToBeTarred.append(lib)
        status = myCmd.close()

        ## Now check if the Data dir is present
        dataDir = 'src/Data/'
        if os.path.isdir(swArea+'/'+dataDir):
            filesToBeTarred.append(dataDir)

        ## Create the tar-ball
        if len(filesToBeTarred)>0:
            cwd = os.getcwd()
            os.chdir(swArea)
            tarcmd = 'tar zcvf ' + self.tgzNameWithPath + ' ' 
            for line in filesToBeTarred:
                tarcmd = tarcmd + line + ' '
            cout = runCommand(tarcmd)
            if not cout:
                raise CrabException('Could not create tar-ball')
            os.chdir(cwd)
        else:
            common.logger.debug(5,"No files to be to be tarred")
        
        return
