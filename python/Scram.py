import common
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *

class Scram:
    def __init__(self, cfg_params):
        self.build = 'tgz'
        self.tgz_name = 'default.tgz'
        self.tgzNameWithPath = None

        self.scramVersion = 0
        scramArea = ''

        if os.environ.has_key("SCRAMRT_LOCALRT"):
            # try scram v1
            self.scramArea = os.environ["SCRAMRT_LOCALRT"]
            self.scramVersion = 1
        elif os.environ.has_key("LOCALRT"):
            # try scram v0
            self.scramArea = os.environ["LOCALRT"]
            self.scramVersion = 0
        else:
            msg = 'Did you do eval `scram(v1) runtime ...` from your ORCA area ?\n'
            raise CrabException(msg)
        common.logger.debug(5, "Scram::Scram() version is "+str(self.scramVersion))
        common.logger.debug(6, "Scram::Scram() area is "+self.scramArea)
        pass

    def commandName(self):
        """ return scram or scramv1 """
        if self.scramVersion == 1: return 'scramv1'
        else: return 'scram'

    def getSWArea_(self):
        """
        Get from SCRAM the local working area location
        """
        return string.strip(self.scramArea)

    def getSWVersion(self):
        """
        Get the version of the sw
        """

        ver = ''
        try:
            ver = os.environ["SCRAMRT_SCRAM_PROJECTVERSION"]
        except KeyError, e:
            msg = 'SCRAMRT_SCRAM_PROJECTVERSION value not found\n'
            common.logger.debug(5,msg)
            ver = string.split(self.scramArea,'/')[-1]
            if ver == '': 
                 msg = 'Cannot find sw version:\n'
                 raise CrabException(msg)
        return string.strip(ver)
        
    def getTarBall(self, exe):
        """
        Return the TarBall with lib and exe
        """
        
        # if it exist, just return it
        self.tgzNameWithPath = common.work_space.shareDir()+self.tgz_name
        if os.path.exists(self.tgzNameWithPath):
            return self.tgzNameWithPath

        # Prepare a tar gzipped file with user binaries.
        self.prepareTgz_(exe)

        return string.strip(self.tgzNameWithPath)


    def getReleaseTop_(self):
       """ get release top """

       result = ''
       envFileName = self.scramArea+"/.SCRAM/Environment"
       try:
           envFile = open(envFileName, 'r')
           for line in envFile:
               line = string.strip(line)
               (k, v) = string.split(line, '=')
               if k == 'RELEASETOP':
                   result=v
                   break
               pass
           pass
       except IOError:
           msg = 'Cannot open scram environment file '+envFileName
           raise CrabException(msg)
           pass
       pass
       return string.strip(result)


    def prepareTgz_(self, executable):

        # First of all declare the user Scram area
        swArea = self.getSWArea_()
        #print "swArea = ", swArea
        swVersion = self.getSWVersion()
        #print "swVersion = ", swVersion
        swReleaseTop = self.getReleaseTop_()
        #print "swReleaseTop = ", swReleaseTop

        ## First find the executable
        cmd = 'which ' + executable
        try:
           exeWithPath = string.strip(runCommand(cmd))
        except AttributeError: 
            raise CrabException('User executable '+executable+' not found')

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
