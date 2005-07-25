import common
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *

class codePreparator:
    def __init__(self, cfg_params):
        try:
            self.build = cfg_params['USER.build_mode']
        except KeyError:
            self.build = 'tgz'
            pass
        
        self.tgz_name = 'default.tgz'
        self.tgzNameWithPath = None
        pass
        

    def prepareTgz_(self, executable):
 
        self.tgzNameWithPath = common.work_space.shareDir()+self.tgz_name

        if os.path.exists(self.tgzNameWithPath):
            scramArea = os.environ["LOCALRT"]
            n = len(string.split(scramArea, '/'))
            swVersion = self.findSwVersion_(scramArea)
            common.analisys_common_info['sw_version'] = swVersion
            return self.tgzNameWithPath

        # Prepare a tar gzipped file with user binaries.

        # First of all declare the user Scram area
        try:
            scramArea = os.environ["LOCALRT"]
            #print scramArea,'\n'
            n = len(string.split(scramArea, '/'))
            swVersion = self.findSwVersion_(scramArea)
            #print swVersion,'\n'
            common.analisys_common_info['sw_version'] = swVersion
            swArea = os.environ["CMS_PATH"]
            #print swArea,'\n'
  #          swArea = "/opt/exp_software/cms"
            if scramArea.find(swArea) == -1:
                # Calculate length of scram dir to extract software version
                crabArea = os.getcwd()
                os.chdir(scramArea)

## SL use SCRAM_ARCH instead of Linux__2.4
                if os.environ.has_key('SCRAM_ARCH'):
                    scramArch = os.environ["SCRAM_ARCH"]
                else:
                    scramArch = 'Linux__2.4'
                #print scramArch,'\n'
        #      libDir = 'lib/'+scramArch+'/'
                libDir = ''
                binDir = 'bin/'+scramArch+'/'


                exe = binDir + executable
### here assume that executable is in user directory
                #print '##'+exe+'##\n'
                if os.path.isfile(exe):
                    cmd = 'ldd ' + exe + ' | grep ' + scramArea
                    myCmd = os.popen(cmd)
                    tarcmd = 'tar zcvf ' + self.tgzNameWithPath + ' ' 
                    for line in (myCmd):
                        line = string.split(line, '=>')
                        lib = 'lib/' + scramArch + '/' + string.strip(line[0]) + ' '
                        tarcmd = tarcmd + lib
                    status = myCmd.close()
                    tarcmd = tarcmd + ' ' + exe
                    cout = runCommand(tarcmd)
                else:
### If not, check if it's standard

                    cmd = 'which ' + executable
                    myCmd = os.popen(cmd)
                    out=myCmd.readline()
                    status = myCmd.close()
                    #print  int(out.find("no "+executable+" in"))!=-1
                    if int(out.find("no "+executable+" in"))!=-1:
                        raise CrabException('User executable '+executable+' not found')
                    exe = string.strip(out[0:-1])
                    #print '##'+exe+'##\n'
                    if not os.path.isfile(exe):
                        raise CrabException('User executable not found')
                    #os.system('touch '+common.tgzNameWithPath)
                os.chdir(crabArea)
            else:
                cmd = 'which ' + executable
                cout = runCommand(cmd)
                if not cout:
                    raise CrabException('Executable not found')
        except KeyError:
            raise CrabException('Undefined SCRAM area')
            pass
        return self.tgzNameWithPath
                
    def findSwVersion_(self, path):
        """ Find Sw version using proper scram env """
        scramEnvFile = path+"/.SCRAM/Environment"
        reVer = re.compile(r'SCRAM_PROJECTVERSION')
                
        try:    
            for line in open(scramEnvFile, "r").readlines():
                if reVer.match(line):
                    ver = string.strip(string.split(line, '=')[1])
                    return ver
                pass
            pass
        except IOError, e:
            msg = 'Cannot find SCRAM project version:\n'
            msg += str(e)
            raise CrabException(msg)
        return
