from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import time
from ProgressBar import ProgressBar
from TerminalController import TerminalController

class PostMortemServer(Actor):
 
    def __init__(self, cfg_params,):
        self.cfg_params = cfg_params
        try:  
            self.server_name = self.cfg_params['CRAB.server_name'] # gsiftp://pcpg01.cern.ch/data/SEDir/
            if not self.server_name.endswith("/"):
                self.server_name = self.server_name + "/"
        except KeyError:
            msg = 'No server selected ...' 
            msg = msg + 'Please specify a server in the crab cfg file' 
            raise CrabException(msg) 
        return
    
    def run(self):
        """
        The main method of the class: retrieve the post mortem output from server
        """
        common.logger.debug(5, "PostMortem server::run() called")

        start = time.time()

        common.scheduler.checkProxy()

        common.taskDB.load()
        WorkDirName =os.path.basename(os.path.split(common.work_space.topDir())[0])
        projectUniqName = 'crab_'+str(WorkDirName)+'_'+common.taskDB.dict("TasKUUID")     
        #Need to add a check on the treashold level 
        # and on the task readness  TODO  
        try:
            ### retrieving poject from the server
            common.logger.message("Retrieving the poject from the server...\n")

            copyHere = common.work_space.jobDir() # MATT
             
            cmd = 'lcg-cp --vo cms --verbose gsiftp://' + str(self.server_name) + str(projectUniqName)+'/res/failed.tgz file://'+copyHere+'failed.tgz'# MATT
            common.logger.debug(5, cmd)
            copyOut = os.system(cmd +' >& /dev/null')
        except:
            msg = ("postMortem output not yet available")
            raise CrabException(msg)

        zipOut = "failed.tgz"
        if os.path.exists( copyHere + zipOut ): # MATT
            cwd = os.getcwd()
            os.chdir( copyHere )# MATT
            common.logger.debug( 5, 'tar -zxvf ' + zipOut )
  	    cmd = 'tar -zxvf ' + zipOut 
            cmd += '; mv .tmpFailed/* .; rm -drf .tmpDone/'
	    cmd_out = runCommand(cmd)
	    os.chdir(cwd)
            common.logger.debug( 5, 'rm -f '+copyHere+zipOut )# MATT 
	    cmd = 'rm -f '+copyHere+zipOut# MATT
	    cmd_out = runCommand(cmd)

	    msg='Logging info for project '+str(WorkDirName)+': \n'      
	    msg+='written to '+copyHere+' \n'      # MATT
	    common.logger.message(msg)
        else:
            common.logger.message("Logging info is not yet ready....\n")

        return

