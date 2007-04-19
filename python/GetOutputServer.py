from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import Statistic
import time
from ProgressBar import ProgressBar
from TerminalController import TerminalController

class GetOutputServer(Actor):
 
    def __init__(self, cfg_params,):
        self.cfg_params = cfg_params
        return
    
    def run(self):
        """
        The main method of the class: retrieve the output from server
        """
        common.logger.debug(5, "GetOutput server::run() called")

        start = time.time()
        server_name = self.cfg_params['CRAB.server_name'] # gsiftp://pcpg01.cern.ch/data/SEDir/

        common.scheduler.checkProxy()

        common.taskDB.load()
        WorkDirName =os.path.basename(os.path.split(common.work_space.topDir())[0])
        projectUniqName = 'crab_'+str(WorkDirName)+'_'+common.taskDB.dict("TasKUUID")     
        #Need to add a check on the treashold level 
        # and on the task readness  TODO  
        try:
            ### retrieving poject from the server
            common.logger.message("Retrieving the poject from the server...\n")
            cmd = 'lcg-cp --vo cms --verbose gsiftp://' + str(server_name) + str(projectUniqName)+'/res/done.tgz file://'+common.work_space.resDir()+'done.tgz'
            copyOut = os.system(cmd +' >& /dev/null')
        except:
            msg = ("Output not yet available")
            raise CrabException(msg)

        zipOut = "done.tgz"
        if os.path.exists( common.work_space.resDir() + zipOut ):
            cwd = os.getcwd()
            os.chdir(common.work_space.resDir())
            common.logger.debug( 5, 'tar -zxvf ' + zipOut )
  	    cmd = 'tar -zxvf ' + zipOut
	    cmd_out = runCommand(cmd)
	    os.chdir(cwd)
            common.logger.debug( 5, 'rm -f '+common.work_space.resDir()+zipOut )
	    cmd = 'rm -f '+common.work_space.resDir()+zipOut
	    cmd_out = runCommand(cmd)

	    msg='Results of project '+str(WorkDirName)+' succesfuly retrieved from the server \n'      
	    msg+='and copied in '+common.work_space.resDir()+' \n'      
	    common.logger.message(msg)
        else:
            common.logger.message(" Output is not yet ready untill job is not finished (check it with the [status] option).\n")

        return

