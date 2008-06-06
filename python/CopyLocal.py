from Actor import *
from crab_util import *
from crab_exceptions import *
from crab_logger import Logger
from Status import Status
import common
import string

class CopyLocal(Actor):
    def __init__(self, cfg_params, nj_list):
        """
        constructor
        """
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        self.status = Status(cfg_params)
        if cfg_params.get('USER.copy_data',0) == 0 :
            raise CrabException("Cannot copy output locally if it has not been stored to SE via USER.copy_data=1")
        self.SE = cfg_params.get('USER.storage_element',None)
        self.SE_port = str(cfg_params.get('USER.storage_element_port',8443))
        self.SE_PATH = cfg_params.get('USER.storage_path',None)
        self.srm_ver = cfg_params.get('USER.srm_version',0)
        if not self.SE or not self.SE_PATH:
            msg = "Error. The [USER] section does not have 'storage_element'"
            msg = msg + " and/or 'storage_path' entries, necessary to copy the output\n"
            common.logger.message(msg)
            raise CrabException(msg)

# other output files to be returned via sandbox or copied to SE
        self.output_file = []
        tmp = cfg_params.get('CMSSW.output_file',None)
        if tmp :
            self.output_file = [x.strip() for x in tmp.split(',')]

    def run(self):
        """
        copy the output to local
        """
        # loop over jobs
        task=common._db.getTask(self.nj_list)
        allMatch={}
        import time
        if not common.logger.debugLevel() :
            try:
                from ProgressBar import ProgressBar
                from TerminalController import TerminalController
                term = TerminalController()
                pbar = ProgressBar(term, 'Copying locally output of '+str(len(self.nj_list))+' jobs')
            except: pbar = None
        ii=0
        totalFiles=len(self.nj_list)*len(self.output_file)
        start = time.time()
        for job in task.jobs:
            id_job = job['jobId'] 
            for of in self.output_file:
                file=numberFile(of,id_job)
                cmd="lcg-cp srm://"+self.SE+":"+self.SE_port+"//"+string.split(self.SE_PATH,"=")[-1]+"/"+file+" file:`pwd`/"+file
                common.logger.debug(3,cmd)
                runCommand(cmd)
                if not common.logger.debugLevel() and pbar: pbar.update(float(ii+1)/float(totalFiles),'please wait')
                ii+=1
            pass
        pass
        stop = time.time()
        common.logger.debug(1, "CopyLocal Time: "+str(stop - start))
        common.logger.write("CopyLocal time :"+str(stop - start))


        # loop over output file
