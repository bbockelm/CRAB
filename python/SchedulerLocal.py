from Scheduler import Scheduler
from crab_exceptions import *
from crab_logger import Logger
import common

import os,string

# Base class for all local scheduler

class SchedulerLocal(Scheduler) :

    def configure(self, cfg_params):

        self.jobtypeName = cfg_params['CRAB.jobtype']

        name=string.upper(self.name())
        self.queue = cfg_params.get(name+'.queue',None)

        self.res = cfg_params.get(name+'.resource',None)

        if (cfg_params.has_key(self.name()+'.env_id')): self.environment_unique_identifier = cfg_params[self.name()+'.env_id']

        self._taskId=str("_".join(common._db.queryTask('name').split('_')[:-1]))

        self.return_data = int(cfg_params.get('USER.return_data',0))

        self.copy_data = int(cfg_params.get("USER.copy_data",0))
        if self.copy_data == 1:
            self._copyCommand = cfg_params.get('USER.copyCommand','rfcp')
            self.SE_path= cfg_params.get('USER.storage_path',None)
            if not self.SE_path:
                if os.environ.has_key('CASTOR_HOME'):
                    self.SE_path=os.environ['CASTOR_HOME']
                else:
                    msg='No USER.storage_path has been provided: cannot copy_output'
                    raise CrabException(msg)
                pass
            pass
            self.SE_path+='/'

        if ( self.return_data == 0 and self.copy_data == 0 ):
           msg = 'Error: return_data = 0 and copy_data = 0 ==> your exe output will be lost\n'
           msg = msg + 'Please modify return_data and copy_data value in your crab.cfg file\n'
           raise CrabException(msg)

        if ( self.return_data == 1 and self.copy_data == 1 ):
           msg = 'Error: return_data and copy_data cannot be set both to 1\n'
           msg = msg + 'Please modify return_data or copy_data value in your crab.cfg file\n'
           raise CrabException(msg)

        ## Get local domain name
        import socket
        tmp=socket.gethostname()
        dot=string.find(tmp,'.')
        if (dot==-1):
            msg='Unkown domain name. Cannot use local scheduler'
            raise CrabException(msg)
        localDomainName = string.split(tmp,'.',1)[-1]
        #common.taskDB.setDict('localSite',localDomainName)
        ## is this ok?
        if not cfg_params.has_key('EDG.se_white_list'):
            cfg_params['EDG.se_white_list']=localDomainName
            common.logger.message("Your domain name is "+str(localDomainName)+": only local dataset will be considered")
        else:
            common.logger.message("Your se_white_list is set to "+str(cfg_params['EDG.se_white_list'])+": only local dataset will be considered")
        

        

        Scheduler.configure(self,cfg_params)
        return

    def userName(self):
        """ return the user name """
        import pwd,getpass
        tmp=pwd.getpwnam(getpass.getuser())[4]
        return "/CN="+tmp.strip()

    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        if not self.environment_unique_identifier:
            raise CrabException('environment_unique_identifier not set')

        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()
        # start with wrapper timing 
        txt  = 'export TIME_WRAP=`date +%s` \n'
        txt += 'export TIME_STAGEOUT=NULL \n\n'

        txt = '# '+self.name()+' specific stuff\n'
        txt += '# strip arguments\n'
        txt += 'echo "strip arguments"\n'
        txt += 'args=("$@")\n'
        txt += 'nargs=$#\n'
        txt += 'shift $nargs\n'
        txt += "# job number (first parameter for job wrapper)\n"
        txt += "NJob=${args[0]}; export NJob\n"

        txt += "out_files=out_files_${NJob}; export out_files\n"
        txt += "echo $out_files\n"
        txt += jbt.outList()

        txt += 'SyncGridJobId=`echo '+self.environment_unique_identifier+'`\n'
        txt += 'MonitorJobID=`echo ${NJob}_${SyncGridJobId}`\n'
        txt += 'MonitorID=`echo ' + self._taskId + '`\n'

        txt += 'echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "SyncGridJobId=`echo $SyncGridJobId`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += 'echo "SyncCE='+self.name()+'.`hostname -d`" | tee -a $RUNTIME_AREA/$repo \n'

        txt += 'middleware='+self.name()+' \n'

        txt += 'dumpStatus $RUNTIME_AREA/$repo \n'

        txt += 'InputSandBox=${args[3]}\n'

        txt += '\n\n'

        return txt

    def wsCopyOutput(self):
        """
        Write a CopyResults part of a job script, e.g.
        to copy produced output into a storage element.
        """
        txt = '\n'
        if not self.copy_data: return txt


        txt += '#\n'
        txt += '# COPY OUTPUT FILE TO '+self.SE_path
        txt += '#\n\n'

        txt += 'export SE_PATH='+self.SE_path+'\n'

        txt += 'export CP_CMD='+self._copyCommand+'\n'

        txt += 'echo ">>> Copy output files from WN = `hostname` to PATH = $SE_PATH using $CP_CMD :"\n'

        txt += 'if [ $job_exit_code -eq 60302 ]; then\n'
        txt += '    echo "--> No output file to copy to $SE"\n'
        txt += '    copy_exit_status=$job_exit_code\n'
        txt += '    echo "COPY_EXIT_STATUS = $copy_exit_status"\n'
        txt += 'else\n'
        txt += '    for out_file in $file_list ; do\n'
        txt += '        echo "Trying to copy output file to $SE_PATH"\n'
        txt += '        $CP_CMD $SOFTWARE_DIR/$out_file ${SE_PATH}/$out_file\n'
        txt += '        copy_exit_status=$?\n'
        txt += '        echo "COPY_EXIT_STATUS = $copy_exit_status"\n'
        txt += '        echo "STAGE_OUT = $copy_exit_status"\n'
        txt += '        if [ $copy_exit_status -ne 0 ]; then\n'
        txt += '            echo "Problem copying $out_file to $SE $SE_PATH"\n'
        txt += '            echo "StageOutExitStatus = $copy_exit_status " | tee -a $RUNTIME_AREA/$repo\n'
        #txt += '            copy_exit_status=60307\n'
        txt += '        else\n'
        txt += '            echo "StageOutSE = $SE" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '            echo "StageOutCatalog = " | tee -a $RUNTIME_AREA/$repo\n'
        txt += '            echo "output copied into $SE/$SE_PATH directory"\n'
        txt += '            echo "StageOutExitStatus = 0" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '        fi\n'
        txt += '    done\n'
        txt += 'fi\n'
        txt += 'exit_status=$copy_exit_status\n'

        return txt
