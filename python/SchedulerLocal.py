from Scheduler import Scheduler
from crab_exceptions import *
from crab_logger import Logger
import common

import os,string

# Base class for all local scheduler

class SchedulerLocal(Scheduler) :

    def configure(self, cfg_params):
        Scheduler.configure(self,cfg_params)

        self.jobtypeName = cfg_params['CRAB.jobtype']

        name=string.upper(self.name())
        self.queue = cfg_params.get(name+'.queue',None)

        self.res = cfg_params.get(name+'.resource',None)

        if (cfg_params.has_key(self.name()+'.env_id')): self.environment_unique_identifier = cfg_params[self.name()+'.env_id']

        self._taskId = common.taskDB.dict('taskId')

        self.return_data = int(cfg_params.get('USER.return_data',0))

        self.copy_data = int(cfg_params.get("USER.copy_data",0))
        if self.copy_data == 1:
            self._copyCommand = cfg_params.get('USER.copyCommand','rfcp')
            self.SE_path= cfg_params.get('USER.storage_path',None)
            self.SE_path+='/'
            if not self.SE_path:
                if os.environ.has_key('CASTOR_HOME'):
                    self.SE_path=os.environ['CASTOR_HOME']
                else:
                    msg='No USER.storage_path has been provided: cannot copy_output'
                    raise CrabException(msg)
                pass
            pass

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
        common.taskDB.setDict('localSite',localDomainName)
        ## is this ok?
        cfg_params['EDG.se_white_list']=localDomainName
        common.logger.message("Your domain name is "+str(localDomainName)+": only local dataset will be considered")

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

        txt = '# '+self.name()+' specific stuff\n'
        txt += '# strip arguments\n'
        txt += 'echo "strip arguments"\n'
        txt += 'args=("$@")\n'
        txt += 'nargs=$#\n'
        txt += 'shift $nargs\n'
        txt += "# job number (first parameter for job wrapper)\n"
        txt += "NJob=${args[0]}\n"

        txt += 'SyncGridJobId=`echo '+self.environment_unique_identifier+'`\n'
        txt += 'MonitorJobID=`echo ${NJob}_${SyncGridJobId}`\n'
        txt += 'MonitorID=`echo ' + self._taskId + '`\n'

        txt += 'echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "SyncGridJobId=`echo $SyncGridJobId`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += 'echo "SyncCE='+self.name()+'.`hostname -d`" | tee -a $RUNTIME_AREA/$repo \n'

        txt += 'middleware='+self.name()+' \n'

        txt += 'dumpStatus $RUNTIME_AREA/$repo \n'

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

        txt += 'if [ $output_exit_status -eq 60302 ]; then\n'
        txt += '    echo "--> No output file to copy to $SE"\n'
        txt += '    copy_exit_status=$output_exit_status\n'
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

    def createXMLSchScript(self, nj, argsList):

        """
        Create a XML-file for BOSS4.
        """

        """
        INDY
        [begin] FIX-ME:
        I would pass jobType instead of job
        """
        index = nj - 1
        job = common.job_list[index]
        jbt = job.type()
        inp_sandbox = jbt.inputSandbox(index)
        #out_sandbox = jbt.outputSandbox(index)
        """
        [end] FIX-ME
        """


        title = '<?xml version="1.0" encoding="UTF-8" standalone="no"?>\n'
        jt_string = ''

        xml_fname = str(self.jobtypeName)+'.xml'
        xml = open(common.work_space.shareDir()+'/'+xml_fname, 'a')

        #TaskName
        dir = string.split(common.work_space.topDir(), '/')
        taskName = dir[len(dir)-2]

        to_write = ''

        req=' '
        req = req + jbt.getRequirements()

        #TaskName
        dir = string.split(common.work_space.topDir(), '/')
        taskName = dir[len(dir)-2]

        xml.write(str(title))

        #First check the X509_USER_PROXY. In not there use the default
        xml.write('<task name="' +str(taskName)+ '" sub_path="' +common.work_space.pathForTgz() + 'share/.boss_cache"' + '>\n')

        xml.write(jt_string)

        if (to_write != ''):
            xml.write('<extraTags\n')
            xml.write(to_write)
            xml.write('/>\n')
            pass

        xml.write('<iterator>\n')
        xml.write('\t<iteratorRule name="ITR1">\n')
        xml.write('\t\t<ruleElement> 1:'+ str(nj) + ' </ruleElement>\n')
        xml.write('\t</iteratorRule>\n')
        xml.write('\t<iteratorRule name="ITR2">\n')
        for arg in argsList:
            xml.write('\t\t<ruleElement> <![CDATA[\n'+ arg + '\n\t\t]]> </ruleElement>\n')
            pass
        xml.write('\t</iteratorRule>\n')
        #print jobList
        xml.write('\t<iteratorRule name="ITR3">\n')
        xml.write('\t\t<ruleElement> 1:'+ str(nj) + ':1:6 </ruleElement>\n')
        xml.write('\t</iteratorRule>\n')

        xml.write('<chain name="' +str(taskName)+'__ITR1_" scheduler="'+str(self.name())+'">\n')
       # xml.write('<chain scheduler="'+str(self.schedulerName)+'">\n')
        xml.write(jt_string)

        #executable

        script = job.scriptFilename()
        xml.write('<program>\n')
        xml.write('<exec> ' + os.path.basename(script) +' </exec>\n')
        xml.write(jt_string)

        xml.write('<args> <![CDATA[\n _ITR2_ \n]]> </args>\n')
        xml.write('<program_types> crabjob </program_types>\n')
        inp_box = common.work_space.pathForTgz() + 'job/' + jbt.scriptName + ','

        if inp_sandbox != None:
            for fl in inp_sandbox:
                inp_box = inp_box + '' + fl + ','
                pass
            pass

        if inp_box[-1] == ',' : inp_box = inp_box[:-1]
        inp_box = '<infiles> <![CDATA[\n' + inp_box + '\n]]> </infiles>\n'
        xml.write(inp_box)

        base = jbt.name()
        stdout = base + '__ITR3_.stdout'
        stderr = base + '__ITR3_.stderr'

        xml.write('<stderr> ' + stderr + '</stderr>\n')
        xml.write('<stdout> ' + stdout + '</stdout>\n')


        out_box = stdout + ',' + \
                  stderr + ',.BrokerInfo,'

        # Stuff to be returned _always_ via sandbox
        for fl in jbt.output_file_sandbox:
            out_box = out_box + '' + jbt.numberFile_(fl, '_ITR1_') + ','
            pass
        pass

        # via sandbox iif required return_data
        if int(self.return_data) == 1:
            for fl in jbt.output_file:
                out_box = out_box + '' + jbt.numberFile_(fl, '_ITR1_') + ','
                pass
            pass

        if out_box[-1] == ',' : out_box = out_box[:-1]
        out_box = '<outfiles> <![CDATA[\n' + out_box + '\n]]></outfiles>\n'
        xml.write(out_box)

        xml.write('<BossAttr> crabjob.INTERNAL_ID=_ITR1_ </BossAttr>\n')

        xml.write('</program>\n')
        xml.write('</chain>\n')

        xml.write('</iterator>\n')
        xml.write('</task>\n')

        xml.close()


        return
