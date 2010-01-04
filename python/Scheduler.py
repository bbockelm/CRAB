
import os
import time
import string
import traceback

import common
from Boss import Boss
from crab_exceptions import *
from crab_util import *
from WMCore.SiteScreening.BlackWhiteListParser import SEBlackWhiteListParser


from ProdCommon.Storage.SEAPI.SElement import SElement, FullPath
from ProdCommon.Storage.SEAPI.SBinterface import *
from ProdCommon.Storage.SEAPI.Exceptions import *

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#

class Scheduler :

    _instance = None

    def getInstance():
        if not Scheduler._instance :
            raise CrabException('Scheduler has no instance.')
        return Scheduler._instance

    getInstance = staticmethod(getInstance)

    def __init__(self, name):
        Scheduler._instance = self
        self._name = string.lower(name)
        self._boss = Boss()
        self.protocolDict = { 'CAF'      : 'rfio' , \
                              'LSF'      : 'rfio' , \
                              'PBS'      : 'rfio' , \
                              'CONDOR_G' : 'srmv2' , \
                              'GLITE'    : 'srm-lcg' , \
                              'GLITE_SLC5'    : 'srm-lcg' , \
                              'GLIDEIN'  : 'srm-lcg' , \
                              'CONDOR'    : 'srmv2',  \
                              'SGE'      : 'srmv2', \
                              'ARC'      : 'srmv2'
                            }
        return

    def name(self):
        return self._name

    def realSchedParams(self,cfg_params):
        """
        """
        return {}

    def configure(self, cfg_params):
        self._boss.configure(cfg_params)
        self.CRAB_useServer = cfg_params.get('CRAB.use_server',0)
        self.CRAB_serverName = cfg_params.get('CRAB.server_name',None)
        seWhiteList = cfg_params.get('GRID.se_white_list',[])
        seBlackList = cfg_params.get('GRID.se_black_list',[])
        self.dontCheckMyProxy=int(cfg_params.get("GRID.dont_check_myproxy",0))
        self.EDG_requirements = cfg_params.get('GRID.requirements',None)
        self.EDG_addJdlParam = cfg_params.get('GRID.additional_jdl_parameters',None)
        if (self.EDG_addJdlParam):
            self.EDG_addJdlParam = string.split(self.EDG_addJdlParam,';')

        self.pset = cfg_params.get('CMSSW.pset',None)
        self.blackWhiteListParser = SEBlackWhiteListParser(seWhiteList, seBlackList, common.logger())

        self.return_data = int(cfg_params.get('USER.return_data',0))
        self.copy_data = int(cfg_params.get('USER.copy_data',0))
        self.publish_data = cfg_params.get("USER.publish_data",0)
        self.local_stage = int(cfg_params.get('USER.local_stage_out',0))
        self.check_RemoteDir =  int(cfg_params.get('USER.check_user_remote_dir',1))

        if int(self.copy_data) == 1:
            self.SE = cfg_params.get('USER.storage_element',None)
            if not self.SE:
                msg = "Error. The [USER] section does not have 'storage_element'"
                common.logger.info(msg)
                raise CrabException(msg)

        if ( int(self.return_data) == 0 and int(self.copy_data) == 0 ):
            msg = 'Error: return_data = 0 and copy_data = 0 ==> your exe output will be lost\n'
            msg = msg + 'Please modify return_data and copy_data value in your crab.cfg file\n'
            raise CrabException(msg)

        if ( int(self.return_data) == 1 and int(self.copy_data) == 1 ):
            msg = 'Error: return_data and copy_data cannot be set both to 1\n'
            msg = msg + 'Please modify return_data or copy_data value in your crab.cfg file\n'
            raise CrabException(msg)

        if ( int(self.copy_data) == 0 and int(self.local_stage) == 1 ):
            msg = 'Error: copy_data = 0 and local_stage_out = 1.\n'
            msg += 'To enable local stage out the copy_data value has to be = 1\n'
            msg = msg + 'Please modify copy_data value in your crab.cfg file\n'
            raise CrabException(msg)

        if ( int(self.copy_data) == 0 and int(self.publish_data) == 1 ):
            msg = 'Error: publish_data = 1 must be used with copy_data = 1\n'
            msg = msg + 'Please modify copy_data value in your crab.cfg file\n'
            common.logger.info(msg)
            raise CrabException(msg)

        if ( int(self.publish_data) == 1 and self._name == 'lsf'):
            msg = 'Error: data publication is not allowed with lsf scheduler, but only with grid scheduler or caf\n'
            common.logger.info(msg)
            raise CrabException(msg)

        if ( int(self.local_stage) == 1 and int(self.publish_data) == 1 ):
            msg = 'Error: currently the publication is not supported with the local stage out. Work in progress....\n'
            common.logger.info(msg)
            raise CrabException(msg)

        self.debug_wrapper = int(cfg_params.get('USER.debug_wrapper',0))
        self.debugWrap=''
        if self.debug_wrapper==1: self.debugWrap='--debug'
        self.loc_stage_out = ''
        if ( int(self.local_stage) == 1 ):
            self.debugWrap='--debug'
            self.loc_stage_out='--local_stage'

        # Time padding for minimal job duration. 
        self.minimal_job_duration = 10

        return

    def boss(self):
        return self._boss

    def rb_configure(self, RB):
        """
        Return a requirement to be add to Jdl to select a specific RB/WMS:
        return None if RB=None
        To be re-implemented in concrete scheduler
        """
        return None

    def ce_list(self):
        return '',None,None

    def se_list(self, id, dest):
        return '',None,None

    def sched_fix_parameter(self):
        return

    def sched_parameter(self,i,task):
        """
        Returns parameter scheduler-specific, to use with BOSS .
        """
        return ''

    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        return ''

    def  wsExitFunc(self):
        """
        Returns part of a job script which does scheduler-specific
        output checks and management.
        """
        return ''

    def checkRemoteDir(self, endpoint, fileList):
        """
        """
        common.logger.info('Checking remote location')
        ## temporary hack for OctX:
        if endpoint.find('${PSETHASH}')>1:
            try:
                psethash = runCommand('edmConfigHash < %s| tail -1'%self.pset)
                endpoint= string.replace(endpoint,'${PSETHASH}/',psethash)
            except:
                msg =  'Problems trying remote dir check... \n'
                msg += '\tPlease check stage out configuration parameters.\n'
                raise CrabException(msg)
        try:
            remoteListTmp = self.listRemoteDir(endpoint)
        except Exception, ex:
            msg =  'Problems trying remote dir check: \n\t%s'%str(ex)
            raise CrabException(msg)
        if remoteListTmp is False:
            return
        if remoteListTmp:
            listJob = common._db.nJobs('list')
            remoteList = []
            for f_path in remoteListTmp:
                remoteList.append(str(os.path.basename(f_path)))
            metaList = []
            for id in listJob:
                for file in fileList :
                    metaList.append('%s'%numberFile(file,id))
            for i in remoteList:
                if i in metaList :
                    msg  = 'You are asking to stage out on a remote directory \n'
                    msg += '\twhich already contains files with same name.\n'
                    msg += '\tPlease change directory or remove the actual content following this HowTo:\n'
                    msg += '\thttps://twiki.cern.ch/twiki/bin/view/CMS/CheckUserRemoteDir\n'
                    raise CrabException(msg)
        else:
            msg = 'Remote directory is empty or not existis\n'
            common.logger.debug(msg)
        return

    def listRemoteDir(self, endpoint):
        """
        """
        protocol = self.protocolDict[common.scheduler.name().upper()]
        try:
            Storage = SElement( FullPath(string.strip(endpoint)), protocol )
        except Exception, ex:
            common.logger.debug(traceback.format_exc())
            raise Exception(str(ex))
        try:
            action = SBinterface( Storage )
        except Exception, ex:
            common.logger.debug(traceback.format_exc())
            raise Exception(str(ex))
        try:
            remoteList = action.dirContent()
        except Exception, ex:
            common.logger.debug(traceback.format_exc())
            raise Exception(str(ex))

        return remoteList

    def checkProxy(self, minTime=10):
        """
        Function to check the Globus proxy.
        """
        if (self.proxyValid): return

        ### Just return if asked to do so
        if (self.dontCheckProxy==1):
            self.proxyValid=1
            return
        CredAPI_config =  { 'credential':'Proxy',\
                            'myProxySvr': self.proxyServer, \
                            'logger': common.logger() \
                          }
        from ProdCommon.Credential.CredentialAPI import CredentialAPI
        CredAPI = CredentialAPI(CredAPI_config)

        if not CredAPI.checkCredential(Time=int(minTime)) or \
           not CredAPI.checkAttribute(group=self.group, role=self.role):
            try:
                CredAPI.ManualRenewCredential(group=self.group, role=self.role)
            except Exception, ex:
                raise CrabException(str(ex))
        if (self.dontCheckMyProxy!=1):
            if not CredAPI.checkMyProxy():
                try:
                    CredAPI.ManualRenewMyProxy()
                except Exception, ex:
                    raise CrabException(str(ex))
        # cache proxy validity
        self.proxyValid=1
        return

    def userName(self):
        """ return the user name """
        return

    def loggingInfo(self,list_id,outfile ):
        """ return logging info about job nj """
        return self.boss().LoggingInfo(list_id,outfile)

    def tags(self):
        return ''

    def listMatch(self, dest, full):
        """ Return the number of differente sites matching the actual requirements """
        start = time.time()
        tags=self.tags()
        ####  fede #####
        whiteL=[]
        blackL=[]
        voTags=['cms']
        if len(dest)!=0: dest = self.blackWhiteListParser.cleanForBlackWhiteList(dest,'list')

        whiteList=self.ce_list()[1]
        if whiteList != None:
            [whiteL.append(x.strip()) for x in whiteList.split(',')]

        blackList=self.ce_list()[2]
        if blackList != None:
            [blackL.append(x.strip()) for x in blackList.split(',')]
        if self.role: voTags.append('VOMS:/cms/Role=%s'%self.role)
        sites= self.boss().listMatch(tags, voTags, dest , whiteL, blackL, full)
        stop = time.time()

        return sites

    def submit(self,list,task):
        """ submit to scheduler a list of jobs """
        if (not len(list)): common.logger.info("No sites where to submit jobs")
        req=str(self.sched_parameter(list[0],task))

        ### reduce collection size...if needed
        new_list = bulkControl(self,list)

        for sub_list in new_list:
            self.boss().submit(task['id'],sub_list,req)
        return

    def delegateProxy(self):
        return

    def queryEverything(self,taskid):
        """
        Query needed info of all jobs with specified boss taskid
        """
        return self.boss().queryEverything(taskid)

    def getOutput(self, taskId, jobRange, outdir):
        """
        Get output for a finished job with id.
        """
        task = self.boss().getOutput(taskId, jobRange, outdir)
        return task

    def cancel(self,ids):
        """
        Cancel the job(s) with ids (a list of id's)
        """
        self._boss.cancel(ids)
        return

    def decodeLogInfo(self, file):
        """
        Parse logging info file and return main info
        """
        return

    def writeJDL(self, list, task):
        """
        Materialize JDL for a list of jobs
        """
        req=str(self.sched_parameter(list[0],task))
        new_list = bulkControl(self,list)
        jdl=[]
        for sub_list in new_list:
            tmp_jdl =  self.boss().writeJDL(task['id'], sub_list, req)
            jdl.append(tmp_jdl)
        return jdl

    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        return ''

    def wsInitialEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        return ''

    def wsExitFunc_common(self):
        """
        """
        txt = ''
        txt += '    if [ $PYTHONPATH ]; then \n'
        txt += '       if [ ! -s $RUNTIME_AREA/fillCrabFjr.py ]; then \n'
        txt += '           echo "WARNING: it is not possible to create crab_fjr.xml to final report" \n'
        txt += '       else \n'
        txt += '           python $RUNTIME_AREA/fillCrabFjr.py $RUNTIME_AREA/crab_fjr_$NJob.xml --errorcode $job_exit_code $executable_exit_status \n'
        txt += '       fi\n'
        txt += '    fi\n'
        txt += '    cd $RUNTIME_AREA  \n'
        txt += '    for file in $filesToCheck ; do\n'
        txt += '        if [ -e $file ]; then\n'
        txt += '            echo "tarring file $file in  $out_files"\n'
        txt += '        else\n'
        txt += '            echo "WARNING: output file $file not found!"\n'
        txt += '        fi\n'
        txt += '    done\n'
        txt += '    if [ $middleware == OSG ]; then\n'
        txt += '        final_list=$filesToCheck\n'
        txt += '        if [ $WORKING_DIR ]; then\n'
        txt += '            remove_working_dir\n'
        txt += '        fi\n'
        txt += '        symlinks -d .\n'
        txt += '    else\n'
        txt += '        final_list=$filesToCheck" .BrokerInfo"\n'
        txt += '    fi\n'
        txt += '    TIME_WRAP_END=`date +%s`\n'
        txt += '    let "TIME_WRAP = TIME_WRAP_END - TIME_WRAP_INI" \n\n'
        # padding for minimal job duration
        txt += '    let "MIN_JOB_DURATION = 60*%d" \n'%self.minimal_job_duration
        txt += '    let "PADDING_DURATION = MIN_JOB_DURATION - TIME_WRAP" \n'
        txt += '    if [ $PADDING_DURATION -gt 0 ]; then \n'
        txt += '        echo ">>> padding time: Sleeping the wrapper for $PADDING_DURATION seconds"\n'
        txt += '        sleep $PADDING_DURATION\n'
        txt += '        TIME_WRAP_END=`date +%s`\n'
        txt += '        let "TIME_WRAP = TIME_WRAP_END - TIME_WRAP_INI" \n'
        txt += '    else \n'
        txt += '        echo ">>> padding time: Wrapper lasting more than $MIN_JOB_DURATION seconds. No sleep required."\n'
        txt += '    fi\n\n'
        # call timing FJR filling
        txt += '    if [ $PYTHONPATH ]; then \n'
        txt += '       if [ ! -s $RUNTIME_AREA/fillCrabFjr.py ]; then \n'
        txt += '           echo "WARNING: it is not possible to create crab_fjr.xml to final report" \n'
        txt += '       else \n'
        txt += '           set -- $CPU_INFOS \n'
        txt += '           echo "CrabUserCpuTime=$1" >>  $RUNTIME_AREA/$repo \n'
        txt += '           echo "CrabSysCpuTime=$2" >>  $RUNTIME_AREA/$repo \n'
        txt += '           echo "CrabCpuPercentage=$3" >>  $RUNTIME_AREA/$repo \n'
        txt += '           python $RUNTIME_AREA/fillCrabFjr.py $RUNTIME_AREA/crab_fjr_$NJob.xml --timing $TIME_WRAP $TIME_EXE $TIME_STAGEOUT \\\"$CPU_INFOS\\\" \n'
        txt += '           echo "CrabWrapperTime=$TIME_WRAP" >> $RUNTIME_AREA/$repo \n'
        txt += '           if [ $TIME_STAGEOUT -lt 0 ]; then \n'
        txt += '               export TIME_STAGEOUT=NULL \n'
        txt += '           fi\n'
        txt += '           echo "CrabStageoutTime=$TIME_STAGEOUT" >> $RUNTIME_AREA/$repo \n'
        txt += '       fi\n'
        txt += '    fi\n'
        txt += '    echo "Disk space used:"\n'
        txt += '    echo "du -sh $RUNTIME_AREA"\n'
        txt += '    du -sh $RUNTIME_AREA \n\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo \n\n'
        return txt

    def wsCopyInput(self):
        """
        Copy input data from SE to WN
        """
        return ""

    def wsCopyOutput(self):
        """
        Write a CopyResults part of a job script, e.g.
        to copy produced output into a storage element.
        """
        return ""

    def declare(self,jobs):
        """
        Declaration of jobs
        """
        self._boss.declare(jobs)

