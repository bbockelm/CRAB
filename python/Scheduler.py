
import os
import time
import string

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
        return

    def name(self):
        return self._name

    def realSchedParams(self,cfg_params):
        """
        """
        return {}

    def configure(self, cfg_params):
        self._boss.configure(cfg_params)
        seWhiteList = cfg_params.get('EDG.se_white_list',[])
        seBlackList = cfg_params.get('EDG.se_black_list',[])
        self.blackWhiteListParser = SEBlackWhiteListParser(seWhiteList, seBlackList, common.logger)
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
        protocol = 'srm-lcg'
        try:
            Storage = SElement( FullPath(endpoint), protocol )
        except ProtocolUnknown, ex:
            msg  = 'ERROR : %s '% str(ex)
            common.logger.message(msg)
        try:
            action = SBinterface( Storage )
        except Exception, ex:
            msg  = 'ERROR : %s '% str(ex)
            common.logger.message(msg)
        try:
            remoteListTmp = action.listDir()
        except Exception, ex:
            msg  = 'ERROR : %s '% str(ex)
            common.logger.message(msg)
        """
        #to be done 
        return

    def checkProxy(self, deep=0):
        """
        Function to check the Globus proxy.
        """
        if (self.proxyValid): return

        ### Just return if asked to do so
        if (self.dontCheckProxy==1):
            self.proxyValid=1
            return
        if deep == 0 :
            minTimeLeft=10*3600 # in seconds
        else:
            minTimeLeft=100*3600 # in seconds

        mustRenew = 0
        timeLeftLocal = runCommand('voms-proxy-info -timeleft 2>/dev/null')
        ## if no valid proxy
        if timeLeftLocal == None:
            mustRenew = 1
        ## if valid check how long
        elif int(timeLeftLocal)<minTimeLeft :
            mustRenew = 1

        ## check first attribute
        att=runCommand('voms-proxy-info -fqan 2>/dev/null | head -1')
        reg="/%s/"%self.VO
        if self.group:
            reg+=self.group
        if self.role:
            reg+="/Role=%s"%self.role
        ## you always have at least  /cms/Role=NULL/Capability=NULL
        if not re.compile(r"^"+reg).search(att):
            if not mustRenew:
                common.logger.message( "Valid proxy found, but with wrong VO group/role.\n")
            mustRenew = 1
        ######


        if mustRenew:
            common.logger.message( "No valid proxy found or remaining time of validity of already existing proxy shorter than 10 hours!\n Creating a user proxy with default length of 192h\n")
            cmd = 'voms-proxy-init -voms '+self.VO
            if self.group:
                cmd += ':/'+self.VO+'/'+self.group
            if self.role:
                cmd += '/role='+self.role
            cmd += ' -valid 192:00'
            try:
                # SL as above: damn it!
                common.logger.debug(10,cmd)
                out = os.system(cmd)
                if (out>0): raise CrabException("Unable to create a valid proxy!\n")
            except:
                msg = "Unable to create a valid proxy!\n"
                raise CrabException(msg)
            pass

        ## now I do have a voms proxy valid, and I check the myproxy server
        renewProxy = 0
        cmd = 'myproxy-info -d -s '+self.proxyServer
        cmd_out = runCommand(cmd,0,20)
        if not cmd_out:
            common.logger.message('No credential delegated to myproxy server '+self.proxyServer+' will do now')
            renewProxy = 1
        else:
            ## minimum time: 5 days
            minTime = 4 * 24 * 3600
            ## regex to extract the right information
            myproxyRE = re.compile("timeleft: (?P<hours>[\\d]*):(?P<minutes>[\\d]*):(?P<seconds>[\\d]*)")
            for row in cmd_out.split("\n"):
                g = myproxyRE.search(row)
                if g:
                    hours = g.group("hours")
                    minutes = g.group("minutes")
                    seconds = g.group("seconds")
                    timeleft = int(hours)*3600 + int(minutes)*60 + int(seconds)
                    if timeleft < minTime:
                        renewProxy = 1
                        common.logger.message('Your proxy will expire in:\n\t'+hours+' hours '+minutes+' minutes '+seconds+' seconds\n')
                        common.logger.message('Need to renew it:')
                    pass
                pass
            pass

        # if not, create one.
        if renewProxy:
            cmd = 'myproxy-init -d -n -s '+self.proxyServer
            out = os.system(cmd)
            if (out>0):
                raise CrabException("Unable to delegate the proxy to myproxyserver "+self.proxyServer+" !\n")
            pass

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

        if len(dest)!=0: dest = self.blackWhiteListParser.cleanForBlackWhiteList(dest,'list')

        whiteL=self.ce_list()[1]
        blackL=self.ce_list()[2]

        sites= self.boss().listMatch(tags, dest , whiteL, blackL, full)
        stop = time.time()

        return sites

    def submit(self,list,task):
        """ submit to scheduler a list of jobs """
        if (not len(list)): common.logger.message("No sites where to submit jobs")
        req=str(self.sched_parameter(list[0],task))

        ### reduce collection size...if needed
        new_list = bulkControl(self,list)

        for sub_list in new_list:
            self.boss().submit(task['id'],sub_list,req)
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
        txt += '    let "TIME_WRAP = TIME_WRAP_END - TIME_WRAP_INI" \n'
        txt += '    if [ $PYTHONPATH ]; then \n'
        txt += '       if [ ! -s $RUNTIME_AREA/fillCrabFjr.py ]; then \n'
        txt += '           echo "WARNING: it is not possible to create crab_fjr.xml to final report" \n'
        txt += '       else \n'
        # call timing FJR filling
        txt += '           echo "CrabCpuTime=$CRAB_EXE_CPU_TIME" >>  $RUNTIME_AREA/$repo \n' # TODO check the right name
        txt += '           python $RUNTIME_AREA/fillCrabFjr.py $RUNTIME_AREA/crab_fjr_$NJob.xml --timing $TIME_WRAP $TIME_EXE $TIME_STAGEOUT \\\"$CRAB_EXE_CPU_TIME\\\" \n'
        txt += '           echo "CrabWrapperTime=$TIME_WRAP" >> $RUNTIME_AREA/$repo \n'
        txt += '           if [ $TIME_STAGEOUT -lt 0 ]; then \n'
        txt += '               export TIME_STAGEOUT=NULL \n'
        txt += '           fi\n'
        txt += '           echo "CrabStageoutTime=$TIME_STAGEOUT" >> $RUNTIME_AREA/$repo \n'
        txt += '       fi\n'
        txt += '    fi\n'
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

