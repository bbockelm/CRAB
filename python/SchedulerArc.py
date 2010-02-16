# -*- coding: utf-8 -*-
# 
# Scheduler for the Nordugrid ARC middleware.
#
# Maintainers:
# Erik Edelmann <erik.edelmann@csc.fi>
# Jesper Koivumäki <jesper.koivumaki@hip.fi>
# 
from SchedulerGrid import SchedulerGrid
from Scheduler import Scheduler
from crab_exceptions import *
from Boss import Boss
import common
import string, time, os
from crab_util import *
from WMCore.SiteScreening.BlackWhiteListParser import CEBlackWhiteListParser, \
                                                      SEBlackWhiteListParser

import sys
import sha # Good for python 2.4, replaced with hashlib in 2.5

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#

class SchedulerArc(SchedulerGrid):
    def __init__(self, name='ARC'):
        SchedulerGrid.__init__(self,name)
        return

    def envUniqueID(self):
        """ 
        Generate shell expression for the 'MonitorJobID' used to identify
        the job at Dashboard; we'll use the arcId.
        """
        id = 'gsiftp://${NORDUGRID_CE}:2811/jobs/${HOME##*/}' # With ARC version >= 0.8 we could use $GRID_GLOBAL_JOBID
        msg = 'JobID for ML monitoring is created for ARC scheduler: %s' % id
        common.logger.debug(msg)
        return id


    def realSchedParams(self,cfg_params):
        """
        Return dictionary with specific parameters, to use
        with real scheduler
        """
        xrsl = ''

        if cfg_params.has_key("GRID.max_cpu_time"):
            s = cfg_params["GRID.max_cpu_time"]
            if s.strip()[0] not in ['"', "'"] and s.strip()[-1] not in ['"', "'"]:
                s = '"' + s.strip() + '"'
            xrsl += '(cpuTime=%s)' % s

        if cfg_params.has_key('GRID.max_wall_clock_time'):
            s = cfg_params["GRID.max_wall_clock_time"]
            if s.strip()[0] not in ['"', "'"] and s.strip()[-1] not in ['"', "'"]:
                s = '"' + s.strip() + '"'
            xrsl += '(wallTime=%s)' % s

        if cfg_params.has_key("GRID.additional_xrsl_parameters"):
            common.logger.warning("additional_xrsl_parameters is deprecated; use 'additional_jdl_parameters' instead!")
            xrsl += cfg_params["GRID.additional_xrsl_parameters"]

        if cfg_params.has_key("GRID.additional_jdl_parameters"):
            xrsl += cfg_params["GRID.additional_jdl_parameters"]

        return {'user_xrsl': xrsl}


    def configure(self,cfg_params):

        if not os.environ.has_key('EDG_WL_LOCATION'):
            # This is an ugly hack needed for SchedulerGrid.configure() to
            # work!
            os.environ['EDG_WL_LOCATION'] = ''

        if not os.environ.has_key('X509_USER_PROXY'):
            # Set X509_USER_PROXY to the default location.  We'll do this
            # because in functions called by Scheduler.checkProxy()
            # voms-proxy-info will be called with '-file $X509_USER_PROXY',
            # so if X509_USER_PROXY isn't set, it won't work.
            os.environ['X509_USER_PROXY'] = '/tmp/x509up_u' + str(os.getuid())

        SchedulerGrid.configure(self, cfg_params)
        self.environment_unique_identifier = None


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
        # cache proxy validity
        self.proxyValid=1
        return


    def ce_list(self):
        ceParser = CEBlackWhiteListParser(self.EDG_ce_white_list,
                                          self.EDG_ce_black_list, common.logger())
        wl = ','.join(ceParser.whiteList()) or None
        bl = ','.join(ceParser.blackList()) or None
        return '', wl, bl


    def se_list(self, id, dest):
        se_white = self.blackWhiteListParser.whiteList()
        se_black = self.blackWhiteListParser.blackList()
        return '', se_white, se_black


    def sched_parameter(self,i,task):
        """
        Returns parameter scheduler-specific, to use with BOSS .
        """
        return self.runtimeXrsl(i, task) + self.clusterXrsl(i, task)


    def runtimeXrsl(self,i,task):
        """
        Return an xRSL-code snippet with required runtime environments
        """
        xrsl = ""
        for t in self.tags():
            xrsl += "(runTimeEnvironment=%s)" % t
        return xrsl


    def clusterXrsl(self,i,task):
        """
        Return an xRSL-code snippet to select a CE ("cluster", in ARC parlance)
        """
        se_dls = task.jobs[i-1]['dlsDestination']
        blah, se_white, se_black = self.se_list(i, se_dls)

        se_list = []
        for se in se_dls:
            if se_white:
                if se in se_white: se_list.append(se)
            elif se_black:
                if se not in se_black: se_list.append(se)
            else:
                se_list.append(se)
        # FIXME: Check that se_list contains at least one SE!

        ce_list = self.listMatch(se_list, False)

        xrsl = ""
        if len(ce_list) > 0:

            # A ce-list with more than one element must be an OR:ed
            # list: (|(cluster=ce1)(cluster=ce2)...)
            if len(ce_list) > 1:
                xrsl += '(|'
            for ce in ce_list:
                xrsl += '(cluster=%s)' % ce
            if len(ce_list) > 1:
                xrsl += ')'
        else:
            common.logger.debug("clusterXrsl: No suitable CE found !?")
            # FIXME: If ce_list == []  ==>  xrsl = ""  ==>  we'll submit
            # "anywhere", which is completely contrary behaviour to what we want!
            # ce_list == [] means there were _no_ CE in ce_infoSys that
            # survived the white- and black-list filter, so we shouldn't submit
            # at all!

        return xrsl


    def wsInitialEnvironment(self):
        """ 
        Code generated here will get executed before anything else in
        the job scripts
        """
        txt = ""
        # When setting env. variables in the xrsl code, spaces has to be
        # escaped by '\':s in ARC 0.6.*.  In ARC 0.8.* however, they must
        # not -- any '\':s in the xrsl code will end up in the final env.
        # var. value.  To work with ARC 0.6,
        # ProdCommon/BossLite/schedulers/SchedulerARC.decode() will use
        # '\':s. To be compatible with 0.8, we therefore have to remove any
        # remaining '\':s in the values of ARC_INPUTFILES and
        # ARC_OUTPUTFILES.  Use '\\134' for '\' instead of '\\' in the tr
        # command for portability reasons.
        txt += "export ARC_INPUTFILES=`tr -d '\\134' <<< $ARC_INPUTFILES`\n"
        txt += "export ARC_OUTPUTFILES=`tr -d '\\134' <<< $ARC_OUTPUTFILES`\n"

        txt += 'if [ -n ""$TMPDIR ] && [ -d $TMPDIR ]; then\n'
        txt += '    echo Moving to $TMPDIR\n'
        txt += '    cp $ARC_INPUTFILES $TMPDIR\n'
        txt += '    cd $TMPDIR\n'
        txt += '    ln -s ${HOME}/$ARC_STDOUT $ARC_STDOUT\n'
        txt += '    ln -s ${HOME}/$ARC_STDERR $ARC_STDERR\n'
        txt += 'fi\n'
        return txt


    def wsExitFunc(self):
        """
        Returns part of a job script which does scheduler-specific
        output checks and management.
        """
        txt = '\n'

        txt += '#\n'
        txt += '# EXECUTE THIS FUNCTION BEFORE EXIT \n'
        txt += '#\n\n'

        txt += 'func_exit() { \n'
        txt += self.wsExitFunc_common()

        txt += '    echo "JOB_EXIT_STATUS = $job_exit_code"\n'
        txt += '    echo "JobExitCode=$job_exit_code" >> $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '    tar zchvf ${out_files}.tgz  $filesToCheck\n'
        txt += '    if [ $RUNTIME_AREA != $HOME ]; then\n'
        txt += '        cd $RUNTIME_AREA\n'
        txt += '        cp $ARC_OUTPUTFILES $HOME\n'
        txt += '        rm -rf *\n'
        txt += '    fi\n'
        txt += '    exit $job_exit_code\n'
        txt += '}\n'
        return txt


    def tags(self):
        task=common._db.getTask()
        tags = ["APPS/HEP/CMSSW-PA"]
        for s in task['jobType'].split('&&'):
            if re.match(' *Member\(".*", .*RunTimeEnvironment', s):
                rte = re.sub(" *Member\(\"", "", s)
                rte = re.sub("\", .*", "", rte)
                tags.append(rte)
        return tags


    def loggingInfo(self,list_id,outfile ):
        """ return logging info about job nj """
        print 'loggingInfo', list_id
        return self.boss().LoggingInfo(list_id,outfile)
