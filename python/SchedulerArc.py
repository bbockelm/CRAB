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
import string, time, os, socket
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
        taskHash = sha.new(common._db.queryTask('name')).hexdigest()
        id = 'https://' + socket.gethostname() + '/' + taskHash + '/${NJob}'
        msg = 'JobID for ML monitoring is created for ARC scheduler: %s' % id
        common.logger.debug(msg)
        return id


    def realSchedParams(self,cfg_params):
        """
        Return dictionary with specific parameters, to use
        with real scheduler
        """
        return {}


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

        ce_list = self.listMatch(se_list, 'False')

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


#    def wsInitialEnvironment(self):
#        return ''


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

        # Remove ".BrokerInfo" that the code generated by
        # self.wsExitFunc_common() adds to $final_list. (This is an ugly
        # hack -- the "good" solution would be to add ARC-knowledge to
        # self.wsExitFunc_common())
        txt += "    final_list=${final_list%.BrokerInfo}\n" 

        txt += '    echo "JOB_EXIT_STATUS = $job_exit_code"\n'
        txt += '    echo "JobExitCode=$job_exit_code" >> $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '    tar zcvf ${out_files}.tgz  ${final_list}\n'
        txt += '    exit $job_exit_code\n'
        txt += '}\n'
        return txt


    def tags(self):
        task=common._db.getTask()
        tags = ["APPS/HEP/CMSSW-PA"]
        for s in task['jobType'].split('&&'):
            if re.match('Member\(".*", .*RunTimeEnvironment', s):
                rte = re.sub(", .*", "", re.sub("Member\(", "", s))
                rte = re.sub("\"", "", rte)
                tags.append(rte)
        return tags


    def submit(self,list,task):
        """ submit to scheduler a list of jobs """
        if (not len(list)):
            common.logger.info("No sites where to submit jobs")
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
        # FIXME: Is this function being used?
        req=str(self.sched_parameter(list[0],task))
        new_list = bulkControl(self,list)
        jdl=[]
        for sub_list in new_list:
            tmp_jdl =  self.boss().writeJDL(task['id'], sub_list, req)
            jdl.append(tmp_jdl)
        return jdl
