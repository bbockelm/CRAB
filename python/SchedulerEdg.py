from SchedulerGrid import SchedulerGrid
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
from EdgConfig import *
from BlackWhiteListParser import BlackWhiteListParser
import common

import os, sys, time

class SchedulerEdg(SchedulerGrid):
    def __init__(self):
        SchedulerGrid.__init__(self,"EDG")

    def configure(self,cfg_params):
        SchedulerGrid.configure(self, cfg_params)
        self.environment_unique_identifier = 'EDG_WL_JOBID'

    def rb_configure(self, RB):
        edg_config = None
        edg_config_vo = None
        rb_param_file = None

        edgConfig = EdgConfig(RB)
        edg_config = edgConfig.config()
        edg_config_vo = edgConfig.configVO()

        if (edg_config and edg_config_vo):
            rb_param_file = 'RBconfig = "'+edg_config+'";\nRBconfigVO = "'+edg_config_vo+'";\n'
        return rb_param_file

    def sched_parameter(self):
        """
        Returns file with requirements and scheduler-specific parameters
        """
        index = int(common.jobDB.nJobs()) - 1
        job = common.job_list[index]
        jbt = job.type()

        lastBlock=-1
        first = []
        for n in range(common.jobDB.nJobs()):
            currBlock=common.jobDB.block(n)
            if (currBlock!=lastBlock):
                lastBlock = currBlock
                first.append(n)

        req = ''
        req = req + jbt.getRequirements()

        if self.EDG_requirements:
            if (req == ' '):
                req = req + self.EDG_requirements
            else:
                req = req +  ' && ' + self.EDG_requirements

        if self.EDG_ce_white_list:
            ce_white_list = string.split(self.EDG_ce_white_list,',')
            for i in range(len(ce_white_list)):
                if i == 0:
                    if (req == ' '):
                        req = req + '((RegExp("' + string.strip(ce_white_list[i]) + '", other.GlueCEUniqueId))'
                    else:
                        req = req +  ' && ((RegExp("' +  string.strip(ce_white_list[i]) + '", other.GlueCEUniqueId))'
                    pass
                else:
                    req = req +  ' || (RegExp("' +  string.strip(ce_white_list[i]) + '", other.GlueCEUniqueId))'
            req = req + ')'

        if self.EDG_ce_black_list:
            ce_black_list = string.split(self.EDG_ce_black_list,',')
            for ce in ce_black_list:
                if (req == ' '):
                    req = req + '(!RegExp("' + string.strip(ce) + '", other.GlueCEUniqueId))'
                else:
                    req = req +  ' && (!RegExp("' + string.strip(ce) + '", other.GlueCEUniqueId))'
                pass
        if self.EDG_clock_time:
            if (req == ' '):
                req = req + 'other.GlueCEPolicyMaxWallClockTime>='+self.EDG_clock_time
            else:
                req = req + ' && other.GlueCEPolicyMaxWallClockTime>='+self.EDG_clock_time

        if self.EDG_cpu_time:
            if (req == ' '):
                req = req + ' other.GlueCEPolicyMaxCPUTime>='+self.EDG_cpu_time
            else:
                req = req + ' && other.GlueCEPolicyMaxCPUTime>='+self.EDG_cpu_time

        for i in range(len(first)): # Add loop DS
            groupReq = req
            self.param='sched_param_'+str(i)+'.clad'
            param_file = open(common.work_space.shareDir()+'/'+self.param, 'w')

            itr4=self.findSites_(first[i])
            for arg in itr4:
                groupReq = groupReq + ' && anyMatch(other.storage.CloseSEs, ('+str(arg)+'))'
            param_file.write('Requirements = '+groupReq +';\n')

            if (self.rb_param_file):
                param_file.write(self.rb_param_file)

            if self.EDG_addJdlParam:
                if self.EDG_addJdlParam[-1] == '': self.EDG_addJdlParam= self.EDG_addJdlParam[:-1] 
                for p in self.EDG_addJdlParam:
                    param_file.write(string.strip(p)+';\n')

            param_file.close()


    def loggingInfo(self, id):
        """
        retrieve the logging info from logging and bookkeeping and return it
        """
        self.checkProxy()
        cmd = 'edg-job-get-logging-info -v 2 ' + id
        cmd_out = runCommand(cmd)
        return cmd_out

    def queryDetailedStatus(self, id):
        """ Query a detailed status of the job with id """
        cmd = 'edg-job-status '+id
        cmd_out = runCommand(cmd)
        return cmd_out

    def findSites_(self, n):
        itr4 =[]
        sites = common.jobDB.destination(n)
        if len(sites)>0 and sites[0]=="":
            return itr4

        itr = ''
        if sites != [""]:#CarlosDaniele
            ##Addedd Daniele
            replicas = self.blackWhiteListParser.checkBlackList(sites,n)
            if len(replicas)!=0:
                replicas = self.blackWhiteListParser.checkWhiteList(replicas,n)

            if len(replicas)==0:
                itr = itr + 'target.GlueSEUniqueID=="NONE" '
                #msg = 'No sites remaining that host any part of the requested data! Exiting... '
                #raise CrabException(msg)
            #####
           # for site in sites:
            for site in replicas:
                #itr = itr + 'target.GlueSEUniqueID==&quot;'+site+'&quot; || '
                itr = itr + 'target.GlueSEUniqueID=="'+site+'" || '
            itr = itr[0:-4]
            itr4.append( itr )
        return itr4

    def tOut(self, list):
        return 120


