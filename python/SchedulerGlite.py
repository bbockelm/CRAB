from SchedulerGrid import SchedulerGrid
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
from GliteConfig import *
import EdgLoggingInfo
import common

import os, sys, time

class SchedulerGlite(SchedulerGrid):
    def __init__(self, name="GLITE"):
        SchedulerGrid.__init__(self,name)

        self.OSBsize = 55000

    def configure(self,cfg_params):
        SchedulerGrid.configure(self, cfg_params)
        self.checkProxy()
        self.environment_unique_identifier = 'GLITE_WMS_JOBID'

    def realSchedParams(self,cfg_params):
        """
        Return dictionary with specific parameters, to use 
        with real scheduler  
        """
        self.rb_param_file=''
        if (cfg_params.has_key('EDG.rb')):
            self.rb_param_file=common.scheduler.rb_configure(cfg_params.get("EDG.rb"))
        self.wms_service=cfg_params.get("EDG.wms_service",'')
        params = { 'service' : self.wms_service, \
                   'config' : self.rb_param_file
                 }
        return  params
      

    def rb_configure(self, RB):
        if not RB: return None
        glite_config = None
        rb_param_file = None

        gliteConfig = GliteConfig(RB)
        glite_config = gliteConfig.config()

        if (glite_config ):
            rb_param_file = glite_config
        return rb_param_file

    def ce_list(self):
        """
        Returns string with requirement CE related
        """
        req = ''
        if self.EDG_ce_white_list:
            ce_white_list = self.EDG_ce_white_list
            tmpCe=[]
            concString = '&&'
            for ce in ce_white_list:
                tmpCe.append('RegExp("' + string.strip(ce) + '", other.GlueCEUniqueId)')
            if len(tmpCe) == 1:
                req +=  " && (" + concString.join(tmpCe) + ") "
            elif len(tmpCe) > 1:
                firstCE = 0
                for reqTemp in tmpCe:
                    if firstCE == 0:
                        req += " && ( (" + reqTemp + ") "
                        firstCE = 1
                    elif firstCE > 0:
                        req += " || (" + reqTemp + ") "
                if firstCE > 0:
                    req += ") "

        if self.EDG_ce_black_list:
            ce_black_list = self.EDG_ce_black_list
            tmpCe=[]
            concString = '&&'
            for ce in ce_black_list:
                tmpCe.append('(!RegExp("' + string.strip(ce) + '", other.GlueCEUniqueId))')
            if len(tmpCe): req += " && (" + concString.join(tmpCe) + ") "

        # requirement added to skip gliteCE
        req += '&& (!RegExp("blah", other.GlueCEUniqueId))'

        return req,self.EDG_ce_white_list,self.EDG_ce_black_list

    def se_list(self, id, dest):
        """
        Returns string with requirement SE related
        """
        hostList=self.findSites_(id,dest)
        req=''
        reqtmp=[]
        concString = '||'

        for arg in hostList:
            reqtmp.append(' Member("'+arg+'" , other.GlueCESEBindGroupSEUniqueID) ')

        if len(reqtmp): req += " && (" + concString.join(reqtmp) + ") "

        return req

    def jdlParam(self):
        """
        Returns
        """
        req=''
        if self.EDG_addJdlParam:
            if self.EDG_addJdlParam[-1] == '': self.EDG_addJdlParam= self.EDG_addJdlParam[:-1]
            for p in self.EDG_addJdlParam:
             #   param_file.write(string.strip(p)+';\n')
                req+=string.strip(p)+';\n' ## BL--DS
        return req

    def specific_req(self):
        """
        Returns string with specific requirements
        """
        req=''
        if self.EDG_clock_time:
            if (not req == ' '): req = req + ' && '
            req = req + 'other.GlueCEPolicyMaxWallClockTime>='+self.EDG_clock_time

        if self.EDG_cpu_time:
            if (not req == ' '): req = req + ' && '
            req = req + ' other.GlueCEPolicyMaxCPUTime>='+self.EDG_cpu_time

        return req

    def sched_fix_parameter(self):
        """
        Returns string with requirements and scheduler-specific parameters
        """
        index = int(common._db.nJobs())
        job = common.job_list[index-1]
        jbt = job.type()
        req = ''
        req = req + jbt.getRequirements()

        if self.EDG_requirements:
            if (not req == ' '): req = req +  ' && '
            req = req + self.EDG_requirements

        Task_Req={'jobType':req}## DS--BL
        common._db.updateTask_(Task_Req)

    def sched_parameter(self,i,task):
        """
        Returns string with requirements and scheduler-specific parameters
        """
        dest=  task.jobs[i-1]['dlsDestination'] ## DS--BL

        req=''
        req +=task['jobType']

        sched_param=''
        sched_param+='Requirements = ' + req +self.specific_req() + self.se_list(i,dest) +\
                                        self.ce_list()[0] +';\n' ## BL--DS
        if self.EDG_addJdlParam: sched_param+=self.jdlParam() ## BL--DS
        sched_param+='MyProxyServer = "' + self.proxyServer + '";\n'
        sched_param+='VirtualOrganisation = "' + self.VO + '";\n'
        sched_param+='RetryCount = '+str(self.EDG_retry_count)+';\n'
        sched_param+='ShallowRetryCount = '+str(self.EDG_shallow_retry_count)+';\n'

        return sched_param

    def decodeLogInfo(self, file):
        """
        Parse logging info file and return main info
        """
        loggingInfo = EdgLoggingInfo.EdgLoggingInfo()
        reason = loggingInfo.decodeReason(file)
        return reason

    def findSites_(self, n, sites):
        itr4 =[]
        if len(sites)>0 and sites[0]=="":
            return itr4
        if sites != [""]:
            ##Addedd Daniele
            replicas = self.blackWhiteListParser.checkBlackList(sites,n)
            if len(replicas)!=0:
                replicas = self.blackWhiteListParser.checkWhiteList(replicas,n)

            itr4 = replicas
            #####
        return itr4

    
    def wsExitFunc(self):
        """
        """
        txt = '\n'

        txt += '#\n'
        txt += '# EXECUTE THIS FUNCTION BEFORE EXIT \n'
        txt += '#\n\n'

        txt += 'func_exit() { \n'
        txt += self.wsExitFunc_common()
        ### specific Glite check for OSB
        txt += '    tar zcvf ${out_files}.tgz  ${final_list}\n'
        txt += '    tmp_size=`ls -gGrta ${out_files}.tgz | awk \'{ print $3 }\'`\n'
        txt += '    rm ${out_files}.tgz\n'  
        txt += '    size=`expr $tmp_size`\n'
        txt += '    echo "Total Output dimension: $size"\n'
        txt += '    limit='+str(self.OSBsize) +' \n'  
        txt += '    echo "WARNING: output files size limit is set to: $limit"\n'
        txt += '    if [ "$limit" -lt "$sum" ]; then\n'
        txt += '        exceed=1\n'
        txt += '        job_exit_code=70000\n'
        txt += '        echo "Output Sanbox too big. Produced output is lost "\n'
        txt += '    else\n'
        txt += '        exceed=0\n'
        txt += '        echo "Total Output dimension $sum is fine."\n'
        txt += '    fi\n'

        txt += '    echo "JOB_EXIT_STATUS = $job_exit_code"\n'
        txt += '    echo "JobExitCode=$job_exit_code" >> $RUNTIME_AREA/$repo\n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo\n'
        txt += '    if [ $exceed -ne 1 ]; then\n'
        txt += '        tar zcvf ${out_files}.tgz  ${final_list}\n'
        txt += '    else\n'
        txt += '        tar zcvf ${out_files}.tgz CMSSW_${NJob}.stdout CMSSW_${NJob}.stderr\n'
        txt += '    fi\n'
        txt += '    exit $job_exit_code\n'

        txt += '}\n'
        return txt

    def userName(self):
        """ return the user name """
        tmp=runCommand("voms-proxy-info -identity")
        return tmp.strip()
