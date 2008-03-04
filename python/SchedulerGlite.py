from SchedulerGrid import SchedulerGrid
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
from GliteConfig import *
import common

import os, sys, time

class SchedulerGlite(SchedulerGrid):
    def __init__(self, name="GLITE"):
        SchedulerGrid.__init__(self,name)

    def configure(self,cfg_params):
        SchedulerGrid.configure(self, cfg_params)
        self.environment_unique_identifier = 'GLITE_WMS_JOBID'

    def rb_configure(self, RB):
        if not RB: return None
        glite_config = None
        rb_param_file = None

        gliteConfig = GliteConfig(RB)
        glite_config = gliteConfig.config()

        if (glite_config ):
            rb_param_file = 'WMSconfig = '+glite_config+';\n'
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

        return req

    def se_list(self, id):
        """
        Returns string with requirement SE related    
        """  
        hostList=self.findSites_(id)
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

    def sched_parameter(self):
        """
        Returns string with requirements and scheduler-specific parameters
        """
        index = int(common._db.nJobs()) - 1
        job = common.job_list[index]
        jbt = job.type()
 
      ### To Be Removed BL--DS
      #  lastDest=''
      #  first = []
      #  last  = []
      #  for n in range(common.jobDB.nJobs()):
      #      currDest=common.jobDB.destination(n)
      #      if (currDest!=lastDest):
      #          lastDest = currDest
      #          first.append(n)
      #          if n != 0:last.append(n-1)
      #  if len(first)>len(last) :last.append(common.jobDB.nJobs())

        req = ''
        req = req + jbt.getRequirements()

        if self.EDG_requirements:
            if (not req == ' '): req = req +  ' && '
            req = req + self.EDG_requirements
        sched_param=''
        sched_param='""pippo""'

        ### Temporary Problem with quote and py2sqlite.... under investigation..         
        for i in range(index):
           # sched_param+='Requirements = ' + req +self.specific_req() + self.se_list(i) +\
           #                                 self.ce_list() +';\n' ## BL--DS
           # if self.EDG_addJdlParam: sched_param+=self.jdlParam() ## BL--DS
           # if (self.rb_param_file): sched_param+=self.rb_param_file ## BL--DS
           # print sched_param
            run_jobReq={'schedulerAttributes':sched_param}## DS--BL
            common._db.updateRunJob_(i,run_jobReq)        

    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        txt = ''
        txt += '# strip arguments\n'
        txt += 'echo "strip arguments"\n'
        txt += 'args=("$@")\n'
        txt += 'nargs=$#\n'
        txt += 'shift $nargs\n'
        txt += "# job number (first parameter for job wrapper)\n"
        txt += "NJob=${args[0]}; export NJob\n"

        txt += '# job identification to DashBoard \n'
        #txt += 'MonitorJobID=`echo ${NJob}_$GLITE_WMS_JOBID`\n'
        #txt += 'SyncGridJobId=`echo $GLITE_WMS_JOBID`\n'
        #txt += 'MonitorID=`echo ' + self._taskId + '`\n'
        txt += 'MonitorJobID=${NJob}_$GLITE_WMS_JOBID \n'
        txt += 'SyncGridJobId=$GLITE_WMS_JOBID \n'
        txt += 'MonitorID='+self._taskId+' \n'
        txt += 'echo "MonitorJobID=$MonitorJobID" > $RUNTIME_AREA/$repo \n'
        txt += 'echo "SyncGridJobId=$SyncGridJobId" >> $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorID=$MonitorID" >> $RUNTIME_AREA/$repo\n'
        #txt += 'echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        #txt += 'echo "SyncGridJobId=`echo $SyncGridJobId`" | tee -a $RUNTIME_AREA/$repo \n'
        #txt += 'echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'

        #txt += 'echo "middleware discovery: " \n'
        txt += 'echo ">>> GridFlavour discovery: " \n'
        txt += 'if [ $VO_CMS_SW_DIR ]; then \n'
        txt += '    middleware=LCG\n'
        #txt += '    echo "SyncCE=`glite-brokerinfo getCE`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "SyncCE=`glite-brokerinfo getCE`" >> $RUNTIME_AREA/$repo \n'
        #txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=$middleware" | tee -a $RUNTIME_AREA/$repo \n'
        #txt += '    echo ">>> middleware =$middleware" \n'
        txt += 'elif [ $OSG_APP ]; then \n'
        txt += '    middleware=OSG \n'
        txt += '    if [ $OSG_JOB_CONTACT ]; then \n'
        txt += '        SyncCE="$OSG_JOB_CONTACT"; \n'
        #txt += '        echo "SyncCE=$SyncCE" | tee -a $RUNTIME_AREA/$repo ;\n'
        txt += '        echo "SyncCE=$SyncCE" >> $RUNTIME_AREA/$repo ;\n'
        txt += '    else\n'
        txt += '        echo "not reporting SyncCE";\n'
        txt += '    fi\n';
        txt += '    echo "GridFlavour=$middleware" | tee -a $RUNTIME_AREA/$repo \n'
        #txt += '    echo ">>> middleware =$middleware" \n'
        txt += 'else \n'
        txt += '    echo "ERROR ==> GridFlavour not identified" \n'
        txt += '    job_exit_code=10030\n'
        txt += '    func_exit \n'
        #txt += '    echo "SET_CMS_ENV 10030 ==> middleware not identified" \n'
        #txt += '    echo "JOB_EXIT_STATUS = 10030" \n'
        #txt += '    echo "JobExitCode=10030" | tee -a $RUNTIME_AREA/$repo \n'
        #txt += '    dumpStatus $RUNTIME_AREA/$repo \n'
        #txt += '    exit 1 \n'
        txt += 'fi \n'

        txt += 'dumpStatus $RUNTIME_AREA/$repo \n'

        txt += '\n\n'

        txt += 'export VO='+self.VO+'\n'
        txt += 'if [ $middleware == LCG ]; then\n'
        txt += '    CloseCEs=`glite-brokerinfo getCE`\n'
        txt += '    echo "CloseCEs = $CloseCEs"\n'
        txt += '    CE=`echo $CloseCEs | sed -e "s/:.*//"`\n'
        txt += '    echo "CE = $CE"\n'
        txt += 'elif [ $middleware == OSG ]; then \n'
        txt += '    if [ $OSG_JOB_CONTACT ]; then \n'
        txt += '        CE=`echo $OSG_JOB_CONTACT | /usr/bin/awk -F\/ \'{print $1}\'` \n'
        txt += '    else \n'
        txt += '        echo "ERROR ==> OSG mode in setting CE name from OSG_JOB_CONTACT" \n'
        txt += '        job_exit_code=10099\n'
        txt += '        func_exit \n'
        #txt += '        echo "SET_CMS_ENV 10099 ==> OSG mode: ERROR in setting CE name from OSG_JOB_CONTACT" \n'
        #txt += '        echo "JOB_EXIT_STATUS = 10099" \n'
        #txt += '        echo "JobExitCode=10099" | tee -a $RUNTIME_AREA/$repo \n'
        #txt += '        dumpStatus $RUNTIME_AREA/$repo \n'
        #txt += '        exit 1 \n'
        txt += '    fi \n'
        txt += 'fi \n'

        return txt

    def loggingInfo(self, id):
        """
        retrieve the logging info from logging and bookkeeping and return it
        """
        self.checkProxy()
        cmd = 'glite-job-logging-info -v 3 ' + id
        cmd_out = runCommand(cmd)
        return cmd_out

    def queryDetailedStatus(self, id):
        """ Query a detailed status of the job with id """
        cmd = 'glite-job-status '+id
        cmd_out = runCommand(cmd)
        return cmd_out


    def tOut(self, list):
        return 180
