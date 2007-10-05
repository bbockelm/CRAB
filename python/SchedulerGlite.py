from SchedulerEdg import SchedulerEdg
from crab_logger import Logger
from crab_exceptions import *
from crab_util import *
from GliteConfig import *
import common

import os, sys, time

class SchedulerGlite(SchedulerEdg):
    def __init__(self):
        SchedulerEdg.__init__(self)

    def rb_configure(self, RB):
        self.glite_config = ''
        self.rb_param_file = ''

        gliteConfig = GliteConfig(RB)
        self.glite_config = gliteConfig.config()

        if (self.glite_config != ''):
            self.rb_param_file = 'WMSconfig = '+self.glite_config+';\n'
            #print "rb_param_file = ", self.rb_param_file
        return self.rb_param_file

    def sched_parameter(self):
        """
        Returns file with requirements and scheduler-specific parameters
        """
        index = int(common.jobDB.nJobs()) - 1
        job = common.job_list[index]
        jbt = job.type()
        
        lastDest=''
        first = []
        last  = []
        for n in range(common.jobDB.nJobs()):
            currDest=common.jobDB.destination(n)
            if (currDest!=lastDest):
                lastDest = currDest
                first.append(n)
                if n != 0:last.append(n-1) 
        if len(first)>len(last) :last.append(common.jobDB.nJobs())
  
        req = ''
        req = req + jbt.getRequirements()
   
  
        if self.EDG_requirements:
            if (not req == ' '): req = req +  ' && '
            req = req + self.EDG_requirements

        if self.EDG_ce_white_list:
            ce_white_list = string.split(self.EDG_ce_white_list,',')
            tmpCe=[]
            concString = '&&'
            for ce in ce_white_list:
                tmpCe.append('RegExp("' + string.strip(ce) + '", other.GlueCEUniqueId)')
            ### MATTY' FIX: if more then one CE: && -> ||
            #print "list CE: " + str(tmpCe)
            if len(tmpCe) == 1:
                req +=  " && (" + concString.join(tmpCe) + ") "
            elif len(tmpCe) > 1:
                firstCE = 0
                for reqTemp in tmpCe:
                    #print reqTemp
                    if firstCE == 0:
                        #print "adding: "+str(" && ( (" + reqTemp + ") ")
                        req += " && ( (" + reqTemp + ") "
                        firstCE = 1
                    elif firstCE > 0:
                        #print "adding: "+str(" || (" + reqTemp + ") ")
                        req += " || (" + reqTemp + ") "
                if firstCE > 0:
                    req += ") "
            ## old code
#            if len(tmpCe): req = req + " && (" + concString.join(tmpCe) + ") "
        
        if self.EDG_ce_black_list:
            ce_black_list = string.split(self.EDG_ce_black_list,',')
            tmpCe=[]
            concString = '&&'
            for ce in ce_black_list:
                tmpCe.append('(!RegExp("' + string.strip(ce) + '", other.GlueCEUniqueId))')
            if len(tmpCe): req = req + " && (" + concString.join(tmpCe) + ") "

        if self.EDG_clock_time:
            if (not req == ' '): req = req + ' && '
            req = req + 'other.GlueCEPolicyMaxWallClockTime>='+self.EDG_clock_time

        if self.EDG_cpu_time:
            if (not req == ' '): req = req + ' && '
            req = req + ' other.GlueCEPolicyMaxCPUTime>='+self.EDG_cpu_time
                 
        for i in range(len(first)): # Add loop DS
            self.param='sched_param_'+str(i)+'.clad'
            param_file = open(common.work_space.shareDir()+'/'+self.param, 'w')

            itr4=self.findSites_(first[i])
            reqSites=''
            reqtmp=[]  
            concString = '||'

            #############
            # MC Changed matching syntax to avoid gang matching
            #############
            for arg in itr4:
                reqtmp.append(' Member("'+arg+'" , other.GlueCESEBindGroupSEUniqueID) ')

            if len(reqtmp): reqSites = reqSites + " && (" + concString.join(reqtmp) + ") "

            # requirement added to skip gliteCE
            reqSites = reqSites + '&& (!RegExp("blah", other.GlueCEUniqueId));\n'

            param_file.write('Requirements = ' + req + reqSites )
   
            if (self.rb_param_file != ''):
                param_file.write(self.rb_param_file)   

            if len(self.EDG_addJdlParam):
                for p in self.EDG_addJdlParam:
                    param_file.write(p)

            param_file.close()   

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
        txt += "NJob=${args[0]}\n"

        txt += '# job identification to DashBoard \n'
        txt += 'MonitorJobID=`echo ${NJob}_$GLITE_WMS_JOBID`\n'
        txt += 'SyncGridJobId=`echo $GLITE_WMS_JOBID`\n'
        txt += 'MonitorID=`echo ' + self._taskId + '`\n'
        txt += 'echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "SyncGridJobId=`echo $SyncGridJobId`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'

        txt += 'echo "middleware discovery " \n'
        txt += 'if [ $VO_CMS_SW_DIR ]; then \n'
        txt += '    middleware=LCG \n'
        txt += '    echo "SyncCE=`glite-brokerinfo getCE`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "middleware =$middleware" \n'
        txt += 'elif [ $GRID3_APP_DIR ]; then\n'
        txt += '    middleware=OSG \n'
        txt += '    echo "SyncCE=`echo $GLITE_WL_LOG_DESTINATION`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "middleware =$middleware" \n'
        txt += 'elif [ $OSG_APP ]; then \n'
        txt += '    middleware=OSG \n'
        txt += '    echo "SyncCE=`echo $GLITE_WL_LOG_DESTINATION`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "GridFlavour=`echo $middleware`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "middleware =$middleware" \n'
        txt += 'else \n'
        txt += '    echo "SET_CMS_ENV 10030 ==> middleware not identified" \n'
        txt += '    echo "JOB_EXIT_STATUS = 10030" \n'
        txt += '    echo "JobExitCode=10030" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    dumpStatus $RUNTIME_AREA/$repo \n'
        txt += '    rm -f $RUNTIME_AREA/$repo \n'
        txt += '    echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '    echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '    exit 1 \n'
        txt += 'fi \n'

        txt += '# report first time to DashBoard \n'
        txt += 'dumpStatus $RUNTIME_AREA/$repo \n'
        txt += 'rm -f $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += 'echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        
        txt += '\n\n'

        #if int(self.copy_data) == 1:
        #   if self.SE:
        #      txt += 'export SE='+self.SE+'\n'
        #      txt += 'echo "SE = $SE"\n'
        #   if self.SE_PATH:
        #      if ( self.SE_PATH[-1] != '/' ) : self.SE_PATH = self.SE_PATH + '/'
        #      txt += 'export SE_PATH='+self.SE_PATH+'\n'
        #      txt += 'echo "SE_PATH = $SE_PATH"\n'

        txt += 'export VO='+self.VO+'\n'
        ### some line for LFC catalog setting 
        txt += 'if [ $middleware == LCG ]; then \n'
        txt += '    if [[ $LCG_CATALOG_TYPE != \''+self.lcg_catalog_type+'\' ]]; then\n'
        txt += '        export LCG_CATALOG_TYPE='+self.lcg_catalog_type+'\n'
        txt += '    fi\n'
        txt += '    if [[ $LFC_HOST != \''+self.lfc_host+'\' ]]; then\n'
        txt += '        export LFC_HOST='+self.lfc_host+'\n'
        txt += '    fi\n'
        txt += '    if [[ $LFC_HOME != \''+self.lfc_home+'\' ]]; then\n'
        txt += '        export LFC_HOME='+self.lfc_home+'\n'
        txt += '    fi\n'
        txt += 'elif [ $middleware == OSG ]; then\n'
        txt += '    echo "LFC catalog setting to be implemented for OSG"\n'
        txt += 'fi\n'
        #####
        if int(self.register_data) == 1:
           txt += 'if [ $middleware == LCG ]; then \n'
           txt += '    export LFN='+self.LFN+'\n'
           txt += '    lfc-ls $LFN\n' 
           txt += '    result=$?\n' 
           txt += '    echo $result\n' 
           ### creation of LFN dir in LFC catalog, under /grid/cms dir  
           txt += '    if [ $result != 0 ]; then\n'
           txt += '       lfc-mkdir $LFN\n'
           txt += '       result=$?\n' 
           txt += '       echo $result\n' 
           txt += '    fi\n'
           txt += 'elif [ $middleware == OSG ]; then\n'
           txt += '    echo " Files registration to be implemented for OSG"\n'
           txt += 'fi\n'
           txt += '\n'

           if self.VO:
              txt += 'export VO='+self.VO+'\n'
           if self.LFN:
              txt += 'if [ $middleware == LCG ]; then \n'
              txt += '    export LFN='+self.LFN+'\n'
              txt += 'fi\n'
              txt += '\n'

        txt += 'if [ $middleware == LCG ]; then\n' 
        txt += '    CloseCEs=`glite-brokerinfo getCE`\n'
        txt += '    echo "CloseCEs = $CloseCEs"\n'
        txt += '    CE=`echo $CloseCEs | sed -e "s/:.*//"`\n'
        txt += '    echo "CE = $CE"\n'
        txt += 'elif [ $middleware == OSG ]; then \n'
        txt += '    if [ $OSG_JOB_CONTACT ]; then \n'
        txt += '        CE=`echo $OSG_JOB_CONTACT | /usr/bin/awk -F\/ \'{print $1}\'` \n'
        txt += '    else \n'
        txt += '        echo "SET_CMS_ENV 10099 ==> OSG mode: ERROR in setting CE name from OSG_JOB_CONTACT" \n'
        txt += '        echo "JOB_EXIT_STATUS = 10099" \n'
        txt += '        echo "JobExitCode=10099" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '        dumpStatus $RUNTIME_AREA/$repo \n'
        txt += '        rm -f $RUNTIME_AREA/$repo \n'
        txt += '        echo "MonitorJobID=`echo $MonitorJobID`" | tee -a $RUNTIME_AREA/$repo \n'
        txt += '        echo "MonitorID=`echo $MonitorID`" | tee -a $RUNTIME_AREA/$repo\n'
        txt += '        exit 1 \n'
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

    def findSites_(self, n):
        itr4 = []
        sites = common.jobDB.destination(n)
        if len(sites)>0 and sites[0]=="":
            return itr4
        if sites != [""]: 
            ##Addedd Daniele
            replicas = self.blackWhiteListParser.checkBlackList(sites,n)
            if len(replicas)!=0:
                replicas = self.blackWhiteListParser.checkWhiteList(replicas,n)
              
            if len(replicas)==0:
                msg = 'No sites remaining that host any part of the requested data! Exiting... '
                raise CrabException(msg)
            itr4 = replicas 
            #####         
        return itr4

    def submitTout(self, list):
        return 180
