
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
        sys.stderr.write("python/SchedulerArc.__init__\n")
        SchedulerGrid.__init__(self,name)
        return

    def envUniqueID(self):
        taskHash = sha.new(common._db.queryTask('name')).hexdigest()
        id = 'https://' + self.name() + '/' + taskHash + '/${NJob}'
        msg = 'JobID for ML monitoring is created for ARC scheduler: %s' % id
        common.logger.debug(5, msg)
        return id

    def realSchedParams(self,cfg_params):
        """
        """
        sys.stderr.write("python/SchedulerArc.realSchedParams\n")
        return {}


    def configure(self,cfg_params):

        if not os.environ.has_key('EDG_WL_LOCATION'):
            # This is an ugly hack needed for SchedulerGrid.configure() to
            # work!
            os.environ['EDG_WL_LOCATION'] = ''

        SchedulerGrid.configure(self, cfg_params)
        self.environment_unique_identifier = None


    def ce_list(self):
        """
        Returns string with requirement CE related
        """
        sys.stderr.write("python/SchedulerArc.ce_list\n")

        ceParser = CEBlackWhiteListParser(self.EDG_ce_white_list,
                                          self.EDG_ce_black_list, common.logger)
        req = ''
        ce_white_list = []
        ce_black_list = []

        if self.EDG_ce_white_list:
            ce_white_list = ceParser.whiteList()
            tmpCe=[]
            for ce in ce_white_list:
                tmpCe.append('RegExp("' + string.strip(ce) + '", other.GlueCEUniqueId)')
            if len(tmpCe) == 1:
                req +=  " && ( " + tmpCe[0] + " ) "
            elif len(tmpCe) > 1:
                concString = ") || ("
                req += " && ( (" + concString.join(tmpCe) +") )"
                # Do we need all those parentesis above? Or could we do:
                #concString = " || "
                #req += " && ( " + concString.join(tmpCe) +" )"

        if self.EDG_ce_black_list:
            ce_black_list = ceParser.blackList()
            tmpCe=[]
            concString = '&&'
            for ce in ce_black_list:
                tmpCe.append('(!RegExp("' + string.strip(ce) + '", other.GlueCEUniqueId))')
            if len(tmpCe): req += " && (" + concString.join(tmpCe) + ") "

        ## requirement added to skip gliteCE
        #req += '&& (!RegExp("blah", other.GlueCEUniqueId))'

        retWL = ','.join(ce_white_list)
        retBL = ','.join(ce_black_list)
        if not retWL:
            retWL = None
        if not retBL:
            retBL = None

        sys.stderr.write("ce_list: %s, %s, %s\n" % (req, str(retWL), str(retBL)))

        return req, retWL, retBL


    def se_list(self, id, dest):
        sys.stderr.write("python/SchedulerArc.se_list\n")
        se_white = self.blackWhiteListParser.whiteList()
        se_black = self.blackWhiteListParser.blackList()
        return '', se_white, se_black


    def sched_parameter(self,i,task):
        """
        Returns parameter scheduler-specific, to use with BOSS .
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

        s = ""
        if len(ce_list) > 0:

            # A ce-list with more than one element must be an OR:ed
            # list: (|(cluster=ce1)(cluster=ce2)...)
            if len(ce_list) > 1:
                s += '(|'
            for ce in ce_list:
                s += '(cluster=%s)' % ce
            if len(ce_list) > 1:
                s += ')'

        # FIXME: If len(ce_list) == 0  ==>  s = ""  ==>  we'll submit
        # "anywhere", which is completely contrary behaviour to what we want!
        # len(ce_list) == 0 means there were _no_ CE in ce_infoSys that
        # survived the white- and black-list filter, so we shouldn't submit
        # at all!

        return s

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
        txt += '    if [ $exceed -ne 1 ]; then\n'
        txt += '        tar zcvf ${out_files}.tgz  ${final_list}\n'
        txt += '    else\n'
        txt += '        tar zcvf ${out_files}.tgz CMSSW_${NJob}.stdout CMSSW_${NJob}.stderr\n'
        txt += '    fi\n'
        txt += '    exit $job_exit_code\n'
        txt += '}\n'
        return txt


    def tags(self):
        sys.stderr.write("python/SchedulerArc.tags\n")
        return ''

    def submit(self,list,task):
        """ submit to scheduler a list of jobs """
        sys.stderr.write("python/SchedulerArc.submit\n")
        if (not len(list)):
            common.logger.message("No sites where to submit jobs")
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
        req=str(self.sched_parameter(list[0],task))
        new_list = bulkControl(self,list)
        jdl=[]
        for sub_list in new_list:
            tmp_jdl =  self.boss().writeJDL(task['id'], sub_list, req)
            jdl.append(tmp_jdl)
        return jdl
