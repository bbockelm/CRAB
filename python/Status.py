from Actor import *
import common, crab_util
import string, os
import Statistic

class Status(Actor):
    def __init__(self, cfg_params, nj_list):
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        self.countDone = 0
        self.countReady = 0
        self.countSched = 0
        self.countRun = 0
        self.countCleared = 0
        self.countToTjob = 0
        
        fileCODE1 = open(common.work_space.logDir()+"/.code","r")
        array = fileCODE1.read().split('::')
        self.ID1 = array[0]
        self.NJC = array[1]
        self.dataset = array[2]
        self.owner = array[3]
        fileCODE1.close()
        #######################################################################################



        Status = crab_util.importName('edg_wl_userinterface_common_LbWrapper', 'Status')
        # Bypass edg-job-status interfacing directly to C++ API
        # Job attribute vector to retrieve status without edg-job-status
        self.level = 0
        # Instance of the Status class provided by LB API
        self.jobStat = Status()

        self.states = [ "Acl", "cancelReason", "cancelling","ce_node","children", \
          "children_hist","children_num","children_states","condorId","condor_jdl", \
          "cpuTime","destination", "done_code","exit_code","expectFrom", \
          "expectUpdate","globusId","jdl","jobId","jobtype", \
          "lastUpdateTime","localId","location", "matched_jdl","network_server", \
          "owner","parent_job", "reason","resubmitted","rsl","seed",\
          "stateEnterTime","stateEnterTimes","subjob_failed", \
          "user tags" , "status" , "status_code","hierarchy"]
        self.hstates = {}
        for key in self.states:
            self.hstates[key]=''

        return

    def run(self):
        """
        The main method of the class.
        """
        common.logger.debug(5, "Status::run() called")

        common.jobDB.load()
        for nj in self.nj_list:
            st = common.jobDB.status(nj)
            self.countToTjob = self.countToTjob + 1
            jid = common.jobDB.jobId(nj)
            if st == 'S':
                result = common.scheduler.queryStatus(jid)
                self.processResult_(nj, result, jid)
                exit = common.jobDB.exitStatus(nj)
                print 'Job %03d:'%(nj+1),jid,result,exit
                pass
            else:
                exit = common.jobDB.exitStatus(nj)
                print 'Job %03d:'%(nj+1),jid,crab_util.crabJobStatusToString(st),exit
                pass
            pass

        common.jobDB.save()

        self.Report_()
        pass


    def processResult_(self, nj, result,jid):

        destination = common.scheduler.queryDest(jid).split(":")[0]
        ID3 =  jid.split("/")[3]
        broker = jid.split("/")[2].split(":")[0]
        resFlag = 0
        ### TODO: set relevant status also to DB

        try: 
            if result == 'Done': 
                self.countDone = self.countDone + 1
                exCode = common.scheduler.getExitStatus(jid)
                common.jobDB.setStatus(nj, 'D')
                jid = common.jobDB.jobId(nj)
                exit = common.scheduler.getExitStatus(jid)
                common.jobDB.setExitStatus(nj, exit)
                Statistic.notify('checkstatus',resFlag,exCode,self.dataset,self.owner,destination,broker,ID3,self.ID1,self.NJC)
            elif result == 'Ready':
                self.countReady = self.countReady + 1
                Statistic.notify('checkstatus',resFlag,'-----',self.dataset,self.owner,destination,broker,ID3,self.ID1,self.NJC)
            elif result == 'Scheduled':
                self.countSched = self.countSched + 1
                Statistic.notify('checkstatus',resFlag,'-----',self.dataset,self.owner,destination,broker,ID3,self.ID1,self.NJC)
            elif result == 'Running':
                self.countRun = self.countRun + 1
                Statistic.notify('checkstatus',resFlag,'-----',self.dataset,self.owner,destination,broker,ID3,self.ID1,self.NJC)
            elif result == 'Aborted':
                common.jobDB.setStatus(nj, 'A')
                Statistic.notify('checkstatus',resFlag,'abort',self.dataset,self.owner,destination,broker,ID3,self.ID1,self.NJC)
                pass
            elif result == 'Cancelled':
                common.jobDB.setStatus(nj, 'K')
                Statistic.notify('checkstatus',resFlag,'cancel',self.dataset,self.owner,destination,broker,ID3,self.ID1,self.NJC)
                pass
            elif result == 'Cleared':
                exCode = common.scheduler.getExitStatus(jid) 
                Statistic.notify('checkstatus',resFlag,exCode,self.dataset,self.owner,destination,broker,ID3,self.ID1,self.NJC)
                self.countCleared = self.countCleared + 1
        except UnboundLocalError:
            common.logger.message('ERROR: UnboundLocalError with ')

    def Report_(self) :

        """ Report #jobs for each status  """  

        #job_stat = common.job_list.loadStatus()

        print ''
        print ">>>>>>>>> %i Total Jobs " % (self.countToTjob)

        if (self.countReady != 0):
            print ''
            print ">>>>>>>>> %i Jobs Ready" % (self.countReady)
        if (self.countSched != 0):
            print ''
            print ">>>>>>>>> %i Jobs Scheduled" % (self.countSched)
        if (self.countRun != 0):
            print ''
            print ">>>>>>>>> %i Jobs Running" % (self.countRun)
        if (self.countCleared != 0):
            print ''
            print ">>>>>>>>> %i Jobs Retrieved (=Cleared)" % (self.countCleared)
            print "          You can resubmit them specifying JOB numbers: crab.py -resubmit JOB_number (or range of JOB) -continue" 
            print "          (i.e -resubmit 1-3 => 1 and 2 and 3 or -resubmit 1,3 => 1 and 3)"       
        # if job_stat[6] or job_stat[7]:
        #     print ''
        #     print ">>>>>>>>> %i Jobs aborted or killed(=cancelled by user)" % (job_stat[6] + job_stat[7])
        #     print "          Resubmit them with: crab.py -resubmit -continue to resubmit all" 
        #     print "          or specifying JOB numbers (i.e -resubmit 1-3 => 1 and 2 and 3 or -resubmit 1,3 => 1 and 3)"       
        #     print "           "       
        if (self.countDone != 0):
            print ">>>>>>>>> %i Jobs Done" % (self.countDone)
            print "          Retrieve them with: crab.py -getoutput -continue to retrieve all" 
            print "          or specifying JOB numbers (i.e -getoutput 1-3 => 1 and 2 and 3 or -getoutput 1,3 => 1 and 3)"
            print('\n')  
        pass


