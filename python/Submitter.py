from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import Statistic
from random import random
import time

class Submitter(Actor):
    def __init__(self, cfg_params, nj_list):
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        
        try:
            self.ML = int(cfg_params['USER.activate_monalisa'])
        except KeyError:
            self.ML = 0
            pass

        return
    
    def run(self):
        """
        The main method of the class.
        """
        common.logger.debug(5, "Submitter::run() called")

        totalCreatedJobs= 0
        listCE = ""
        start = time.time()
        for nj in range(common.jobDB.nJobs()):
            if (common.jobDB.status(nj)=='C') or (common.jobDB.status(nj)=='RC'): totalCreatedJobs +=1
            pass

        if (totalCreatedJobs==0):
            common.logger.message("No jobs to be submitted: first create them")
            return
        
        firstJob=self.nj_list[0]
        match = common.scheduler.listMatch(firstJob)
        if match:
            common.logger.message("Found "+str(match)+" compatible site(s)")
        else:
            raise CrabException("No compatible site found!")
        #########
        # Loop over jobs
        njs = 0
        try:
            for nj in self.nj_list:
                st = common.jobDB.status(nj)
#                print "nj = ", nj 
#                print "st = ", st
                if st != 'C' and st != 'K' and st != 'A' and st != 'RC':
                    long_st = crabJobStatusToString(st)
                    msg = "Job # %d not submitted: status %s"%(nj+1, long_st)
                    common.logger.message(msg)
                    continue

                common.logger.message("Submitting job # "+`(nj+1)`)
                jid = common.scheduler.submit(nj)

                common.jobDB.setStatus(nj, 'S')
                common.jobDB.setJobId(nj, jid)
                common.jobDB.setTaskId(nj, self.cfg_params['taskId'])
                njs += 1

                ############################################   

                if st == 'C':
                    resFlag = 0
                elif st == 'RC':
                    resFlag = 2
                else:            
                    resFlag = 0
                    pass
                  
                try:
                    Statistic.Monitor('submit',resFlag,jid,'-----')
                except:
                    pass
                
                fl = open(common.work_space.shareDir() + '/' + self.cfg_params['apmon'].fName, 'r')
                self.cfg_params['sid'] = jid
                #### FF: per il momento commentiamo nevtJob che non c'e' piu' nel jobdb
                #nevtJob = common.jobDB.maxEvents(nj)

                # OLI: JobID treatment, special for Condor-G scheduler
                jobId = ''
                if common.scheduler.boss_scheduler_name == 'condor_g':
                    # create hash of cfg file
                    hash = makeCksum(common.work_space.cfgFileName())
                    jobId = str(nj + 1) + '_' + hash + '_' + self.cfg_params['sid']
                    common.logger.debug(5,'JobID for ML monitoring is created for CONDOR_G scheduler:'+jobId)
                else:
                    jobId = str(nj + 1) + '_' + self.cfg_params['sid']
                    common.logger.debug(5,'JobID for ML monitoring is created for EDG scheduler'+jobId)

                if ( jid.find(":") != -1 ) :
                    rb = jid.split(':')[1]
                    self.cfg_params['rb'] = rb.replace('//', '')
                else :
                    self.cfg_params['rb'] = 'OSG'

                #### FF: per il momento commentiamo nevtJob che non c'e' piu' nel jobdb
                #params = {'nevtJob': nevtJob, 'jobId': jobId, 'sid': self.cfg_params['sid'], \
                #          'broker': self.cfg_params['rb'], 'bossId': common.jobDB.bossId(nj)}
                params = {'jobId': jobId, 'sid': self.cfg_params['sid'], \
                          'broker': self.cfg_params['rb'], 'bossId': common.jobDB.bossId(nj)}
                for i in fl.readlines():
                    val = i.split(':')
                    params[val[0]] = string.strip(val[1])
                self.cfg_params['apmon'].sendToML(params)
        except:
            exctype, value = sys.exc_info()[:2]
            print "Type: %s Value: %s"%(exctype, value)
            common.logger.message("Submitter::run Exception raised: %s %s"%(exctype, value))
            common.jobDB.save()
        
        stop = time.time()
        common.logger.debug(1, "Submission Time: "+str(stop - start))
        #print "Submission Time: %d "%(stop - start)
        common.jobDB.save()
            
        msg = '\nTotal of %d jobs submitted'%njs
        if njs != len(self.nj_list) :
            msg += ' (from %d requested).'%(len(self.nj_list))
            pass
        else:
            msg += '.'
            pass
        common.logger.message(msg)
        return
