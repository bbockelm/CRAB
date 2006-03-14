from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import Statistic
from random import random
import time

class Submitter(Actor):
    # marco
    def __init__(self, cfg_params, nj_list, job_type):
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        self.job_type = job_type
        
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
        # marco. Common job type parameters to be sent to ML and Daniele's Monitor
        jobtype_p = self.job_type.getParams()
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
            common.logger.message("Found "+str(match)+"compatible sites")
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
                  
                #try:
                #    print 'submitter prima '

#                Statistic.Monitor('submit',resFlag,jid,'-----', jobtype_p)
                Statistic.Monitor('submit',resFlag,jid,'-----')
                #    print 'submitter Dopo     '
 

                #except:
                #    pass
                
                if (self.ML==1):
                    try:
                        #List of parameters to be sent to ML monitor system
                        # Marco. Should be better to put it in the SchedulerEdg/gLite class 
                        listCE = ','.join(common.analisys_common_info['sites'])
                        self.cfg_params['GridName'] = runCommand("grid-proxy-info -identity")
                        common.logger.debug(5, "GRIDNAME: "+self.cfg_params['GridName'])
                        self.cfg_params['jobId'] = str(nj + 1)
                        self.cfg_params['sid'] = jid
                        nevtJob = common.jobDB.maxEvents(nj)
                        taskType = 'analysis'
                        rb = jid.split(':')[1]
                        self.cfg_params['rb'] = rb.replace('//', '')

                        params = {'jobId': str(nj + 1) + '_' + self.cfg_params['sid'] ,'taskId': self.job_type.getTaskid(), 'sid': self.cfg_params['sid'], \
                                  'nevtJob': nevtJob, 'tool': common.prog_name, 'tool_ui': os.environ['HOSTNAME'], \
                                  'scheduler': self.cfg_params['CRAB.scheduler'], 'GridName': self.cfg_params['GridName'], 'taskType': taskType, \
                                  'vo': self.cfg_params['EDG.virtual_organization'], 'broker': self.cfg_params['rb'], 'user': self.cfg_params['user'], 'TargetCE': listCE, 'bossId': common.jobDB.bossId(nj)}
                        for i in jobtype_p.iterkeys():
                            params[i] = jobtype_p[i]
#                        for j, k in params.iteritems():
#                            print "Values: %s %s"%(j, k)
                        self.cfg_params['apmon'].fillDict(params)
                        self.cfg_params['apmon'].sendToML()
                    except:
                        exctype, value = sys.exc_info()[:2]
                        common.logger.message("Submitter::run Exception raised: %s %s"%(exctype, value))
                        pass
                pass # use ML
            if (self.ML==1):
                self.cfg_params['apmon'].free()
        except:
            exctype, value = sys.exc_info()[:2]
            print "Type: %s Value: %s"%(exctype, value)
            common.logger.message("Submitter::run Exception raised: %s %s"%(exctype, value))
            common.jobDB.save()
        
        stop = time.time()
        common.logger.debug(5, "Submission Time: "+str(stop - start))
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
