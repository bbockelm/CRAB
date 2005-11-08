from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import Statistic
#
# Marco
#
from random import random

class Submitter(Actor):
    def __init__(self, cfg_params, nj_list):
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        try:
            self.ML = int(cfg_params['USER.activate_monalisa'])
        except KeyError:
            self.ML = 0
            pass
            
        if (self.ML==1): self.mon = ApmonIf()
        
        return
    
    def run(self):
        """
        The main method of the class.
        """
        common.logger.debug(5, "Submitter::run() called")

        totalCreatedJobs= 0
        for nj in range(common.jobDB.nJobs()):
            if (common.jobDB.status(nj)=='C'): totalCreatedJobs +=1
            pass

        if (totalCreatedJobs==0):
            common.logger.message("No jobs to be submitted: first create them")
        
        firstJob=self.nj_list[0]
        match = common.scheduler.listMatch(firstJob)
        if match:
            common.logger.message("Found compatible resources "+str(match))
        else:
            raise CrabException("No compatible resources found!")
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
                #    print 'submitter prima	'

                Statistic.Monitor('submit',resFlag,jid,'-----')  
                #    print 'submitter Dopo     '
 

                #except:
                #    pass
                
                if (self.ML==1):
                    try:
                        # SL this is crap! Should not be here!!!!
                        #List of parameters to be sent to ML monitor system
                        user = os.getlogin()
                        #
                        # Marco
                        #
                        #taskId = os.getlogin()+'_'+string.split(common.work_space.topDir(),'/')[-2] 
                        taskId = os.getlogin()+'_'+self.cfg_params['USER.dataset']+'_'+self.cfg_params['USER.owner']+'_'+str(random()*100)
                        dataset = self.cfg_params['USER.dataset']
                        owner = self.cfg_params['USER.owner']
                        jobId = str(nj)
                        sid = jid
                        try:
                            application = os.path.basename(os.environ['SCRAMRT_LOCALRT'])
                        except KeyError:
                            application = os.path.basename(os.environ['LOCALRT'])

                        nevtJob = common.jobDB.maxEvents(nj)
                        exe = self.cfg_params['USER.executable']
                        tool = common.prog_name
                        scheduler = self.cfg_params['CRAB.scheduler']
                        taskType = 'analysis'
                        vo = 'cms'
                        rb = sid.split(':')[1]
                        rb = rb.replace('//', '')
                        params = {'taskId': taskId, 'jobId': jobId, 'sid': sid, 'application': application, \
                                  'exe': exe, 'nevtJob': nevtJob, 'tool': tool, 'scheduler': scheduler, \
                                  'user': user, 'taskType': taskType, 'vo': vo, 'dataset': dataset, 'owner': owner, 'broker': rb}
                        self.mon.fillDict(params)
                        self.mon.sendToML()
                    except:
                        exctype, value = sys.exc_info()[:2]
                        common.logger.message("Submitter::run Exception raised: %s %s"%(exctype, value))
                        pass
                pass # use ML
        except:
            exctype, value = sys.exc_info()[:2]
            print "Type:%s Value:%s"%(exctype, value)
            common.logger.message("Submitter::run Exception raised: %s %s"%(exctype, value))
            common.jobDB.save()

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
    








