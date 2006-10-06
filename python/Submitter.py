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

        return
    
    def run(self):
        """
        The main method of the class: submit jobs in range self.nj_list
        """
        common.logger.debug(5, "Submitter::run() called")

        totalCreatedJobs= 0
        start = time.time()
        for nj in range(common.jobDB.nJobs()):
            if (common.jobDB.status(nj)=='C') or (common.jobDB.status(nj)=='RC'): totalCreatedJobs +=1
            pass

        if (totalCreatedJobs==0):
            common.logger.message("No jobs to be submitted: first create them")
            return
        #########
        # Loop over jobs
        njs = 0
        try:
            first = []
            last  = []
            lastBlock=-1
            for nj in self.nj_list:
                same=0
                # first check that status of the job is suitable for submission
                st = common.jobDB.status(nj)
                if st != 'C' :# and st != 'K' and st != 'A' and st != 'RC': ## commentato per ora...quindi NON risotomette
                    long_st = crabJobStatusToString(st)
                    msg = "Job # %d not submitted: status %s"%(nj+1, long_st)
                    common.logger.message(msg)
                    continue
                currBlock = common.jobDB.block(nj)
                # SL perform listmatch only if block has changed
                if (currBlock!=lastBlock):
                    if common.scheduler.boss_scheduler_name != "condor_g" :
                        match = common.scheduler.listMatch(nj, currBlock)
                    else :
                        match = "1"
                    lastBlock = currBlock
                else:
                    common.logger.debug(1,"Sites for job "+str(nj+1)+" the same as previous job")
                    same=1
                if match:
                    if not same:
                        ######################  here create index list start:end  
                        first.append(nj)
                        if nj != 0:last.append(nj-1) 
                        ######################
                        common.logger.message("Found "+str(match)+" compatible site(s) for job "+str(nj+1))
                    else:
                        common.logger.debug(1,"Found "+str(match)+" compatible site(s) for job "+str(nj+1))
                else:
                    common.logger.message("No compatible site found, will not submit job "+str(nj+1))
                    continue

            ############## TODO improve the follow control .....
            if len(first)>len(last): 
                if common.jobDB.nJobs() == 1 : 
                    last.append(0) # List of last index
                else: 
                    last.append(self.nj_list[len(self.nj_list)-1]) #List of last index 
            else:
                if first[len(first)-1] > last[len(last)-1]:
                    last.remove(last[len(last)-1])
                    last.append(self.nj_list[len(self.nj_list)-1])
               
            for ii in range(len(first)): # Add loop DS
                common.logger.message('Submitting job from '+str(first[ii]+1)+' to '+str(last[ii]+1))
                #common.logger.message("Submitting job # "+`(nj+1)`)  
                jidLista = common.scheduler.submit(first[ii],last[ii],ii)
       
                ####
                for jj in range(len(jidLista)): # Add loop over SID returned from group submission  DS
                    nj= int(jj+int(first[ii]))
                    jid=jidLista[jj]
                    common.logger.message("Submitted job # "+`(nj+1)`)
       
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
                    params = {'jobId': jobId, \
                              'sid': self.cfg_params['sid'], \
                              'broker': self.cfg_params['rb'], \
                              'bossId': common.jobDB.bossId(nj), \
                              'TargetCE': string.join((common.jobDB.destination(nj)),",")}
               
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
