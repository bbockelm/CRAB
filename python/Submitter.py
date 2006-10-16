from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import Statistic
#from random import random
import time
from ProgressBar import ProgressBar
from TerminalController import TerminalController

class Submitter(Actor):
    def __init__(self, cfg_params, nj_list):
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        
        if common.scheduler.boss_scheduler_name == 'condor_g':
            # create hash of cfg file
            self.hash = makeCksum(common.work_space.cfgFileName())
        else:
            self.hash = ''

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
            list=''
            list_of_list = []   
            lastBlock=-1
            count = 0
            for nj in self.nj_list:
                same=0
                # first check that status of the job is suitable for submission
                st = common.jobDB.status(nj)
                if st != 'C'  and st != 'K' and st != 'A' and st != 'RC': ## commentato per ora...quindi NON risotomette
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
                        common.logger.message("Found "+str(match)+" compatible site(s) for job "+str(nj+1))
                    else:
                        common.logger.debug(1,"Found "+str(match)+" compatible site(s) for job "+str(nj+1))
                   # job list is string because boss can't manage list  
                    list = list+str(nj+1)+',' 
                   # list.append(nj+1)
                    if nj < self.nj_list[len(self.nj_list)-1]:
                        nextBlock = common.jobDB.block(self.nj_list[count+1])
                        if  currBlock != nextBlock :
                            list_of_list.append([currBlock,list])
                            list=''    
                    else:
                        list_of_list.append([currBlock,list])
                else:
                    common.logger.message("No compatible site found, will not submit job "+str(nj+1))
                    continue
                count += 1
            ### Progress Bar indicator, deactivate for debug
            if not common.logger.debugLevel() :
                term = TerminalController()

            for ii in range(len(list_of_list)): # Add loop DS
                common.logger.message('Submitting jobs '+str(list_of_list[ii][1]))
                if not common.logger.debugLevel() :
                    try: pbar = ProgressBar(term, 'Submitting '+str(len(self.nj_list))+' jobs')
                    except: pbar = None
                #common.logger.message("Submitting job # "+`(nj+1)`)  
                jidLista = common.scheduler.submit(list_of_list[ii])
                ####
                for jj in range(len(jidLista)): # Add loop over SID returned from group submission  DS
                   # nj= int(jj+int(list[0]))
                    nj= int(str(list_of_list[ii][1]).split(',')[jj])-1
                    jid=jidLista[jj]
                    common.logger.debug(1,"Submitted job # "+`(nj+1)`)
                    common.jobDB.setStatus(nj, 'S')
                    common.jobDB.setJobId(nj, jid)
                    common.jobDB.setTaskId(nj, self.cfg_params['taskId'])
                    njs += 1
                    if not common.logger.debugLevel():
                        if pbar :
                            pbar.update(float(jj+1)/float(len(jidLista)),'please wait')
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
                        jobId = str(nj + 1) + '_' + self.hash + '_' + self.cfg_params['sid']
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

                    common.logger.debug(5,'Submission DashBoard report: '+str(params))
                        
                    self.cfg_params['apmon'].sendToML(params)

        except:
            exctype, value = sys.exc_info()[:2]
            print "Type: %s Value: %s"%(exctype, value)
            common.logger.message("Submitter::run Exception raised: %s %s"%(exctype, value))
            common.jobDB.save()
        
        stop = time.time()
        common.logger.debug(1, "Submission Time: "+str(stop - start))
        common.logger.write("Submission time :"+str(stop - start))
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
