from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
#from random import random
import time
from ProgressBar import ProgressBar
from TerminalController import TerminalController

class Submitter(Actor):
    def __init__(self, cfg_params, parsed_range, val):
        self.cfg_params = cfg_params

        # get user request
        nsjobs = -1
        chosenJobsList = None
        if val:
            if val=='all': 
                pass
            elif (type(eval(val)) is int) and eval(val) > 0:
                # positive number
                nsjobs = eval(val)
            # NEW PART # Fabio
            # put here code for LIST MANAGEMEN
            elif (type(eval(val)) is tuple)or( type(eval(val)) is int and eval(val)<0 ) :
                chosenJobsList = parsed_range
                chosenJobsList = [i-1 for i in chosenJobsList ]
                nsjobs = len(chosenJobsList) 
            #
            else:
                msg = 'Bad submission option <'+str(val)+'>\n'
                msg += '      Must be an integer or "all"'
                msg += '      Generic range is not allowed"'
                raise CrabException(msg)
            pass
     
        common.logger.debug(5,'nsjobs '+str(nsjobs))
        # total jobs
        nj_list = []
        # get the first not already submitted
        common.logger.debug(5,'Total jobs '+str(common.jobDB.nJobs()))
        jobSetForSubmission = 0
        jobSkippedInSubmission = []
        datasetpath=self.cfg_params['CMSSW.datasetpath']

        # NEW PART # Fabio
        # modified to handle list of jobs by the users # Fabio
        tmp_jList = range(common.jobDB.nJobs())
        if chosenJobsList != None:
            tmp_jList = chosenJobsList
        # build job list
        from BlackWhiteListParser import BlackWhiteListParser
        self.blackWhiteListParser = BlackWhiteListParser(self.cfg_params)
        for nj in tmp_jList:
            cleanedBlackWhiteList = self.blackWhiteListParser.cleanForBlackWhiteList(common.jobDB.destination(nj)) # More readable # Fabio
            if (cleanedBlackWhiteList != '') or (datasetpath == "None" ) or (datasetpath == None): ## Matty's fix
                if (common.jobDB.status(nj) not in ['R','S','K','Y','A','D','Z']):
                    jobSetForSubmission +=1
                    nj_list.append(nj)
                else: 
                    continue
            else :
                jobSkippedInSubmission.append(nj+1)
            #
            if nsjobs >0 and nsjobs == jobSetForSubmission:
                break
            pass
        del tmp_jList
        #

        if nsjobs>jobSetForSubmission:
            common.logger.message('asking to submit '+str(nsjobs)+' jobs, but only '+str(jobSetForSubmission)+' left: submitting those')
        if len(jobSkippedInSubmission) > 0 :
            #print jobSkippedInSubmission
            #print spanRanges(jobSkippedInSubmission)
            mess =""
            for jobs in jobSkippedInSubmission:
                mess += str(jobs) + ","
            common.logger.message("Jobs:  " +str(mess) + "\n      skipped because no sites are hosting this data\n")
            self.submissionError()
            pass
        # submit N from last submitted job
        common.logger.debug(5,'nj_list '+str(nj_list))
                 
        
        if common.scheduler.name() == 'CONDOR_G':
            # create hash of cfg file
            self.hash = makeCksum(common.work_space.cfgFileName())
        else:
            self.hash = ''

        self.nj_list = nj_list

        self.UseServer=int(self.cfg_params.get('CRAB.server_mode',0))

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

        # submit pre DashBoard information
        params = {'jobId':'TaskMeta'}
 
        fl = open(common.work_space.shareDir() + '/' + common.apmon.fName, 'r')
        for i in fl.readlines():
            key, val = i.split(':')
            params[key] = string.strip(val)
        fl.close()

        common.logger.debug(5,'Submission DashBoard Pre-Submission report: '+str(params))
                        
        common.apmon.sendToML(params)

        # modified to support server mode
        # The boss declare step is performed here 
        # only if  crab is used server mode 
        if (self.UseServer== 9999):
            if not common.scheduler.taskDeclared( common.taskDB.dict('projectName') ): #os.path.basename(os.path.split(common.work_space.topDir())[0]) ):
                common.logger.debug(5,'Declaring jobs to BOSS')
                common.scheduler.declare()   #Add for BOSS4
            else:
                common.logger.debug(5,'Jobs already declared into BOSS')
            common.jobDB.save()
            common.taskDB.save()
                                                                                                                                               
        #########
        #########
        # Loop over jobs
        njs = 0
        try:
            list=[]
            list_of_list = []   
            lastBlock=-1
            count = 0
            for nj in self.nj_list:
                same=0
                # first check that status of the job is suitable for submission
                st = common.jobDB.status(nj)
                if st != 'C'  and st != 'K' and st != 'A' and st != 'RC':
                    long_st = crabJobStatusToString(st)
                    msg = "Job # %d not submitted: status %s"%(nj+1, long_st)
                    common.logger.message(msg)
                    continue
     
                currBlock = common.jobDB.block(nj)
                # SL perform listmatch only if block has changed
                if (currBlock!=lastBlock):
                    if common.scheduler.name() != "CONDOR_G" :
                        ### SL TODO to be moved in blackWhiteListParser 
                        ### MATTY:  patch for white-black list with the list-mathc in glite ###
                        whiteL = []
                        blackL = []
                        if self.cfg_params['CRAB.scheduler'].find("glite") != -1:
                            if 'EDG.ce_white_list' in self.cfg_params.keys():
                                #print self.cfg_params['EDG.ce_white_list'].strip().split(",")
                                if self.cfg_params['EDG.ce_white_list'].strip() != "" and self.cfg_params['EDG.ce_white_list'] != None:
                                    for ceW in self.cfg_params['EDG.ce_white_list'].strip().split(","):
                                        if len(ceW.strip()) > 0 and ceW.strip() != None:
                                            whiteL.append(ceW.strip())
                                        #print "ADDING white ce = "+str(ceW.strip())
                            if 'EDG.ce_black_list' in self.cfg_params.keys():
                                #print self.cfg_params['EDG.ce_black_list'].strip().split(",")
                                if self.cfg_params['EDG.ce_black_list'].strip() != "" and self.cfg_params['EDG.ce_black_list'] != None:
                                    for ceB in self.cfg_params['EDG.ce_black_list'].strip().split(","):
                                        if len(ceB.strip()) > 0 and ceB.strip() != None:
                                            blackL.append(ceB.strip())
                                        #print "ADDING ce = "+str(ceB.strip())
                        match = common.scheduler.listMatch(nj, currBlock, whiteL, blackL)
                        #######################################################################
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
                    list.append(common.jobDB.bossId(nj))
     
                    if nj == self.nj_list[-1]: # check that is not the last job in the list
                        list_of_list.append([currBlock,list])
                    else: # check if next job has same group
                        nextBlock = common.jobDB.block(nj+1)
                        if  currBlock != nextBlock : # if not, close this group and reset
                            list_of_list.append([currBlock,list])
                            list=[]
                else:
                    common.logger.message("No compatible site found, will not submit job "+str(nj+1))
                    self.submissionError()
                    continue
                count += 1
            ### Progress Bar indicator, deactivate for debug
            if not common.logger.debugLevel() :
                term = TerminalController()
     
            for ii in range(len(list_of_list)): # Add loop DS
                common.logger.debug(1,'Submitting jobs '+str(list_of_list[ii][1]))
                if not common.logger.debugLevel() :
                    try: pbar = ProgressBar(term, 'Submitting '+str(len(list_of_list[ii][1]))+' jobs')
                    except: pbar = None

                jidLista, bjidLista = common.scheduler.submit(list_of_list[ii])
                bjidLista = map(int, bjidLista) # cast all bjidLista to int

                if not common.logger.debugLevel():
                    if pbar :
                        pbar.update(float(ii+1)/float(len(list_of_list)),'please wait')
     
                for jj in bjidLista: # Add loop over SID returned from group submission  DS
                    tmpNj = jj - 1

                    jid=jidLista[bjidLista.index(jj)]
                    common.logger.debug(5,"Submitted job # "+ `(jj)`)
                    common.jobDB.setStatus(tmpNj, 'S')
                    common.jobDB.setJobId(tmpNj, jid)
                    common.jobDB.setTaskId(tmpNj, common.taskDB.dict('taskId'))

                    njs += 1
               
                    ##### DashBoard report #####################   
                    ## To distinguish if job is direct or through the server   
                    if (self.UseServer == 0):
                        Sub_Type = 'Direct'
                    else:   
                        Sub_Type = 'Server'
               
                    try:
                        resFlag = 0
                        if st == 'RC': resFlag = 2
                    except:
                        pass
                    
                    # OLI: JobID treatment, special for Condor-G scheduler
                    jobId = ''
                    localId = ''
                    if common.scheduler.name() == 'CONDOR_G':
                        rb = 'OSG'
                        jobId = str(jj) + '_' + self.hash + '_' + jid
                        common.logger.debug(5,'JobID for ML monitoring is created for CONDOR_G scheduler:'+jobId)
                    elif common.scheduler.name() == 'lsf' or common.scheduler.name() == 'caf':
                        jobId="https://"+common.scheduler.name()+":/"+jid+"-"+string.replace(common.taskDB.dict('taskId'),"_","-")+"-"+str(jj)
                        common.logger.debug(5,'JobID for ML monitoring is created for LSF scheduler:'+jobId)
                        rb = common.scheduler.name()
                        localId = jid
                    else:
                        jobId = str(jj) + '_' + jid
                        common.logger.debug(5,'JobID for ML monitoring is created for EDG scheduler'+jobId)
                        rb = jid.split(':')[1]
                        rb = rb.replace('//', '')

                    if len(common.jobDB.destination(tmpNj)) <= 2 :
                        T_SE=string.join((common.jobDB.destination(tmpNj)),",")    
                    else :
                        T_SE=str(len(common.jobDB.destination(tmpNj)))+'_Selected_SE'

                    params = {'jobId': jobId, \
                              'sid': jid, \
                              'broker': rb, \
                              'bossId': jj, \
                              'SubmissionType': Sub_Type, \
                              'TargetSE': T_SE, \
                              'localId' : localId}
                    common.logger.debug(5,str(params))
               
                    fl = open(common.work_space.shareDir() + '/' + common.apmon.fName, 'r')
                    for i in fl.readlines():
                        key, val = i.split(':')
                        params[key] = string.strip(val)
                    fl.close()
     
                    common.logger.debug(5,'Submission DashBoard report: '+str(params))
                        
                    common.apmon.sendToML(params)
                pass
            pass

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

        if (njs < len(self.nj_list) or len(self.nj_list)==0):
            self.submissionError()
        return

    def submissionError(self):
        ## add some more verbose message in case submission is not complete
        msg =  'Submission performed using the Requirements: \n'
        msg += common.taskDB.dict("jobtype")+' version: '+common.taskDB.dict("codeVersion")+'\n'
        if self.cfg_params.has_key('EDG.se_white_list'):
            msg += 'SE White List: '+self.cfg_params['EDG.se_white_list']+'\n'
        if self.cfg_params.has_key('EDG.se_black_list'):
            msg += 'SE Black List: '+self.cfg_params['EDG.se_black_list']+'\n'
        if self.cfg_params.has_key('EDG.ce_white_list'):
            msg += 'CE White List: '+self.cfg_params['EDG.ce_white_list']+'\n'
        if self.cfg_params.has_key('EDG.ce_black_list'):
            msg += 'CE Black List: '+self.cfg_params['EDG.ce_black_list']+'\n'
        msg += '(Hint: please check if '+common.taskDB.dict("jobtype")+' is available at the Sites)\n'

        common.logger.message(msg)
            
            
        return
