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
            if val=='range':  # for Resubmitter
                chosenJobsList = parsed_range
            elif val=='all':
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
        common.logger.debug(5,'Total jobs '+str(common._db.nJobs()))
        jobSetForSubmission = 0
        jobSkippedInSubmission = []
        datasetpath=self.cfg_params['CMSSW.datasetpath']

        # NEW PART # Fabio
        # modified to handle list of jobs by the users # Fabio
        tmp_jList = range(common._db.nJobs())
        if chosenJobsList != None:
            tmp_jList = chosenJobsList
        # build job list
        from BlackWhiteListParser import BlackWhiteListParser
        self.blackWhiteListParser = BlackWhiteListParser(self.cfg_params)
        for nj in tmp_jList:
            jobs=[]
            jobs.append(nj)  
            cleanedBlackWhiteList = self.blackWhiteListParser.cleanForBlackWhiteList(common._db.queryJob('dlsDestination',jobs)) # More readable # Fabio
            if (cleanedBlackWhiteList != '') or (datasetpath == "None" ) or (datasetpath == None): ## Matty's fix
                if (common._db.queryRunJob('status',jobs) not in ['R','S','K','Y','A','D','Z']):
                    jobSetForSubmission +=1
                    nj_list.append(nj+1)## Warning added +1 for jobId BL--DS 
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


        if common.scheduler.name().upper() == 'CONDOR_G':
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

        totalCreatedJobs = 0
        start = time.time()
        for nj in range(common._db.nJobs()):
            jobs=[]
            jobs.append(nj)
            st = common._db.queryRunJob('status',jobs)[0]
            if ( st in ['C','RC']):totalCreatedJobs +=1
            pass

        if (totalCreatedJobs==0):
            common.logger.message("No jobs to be submitted: first create them")
            return

        # submit pre DashBoard information
        params = {'jobId':'TaskMeta'}

        fl = open(common.work_space.shareDir() + '/' + common.apmon.fName, 'r')
        for i in fl.readlines():
            try:
                key, val = i.split(':')
                params[key] = string.strip(val)
            except ValueError: # Not in the right format
                pass
        fl.close()

        common.logger.debug(5,'Submission DashBoard Pre-Submission report: '+str(params))

        common.apmon.sendToML(params)

        ### define here the list of distinct destinations sites list    
       # distinct_dests =  common._db.queryDistJob('dlsDestination')
        distinct_dests = common._db.queryDistJob_Attr('dlsDestination', 'jobId' ,self.nj_list)


        ### define here the list of jobs Id for each distinct list of sites
        sub_jobs =[] # list of jobs Id list to submit
        match_jobs =[] # list of jobs Id to match
        all_jobs=[] 
        count=0
        for distDest in distinct_dests: 
             all_jobs.append(common._db.queryAttrJob({'dlsDestination':distDest},'jobId'))
             sub_jobs_temp=[]
             for i in self.nj_list:
                 if i in all_jobs[0]: sub_jobs_temp.append(i) 
             if len(sub_jobs_temp)>0:
                 sub_jobs.append(sub_jobs_temp)   
                 match_jobs.append(sub_jobs[count][0])
                 count +=1
        sel=0
        matched=[] 
        for id_job in match_jobs:
            if common.scheduler.name().upper() != "CONDOR_G" :
                #match = common.scheduler.listMatch(id_job)
                match = "1"
            else :
                match = "1"
            if match:
               common.logger.message("Found "+str(match)+" compatible site(s) for job "+str(id_job))
               matched.append(sel)
            else:
               common.logger.message("No compatible site found, will not submit jobs "+str(sub_jobs[sel]))
               self.submissionError()
            sel += 1

        ### Progress Bar indicator, deactivate for debug
        if not common.logger.debugLevel() :
                term = TerminalController()
  
        if len(matched)>0: 
            common.logger.message(str(len(matched))+" blocks of jobs will be submitted")
            for ii in matched: 
                common.logger.debug(1,'Submitting jobs '+str(sub_jobs[ii]))
                if not common.logger.debugLevel() :
                    try: pbar = ProgressBar(term, 'Submitting '+str(len(sub_jobs[ii]))+' jobs')
                    except: pbar = None
                print sub_jobs[ii]

                common.scheduler.submit(sub_jobs[ii])

                ### Ask To StefanoL  
                if not common.logger.debugLevel():
                    if pbar :
                        pbar.update(float(ii+1)/float(len(sub_jobs)),'please wait')
                ### check the if the submission succeded Maybe not neede 
                if not common.logger.debugLevel():
                    if pbar :
                        pbar.update(float(ii+1)/float(len(sub_jobs)),'please wait')
                ### check the if the submission succeded Maybe not neede 
                if not common.logger.debugLevel():
                    if pbar :
                        pbar.update(float(ii+1)/float(len(sub_jobs)),'please wait')
                ### check the if the submission succeded Maybe not neede 
                if not common.logger.debugLevel():
                    if pbar :
                        pbar.update(float(ii+1)/float(len(sub_jobs)),'please wait')

                ### check the if the submission succeded Maybe not needed or at least simplified 
                njs=0 
                jid = common._db.queryRunJob('schedulerId',sub_jobs[ii])
                st = common._db.queryRunJob('status',sub_jobs[ii])
                run_jobToSave = {'status' :'S'}
                for j in range(len(sub_jobs[ii])): # Add loop over SID returned from group submission  DS
                    if jid[j] != '': 
                    #if (st[j]=='S'):
                        common._db.updateRunJob_(nj+1, run_jobToSave ) ## New BL--DS
                        common.logger.debug(5,"Submitted job # "+ str(sub_jobs[ii][j]))
                        njs += 1
#
#                    ##### DashBoard report #####################
#                        Sub_Type = 'Direct'
#
#                    # OLI: JobID treatment, special for Condor-G scheduler
#                    jobId = ''
#                    localId = ''
#                    if common.scheduler.name().upper() == 'CONDOR_G':
#                        rb = 'OSG'
#                        jobId = str(jj) + '_' + self.hash + '_' + jid
#                        common.logger.debug(5,'JobID for ML monitoring is created for CONDOR_G scheduler:'+jobId)
#                    elif common.scheduler.name() == 'lsf' or common.scheduler.name() == 'caf':
#                        jobId="https://"+common.scheduler.name()+":/"+jid+"-"+string.replace(common.taskDB.dict('taskId'),"_","-")+"-"+str(jj)
#                        common.logger.debug(5,'JobID for ML monitoring is created for LSF scheduler:'+jobId)
#                        rb = common.scheduler.name()
#                        localId = jid
#                    else:
#                        jobId = str(jj) + '_' + jid
#                        common.logger.debug(5,'JobID for ML monitoring is created for EDG scheduler'+jobId)
#                        rb = jid.split(':')[1]
#                        rb = rb.replace('//', '')
#
#                    if len(common.jobDB.destination(tmpNj)) <= 2 :
#                        T_SE=string.join((common.jobDB.destination(tmpNj)),",")
#                    else :
#                        T_SE=str(len(common.jobDB.destination(tmpNj)))+'_Selected_SE'
#
#                    params = {'jobId': jobId, \
#                              'sid': jid, \
#                              'broker': rb, \
#                              'bossId': jj, \
#                              'SubmissionType': Sub_Type, \
#                              'TargetSE': T_SE, \
#                              'localId' : localId}
#                    common.logger.debug(5,str(params))
#
#                    fl = open(common.work_space.shareDir() + '/' + common.apmon.fName, 'r')
#                    for i in fl.readlines():
#                        key, val = i.split(':')
#                        params[key] = string.strip(val)
#                    fl.close()
#
#                    common.logger.debug(5,'Submission DashBoard report: '+str(params))
#
#                    common.apmon.sendToML(params)
#                pass
#            pass
#
        else:
            common.logger.message("The whole task doesn't found compatible site ")

        stop = time.time()
        common.logger.debug(1, "Submission Time: "+str(stop - start))
        common.logger.write("Submission time :"+str(stop - start))

        msg = '\nTotal of %d jobs submitted'%njs
        if njs != len(self.nj_list) :
            msg += ' (from %d requested).\n'%(len(self.nj_list))
            pass
        else:
            msg += '.\n'
        if self.cfg_params.has_key('EDG.se_white_list'):
            msg += 'SE White List: '+self.cfg_params['EDG.se_white_list']+'\n'
        if self.cfg_params.has_key('EDG.se_black_list'):
            msg += 'SE Black List: '+self.cfg_params['EDG.se_black_list']+'\n'
        if self.cfg_params.has_key('EDG.ce_white_list'):
            msg += 'CE White List: '+self.cfg_params['EDG.ce_white_list']+'\n'
        if self.cfg_params.has_key('EDG.ce_black_list'):
            msg += 'CE Black List: '+self.cfg_params['EDG.ce_black_list']+'\n'
        msg += '(Hint: By whitelisting you force the job to run at this particular site(s).\nPlease check if the dataset is available at this site!)\n'

        common.logger.message(msg)


        return
