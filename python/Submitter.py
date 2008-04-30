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
            elif (type(eval(val)) is tuple)or( type(eval(val)) is int and eval(val)<0 ) :
                chosenJobsList = parsed_range
                nsjobs = len(chosenJobsList)
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
        self.complete_List = common._db.nJobs('list')
        common.logger.debug(5,'Total jobs '+str(len(self.complete_List)))
        jobSetForSubmission = 0
        jobSkippedInSubmission = []
        datasetpath=self.cfg_params['CMSSW.datasetpath']
        if string.lower(datasetpath)=='none':
            datasetpath = None
        tmp_jList = self.complete_List
        if chosenJobsList != None:
            tmp_jList = chosenJobsList
        # build job list
        from BlackWhiteListParser import BlackWhiteListParser
        self.blackWhiteListParser = BlackWhiteListParser(self.cfg_params)
        dlsDest=common._db.queryJob('dlsDestination',tmp_jList)
        jStatus=common._db.queryRunJob('status',tmp_jList)
        for nj in range(len(tmp_jList)):
            cleanedBlackWhiteList = self.blackWhiteListParser.cleanForBlackWhiteList(dlsDest[nj]) 
            if (cleanedBlackWhiteList != '') or (datasetpath == None): ## Matty's fix
                ##if ( jStatus[nj] not in ['R','S','K','Y','A','D','Z']): ## here the old flags
                if ( jStatus[nj] not in ['SS','SU','SR','R','S','K','Y','A','D','Z','E']):
                    #nj_list.append(nj+1)## Warning added +1 for jobId BL--DS 
                    jobSetForSubmission +=1
                    nj_list.append(tmp_jList[nj])## Warning added +1 for jobId BL--DS 
                else:
                    continue
            else :
                jobSkippedInSubmission.append(tmp_jList[nj])
            if nsjobs >0 and nsjobs == jobSetForSubmission:
                break
            pass
        
        if nsjobs>jobSetForSubmission:
            common.logger.message('asking to submit '+str(nsjobs)+' jobs, but only '+str(jobSetForSubmission)+' left: submitting those')
        if len(jobSkippedInSubmission) > 0 :
            mess =""
            for jobs in jobSkippedInSubmission:
                mess += str(jobs) + ","
            common.logger.message("Jobs:  " +str(mess) + "\n      skipped because no sites are hosting this data\n")
            self.submissionError()
            pass
        # submit N from last submitted job
        common.logger.debug(5,'nj_list '+str(nj_list))


        self.nj_list = nj_list
        return

    def run(self):
        """
        The main method of the class: submit jobs in range self.nj_list
        """
        common.logger.debug(5, "Submitter::run() called")

        start = time.time()

        check = self.checkIfCreate() 
        
        if check == 0 :
            self.SendMLpre()
            
            list_matched , task = self.performMatch()        
            njs = self.perfromSubmission(list_matched, task)  
        
            stop = time.time()
            common.logger.debug(1, "Submission Time: "+str(stop - start))
            common.logger.write("Submission time :"+str(stop - start))
        
            msg = '\nTotal of %d jobs submitted'%njs
            if njs != len(self.nj_list) :
                msg += ' (from %d requested).'%(len(self.nj_list))
            else:
                msg += '.'
            common.logger.message(msg)
        
            if (njs < len(self.nj_list) or len(self.nj_list)==0):
                self.submissionError()


    def checkIfCreate(self): 
        """
        """
        code = 0
        totalCreatedJobs = 0
        jList=common._db.nJobs('list')
        st = common._db.queryRunJob('status',jList)
        for nj in range(len(jList)):
            if ( st[nj] in ['C','RC']):totalCreatedJobs +=1
            pass

        if (totalCreatedJobs==0):
              common.logger.message("No jobs to be submitted: first create them")
              code = 1  
        return code         


    def performMatch(self):    
        """
        """ 
        common.logger.message("Checking available resources...")
        ### define here the list of distinct destinations sites list    
       # distinct_dests =  common._db.queryDistJob('dlsDestination')
        distinct_dests = common._db.queryDistJob_Attr('dlsDestination', 'jobId' ,self.nj_list)


        ### define here the list of jobs Id for each distinct list of sites
        self.sub_jobs =[] # list of jobs Id list to submit
        jobs_to_match =[] # list of jobs Id to match
        all_jobs=[] 
        count=0
        for distDest in distinct_dests: 
             all_jobs.append(common._db.queryAttrJob({'dlsDestination':distDest},'jobId'))
             sub_jobs_temp=[]
             for i in self.nj_list:
                 if i in all_jobs[count]: sub_jobs_temp.append(i) 
             if len(sub_jobs_temp)>0:
                 self.sub_jobs.append(sub_jobs_temp)   
                 jobs_to_match.append(self.sub_jobs[count][0])
             count +=1
        sel=0
        matched=[] 

        task=common._db.getTask()

        for id_job in jobs_to_match :
            match = common.scheduler.listMatch(distinct_dests[sel])
            if len(match)>0:
                common.logger.message("Found "+str(len(match))+" compatible site(s) for job "+str(id_job))
                matched.append(sel)
            else:
                common.logger.message("No compatible site found, will not submit jobs "+str(self.sub_jobs[sel]))
                self.submissionError()
            sel += 1

        return matched , task 

    def perfromSubmission(self,matched,task):

        njs=0 
   
        ### Progress Bar indicator, deactivate for debug
        if not common.logger.debugLevel() :
            term = TerminalController()
  
        if len(matched)>0: 
            common.logger.message(str(len(matched))+" blocks of jobs will be submitted")
            for ii in matched: 
                common.logger.debug(1,'Submitting jobs '+str(self.sub_jobs[ii]))

                try:
                    common.scheduler.submit(self.sub_jobs[ii],task)
                except CrabException:
                    raise CrabException("Job not submitted")

                if not common.logger.debugLevel() :
                    try: pbar = ProgressBar(term, 'Submitting '+str(len(self.sub_jobs[ii]))+' jobs')
                    except: pbar = None
                if not common.logger.debugLevel():
                    if pbar :
                        pbar.update(float(ii+1)/float(len(self.sub_jobs)),'please wait')
                ### check the if the submission succeded Maybe not neede 
                if not common.logger.debugLevel():
                    if pbar :
                        pbar.update(float(ii+1)/float(len(self.sub_jobs)),'please wait')

                ### check the if the submission succeded Maybe not needed or at least simplified 
                #njs=0 
                sched_Id = common._db.queryRunJob('schedulerId', self.sub_jobs[ii])
                listId=[]
                run_jobToSave = {'status' :'S'}
                listRunField = []
                for j in range(len(self.sub_jobs[ii])): # Add loop over SID returned from group submission  DS
                    if str(sched_Id[j]) != '': 
                    #if (st[j]=='S'):
                        listId.append(self.sub_jobs[ii][j]) 
                        listRunField.append(run_jobToSave) 
                        common.logger.debug(5,"Submitted job # "+ str(self.sub_jobs[ii][j]))
                        njs += 1
                common._db.updateRunJob_(listId, listRunField) ## New BL--DS

                self.SendMLpost(self.sub_jobs[ii])

        else:
            common.logger.message("The whole task doesn't found compatible site ")

        return njs

    def submissionError(self):
        ## add some more verbose message in case submission is not complete
        msg =  'Submission performed using the Requirements: \n'
        ### TODO_ DS--BL
        #msg += common.taskDB.dict("jobtype")+' version: '+common.taskDB.dict("codeVersion")+'\n'
        #msg += '(Hint: please check if '+common.taskDB.dict("jobtype")+' is available at the Sites)\n'
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

    def collect_MLInfo(self):
        """
        Preapre DashBoard information
        """

        taskId=str("_".join(common._db.queryTask('name').split('_')[:-1]))
        gridName = string.strip(common.scheduler.userName())
        common.logger.debug(5, "GRIDNAME: "+gridName)
        taskType = 'analysis'
       # version 
        
        self.datasetPath =  self.cfg_params['CMSSW.datasetpath']
        if string.lower(self.datasetPath)=='none':
            self.datasetPath = None
        self.executable = self.cfg_params.get('CMSSW.executable','cmsRun')
        VO = self.cfg_params.get('EDG.virtual_organization','cms')

        params = {'tool': common.prog_name,\
                  'JSToolVersion': common.prog_version_str, \
                  'tool_ui': os.environ['HOSTNAME'], \
                  'scheduler': common.scheduler.name(), \
                  'GridName': gridName, \
                  'taskType': taskType, \
                  'vo': VO, \
                  'user': os.environ['USER'], \
                  'taskId': taskId, \
                  'datasetFull': self.datasetPath, \
                  #'application', version, \
                  'exe': self.executable } 

        return params
   
    def SendMLpre(self):
        """
        Send Pre info to ML 
        """
        params = self.collect_MLInfo()
 
        params['jobId'] ='TaskMeta'
 
        common.apmon.sendToML(params)
 
        common.logger.debug(5,'Submission DashBoard Pre-Submission report: '+str(params))
        
        return

    def SendMLpost(self,allList):
        """
        Send post-submission info to ML  
        """  
        task = common._db.getTask(allList) 

        params = {}
        for k,v in self.collect_MLInfo().iteritems():
            params[k] = v
  

        taskId= str("_".join(str(task['name']).split('_')[:-1]))
   
        Sub_Type = 'Direct'
        for job in task.jobs:
            jj = job['id']          
            jobId = ''
            localId = ''
            jid = str(job.runningJob['schedulerId']) 
            if common.scheduler.name().upper() == 'CONDOR_G':
                self.hash = makeCksum(common.work_space.cfgFileName())
                rb = 'OSG'
                jobId = str(jj) + '_' + self.hash + '_' + jid
                common.logger.debug(5,'JobID for ML monitoring is created for CONDOR_G scheduler:'+jobId)
            elif common.scheduler.name() in ['lsf', 'caf']:
                jobId="https://"+common.scheduler.name()+":/"+jid+"-"+string.replace(str(taskId),"_","-")
                common.logger.debug(5,'JobID for ML monitoring is created for LSF scheduler:'+jobId)
                rb = common.scheduler.name()
                localId = jid
            else:
                jobId = str(jj) + '_' + str(jid)
                common.logger.debug(5,'JobID for ML monitoring is created for gLite scheduler'+jobId)
                rb = str(job.runningJob['service'])
        
            dlsDest = job['dlsDestination'] 
            if len(dlsDest) <= 2 :
                T_SE=string.join(str(dlsDest),",")
            else : 
                T_SE=str(len(dlsDest))+'_Selected_SE'


            infos = { 'jobId': jobId, \
                      'sid': jid, \
                      'broker': rb, \
                      'bossId': jj, \
                      'SubmissionType': Sub_Type, \
                      'TargetSE': T_SE, \
                      'localId' : localId}

            for k,v in infos.iteritems():
                params[k] = v

            common.logger.debug(5,'Submission DashBoard report: '+str(params))
            common.apmon.sendToML(params)
        return


