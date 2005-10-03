from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import Statistic

class Submitter(Actor):
    def __init__(self, cfg_params, nj_list):
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        ############################################# 
        fileCODE1 = open(common.work_space.logDir()+"/.code","r")
        array = fileCODE1.read().split('::')
        self.ID1 = array[0]
        self.NJC = array[1]   
        self.dataset = array[2]
        self.owner = array[3]  
        fileCODE1.close()
        
        return
    
    def run(self):
        """
        The main method of the class.
        """
        common.logger.debug(5, "Submitter::run() called")
        # Add an instance of ApmonIf to send relevant parameters to ML
        mon = ApmonIf()
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
            if st != 'C' and st != 'K' and st != 'A' and st != 'RC':
                long_st = crabJobStatusToString(st)
                #msg = "Job # %d is not submitted: status %s"%(nj+1, long_st)
                #common.logger.message(msg)
                continue

            common.logger.message("Submitting job # "+`(nj+1)`)
            jid = common.scheduler.submit(nj)

            common.jobDB.setStatus(nj, 'S')
            common.jobDB.setJobId(nj, jid)
            njs += 1

            destination = common.scheduler.queryDest(jid).split(":")[0]
            print "Destinazione: ", destination
            ID3 =  jid.split("/")[3]
            broker = jid.split("/")[2].split(":")[0]
            if st == 'C':
                resFlag = 0
            elif st == 'RC':
                resFlag = 2
            else:            
                resFlag = 0
                pass

            Statistic.notify('submit',resFlag,'-----',self.dataset,self.owner,destination,broker,ID3,self.ID1,self.NJC)
            
            # List of parameters to be sent to ML monitor system
            user = os.getlogin()
            taskId = os.getlogin()+'_'+string.split(common.work_space.topDir(),'/')[-2] 
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
            dataset = self.cfg_params['USER.dataset']
            owner = self.cfg_params['USER.owner']
            rb = sid.split(':')[1]
            rb = rb.replace('//', '')
            params = {'taskId': taskId, 'jobId': jobId, 'sid': sid, 'application': application, \
                      'exe': exe, 'nevtJob': nevtJob, 'tool': tool, 'scheduler': scheduler, \
                      'user': user, 'taskType': taskType, 'vo': vo, 'dataset': dataset, 'owner': owner, 'broker': rb}
            mon.fillDict(params)
            mon.sendToML()
            pass
        except:
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
    
