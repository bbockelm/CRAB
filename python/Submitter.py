from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf

class Submitter(Actor):
    def __init__(self, cfg_params, nj_list):
        self.cfg_params = cfg_params
        self.nj_list = nj_list
        return
    
    def run(self):
        """
        The main method of the class.
        """

        common.logger.debug(5, "Submitter::run() called")
        
        # Add an instance of ApmonIf to send relevant parameters to ML
        mon = ApmonIf()
        # run a list-match on first job
        firstJob=self.nj_list[0]
        match = common.scheduler.listMatch(firstJob)
        if match:
            common.logger.message("Found compatible resources "+str(match))
        else:
            raise CrabException("No compatible resources found!")
        # Loop over jobs

        njs = 0
        for nj in self.nj_list:
            st = common.jobDB.status(nj)
            if st != 'C' and st != 'K' and st != 'A':
                long_st = crabJobStatusToString(st)
                msg = "Job # %d is not submitted: status %s"%(nj+1, long_st)
                common.logger.message(msg)
                continue

            common.logger.message("Submitting job # "+`(nj+1)`)

            jid = common.scheduler.submit(nj)

            common.jobDB.setStatus(nj, 'S')
            common.jobDB.setJobId(nj, jid)
            njs += 1
            pass

        ####
        
        common.jobDB.save()
        
        # List of parameters to be sent to monitor system
        user = os.getlogin()
        taskId = os.getlogin()+'_'+string.split(common.work_space.topDir(),'/')[-2] 
        jobId = str(nj)
        sid = jid
        try:
            application = os.path.basename(os.environ['SCRAMRT_LOCALRT'])
        except KeyError:
            application = os.path.basename(os.environ['LOCALRT'])
        try: 
            nevtJob = self.cfg_params['USER.job_number_of_events']
        except KeyError:
            pass
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
                  'user': user, 'taskType': taskType, 'vo': vo, 'dataset': dataset, 'owner': owner, 'broker', rb`}
        for i in params.keys():
            print "key, value: %s %s" % (i, params[i])
        mon.fillDict(params)
        mon.sendToML()

        msg = '\nTotal of %d jobs submitted'%njs
        if njs != len(self.nj_list) :
            msg += ' (from %d requested).'%(len(self.nj_list))
            pass
        else:
            msg += '.'
            pass
        common.logger.message(msg)
        return
    
