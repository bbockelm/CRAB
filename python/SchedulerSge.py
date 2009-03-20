#!/usr/bin/env python
"""
basic SGE CLI interaction class
"""

__revision__ = "$Id: SchedulerSge.py,v 1.5 2009/01/20 18:49:45 gcodispo Exp $"
__version__ = "$Revision: 1.5 $"

import re, os

from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

class SchedulerSge (SchedulerInterface) :
    """
    basic class to handle sge jobs
    """
    def __init__( self, **args):

        # call super class init method
        super(SchedulerSge, self).__init__(**args)
        self.statusMap = {
            'd':'K',
            'E':'DA',
            'h':'R',
            'r':'R',
            'R':'R',
            's':'R',
            'S':'R',
            't':'SS',
            'T':'R',
            'w':'R',
            'qw':'R',
            'Eqw':'K',
            'DONE':'SD'
            }
  
        
    def checkUserProxy( self, cert='' ):
        return

    def jobDescription ( self, obj, requirements='', config='', service = '' ):
        """
        retrieve scheduler specific job description
        return it as a string
        """
        args='' 
        if type(obj) == RunningJob or type(obj) == Job:
            return self.decode(obj, requirements)
        elif type(obj) == Task :
            task = obj
            for job in task.getJobs() :
                args += self.decode(job, task, requirements)+'  \n'
            return args 


    def submit ( self, obj, requirements='', config='', service = '' ) :
        """
        set up submission parameters and submit

        return jobAttributes, bulkId, service

        - jobAttributs is a map of the format
              jobAttributes[ 'name' : 'schedulerId' ]
        - bulkId is an eventual bulk submission identifier
        - service is a endpoit to connect withs (such as the WMS)
        """

        #print "config"+config
        if type(obj) == RunningJob or type(obj) == Job:
            return self.submitJob(obj, requirements)
        elif type(obj) == Task :
            return self.submitTask (obj, requirements ) 

    def submitTask ( self, task, requirements=''):
        ret_map={}
        for job in task.getJobs() :
            map, taskId, queue = self.submitJob(job, task, requirements)
            ret_map.update(map)

        return ret_map, taskId, queue

    def submitJob ( self, job, task=None, requirements=''):
        """Need to copy the inputsandbox to WN before submitting a job"""
        
        arg = self.decode(job, task, requirements )

        command = "qsub " + arg 
        #print command + "\n"
        out, ret = self.ExecuteCommand(command)
        #print "out:" + out + "\n"
        r = re.compile("Your job (\d+) .* has been submitted")

        m= r.search(out)
        if m is not None:
            jobId =m.group(1)
            command = "qstat -j " +  jobId
            #out, ret = self.ExecuteCommand(command)
            #print "out:" + out + "\n"
            #queue = m.group(2)
            queue = "all"
        else:
            #rNot = re.compile("Job not submitted.*<(\w+)>")
            #m= rNot.search(out)
            #if m is not None:
            #    print m
            #    print "Job NOT submitted"
            #    print out
            raise SchedulerError('error', out)
        taskId = None 
        #print "Your job identifier is: ", taskId, queue
        map={ job[ 'name' ] : jobId }
        return map, taskId, queue


    def decode (self, job, task=None, requirements='' , config ='', service='' ):
        """
        prepare file for submission
        """

        txt = "#batch script for SGE jobs\n"
        txt += "MYTMPDIR=$TMP/$JOB_NAME\n"
        txt += "mkdir -p $MYTMPDIR \n"
        txt += "cd $MYTMPDIR\n"
        # Need to copy InputSandBox to WN
        if task:
            subDir=task[ 'startDirectory' ]
            for inpFile in task[ 'globalSandbox' ].split(','):
                #txt += "cp "+subDir+inpFile+" . \n"
                txt += "cp "+inpFile+" . \n"
        ## Job specific ISB
        #for inpFile in job[ 'inputFiles' ]:
        #    if inpFile != '': txt += self.cpCmd+" "+self.rfioSer+"/"+inpFile+" . \n"

        ## Now the actual wrapper
        args = job[ 'arguments' ]
        exe = job[ 'executable' ]
        txt += "./"+os.path.basename(exe)+" "+args+"\n"
        
        ## And finally copy back the output
        outputDir=task['outputDirectory']
        for outFile in job['outputFiles']:
            #print "outputFile:"+outFile
            txt += "cp "+outFile+" "+outputDir+"/. \n"

        txt += "cd $SGE_O_HOME\n"
        txt += "rm -rf $MYTMPDIR\n"
        arg = ""
        if job[ 'standardInput' ] != '':
            arg += ' -i %s ' % job[ 'standardInput' ]
            
        #delete old log files as SGE will append to the file     
        if os.path.exists(job[ 'standardOutput' ]):
            os.remove(job[ 'standardOutput' ])
            
        if os.path.exists(job[ 'standardError' ]):
            os.remove(job[ 'standardError' ])
            
        arg += ' -o %s ' % job[ 'standardOutput' ]
        arg += ' -e %s ' % job[ 'standardError' ]

 #       jobrundir = outputDir
 #       jobrundir += "/%s" % job['id']
 #       if not os.path.exists(jobrundir):
 #           os.mkdir(jobrundir)
            
        arg +='-wd '+outputDir+ ' '
 #       txt += "rm -rf "+jobrundir+"\n"
        # blindly append user requirements
        arg += requirements
        arg += '-N '+task[ 'name' ]+' '
        #create job script
        f = open(outputDir+'/batchscript', 'wc')
        f.write("#!/bin/sh\n")
        f.write(txt)
        f.close()
        
        # and finally the wrapper
        #arg +=  '  %s ' % txt
        arg += outputDir+"/batchscript"
        return arg 


    ##########################################################################
    def query(self, obj, service='', objType='node') :
        """
        query status and eventually other scheduler related information
        """

        # the object passed is a Task:
        #   check whether parent id are provided, make a list of ids
        #     and check the status
        if type(obj) == Task :
            schedIds = []

            # query performed through single job ids
            if objType == 'node' :
                for job in obj.jobs :
                    if self.valid( job.runningJob ) and \
                           job.runningJob['status'] != 'SD':
                        schedIds.append( job.runningJob['schedulerId'] )
                jobAttributes = self.queryLocal( schedIds, objType )

            # query performed through a bulk id
            elif objType == 'parent' :
                for job in obj.jobs :
                    if self.valid( job.runningJob ) \
                      and job.runningJob['status'] != 'SD' \
                      and job.runningJob['schedulerParentId'] not in schedIds:
                        schedIds.append( job.runningJob['schedulerParentId'] )
                jobAttributes = self.queryLocal( schedIds, objType )

            # status association
            for job in obj.jobs :
                try:
                    valuesMap = jobAttributes[ job.runningJob['schedulerId'] ]
                except:
                    continue
                for key, value in valuesMap.iteritems() :
                    job.runningJob[key] = value

        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))



    def queryLocal(self, schedIdList, objType='node' ) :

        """
        query status and eventually other scheduler related information
        It may use single 'node' scheduler id or bulk id for association
        
        return jobAttributes

        where jobAttributes is a map of the format:
           jobAttributes[ schedId :
                                    [ key : val ]
                        ]
           where key can be any parameter of the Job object and at least status
                        
        """
        ret_map={}
        #print schedIdList, service, objType
        r = re.compile("(\d+) .* "+os.getlogin()+" \W+(\w+) .* (\S+)@(\w+)")
        rnohost = re.compile("(\d+) .* "+os.getlogin()+" \W+(\w+) ")
        cmd='qstat -u '+os.getlogin()
        #print cmd
        out, ret = self.ExecuteCommand(cmd)
        #print "<"+out+">"
        for line in out.split('\n'):
            #print line
            queue=None
            host=None
            id=None
            st=None
            mfull= r.search(line)
            if (mfull):
                #print "matched"
                id,st,queue,host=mfull.groups()
            else:
                mnohost = rnohost.search(line)
                if (mnohost):
                    id,st = mnohost.groups()
                    pass
                pass
            #print "got id %s" % id
            if(id) and (id in schedIdList):
                map={}
                map[ 'status' ] = self.statusMap[st]
                map[ 'statusScheduler' ] = st
                if (host): map[ 'destination' ] = host
                ret_map[id]=map
                #print "set state to "+map['statusScheduler']
                pass
            pass
        
        #set all missing jobs to done
        for jobid in schedIdList:
            jobid = jobid.strip()
            if not jobid in ret_map:
                #print "job "+jobid+" not found in qstat list setting to DONE"
                id = jobid
                st = "DONE"
                map={}
                map[ 'status' ] = self.statusMap[st]
                map[ 'statusScheduler' ]= st
                ret_map[id]=map
                pass
            pass
        
        return ret_map
    

    def getOutput( self, obj, outdir):
        """
        retrieve output or just put it in the destination directory

        does not return
        """
        #output ends up in the wrong location with a user defined
        #output directory...Thus we have to move it to the correct
        #directory here....
        #print "SchedulerSGE:getOutput called!"
            
        if type(obj) == Task :
            oldoutdir=obj[ 'outputDirectory' ]
            if(outdir != oldoutdir):
                for job in obj.jobs:
                    jobid = job[ 'id' ];
                    #print "job:"+str(jobid)
                    if self.valid( job.runningJob ):
                        #print "is valid"                       
                        for outFile in job['outputFiles']:
                            #print "outputFile:"+outFile
                            command = "mv "+oldoutdir+"/"+outFile+" "+outdir+"/. \n"
                            #print command
                            out, ret = self.ExecuteCommand(command)
                            if (out!=""):
                                raise SchedulerError('unable to move file', out)
                                #raise SchedulerError("unable to move file "+oldoutdir+"/"+outFile+" ",out)
                            pass
                        pass
                    pass
                pass
            pass
        

    def kill( self, obj ):
        """
        kill the job instance

        does not return
        """
        r = re.compile("has registered the job (\d+) for deletion")
        rFinished = re.compile("Job <(\d+)>: Job has already finished")
        # for jobid in schedIdList:
        for job in obj.jobs:
            if not self.valid( job.runningJob ):
                continue
            jobid = str( job.runningJob[ 'schedulerId' ] ).strip()
            cmd='qdel '+str(jobid)
            out, ret = self.ExecuteCommand(cmd)
            #print "kill:"+out
            mKilled= r.search(out)
            if not mKilled:
                raise SchedulerError ( "Unable to kill job "+jobid+" . Reason: ", out )
            pass
        pass

    def postMortem ( self, schedIdList, outfile, service ) :
        """
        execute any post mortem command such as logging-info
        and write it in outfile
        """

