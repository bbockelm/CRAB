from WorkSpace import WorkSpace
from JobList import JobList
from Scheduler import Scheduler
from crab_logger import Logger
from crab_exceptions import *
import common

import string

class ScriptWriter:
    def __init__(self, template):
        # pattern -> action
        self.actions = {
            'title' : self.title,
            'setup_monitoring' : None,
            'setup_scheduler_environment' : None,
            'setup_jobtype_environment' : None,
            'copy_input_data' : None,
            'build_executable' : None,
            'run_executable' : None,
            'stop_monitoring' : None,
            'move_output' : None,
            'register_results' : None,
            'make_summary' : None,
            'notify' : None
            }
        
        self.template = template
        return

    def setAction(self, pattern, action):
        self.actions[pattern] = action
        return
    
    def modifyTemplateScript(self, nj):
        """
        Create a script from scratch.
        """
        tpl = open(self.template, 'r')
        script = open(common.job_list[nj].scriptFilename(), 'w')

        for line in tpl:
            if len(line) > 6 and line[:6] == '#CRAB ':
                act_str = string.strip(line[6:])
                try:
                    action = self.actions[act_str]
                except KeyError:
                    continue

                if action:
                    txt = action()
                    script.write(txt)
                    pass
                else:
                    script.write(line)
                pass
            else:
                script.write(line)
                pass
            pass

        script.close()
        tpl.close()
        return

    def title(self):
        txt = '# This script was generated by '+common.prog_name
        txt += ' (version '+common.prog_version_str+').\n'
        return txt
    
    def head(self, script, nj):
        """ Write header part of a job script. """
        script.write('#!/bin/sh\n')
        script.write('#\n')
        script.write('# HEAD\n')
        script.write('#\n')

        script.write('# This script was generated by '+common.prog_name+
                     ' (version '+common.prog_version_str+')\n')
        script.write('#\n')
        script.write('RUNTIME_AREA=`pwd` \n\n')

        #if common.use_jam:
        #    script.write('if [ ! -s $RUNTIME_AREA/'+ common.run_jam +' ]; then \n')
        #    script.write('   echo "$RUNTIME_AREA/'+ common.run_jam +' not found" \n')
        #    script.write('   echo "not possible to run jam monitoring!" \n')
        #    script.write('   exit 1 \n')
        #    script.write('else \n')
        #    script.write('   chmod u+x $RUNTIME_AREA/'+ common.run_jam +' \n') 
        #    script.write('   perl $RUNTIME_AREA/'+ common.run_jam +' --name='+common.output_jam+' --mode=init \n')
        #    script.write('fi \n')
        #    pass
        
        script.write('echo "Today is `date`"\n')
        script.write('echo "Job submitted on host `hostname`" \n')
        script.write('uname -a\n')
        script.write('echo "Working directory `pwd`"\n')
        script.write('ls -Al \n\n')
        
        #if common.use_jam:
        #    script.write('list=`ls -Al` \n')
        #    script.write('perl $RUNTIME_AREA/'+ common.run_jam +' --name='+common.output_jam+' --event=List --det="$list" \n')
        #    pass

        script.write('#\n')
        script.write('# END OF HEAD\n')
        script.write('#\n')
        return
    
    def setupEnvironment(self, script, nj):
        """ Write SetupEnvironment part of a job script."""

        script.write('#\n')
        script.write('# SETUP ENVIRONMENT\n')
        script.write('#\n')
        #TODO
        #common.scheduler.writeScript_SetupEnvironment(script, nj)
        #common.job_list[nj].type().writeScript_SetupEnvironment(script, nj)
        script.write('#\n')
        script.write('# END OF SETUP ENVIRONMENT\n')
        script.write('#\n')
        return

    def copyInputData(self, script, nj):
        #TODO
        #common.scheduler.writeScript_CopyInputData(script, nj)
        return

    def addFiles(self, script, nj):
        """
        Add into the script the content of some job-specific files.
        """
        #TODO: ???
        #common.job_list.type().writeScript_AddFiles(script, nj)
        return

    def runExe(self, script, nj):
        """
        Write part of a job script which execute the application.
        nj -- integer job number from 0 to ...
        """
        script.write('#\n')
        script.write('# PREPARE AND RUN EXECUTABLE\n')
        script.write('#\n')

        jbt = common.job_list.type()

        jbt.writeScript_BuildExe(script, nj)
        script.write('#\n')
    
        job = common.job_list[nj]
        exe = job.type().executableName()

        #TODO: should we create a dependecy on the BOSS requirements ?
        # Following 3 'echo's are needed by BOSS filters
        #if common.use_boss or common.flag_mksmry:
        #    script.write("# Following 3 'echo's are needed by BOSS filters\n")
        #    script.write('echo "AssignmentID='+ exp.getRefDBId() +'"\n')
        #    local_string=string.split(os.path.basename(job.scriptFilename()),'.')
        #    script.write('echo ">>>>>> Dump of ' + local_string[0] +
        #                 '.cards file"\n')
        #    script.write('echo "PID is $$"\n')
        #    pass
        
        script.write('\n')
        script.write('echo "Executable '+exe+'\"\n')
        script.write('which ' +exe+'\n')
        script.write('res=$?\n')
        script.write('if [ $res -ne 0 ];then \n')
        script.write('   echo "The executable not found on WN `hostname`" \n')
        script.write('   exit 1 \n')
        script.write('fi \n\n') 

        script.write('echo "'+jbt.name()+' started at `date`"\n')

        jbt.writeScript_RunExe(script, nj)

        script.write('executable_exit_status=$?\n')
        script.write('echo "'+jbt.name()+' ended at `date`."\n')

        #TODO: should we create a dependecy on the BOSS requirements ?
        # Following 'echo' is needed by BOSS filters
        #script.write('echo "'+string.lower(jbt.name())+' exited with status $executable_exit_status"\n')
        script.write('if [ $executable_exit_status -ne 0 ]; then \n')
        script.write('   echo "Processing of job failed" \n')
        script.write('fi \n')
        script.write('exit_status=$executable_exit_status\n')

        #if common.use_jam:
        #    script.write('perl $RUNTIME_AREA/'+ common.run_jam +' --name='+common.output_jam+' --event=exit_status --det="$exit_status" \n')
        #    pass
        
        script.write('#\n')
        script.write('# END OF PREPARE AND RUN EXECUTABLE\n')
        script.write('#\n')
        return

    def move_output(self, script,nj):
        return

    def registerResults(self, script,nj):
        return

    def tail(self, script, nj):
        """ Write a tailer part of a job script."""

        script.write('#\n')
        script.write('# TAIL\n')
        script.write('#\n')
        script.write('pwd\n')
        script.write('echo "ls -Al"\n')
        script.write('ls -Al\n')
        
        #if common.use_jam:
        #    script.write('list=`ls -Al` \n')
        #    script.write('perl $RUNTIME_AREA/'+ common.run_jam +' --name='+common.output_jam+' --event=List_end --det="$list" \n')
        #    pass
   
        script.write('#\n')

        #TODO
        #if common.flag_mksmry:
        #    script.write('chmod u+x postprocess\n')
        #    script.write('cat `ls -1 *.stdout` | ./postprocess | sort | uniq > sumry\n')
        #    pass

        #TODO
        # summary file may need jobtype specific info, e.g.
        # for CMS Oscar it needs Pool catalogues,
        # so we delegate operations to the related jobtype object.
        #common.job_list.type().writeScript_Tail(script, nj)
    
        #if common.flag_notify:
        #    script.write('if [[ $executable_exit_status -eq 0 && $replica_exit_status -eq 0 ]]; then\n')
        #    if common.flag_mksmry:
        #        script.write('    cat sumry | mail -s "job_finished" '+
        #                     common.email +'\n')
        #    else:
        #        script.write('    mail -s "job_finished" '+
        #                     common.email +' <<EOF\n')
        #        n1 = nj + 1
        #        script.write('Job # '+`n1`+' finished\n')
        #        script.write('EOF\n')
        #        pass
        #    script.write('fi\n')
        #    pass

        script.write('echo ">>>>>>>> End of job at date `date`" \n')
        
        #if common.use_jam:
        #    script.write('perl $RUNTIME_AREA/'+ common.run_jam +' --name='+common.output_jam+' --event=exit --det="$exit_status" \n')
        #    pass
        
        script.write('exit $exit_status\n')
        return

