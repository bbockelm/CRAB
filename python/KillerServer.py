import os, common, string
from Actor import *

import xml.dom.minidom
import xml.dom.ext

class KillerServer(Actor):
    # Matteo for kill by range
    def __init__(self, cfg_params, range, parsedRange=[]):
        self.cfg_params = cfg_params
        self.range = range
        self.parsedRange = parsedRange 
        return

#    def __init__(self, cfg_params):
#        self.cfg_params = cfg_params
#        return

    def run(self):
        """
        The main method of the class: kill a complete task
        """
        common.logger.debug(5, "Killer::run() called")
        
        common.jobDB.load()
        server_name = self.cfg_params['CRAB.server_name'] # gsiftp://pcpg01.cern.ch/data/SEDir/
        WorkDirName =os.path.basename(os.path.split(common.work_space.topDir())[0])
        projectUniqName = 'crab_'+str(WorkDirName)+'_'+common.taskDB.dict('TasKUUID')
        #common.taskDB.load()
        #common.taskDB.setDict('projectName',projectUniqName)
        #common.taskDB.save()

        ### Here start the kill operation  
        pSubj = os.popen3('openssl x509 -in /tmp/x509up_u`id -u` -subject -noout')[1].readlines()[0]
       
        try: 
            self.cfile = xml.dom.minidom.Document()
            root = self.cfile.createElement("TaskCommand")
            node = self.cfile.createElement("TaskAttributes")
            node.setAttribute("Task", projectUniqName)
            node.setAttribute("Subject", string.strip(pSubj))
            node.setAttribute("Command", "kill")
            node.setAttribute("Range", str(self.parsedRange)) # Matteo for kill by range
            root.appendChild(node)
            self.cfile.appendChild(root)
            self.toFile(WorkDirName + '/share/command.xml')
            cmd = 'lcg-cp --vo cms file://'+os.getcwd()+'/'+str(WorkDirName)+'/share/command.xml gsiftp://' + str(server_name) + str(projectUniqName)+'.xml'
            retcode = os.system(cmd)
            if retcode: raise CrabException("Failed to ship kill command to server")
            else: common.logger.message("Kill command succesfully shipped to server")
        except RuntimeError,e:
            msg +="Project "+str(WorkDirName)+" not killed: \n"      
            raise CrabException(msg + e.__str__())

        # synch the range of submitted jobs to server (otherwise You wont be able to submit them again) # Fabio
        file = open(common.work_space.shareDir()+'/submit_directive','r')
        subms = str(file.readlines()[0]).split('\n')[0]
        file.close()
        if self.range=='all':
            subms = []
        elif self.range != None and self.range != "": 
            if len(self.range)!=0:
                subms = eval(subms)
                for i in self.parsedRange:
                    if i in subms:
                        subms.remove(i)
        
        file = open(common.work_space.shareDir()+'/submit_directive','w')
        file.write(str(subms))
        file.close()
        #  
        return
                
    def toFile(self, filename):
        filename_tmp = filename+".tmp"
        file = open(filename_tmp, 'w')
        xml.dom.ext.PrettyPrint(self.cfile, file)
        file.close()
        os.rename(filename_tmp, filename) # this should be an atomic operation thread-safe and multiprocess-safe
