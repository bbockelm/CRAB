from Actor import *
from crab_util import *
import common
from ApmonIf import ApmonIf
import Statistic
import time
from ProgressBar import ProgressBar
from TerminalController import TerminalController

import commands
from TaskDB import TaskDB
import os, subprocess, errno, time, sys, re
PIPE = subprocess.PIPE

import select
import fcntl

import xml.dom.minidom
import xml.dom.ext

class SubmitterServer(Actor):
    def __init__(self, cfg_params, parsedRange, nominalRange):

        self.cfg_params = cfg_params
        self.submitRange = parsedRange
        self.nominalRange = nominalRange # unparsed version of the range # useful to distinguish 'all'
        
        try:  
            self.server_name = self.cfg_params['CRAB.server_name'] # gsiftp://pcpg01.cern.ch/data/SEDir/
        except KeyError:
            msg = 'No server selected ...' 
            msg = msg + 'Please specify a server in the crab cfg file' 
            raise CrabException(msg) 
        return

    def run(self):
        """
        The main method of the class: submit jobs in range self.nj_list
        """

        # partial submission code ## Fabio
        if os.path.exists(common.work_space.shareDir()+'/submit_directive') == True:
            common.logger.debug(5, "SubmitterServer::run() called for subsequent submission")
            self.subseqSubmission()
            return
        # standard submission to the server
        
        common.logger.debug(5, "SubmitterServer::run() called")

        totalCreatedJobs= 0
        start = time.time()
        flagSubmit = 1
        common.jobDB.load()

        list_nJ = []

        if self.nominalRange == 'all':
           list_nJ = self.submitRange
        else:
           for x in self.submitRange:
               ### FEDE removed -1 ####
               list_nJ.append(int(x))
               ########################
        for nj in list_nJ:
            #### FEDE added -1 #####
            if (common.jobDB.status(nj -1)=='C') or (common.jobDB.status(nj -1)=='RC'):
            #################
                totalCreatedJobs +=1
            else:
                flagSubmit = 0

        if not flagSubmit:
            if totalCreatedJobs > 0:
                common.logger.message("Not all jobs are created: before submit create all of them")
                return
            else:
                common.logger.message("Impossible to submit jobs that are already submitted")
                return
        elif (totalCreatedJobs==0):
            common.logger.message("No jobs to be submitted: first create them")
            return

        # submit pre DashBoard information
        params = {'jobId':'TaskMeta'}
               
        fl = open(common.work_space.shareDir() + '/' + self.cfg_params['apmon'].fName, 'r')
        for i in fl.readlines():
            val = i.split(':')
            params[val[0]] = string.strip(val[1])
            fl.close()

        common.logger.debug(5,'Submission DashBoard Pre-Submission report: '+str(params))
                        
        self.cfg_params['apmon'].sendToML(params)

        ### Here start the server submission 
        pSubj = os.popen3('openssl x509 -in /tmp/x509up_u`id -u` -subject -noout')[1].readlines()[0]
       
        userSubj='userSubj'
        userSubjFile = open(common.work_space.shareDir()+'/'+userSubj,'w')
        userSubjFile.write(str(pSubj))   
        userSubjFile.close()  

        ### Create submission range directive file # Fabio
        submDirectiveFile = open(common.work_space.shareDir()+'/submit_directive','w')
        if self.nominalRange == 'all':
             submDirectiveFile.write('all\n')
        else:
             submDirectiveFile.write(str(self.submitRange))
        submDirectiveFile.close()
        # 
    
        WorkDirName =os.path.basename(os.path.split(common.work_space.topDir())[0])
        common.scheduler.checkProxy()

        try: 
            flag = " --myproxy"
            common.logger.message("Registering a valid proxy to the server\n")
            cmd = 'asap-user-register --server '+str(self.server_name).split("/")[0] + flag
            attempt = 3
            while attempt:
                shell, command, tail = ('sh', cmd, '\n')
                a = Popen(shell, stdin=PIPE, stdout=PIPE)
                send_all(a, command + tail)
                print recv_some(a)
                send_all(a, 'exit' + tail)
                ret = a.wait()
                if (not ret): 
                    print "Asap register ok"
                    break
                else: attempt = attempt - 1
                if (attempt == 0): return CrabException("ASAP ERROR: Unable to ship a valid proxy to the server "+str(server_name).split("/")[0]+"\n")
        except:  
            msg = "ASAP ERROR: Unable to ship a valid proxy to the server \n"
            msg +="Project "+str(WorkDirName)+" not Submitted \n"      
            raise CrabException(msg)

        projectUniqName = 'crab_'+str(WorkDirName)+'_'+common.taskDB.dict('TasKUUID')
        common.taskDB.load()
        common.taskDB.setDict('projectName',projectUniqName)
        common.taskDB.save()

        ### create a tar ball
        common.logger.debug( 5, 'tar -zcvf '+str(WorkDirName)+'.tgz '+str(WorkDirName) )
        cmd = 'tar -zcvf '+str(WorkDirName)+'.tgz '+str(WorkDirName)
        cmd_out = runCommand(cmd)
    
        try: 
            ### submit poject to the server   
            #projectUniqName = 'crab_'+str(WorkDirName)+'_'+common.taskDB.dict('TasKUUID')     
            common.logger.message("Sending the project to the server...\n")
            cmd = 'lcg-cp --vo cms file://'+os.getcwd()+'/'+str(WorkDirName)+'.tgz gsiftp://' + str(self.server_name) + str(projectUniqName)+'.tgz'
            shipProject = os.system(cmd +' >& /dev/null')
            common.logger.debug( 5, 'rm -f '+str(WorkDirName)+'.tgz' )
            cmd = 'rm -f '+str(WorkDirName)+'.tgz'
            cmd_out = runCommand(cmd)
            if (shipProject>0):
                raise CrabException("ERROR : Unable to ship the project to the server \n "+str(self.server_name).split("/")[0]+"\n")
            else:
                msg='Project '+str(WorkDirName)+' succesfuly submitted to the server \n'      
                common.logger.message(msg)
                if self.nominalRange == 'all':
                    list_nJ = self.submitRange
                else:
                    for x in self.submitRange:
                        ### FEDE removed -1 ###
                        list_nJ.append(int(x))
                        #######################
                #   
                for nj in list_nJ:
                    ### FEDE added -1 ####
                    common.jobDB.setStatus(nj -1, 'S')
                    #####################
                    common.jobDB.save()
        except Exception, ex:  
            print str(ex)
            cmd = 'rm -f '+str(WorkDirName)+'.tgz'
            cmd_out = runCommand(cmd)
            msg = "ERROR : Unable to ship the project to the server \n"
            msg +="Project "+str(WorkDirName)+" not Submitted \n"      
            raise CrabException(msg)
        return

###################################
    #Partial submission code # Fabio
###################################
    def subseqSubmission(self):
        # prepare standard xml messages data
        common.jobDB.load()
        server_name = self.cfg_params['CRAB.server_name'] # gsiftp://pcpg01.cern.ch/data/SEDir/
        WorkDirName =os.path.basename(os.path.split(common.work_space.topDir())[0])
        projectUniqName = 'crab_'+str(WorkDirName)+'_'+common.taskDB.dict('TasKUUID')
        
        pSubj = os.popen3('openssl x509 -in  /tmp/x509up_u`id -u` -subject -noout')[1].readlines()[0]

        # cross check the specified range w.r.t. the previously submitted jobs
        # build the difference set between the directives and finally materialize
        # the hystory of the submissions
        prev_subms = 'None'
        delta_subm = []
        new_subms = ''

        file = open(common.work_space.shareDir()+'/submit_directive','r')
        prev_subms = str(file.readlines()[0]).split('\n')[0]
        file.close()
        
        if prev_subms == 'all':
            common.logger.message("Impossible to submit jobs that are already submitted")
            return

        prev_subms = eval(prev_subms)
        delta_subm = [i for i in self.submitRange if i not in prev_subms]
        new_subms = prev_subms + delta_subm
        if self.nominalRange == 'all': 
            new_subms = 'all'

        if len(delta_subm) == 0:
            common.logger.message("Impossible to submit jobs that are already submitted")
            return
            
        file = open(common.work_space.shareDir()+'/submit_directive','w')
        file.write(str(new_subms))
        file.close()
        
        # submit the xml message with delta_subm range
        try: 
            self.cfile = xml.dom.minidom.Document()
            root = self.cfile.createElement("TaskCommand")
            node = self.cfile.createElement("TaskAttributes")
            node.setAttribute("Task", projectUniqName)
            node.setAttribute("Subject", string.strip(pSubj))
            node.setAttribute("Command", "submit_range")
            node.setAttribute("Range", str(delta_subm))
            root.appendChild(node)
            self.cfile.appendChild(root)
            file = open(WorkDirName + '/share/command.xml', 'w')
            xml.dom.ext.PrettyPrint(self.cfile, file)
            file.close()
            
            cmd = 'lcg-cp --vo cms file://'+os.getcwd()+'/'+str(WorkDirName)+'/share/command.xml gsiftp://' + str(server_name) + str(projectUniqName)+'.xml'
            retcode = os.system(cmd)
            if retcode: 
                raise CrabException("Failed to ship submission command to server")
            else: 
                common.logger.message("Submission command succesfully shipped to server")
                for nj in delta_subm:
                    print "Setting job " +str(nj-1)
                    common.jobDB.setStatus((nj-1), 'S')
                    common.jobDB.save()
        except RuntimeError,e:
            msg +="Project "+str(WorkDirName)+" not submitted: \n"      
            raise CrabException(msg + e.__str__())
        return

####################################################
             
class Popen(subprocess.Popen):
    def recv(self, maxsize=None):
        return self._recv('stdout', maxsize)
    
    def recv_err(self, maxsize=None):
        return self._recv('stderr', maxsize)

    def send_recv(self, input='', maxsize=None):
        return self.send(input), self.recv(maxsize), self.recv_err(maxsize)

    def get_conn_maxsize(self, which, maxsize):
        if maxsize is None:
            maxsize = 1024
        elif maxsize < 1:
            maxsize = 1
        return getattr(self, which), maxsize
    
    def _close(self, which):
        getattr(self, which).close()
        setattr(self, which, None)

    def send(self, input):
        if not self.stdin:
            return None

        if not select.select([], [self.stdin], [], 0)[1]:
            return 0

        try:
            written = os.write(self.stdin.fileno(), input)
        except OSError, why:
            if why[0] == errno.EPIPE: #broken pipe
                return self._close('stdin')
            raise

        return written

    def _recv(self, which, maxsize):
        conn, maxsize = self.get_conn_maxsize(which, maxsize)
        if conn is None:
            return None
        
        flags = fcntl.fcntl(conn, fcntl.F_GETFL)
        if not conn.closed:
            fcntl.fcntl(conn, fcntl.F_SETFL, flags| os.O_NONBLOCK)
        
        try:
            if not select.select([conn], [], [], 0)[0]:
                return ''
            
            r = conn.read(maxsize)
            if not r:
                return self._close(which)
    
            if self.universal_newlines:
                r = self._translate_newlines(r)
            return r
        finally:
            if not conn.closed:
                fcntl.fcntl(conn, fcntl.F_SETFL, flags)

def recv_some(p, t=.1, e=1, tr=5, stderr=0):
    message = "Other end disconnected!"
    if tr < 1:
        tr = 1
    x = time.time()+t
    y = []
    r = ''
    pr = p.recv
    if stderr:
        pr = p.recv_err
    while time.time() < x or r:
        r = pr()
        if r is None:
            if e:
                raise Exception(message)
            else:
                break
        elif r:
            y.append(r)
        else:
            time.sleep(max((x-time.time())/tr, 0))
    return ''.join(y)
    
def send_all(p, data):
    while len(data):
        sent = p.send(data)
        if sent is None:
            raise Exception(message)
        data = buffer(data, sent)
