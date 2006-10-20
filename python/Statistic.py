#!/usr/bin/env python

from socket import *
import os
import common
import time

def Monitor(operation,Resubmit,jid,exitCode,dest):
       common.taskDB.load()
       array = common.taskDB.dict("CODE").split('::')
       time = array[0]
       jobtype = array[1]
       NjobCre = array[2]

       if ( jobtype == 'ORCA' ) or ( jobtype == 'ORCA_PUBDB')  or  ( jobtype == 'CMSSW'):
           dataset = array[3]
           owner = array[4]
       elif jobtype == 'FAMOS':
           inputData = array[3]  
           executable = array[4]
           pass
          

       SID = jid
       brok = dest

       port = 8888
       address = 'crabstat.pg.infn.it'
                                                                                                                             
       for name in os.popen('hostname -f').readlines():
       # for name in os.popen('whoami').readlines():
           name = name.strip()
           UIname = name.split(" ")[0]
                                                                                                                             
                                                                                                                             
       sockobj = socket(AF_INET,SOCK_DGRAM)
       sockobj.connect((address,port))
       if ( jobtype == 'ORCA' ) or ( jobtype == 'ORCA_PUBDB') or ( jobtype == 'CMSSW'):
           sockobj.send(str(UIname)+'::'+str(operation)+'::'+str(jobtype)+'::'+str(Resubmit)+'::'+str(exitCode)+'::'+str(dataset)+'::'+str(owner)+'::'+str(dest)+'::'+str(brok)+'::'+str(jid)+'::'+str(time)+'::'+str(NjobCre))
       elif jobtype == 'FAMOS':
          # sockobj.send(str(UIname)+'::'+str(operation)+'::'+str(jobtype)+'::'+str(Resubmit)+'::'+str(exitCode)+'::'+str(inputData)+'::'+str(executable)+'::'+str(dest)+'::'+str(brok)+'::'+str(SID)+'::'+str(time)+'::'+str(NjobCre))
           pass 
       sockobj.close()
 

