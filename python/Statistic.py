#!/usr/bin/env python2.2

from socket import *
import os
import common

def Monitor(operation,Resubmit,jid,exitCode):
       fileCODE1 = open(common.work_space.shareDir()+"/code","r")
       array = fileCODE1.read().split('::')
       time = array[0]
       NjobCre = array[1]
       ### commented for FAMOS
       # dataset = array[2]
       # owner = array[3]
       fileCODE1.close()
          
       try:
           dest = common.scheduler.queryDest(jid).split(":")[0]
       except:
           dest =  " "
    

       SID =  jid.split("/")[3]
       brok = jid.split("/")[2].split(":")[0]

       port = 8888
       address = 'crabstat.pg.infn.it'
                                                                                                                             
       for name in os.popen('hostname -f').readlines():
       # for name in os.popen('whoami').readlines():
           name = name.strip()
           UIname = name.split(" ")[0]
                                                                                                                             
                                                                                                                             
       sockobj = socket(AF_INET,SOCK_DGRAM)
       sockobj.connect((address,port))
       ### commented for FAMOS
       #sockobj.send(str(UIname)+'::'+str(operation)+'::'+str(Resubmit)+'::'+str(exitCode)+'::'+str(dataset)+'::'+str(owner)+'::'+str(dest)+'::'+str(brok)+'::'+str(SID)+'::'+str(time)+'::'+str(NjobCre))
       sockobj.send(str(UIname)+'::'+str(operation)+'::'+str(Resubmit)+'::'+str(exitCode)+'::'+str(dest)+'::'+str(brok)+'::'+str(SID)+'::'+str(time)+'::'+str(NjobCre))
                                                                                                                             
       sockobj.close()
 

