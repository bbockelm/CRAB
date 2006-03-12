#!/usr/bin/env python2.2

from socket import *
import os
import common

def Monitor(operation,Resubmit,jid,exitCode):
       fileCODE1 = open(common.work_space.shareDir()+"/.code","r")
       array = fileCODE1.read().split('::')
       time = array[0]
       jobtype = array[1]
       NjobCre = array[2]
       if ( jobtype == 'ORCA' ) or ( jobtype == 'ORCA_DBSDLS'):
           dataset = array[3]
           owner = array[4]
       elif jobtype == 'FAMOS':
           pass
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
       if ( jobtype == 'ORCA' ) or ( jobtype == 'ORCA_DBSDLS'):
           sockobj.send(str(UIname)+'::'+str(operation)+'::'+str(jobtype)+'::'+str(Resubmit)+'::'+str(exitCode)+'::'+str(dataset)+'::'+str(owner)+'::'+str(dest)+'::'+str(brok)+'::'+str(SID)+'::'+str(time)+'::'+str(NjobCre))
       elif jobtype == 'FAMOS':
           pass 
       sockobj.close()
 

