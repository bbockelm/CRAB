#!/usr/bin/env python2.2

from socket import *
import os

def notify(operation,Resubmit,exitCode,dataset,owner,dest,brok,SID,time,NjobCre):

    port = 8888
    address = 'crabstat.pg.infn.it'   

    for name in os.popen('hostname -f').readlines():
   # for name in os.popen('whoami').readlines():                           
        name = name.strip()
        UIname = name.split(" ")[0]


    sockobj = socket(AF_INET,SOCK_DGRAM)
    sockobj.connect((address,port))
    sockobj.send(str(UIname)+'::'+str(operation)+'::'+str(Resubmit)+'::'+str(exitCode)+'::'+str(dataset)+'::'+str(owner)+'::'+str(dest)+'::'+str(brok)+'::'+str(SID)+'::'+str(time)+'::'+str(NjobCre))    
    #sockobj.send(str(UInam)+';STARTED:1')
    sockobj.close()
