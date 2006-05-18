#!/usr/bin/env python

import sys, os, commands,string, re
import exceptions

try:
     import xml.dom.ext.reader
except:
     print "\t PyXML not found: ready to install..."
     crabdir=os.getenv('CRABDIR')
## Let the user set up PyXML by hand
     #msg="Need to setup the PyXML python module. Do the following:\n"
     #msg+="  cd %s/DLSAPI\n"%crabdir
     #msg+="  ./InstallPyXML.sh"
     #print msg
## set up PyXML automatically
     cmd='cd %s/DLSAPI;\n ./InstallPyXML.sh'%crabdir
     ecode = os.system(cmd)
     if ecode!=0:
        msg="\t Failed running : %s"%cmd
        raise Exception(msg)
     else:
        print "\t Updating PYTHONPATH"
        libPath="%s/DLSAPI"%crabdir
        sys.path.append(libPath)

sys.exit(0)
