#!/usr/bin/env python
import sys
import os
import socket

# Add GLITE_WMS_LOCATION to the python path
try:
    path = os.environ['GLITE_WMS_LOCATION']
except:
    print  "error : the GLITE_WMS_LOCATION variable is not set."
    sys.exit(1)
    
libPath=os.path.join(path, "lib")
sys.path.append(libPath)
libPath=os.path.join(path, "lib", "python")
sys.path.append(libPath)
# rimuovere al piu' presto!!!
try :
    sys.path.append("/home/codispot/SOAPpy-0.12.0/build/lib")
except :
    pass
  
try:
    import SOAPpy
except:
    print "error : SOAPpy not found in %s"%sys.path.__str__()
    sys.exit(2);
    
try:
    from wmproxymethods import Wmproxy
    from wmproxymethods import BaseException
    from wmproxymethods import HTTPException
    from wmproxymethods import SocketException
    from wmproxymethods import WMPException
    from wmproxymethods import ApiException
except:
    print "error : wmproxymethods not found in %s"%sys.path.__str__()
    sys.exit(2);
#
# ---------------------- General pourpose routines --------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
#
# Initialize variables used by the scheduler before parsing classad
def initSched () :
    ifile,ofile=os.popen4("voms-proxy-info")
    sfile=ofile.read().strip()
    if sfile=="Couldn't find a valid proxy.":
       print sfile
       print "error"
       print "execute voms-proxy-init --voms "
       sys.exit()
    elif  sfile.split("timeleft  :")[1].strip()=="0:00:00":
       print "error"
       print "proxy expired"
       sys.exit()
#
# Read a file containing a classad 
def parseClassAd (BossClassad, subdir):
    try:
        cladadd = ""
        cladfile=open("%s/%s"%(subdir,BossClassad),"r")
        clad=cladfile.read().strip()
        cladfile.close()
    except:
        raise

    vofile = ""
    configfile = ""
    url = ""
    vo = ""
    try:    
        while clad[0]=='[':
            clad=clad[1:]
            clad=clad[:-1]
            clad.strip()
        cladMap = clad.split(';')
        for p in cladMap:
            p = p.strip()
            if len(p) == 0 or p[0]=='#' :
                continue
            key = p.split('=')[0].strip()
            val = p.split('=')[1].strip()
            if ( key.lower()== "wmsconfig" ) :
                configfile = val.replace("\"", "")
            elif ( key.lower() == "wmproxyendpoints" ) :
                url = val.replace("\"", "")
            elif ( key.lower() == "virtualorganisation" ) :
                vo = val.replace("\"", "")
            else :
                cladadd = cladadd + p + ";\n"
#
# check for vo
#
        jdl=""
        if ( len(configfile) != 0 ):
            fileh = open(configfile, "r" )
            jdl=fileh.read();
            fileh.close
            mylist=jdl.lower().split("virtualorganisation")
            try :
                d=mylist[1]
                d=d.split("\"")[1]
                vo = d.strip()
            except :
                pass
            mylist=jdl.split("WMProxyEndpoints")
            try :
                d=mylist[1]
                d=d.split("\"")[1]
                url = d.strip()
            except :
                pass
        if  ( len(vo) == 0 ) :
            print "Jdl mandatory attribute is missing : VirtualOrganisation"
            print "Please add it in your schclassad"
            raise
        else :
            cladadd += "VirtualOrganisation = \"%s\";\n"%vo
#
# check for wms
#
        if ( len(url) == 0  ) :
            if ( len(configfile) == 0 ):
                path = os.environ['GLITE_WMS_LOCATION']
                configfile = "%s/etc/%s/glite_wms.conf"%(path,vo)
                try:
                    fileh = open(configfile, "r" )
                    jdl=fileh.read();
                    fileh.close
                except IOError, (errno,strerror):
                    print "error reading",configfile
                    print "%d %s\n"%(errno,strerror)
                    raise
                except :
                    print "error reading",path
                    print sys.exc_info()[0]
                    print sys.exc_info()[1]
                    raise
            mylist=jdl.split("WMProxyEndpoints")
            try :
                d=mylist[1]
                d=d.split("\"")[1]
                url = d.strip()
            except :
                pass
        if ( len(url) == 0  ) :
            print "Missing WMS"
            raise
#
        return url,cladadd
    except:
        raise

#
# --------------------- Scheduler specific routines -------------------------
#     (Update the routines of this section to match your scheduler needs)
#
# Submit the job and return the scheduler id
def submit (jdl, subdir, task_id, resub, zippedISB, sandboxMap, url):
    try :
# first check if the sandbox dir can be created        
        if os.path.exists("%s/SandboxDir"%subdir) != 0:
            print "Presence of this directory is dangerous."
            print "%s/SandboxDir"%subdir
            print "Remove it and try again the submission"
            sys.exit()
        wmproxy = Wmproxy(url)
        wmproxy.soapInit()
# tmp: delegate proxy a mano!
        ifile,ofile=os.popen4("glite-wms-job-delegate-proxy -d bossproxy --endpoint %s"%url)
        sfile=ofile.read()
        if sfile.find("Error -")>=0:
            print "error"
            print sfile
            sys.exit()
        delegationId ="bossproxy"
# it will be substituted by something like:
# ns=wmproxy.getGrstNs()
# wmproxy.getProxyReq(delegationId,ns)
# GRSTx509MakeProxyCert(&certtxt, stderr, (char*)request.c_str(),
# wmproxy.putProxy(delegationId, certtxt)
        task = wmproxy.jobRegister ( jdl, delegationId )
        taskId = task.getJobId()
        dag = task.getChildren()
        destURI = wmproxy.getSandboxDestURI(taskId)
        basedir = "SandboxDir"+destURI[0].split("/SandboxDir")[1]
#        print basedir           
        ifile,ofile=os.popen4("mkdir -p %s"%basedir)
        sfile=ofile.read()
        if len(sfile)!=0:
            print "mkdir error"
            print sfile
            sys.exit()
        ifile,ofile=os.popen4("mv %s %s"%(sandboxMap,basedir))
        sfile=ofile.read()
        if len(sfile)!=0:
            print "mv error"
            print sfile
            raise
        ifile,ofile=os.popen4("chmod 773 SandboxDir; chmod 773 SandboxDir/*")
        sfile=ofile.read()
#        ifile,ofile=os.popen4("tar pczf %s SandboxDir"%zippedISB)
        ifile,ofile=os.popen4("tar pczf %s %s/*"%(zippedISB,basedir))
        sfile=ofile.read()
        if len(sfile)!=0:
            print "tar error"
            print sfile
            raise
        command = "globus-url-copy file://%s/%s %s/%s"%(subdir,zippedISB,destURI[0],zippedISB)
#        print command
        ifile,ofile=os.popen4(command)
        sfile=ofile.read()
        if sfile.upper().find("ERROR")>=0 or sfile.find("wrong format")>=0 :
            print "globus-url-copy error"
            print sfile
            raise
        wmproxy.jobStart(taskId)
        for job in dag:
            name = job.getNodeName()
            name = name.replace("Node_","")
            jobId = job.getJobId()
            print "%s\t%d\t%s\t%s" %(name,resub,jobId,taskId)
    except BaseException, err:
        print "wmproxy error"
        ifile,ofile=os.popen4("rm -rf SandboxDir %s"%zippedISB)
        sfile=ofile.read()
        print err.toString()
        sys.exit()
    except:
        ifile,ofile=os.popen4("rm -rf SandboxDir %s"%zippedISB)
        sfile=ofile.read()
        print "error: exiting"
        sys.exit()
# delete temporary files
#    ifile,ofile=os.popen4("rm -rf SandboxDir")
    ifile,ofile=os.popen4("rm -rf SandboxDir %s"%zippedISB)
    sfile=ofile.read()
    print sfile
    if len(sfile)!=0:
        print sfile
        sys.exit()
#
#    return $id;
#
# ---------------------------- Start of main ---------------------------------
#
# ------------------- Get info on local environment --------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# submitting user
#$subuser = 
# submitting host
#$subhost = 
# submitting path
try :
    subdir = os.getcwd()
except:
    print "error"
    print sys.exc_info()[0]
    sys.exit()
#
# ------------------- Optional logging of submission -------------------------
#   (change file name and comment/uncomment the open statement as you wish)
#$logFile = "$subdir/bossSubmit.log";
#open (LOG, ">>$logFile") || {print STDERR "unable to write to $logFile. Logging disabled\n"};
#
# --------------------------- Get arguments ----------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# check number of arguments
correctlen = 5;
args=sys.argv
if len(args)!=correctlen:
    print "error"
    print "Wrong number of arguments to sub script: $len, expected: %d\n"%correctlen
    #print "task_id missing"
    sys.exit()
try :
# Boss task ID
    task_id=int(args[1])
# stdinput 
    stdin = args[2];
# common sandbox
    commonSandbox = args[3];
# logfile prefix
    log = args[4]
#
except ValueError:
    print "error"
    print "invalid task_id %s" % args[1]
    sys.exit()
except:
    print "error"
    print sys.exc_info()[0]
    sys.exit()

if task_id < 1 :
    print "error"
    print "task_id must be > 0.\nYour task_id is %s" % args[1]
    sys.exit()
#if (LOG) {
#    print LOG "\n====>> New scheduler call number $jid\n";
#    print LOG "$jid: Redirecting stderr & stdout to log file $stdout\n";
#}
#
# ------------------------ Other configuration -------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# The name of the executable to be submitted to the scheduler
i,o=os.popen4("which jobExecutor")
executable=o.read().strip()
ifile,ofile=os.popen4("cp -a %s %s"%(executable,subdir))
sfile=ofile.read()
if sfile.find("cannot")>=0 :
    print "error"
    print sfile
    sys.exit()
#
# ----- Scheduler specific initialization (before parsing classad -----------
# (do not modify this section unless for fixing bugs - please inform authors!)
initSched()
#
# ------------------- Read jobList file and loop over jobs ------------------- 
try:
    file=open("%s/submit_%d"%(subdir,task_id),"r")
except IOError, (errno,strerror):
    print "error"
    print "%s/submit_%d"%(subdir,task_id) 
    print "%d %s\n"%(errno,strerror)
    sys.exit()
except :
    print "error"
    print sys.exc_info()[0]
    print sys.exc_info()[1]
    sys.exit()
ranges=file.readlines();
count =0
#
# ------ Get additional information from classad file (if any)----------------
# (do not modify this section unless for fixing bugs - please inform authors!)
bossClassad = "BossClassAdFile_%d"%task_id
try:
    url,schedClassad = parseClassAd (bossClassad, subdir)
except:
    print bossClassad
    print sys.exc_info()[0]
    print sys.exc_info()[1]
    sys.exit()
#
for i in ranges:
    n=i.strip().split(":")
    if len(n)==0:
        try:
            start=int(i.strip())
            end=start
            resub=1
        except:
            print "error"
    
            print "wrong interval format"
            sys.exit()
    if len(n)==1:
        try:
            start=int(n[0])
            end=start
            resub=1
        except:
            print "error"

            print "wrong interval format"
            sys.exit()
    if len(n)==2:
        try:
            start=int(n[0])
            end=int(n[1])
            resub=1
        except:
            print "error"

            print "wrong interval format"
            sys.exit()
    if len(n)>2:
        try:
            start=int(n[0])
            end=int(n[1])+1
            resub=int(n[2])
        except:
            print "error"

            print "wrong interval format"
            sys.exit()
#
    sandboxMap = commonSandbox + " " + stdin + " jobExecutor "
    for id in range(start,end) :
        sandboxMap += "BossArchive_%d_%d_%d.tgz "%(task_id,id,resub)
#        
    stdout = "%s_%d__PARAM_.log"%(log,task_id)
    bossGlobalArchive="%s/%s"%(subdir,commonSandbox)
    stdinfile="%s/%s"%(subdir,stdin)
    InputSandbox = "\"file://%s\",\"file://%s\",\"file://%s\","%(bossGlobalArchive,executable,stdinfile)
    InputSandbox+="\"file://%s/BossArchive_%d__PARAM__%d.tgz\""%(subdir,task_id,resub)
    zippedISB = "BossFullArchive.tar.gz"
    jdl = "[\n"
    jdl += schedClassad
    jdl += "JobType = \"Parametric\" ;\n"
    jdl += "AllowZippedISB = true;\n"
    jdl += "ZippedISB = \"%s\";\n"%zippedISB
    jdl += "Parameters = %d ;\n" %end
    jdl += "ParameterStart = %d ;\n" %start
    jdl += "Executable = \"jobExecutor\" ;\n"
    jdl += "Arguments = \"_PARAM_\" ;\n"
    jdl += "StdInput = \"%s\" ;\n"%stdin
    jdl += "StdOutput = \"%s\";\n"%stdout
    jdl += "StdError = \"%s\";\n"%stdout
    jdl += "InputSandbox = {%s};\n"%InputSandbox
    jdl += "OutputSandbox = {\"BossOutArchive_%d__PARAM__%d.tgz\",\"%s\"};\n"%(task_id,resub,stdout)
    jdl += "]"
    ofile=open("parjdl_%d_%d"%(task_id,count),"w")
    ofile.write(jdl)
    ofile.close()
    #
#
# --------------------------- Ready to submit --------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
    submit (jdl, subdir, task_id, resub, zippedISB, sandboxMap, url)
#
# ----------------------------- End of main ----------------------------------
