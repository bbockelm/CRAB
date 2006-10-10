#!/usr/bin/env python
import sys
import os
import socket

# Add GLITE_WMS_LOCATION to the python path
try:
    path = os.environ['GLITE_LOCATION']
    libPath=os.path.join(path, "lib")
    sys.path.append(libPath)
    libPath=os.path.join(path, "lib", "python")
    sys.path.append(libPath)
except:
    msg = "Error: the GLITE_LOCATION variable is not set."
#
try:
    path = os.environ['GLITE_WMS_LOCATION']
    libPath=os.path.join(path, "lib")
    sys.path.append(libPath)
    libPath=os.path.join(path, "lib", "python")
    sys.path.append(libPath)
except:
    msg = "Error: the GLITE_WMS_LOCATION variable is not set."
#
# rimuovere al piu' presto!!!
try :
    sys.path.append("/home/codispot/SOAPpy-0.12.0/build/lib")
except :
    pass
#
try:
    import SOAPpy
except:
    print "error : SOAPpy not found in %s"%sys.path.__str__()
    sys.exit(2);
#
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
    pass
    # ifile,ofile=os.popen4("voms-proxy-info")
    # sfile=ofile.read().strip()
    # if sfile=="Couldn't find a valid proxy.":
    #    print sfile
    #    print "error"
    #    print "execute voms-proxy-init --voms "
    #    sys.exit()
    # elif  sfile.split("timeleft  :")[1].strip()=="0:00:00":
    #    print "error"
    #    print "proxy expired"
    #    sys.exit()
#
# Read a file containing a classad
def parseClassAd (BossClassad, process='n'):
#
    cladDict = {}
    endpoints = []
    configfile = ""
    cladDict,endpoints,configfile = processClassAd ( BossClassad, endpoints )
#
    if process=='y' :
        dummyfile = ""
        if ( len(configfile) != 0 ):
            cladDict,endpoints,dummyfile = processClassAd(configfile, endpoints)
        if len(endpoints) == 0 and len(configfile) == 0 :
            try:
                path = os.environ['GLITE_WMS_LOCATION']
                vo = cladDict['virtualorganisation'].replace("\"", "")
                configfile = "%s/etc/%s/glite_wms.conf"%(path,vo)
                cladDict,endpoints,dummyfile = processClassAd(configfile, endpoints)
            except :
                pass
        if ( len(endpoints) == 0  ) :
            print "Missing WMS"
            raise
        # always allowZippedISB
        cladDict[ "allowzippedisb" ] = "true"
# make the actual jdl
    cladadd = ''
    for k, v in cladDict.iteritems():
        cladadd += k + ' = ' + v + ';\n'
    return endpoints,cladadd
#
# Parse config classad
def processClassAd(  file, endpoints ):
    cladDict = {}
    configfile = ""
    try:
        fileh = open(file, "r" )
        jdl=fileh.read().strip();
        fileh.close
        if len(jdl) == 0 :
            raise
        while jdl[0]=='[':
            jdl=jdl[1:-1].strip()
        if jdl.find("WmsClient") >=0 :
            jdl = (jdl.split("WmsClient")[1]).strip()
            while jdl[0]=='[' or jdl[0]== '=' :
                jdl=jdl[1:-1].strip()
        cladMap = jdl.split(';')
        for p in cladMap:
            p = p.strip()
            if len(p) == 0 or p[0]=='#' :
                continue
            index = p.find('=')
            key = p[0:index].strip().lower()
            val = p[index+1:].strip()
            if ( key == "wmsconfig" ) :
                configfile = val.replace("\"", "")
            elif ( key == "wmproxyendpoints" ) :
                url = val[ val.find('{') +1 : val.find('}') ]
                endpoints = endpoints + url.split(',')
            else :
                cladDict[ key ] = val
    except:
        raise
    return cladDict,endpoints,configfile
#
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
            raise
        wmproxy = Wmproxy(url)
        wmproxy.soapInit()
# tmp: delegate proxy a mano!
        ifile,ofile=os.popen4("glite-wms-job-delegate-proxy -d bossproxy --endpoint %s"%url)
        sfile=ofile.read()
        if sfile.find("Error -")>=0:
            print "Warning"
            print sfile
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
            raise
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
            name = name.replace("BossJob_","")
            jobId = job.getJobId()
            print "%s\t%d\t%s\t%s" %(name,resub,jobId,taskId)
# cleaning up everything: delete temporary files and exit
#        ifile,ofile=os.popen4("rm -rf SandboxDir")
        ifile,ofile=os.popen4("rm -rf SandboxDir %s"%zippedISB)
        sfile=ofile.read()
        if len(sfile)!=0:
            print sfile
    except:
        ifile,ofile=os.popen4("rm -rf SandboxDir %s"%zippedISB)
        sfile=ofile.read()
        raise
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
#
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
#
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
    print "%s/submit_%d"%(subdir,task_id) 
    print "%d %s\n"%(errno,strerror)
    sys.exit()
except :
    print sys.exc_info()[0]
    print sys.exc_info()[1]
    sys.exit()
ranges=file.readlines();
count =0
#
# ------ Get additional information from classad file (if any)----------------
# (do not modify this section unless for fixing bugs - please inform authors!)
#
bossClassad = "BossClassAdFile_%d"%task_id
try:
    endpoints,schedClassad = parseClassAd (subdir + '/' + bossClassad, 'y')
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
            print "wrong interval format:"+i
            sys.exit()
    if len(n)==1:
        try:
            start=int(n[0])
            end=start
            resub=1
        except:
            print "wrong interval format:"+i
            sys.exit()
    if len(n)==2:
        try:
            start=int(n[0])
            end=int(n[1])
            resub=1
        except:
            print "wrong interval format:"+i
            sys.exit()
    if len(n)>2:
        try:
            start=int(n[0])
            end=int(n[1])+1
            resub=int(n[2])
        except:
            print "wrong interval format:"+i
            sys.exit()
#
    jdl = "[\n"
    jdl += "Type = \"collection\" ;\n"
    bossGlobalArchive="%s/%s"%(subdir,commonSandbox)
    stdinfile="%s/%s"%(subdir,stdin)
    zippedISB = "BossFullArchive.tar.gz";
    GlobalSandbox = "\"file://%s\",\"file://%s\",\"file://%s\""%(bossGlobalArchive,executable,stdinfile)
    jdl += "ZippedISB = \"%s\";\n"%zippedISB
    jdl += schedClassad
    jdl += "Nodes = {\n"
#
###    sandboxMap["global"] = commonSandbox + " " + stdin + " jobExecutor"
    sandboxMap = commonSandbox + " " + stdin + " jobExecutor "
#
    ISBindex = 3
    for id in range(start,end):
        InputSandbox="BossArchive_%d_%d_%d.tgz"%(task_id,id,resub)
        sandboxMap += InputSandbox + " "
        GlobalSandbox += ",\"file://%s/%s\""%(subdir,InputSandbox)
        InputSandbox="root.inputsandbox[0],root.inputsandbox[1],root.inputsandbox[2],root.inputsandbox[%d]"%ISBindex
        ISBindex = ISBindex + 1
        stdout = "%s_%d_%d.log"%(log,task_id,id)
        jdl += "[\n"
        jdl += "NodeName = \"BossJob_%d\";\n"%id
        jdl += "Executable = \"jobExecutor\" ;\n"
        jdl += "Arguments = \"%d\" ;\n"%id
        jdl += "StdInput = \"%s\" ;\n"%stdin
        jdl += "StdOutput = \"%s\";\n"%stdout
        jdl += "StdError = \"%s\";\n"%stdout
        jdl += "InputSandbox = {%s};\n"%InputSandbox
        jdl += "OutputSandbox = {\"BossOutArchive_%d_%d_%d.tgz\",\"%s\"};\n"%(task_id,id,resub,stdout)
        try:
            jobcladadd="BossClassAdFile_%d_%d"%(task_id,id)
            jdl +=  parseClassAd ( subdir + '/' + jobcladadd )
        except:
            pass
        if (id+1)==end :
            jdl += "]\n"
        else :
            jdl += "],\n"
    jdl += "};\n"
    jdl += "InputSandbox = {%s} ;\n"%GlobalSandbox
    jdl += "]"
    ofile=open("parjdl_%d"%(task_id),"w")
    ofile.write(jdl)    
    ofile.close()
#
# --------------------------- Ready to submit --------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
    for url in endpoints :
        try :
            url = url.replace("\"", "").strip()
            if  len( url ) == 0 or url[0]=='#' :
                continue
            submit (jdl, subdir, task_id, resub, zippedISB, sandboxMap, url)
            sys.exit(0)
        except BaseException, err:
            print err.toString()
            print "failed submission to",url
            continue
        except SystemExit, exit:
            sys.exit()
        except :
            print "submission failed"
            sys.exit()            
#
# ----------------------------- End of main ----------------------------------
