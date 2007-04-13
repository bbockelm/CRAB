#!/usr/bin/env python
import sys
import os
import traceback
import tempfile
#
# global variables
subdir = ""
task_id = ""
file = 0
ranges = ""
jdl = ""
zippedISB = "BossFullArchive.tar.gz"
#
# log system
logFile = 0
#
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
    print "error"
    error = str ( traceback.format_exception(sys.exc_info()[0],
                                             sys.exc_info()[1],
                                             sys.exc_info()[2]) )
    if logFile :
        logFile.write ( msg )
        logFile.write ( error + '\n\n' )
    else :
        print error
    sys.exit()
#
# ---------------------- General pourpose routines ---------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
#
# Initialize variables used by the scheduler before parsing classad
def initSched () :
    ifile,ofile=os.popen4("voms-proxy-info")
    sfile=ofile.read().strip()
    if sfile=="Couldn't find a valid proxy.":
        if logFile : logFile.write ( sfile + '\n' )
        print "proxy not found"
        sys.exit()
    elif  sfile.split("timeleft  :")[1].strip()=="0:00:00":
        if logFile : logFile.write ( sfile + '\n' )
        print "proxy expired"
        sys.exit()
#
# Read a file containing a classad
def parseClassAd (BossClassad, process='n'):
#
    cladDict = {}
    cladAddDict = {}
    endpoints = []
    configfile = ""
    cladDict,endpoints,configfile = processClassAd ( BossClassad, endpoints )
#
    if process=='y' :
        dummyfile = ""
        if ( len(configfile) != 0 ):
            cladAddDict,endpoints,dummyfile = processClassAd(configfile, endpoints)
        if len(endpoints) == 0 and len(configfile) == 0 :
            try:
                if 'virtualorganisation' in cladDict :
                    vo = cladDict['virtualorganisation'].replace("\"", "")
                elif 'virtualorganisation' in cladAddDict :
                    vo = cladAddDict['virtualorganisation'].replace("\"", "")
                else :
                    print "Missing VirtualOrganisation"
                    raise "Missing VirtualOrganisation"
                path = os.environ['GLITE_WMS_LOCATION']
                configfile = "%s/etc/%s/glite_wms.conf"%(path,vo)
                cladAddDict,endpoints,dummyfile = processClassAd(configfile, endpoints)
            except :
                pass
        if ( len(endpoints) == 0  ) :
            print "Missing WMS"
            raise "Missing WMS"
# merge with config file
    for k, v in cladDict.iteritems():
        cladAddDict[k] = v
# always allowZippedISB
    cladAddDict[ "allowzippedisb" ] = "true"
# make the actual jdl
    cladadd = ''
    for k, v in cladAddDict.iteritems():
        cladadd += k + ' = ' + v + ';\n'
    return endpoints,cladadd
#
# Parse config classad
def processClassAd(  file, endpoints ):
    cladDict = {}
    configfile = ""
    try:
        fileh = open(file, "r" )
        userClad=fileh.read().strip();
        fileh.close
        if len(userClad) == 0 :
            raise "empty userClad"
        while userClad[0]=='[':
            userClad=userClad[1:-1].strip()
        if userClad.find("WmsClient") >=0 :
            userClad = (userClad.split("WmsClient")[1]).strip()
            while userClad[0]=='[' or userClad[0]== '=' :
                userClad=userClad[1:-1].strip()
        cladMap = userClad.split(';')
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
                url = url.replace("\n", " ")
                url = url.replace("#", ",#")
                endpoints = endpoints + url.split(',')
            else :
                cladDict[ key ] = val
    except:
        raise
    return cladDict,endpoints,configfile
#
# --------------------- Scheduler specific routines -------------------------
#     (Update the routines of this section to match your scheduler needs)
#
# Submit the job and return the scheduler id
def submit ( resub, sandboxMap, url ):
    try :
# first check if the sandbox dir can be created
        if os.path.exists("%s/SandboxDir"%subdir) != 0:
            if logFile :
                logFile.write ( "%s/SandboxDir"%subdir + 'already exists\n' )
                logFile.write ( 'Presence of this directory is dangerous\n' )
                logFile.write ( 'Remove it and try again the submission\n' )
            raise "existing SandboxDir"
        wmproxy = Wmproxy(url)
        wmproxy.soapInit()
# tmp: delegate proxy a mano!
        ifile,ofile=os.popen4("glite-wms-job-delegate-proxy -d bossproxy --endpoint %s"%url)
        sfile=ofile.read()
        if sfile.find("Error -")>=0:
            if logFile : logFile.write ( "Warning : \n" +sfile + '\n' )
        delegationId ="bossproxy"
# it will be substituted by something like:
# ns=wmproxy.getGrstNs()
# wmproxy.getProxyReq(delegationId,ns)
# GRSTx509MakeProxyCert(&certtxt, stderr, (char*)request.c_str(),
# wmproxy.putProxy(delegationId, certtxt)
        task = wmproxy.jobRegister ( jdl, delegationId )
        taskId = task.getJobId()
        if logFile : logFile.write ( "Parent ID : " +taskId + '\n' )
        dag = task.getChildren()
        destURI = wmproxy.getSandboxDestURI(taskId)
        basedir = "SandboxDir"+destURI[0].split("/SandboxDir")[1]
        if logFile : logFile.write ( "destURI : " + basedir + '\n' )
        ifile,ofile=os.popen4("mkdir -p %s"%basedir)
        sfile=ofile.read()
        if len(sfile)!=0:
            if logFile : logFile.write ( "Error : \n" +sfile + '\n' )
            raise "mkdir error"
        ifile,ofile=os.popen4("cp %s %s"%(sandboxMap,basedir))
        sfile=ofile.read()
        if len(sfile)!=0:
            if logFile : logFile.write ( "Error : \n" +sfile + '\n' )
            raise "cp error"
        ifile,ofile=os.popen4("chmod 773 SandboxDir; chmod 773 SandboxDir/*")
        sfile=ofile.read()
#        ifile,ofile=os.popen4("tar pczf %s SandboxDir"%zippedISB)
        ifile,ofile=os.popen4("tar pczf %s %s/*"%(zippedISB,basedir))
        sfile=ofile.read()
        if len(sfile)!=0:
            if logFile : logFile.write ( "Error : \n" +sfile + '\n' )
            raise "tar error"
        command = "globus-url-copy file://%s/%s %s/%s"%(subdir,zippedISB,destURI[0],zippedISB)
        ifile,ofile=os.popen4(command)
        sfile=ofile.read()
        if sfile.upper().find("ERROR")>=0 or sfile.find("wrong format")>=0 :
            if logFile : logFile.write ( "Error : \n" +sfile + '\n' )
            raise "globus-url-copy error"
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
            if logFile : logFile.write ( "Warning : \n" +sfile + '\n' )
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
try :
    subdir = os.getcwd()
#$subuser = 
# submitting host
#$subhost = 
# submitting path
#
# --------------------------- Get arguments ----------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# check number of arguments
#
    correctlen = 5;
    args=sys.argv
    if len(args) < correctlen:
        print "Wrong number of arguments to sub script: %d expected: %d"%(len(args),correctlen)
        sys.exit()
    # Boss task ID
    task_id=int(args[1])
    # common sandbox
    commonSandbox = args[2];
    # logfile prefix
    log = args[3]
    # jobId list file
    jobList = args[4]
    # log system
    if len(args) == 6:
        logFile = open(args[5],'a')
        logFile.write( "\n\t************************************************\n\n" )
        logFile.write( "Submitting jobs from task %d\n"%task_id );
#
# ------------------------ Other configuration -------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# The name of the executable to be submitted to the scheduler
    i,o=os.popen4("which jobExecutor")
    executable=o.read().strip()
#    ifile,ofile=os.popen4("cp %s %s"%(executable,subdir))
#    sfile=ofile.read()
#    if sfile.find("cannot")>=0 :
#        if logFile : logFile.write ( "Error : \n" +sfile + '\n' )
#        raise
#
# ----- Scheduler specific initialization (before parsing classad) -----------
# (do not modify this section unless for fixing bugs - please inform authors!)
    count =0
    bossClassad = "BossClassAdFile_%d"%task_id
#
# -------------------- Common part for every submission ----------------------
#  Just comment it to avoid proxy check
    initSched()
#
# ------------------- Read jobList file and loop over jobs ------------------- 
# (do not modify this section unless for fixing bugs - please inform authors!)
#
    file=open(jobList)
    ranges=file.readlines();
except :
    print "error"
    error = str ( traceback.format_exception(sys.exc_info()[0],
                                             sys.exc_info()[1],
                                             sys.exc_info()[2]) )
    if logFile : logFile.write ( error + '\n\n' )
    else : print error
#
# ------------------ Preparing per range submission --------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
#
try:
    endpoints,schedClassad = parseClassAd (subdir + '/' + bossClassad, 'y')
except:
    print bossClassad
    print sys.exc_info()[0]
    print sys.exc_info()[1]
    sys.exit()
#
for i in ranges:
    try :
        n=i.strip().split(":")
        if len(n)==0:
            start=int(i.strip())
            end=start
            resub=1
        elif len(n)==1:
            start=int(n[0])
            end=start
            resub=1
        elif len(n)==2:
            start=int(n[0])
            end=int(n[1])
            resub=1
        elif len(n)>2:
            start=int(n[0])
            end=int(n[1])+1
            resub=int(n[2])
        else :
            raise
    except:
        print "error"
        error = str ( traceback.format_exception(sys.exc_info()[0],
                                                 sys.exc_info()[1],
                                                 sys.exc_info()[2]) )
        if logFile :
            logFile.write ( "wrong range: %s\n" %i )
            logFile.write ( error  + '\n\n' )
        else :
            print "wrong range: ",i 
            print error
        continue
#
    try :
        GlobalSandbox = "\"file://%s/%s\",\"file://%s\""%(subdir,commonSandbox,executable)
        jdl = "[\n"
        jdl += "Type = \"collection\" ;\n"
        jdl += "ZippedISB = \"%s\";\n"%zippedISB
        jdl += schedClassad
        jdl += "Nodes = {\n"
        sandboxMap = commonSandbox + " " + executable + " "
#
        ISBindex = 1
        for id in range(start,end):
            ISBindex += 1
            InputSandbox = "root.inputsandbox[0],root.inputsandbox[1]"
            InputSandbox += ",root.inputsandbox[%d]"%ISBindex
            stdin="BossWrapperSTDIN_%d_%d_%d.clad"%(task_id,id,resub);
            stdinpath = "%s/%s"%(subdir,stdin)
            sandboxMap += stdinpath + " "
            GlobalSandbox += ",\"file://%s\""%(stdinpath)
            specArchive = "%s/BossArchive_%d_%d.tgz"%(subdir,task_id,id)
            if os.path.exists( specArchive ) :
                ISBindex += 1
                InputSandbox += ",root.inputsandbox[%d]"%ISBindex
                sandboxMap += specArchive + " "
                GlobalSandbox += ",\"file://%s\""%(specArchive)
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
    except:
        print "error"
        error = str ( traceback.format_exception(sys.exc_info()[0],
                                                 sys.exc_info()[1],
                                                 sys.exc_info()[2]) )
        if logFile : logFile.write( error + '\n\n' )
        else : print error
#
# --------------------------- Ready to submit --------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
    if logFile : logFile.write("Submitting jobs from task %d\n"%(task_id))
    for url in endpoints :
        try :
            url = url.replace("\"", "").strip()
            if  len( url ) == 0 or url[0]=='#' :
                continue
            if logFile : logFile.write( url + '\n' )
            submit ( resub, sandboxMap, url )
            break
        except BaseException, err:
            if logFile :
                logFile.write( err.toString() )
                logFile.write( "\nfailed submission to " + url )
            continue
        except SystemExit, exit:
            print "submission failed"
            error = str ( traceback.format_exception(sys.exc_info()[0],
                                                     sys.exc_info()[1],
                                                     sys.exc_info()[2]) )
            if logFile : logFile.write( error )
            break
        except :
            print "submission failed"
            error = str ( traceback.format_exception(sys.exc_info()[0],
                                                     sys.exc_info()[1],
                                                     sys.exc_info()[2]) )
            if logFile : logFile.write( error )
            break
#
# close log file
if logFile :
    logFile.write( "\nUsed jdl : \n" + jdl + '\n' )
    logFile.write( "\n\t************************************************\n\n" )
    logFile.close()
#
# ----------------------------- End of main ----------------------------------
