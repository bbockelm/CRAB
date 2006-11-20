#!/usr/bin/env python
import sys
import os
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
##
# Read a file containing a classad 
def parseClassAd (BossClassad, subdir):
    try:
        cladadd = ""
        configfile = ""
        cladfile=open("%s/%s"%(subdir,BossClassad),"r")
        clad=cladfile.read().strip()
        cladfile.close()
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
            else :
                cladadd = cladadd + p + ";\n"
        return cladadd,configfile
    except:
        raise
#
# Submit the job and return the scheduler id
def submit (subdir, task_id, resub,config ):
    configstr=""
    if len(config) != 0 :
        configstr = "-c " + config
# proxy delegation
#    ifile,ofile=os.popen4("glite-wms-job-delegate-proxy -d bossproxy "+configstr)                          
#    sfile=ofile.read()
#    if sfile.find("Error -")>=0:
#       print "error"
#       print sfile
#       sys.exit()
# submit        
#    ifile,ofile=os.popen4("glite-wms-job-submit -d bossproxy %s %s/parjdl_%d"%(configstr,subdir,task_id))
    ifile,ofile=os.popen4("glite-wms-job-submit -a %s %s/parjdl_%d"%(configstr,subdir,task_id))
    sfile=ofile.read()
    try:
        c=sfile.split("Your job identifier is:")[1].strip()
        parent=c.split("=")[0].strip()
    except:
        print sfile
        print "submission failed"
        sys.exit()
#
# retrieving children ids
#
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
    from glite_wmsui_LbWrapper import Status
    from glite_wmsui_AdWrapper import DagWrapper
    import warnings
    ## Get rid of some useless warning
    warnings.simplefilter("ignore", RuntimeWarning)
    from Job import JobStatus
#
# instatiating status object
    status =   Status()
#
# Loading dictionary with available parameters list
    states = JobStatus.states
    status.getStatus(parent,1)
    err, apiMsg = status.get_error()
    if err:
        print "error in retrieving children ids"
        sys.exit()
    jobidInfo = status.loadStatus(0)
    jobidMap={}
    jdl = jobidInfo[ states.index("jdl")  ]
    dagad = DagWrapper()
    if dagad.fromString(jdl):
        err , apiMsg = dagad.get_error ()
        errMsg('Warning','UI_JDL_WRONG_SYNTAX' ,  apiMsg)
    else:
        vectMap=dagad.getMap()
        err , apiMsg = dagad.get_error ()
        if err:
            errMsg('Warning','UI_JDL_WRONG_SYNTAX' ,  apiMsg)
        else:
            for i in range(len(vectMap)/2):
                print "%s\t%d\t%s\t%s" %(vectMap[i*2+1].split('_')[1],resub,vectMap[i*2],parent)
    return
#
# ----------------------------------------------------------------------------
#
#
# ---------------------------- Start of main ---------------------------------
#
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
#
bossClassad = "BossClassAdFile_%d"%task_id
for i in ranges:
    n=i.strip().split(":")
    if len(n)==0:
        try:
            start=int(i.strip())
            end=start
            resub=1
        except:
            print "error"
            print "wrong interval format:"+i
            sys.exit()
    if len(n)==1:
        try:
            start=int(n[0])
            end=start
            resub=1
        except:
            print "error"
            print "wrong interval format:"+i
            sys.exit()
    if len(n)==2:
        try:
            start=int(n[0])
            end=int(n[1])
            resub=1
        except:
            print "error"
            print "wrong interval format:"+i
            sys.exit()
    if len(n)>2:
        try:
            start=int(n[0])
            end=int(n[1])+1
            resub=int(n[2])
        except:
            print "error"
            print "wrong interval format:"+i
            sys.exit()

    ofile=open("parjdl_%d"%(task_id),"w")
    ofile.write("[\n")
    ofile.write("Type = \"collection\" ;\n")
    GlobalSandbox="\"file://%s/%s\","%(subdir,commonSandbox)
    GlobalSandbox+="\"file://%s/%s\","%(subdir,stdin)
    GlobalSandbox+="\"file://%s\""%executable
    ofile.write("InputSandbox = {%s} ;\n"%GlobalSandbox)
#    ofile.write("AllowZippedISB = true;\n")
    config=""
    try:
        cladadd,config=parseClassAd (bossClassad, subdir)
        ofile.write( cladadd )
    except:
        pass
#        print "error"
#        print bossClassad
#        print sys.exc_info()[0]
#        print sys.exc_info()[1]
#        sys.exit()
    ofile.write("Nodes = {\n")
        
    for i in range(start,end):
        InputSandbox="\"file://%s/BossArchive_%d_%d_%d.tgz\","%(subdir,task_id,i,resub)
        InputSandbox+="root.InputSandbox"
        stdout = "%s_%d_%d.log"%(log,task_id,i)
        ofile.write("[\n")
        ofile.write("NodeName = \"BossJob_%d\";\n"%i);
        ofile.write("Executable = \"jobExecutor\" ;\n")
        ofile.write("Arguments = \"%d\" ;\n"%i)
        ofile.write("StdInput = \"%s\" ;\n"%stdin)
        ofile.write("StdOutput = \"%s\";\n"%stdout)
        ofile.write("StdError = \"%s\";\n"%stdout)
        ofile.write("InputSandbox = {%s};\n"%InputSandbox)
        ofile.write("OutputSandbox = {\"BossOutArchive_%d_%d_%d.tgz\",\"%s\"};\n"%(task_id,i,resub,stdout))
        ofile.write("UserTags = [ \n")
        ofile.write("bossjobid=\"%d\";\n"%i)
        ofile.write("];\n")
        try:
            jobcladadd="BossClassAdFile_%d_%d"%(task_id,i)
            ofile.write( parseClassAd (jobcladadd, subdir))
        except:
            pass
        if (i+1)==end :
            ofile.write("]\n")
        else :
            ofile.write("],\n")   
    ofile.write("};\n")
    ofile.write("]")
    ofile.close()
#
# --------------------------- Ready to submit --------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
    submit (subdir, task_id, resub, config)
#
# ----------------------------- End of main ----------------------------------
