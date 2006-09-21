#!/usr/bin/env python
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
# --------------------- Scheduler specific routines -------------------------
#     (Update the routines of this section to match your scheduler needs)
#
# Submit the job and return the scheduler id
def submit (subdir, task_id, resub, count,config ):
    configstr=""
    if len(config) != 0 :
        configstr = "-c " + config
# proxy delegation
    ifile,ofile=os.popen4("glite-wms-job-delegate-proxy -d bossproxy "+configstr)
    sfile=ofile.read()
    if sfile.find("Error -")>=0:
       print "error"
       print sfile
       sys.exit()
# submit
    ifile,ofile=os.popen4("glite-wms-job-submit -d bossproxy %s %s/parjdl_%d_%d"%(configstr,subdir,task_id,count))
    sfile=ofile.read()
    if sfile.find("Error -")>=0:
        print "error in submission"
        print sfile
        sys.exit()
    c=sfile.split("Your job identifier is:")[1].strip()
    parent=c.split("=")[0].strip()
#
# retrieving children ids
#
    ifile, ofile=os.popen4("glite-wms-job-status "+ parent)
    sfile=ofile.read()
    if sfile.find("Error")>=0:
        print "error in retrieving children ids"
        print sfile
        sys.exit()
    d=sfile.split("- Nodes information")[1].strip()
#
    j=[]
    d=d.split("Status info for the Job :")[1:]
    for job in d:
        j.append(job.split("\n")[0].strip())
#
    for job in j:
        ifile, ofile=os.popen4("glite-job-status -v 3 %s" % job)
        sfile=ofile.read()
        c=sfile.split("BossJobID = \"")[1].strip()
#        c=sfile.split("NodeName = \"")[1].strip()
        c=c.split("\"")[0].strip()
        print "%s\t%d\t%s\t%s" %(c,resub,job,parent)
#
# delete temporary files
#    unlink "$tmpfile";
#    unlink "BossArchive_${jid}.tgz";
#    return $id;
#
# ----------------------------------------------------------------------------
#
#
# ---------------------------- Start of main ---------------------------------
#
import os
import sys
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
# INSERIRE NEL LOOP
bossClassad = "BossClassAdFile_%d"%task_id
try:
    schedClassad,config=parseClassAd (bossClassad, subdir)
except:
    print "error"
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
    stdout = "%s_%d__PARAM_.log"%(log,task_id)
    InputSandbox="\"file://%s/BossArchive_%d__PARAM__%d.tgz\","%(subdir,task_id,resub)
    InputSandbox+="\"file://%s/%s\","%(subdir,commonSandbox)
    InputSandbox+="\"file://%s/%s\","%(subdir,stdin)
    InputSandbox+="\"file://%s\""%executable
    ofile=open("parjdl_%d_%d"%(task_id,count),"w")
    ofile.write("[\n")
    ofile.write(schedClassad)
    ofile.write("JobType = \"Parametric\" ;\n")
    ofile.write("AllowZippedISB = true;\n")
    ofile.write("Parameters = %d ;\n" % end)
    ofile.write("ParameterStart = %d ;\n" %start)
    ofile.write("Executable = \"jobExecutor\" ;\n")
    ofile.write("Arguments = \"_PARAM_\" ;\n")
    ofile.write("StdInput = \"%s\" ;\n"%stdin)
    ofile.write("StdOutput = \"%s\";\n"%stdout)
    ofile.write("StdError = \"%s\";\n"%stdout)
    ofile.write("InputSandbox = {%s};\n"%InputSandbox)
    ofile.write("OutputSandbox = {\"BossOutArchive_%d__PARAM__%d.tgz\",\"%s\"};\n"%(task_id,resub,stdout))
    ofile.write("UserTags = [ \n")
    ofile.write("BossJobID=\"_PARAM_\";\n")
    ofile.write("];\n")
    ofile.write("]")
    ofile.close()
#    sys.exit()
    count+=1
#
# --------------------------- Ready to submit --------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
for i in range(count):
    submit (subdir, task_id, resub, i, config)
#
# ----------------------------- End of main ----------------------------------
