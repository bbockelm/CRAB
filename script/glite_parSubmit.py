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
#
# log system
logFile = 0
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
# --------------------- Scheduler specific routines --------------------------
#     (Update the routines of this section to match your scheduler needs)
#
# Submit the job and return the scheduler id
def submit ( fname, resub, config ):
    configstr=""
    if len(config) != 0 :
        configstr = "-c " + config
# proxy delegation
#    ifile,ofile=os.popen4("glite-wms-job-delegate-proxy -d bossproxy "+configstr)
#    sfile=ofile.read()
#    if sfile.find("Error -")>=0:
#        if logFile : logFile.write ( sfile + '\n' )
#        sys.exit()
# submit
#    command = "glite-wms-job-submit -d bossproxy %s %s"%(configstr,fname)
    command = "glite-wms-job-submit -a %s %s"%(configstr,fname)
    ifile,ofile=os.popen4(command)
    sfile=ofile.read()
    if logFile :
        logFile.write ( "Executing command : `" + command + '`\n' )
        logFile.write ( sfile + '\n' )
    try:
        c=sfile.split("Your job identifier is:")[1].strip()
        parent=c.split("=")[0].strip()
    except:
        print "error"
        if logFile : logFile.write( 'error\n\n' )
        else : print sfile
        sys.exit()
#
# retrieving children ids
#
    try:
        path = os.environ['GLITE_WMS_LOCATION']
        libPath=os.path.join(path, "lib")
        sys.path.insert(0,libPath)
        libPath=os.path.join(path, "lib", "python")
        sys.path.append(libPath)
    except:
        msg = "Error: the GLITE_WMS_LOCATION variable is not set.\n"
    #
    try:
        path = os.environ['GLITE_LOCATION']
        libPath=os.path.join(path, "lib")
        sys.path.insert(0,libPath)
        libPath=os.path.join(path, "lib", "python")
        sys.path.append(libPath)
    except:
        msg = "Error: the GLITE_LOCATION variable is not set.\n"
    #
    # remove runtime warnings
    #
    try:
        import warnings
        warnings.simplefilter("ignore", RuntimeWarning)
        warnings.simplefilter("ignore", DeprecationWarning)
    except:
        pass
    #
    try:
        from glite_wmsui_LbWrapper import Status
        from glite_wmsui_AdWrapper import DagWrapper
        from Job import JobStatus
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
# instatiating status object
    status =   Status()
#
# Loading dictionary with available parameters list
    states = JobStatus.states
    status.getStatus(parent,1)
    err, apiMsg = status.get_error()
    if err:
        if logFile : logFile.write ( apiMsg )
        print "error in retrieving children ids"
        sys.exit()
    jobidInfo = status.loadStatus(0)
    jobidMap={}
    jdl = jobidInfo[ states.index("jdl")  ]
    dagad = DagWrapper()
    if dagad.fromString(jdl):
        err , apiMsg = dagad.get_error ()
        apiMsg = "Error in JDL  file: Attribute syntax error (%s)" %apiMsg
        if logFile : logFile.write ( apiMsg )
        else : print apiMsg
    else:
        vectMap=dagad.getMap()
        err , apiMsg = dagad.get_error ()
        if err:
            apiMsg = "Error in JDL  file while looking for jobid's: Attribute syntax error (%s)" %apiMsg
            if logFile : logFile.write ( apiMsg )
            else : print apiMsg
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
#
try :
    subdir = os.getcwd()
# submitting user
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
# ------ Get additional information from classad file (if any)----------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# 
if logFile :
    logFile.write ( "Warning: Only task level classad will be considered,\n" )
    logFile.write ( "Warning: no per job classads will be used, even if declared\n" )
    logFile.write ( "Warning: if specific sandbox are neede, they must be there for all jobs\n" )

config = ""
schedClassad = ""
try:
    schedClassad,config=parseClassAd (bossClassad, subdir)
    if logFile :
        logFile.write ( "Found classad for the task\n" )
except:
    pass
#
# ------------------ Preparing per range submission --------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
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
    has_spec_files = 0
    try :
#
        stdin = "BossWrapperSTDIN_%d__PARAM__%d.clad"%(task_id,resub);
        stdout = "%s_%d__PARAM_.log"%(log,task_id)
        InputSandbox="\"file://%s/%s\","%(subdir,commonSandbox)
        InputSandbox+="\"file://%s/%s\","%(subdir,stdin)
        InputSandbox+="\"file://%s\""%executable
        # handling of specific sandboxes
        for id in range(start,end) :
            specArchive = "%s/BossArchive_%d_%d.tgz"%(subdir,task_id,id)
            if os.path.exists( specArchive ) :
                has_spec_files += 1
        if has_spec_files == 0 :
            pass
        elif has_spec_files == (end - start) :
            InputSandbox+=",\"file://%s/BossArchive_%d__PARAM_.tgz\""%(subdir,task_id)
        else :
            raise "missing specific sanbox for some jobs. Abort"
# write to file
        try :
            ofile,fname = tempfile.mkstemp( "", "glite_par_%d_%d"%(task_id,count),subdir)
        except :
            fname = ("%s/glite_par_%d_%d_%s")%(subdir,task_id,count,tempfile.gettempprefix()[1:])
        ofile=open( fname,"w" )
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
        ofile.write("bossjobid=\"_PARAM_\";\n")
        ofile.write("];\n")
        ofile.write("]\n")
        ofile.close()
        count+=1
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
    for i in range(count):
        if logFile : logFile.write("Submitting jobs from task %d\n"%(task_id))
        try : 
            submit (fname, resub, config)
        except SystemExit, exit:
            print "submission failed"
            continue
        except:
            print "error"
            error = str ( traceback.format_exception(sys.exc_info()[0],
                                                     sys.exc_info()[1],
                                                     sys.exc_info()[2]) )
            if logFile : logFile.write( error + '\n\n' )
            else : print error
#        os.unlink(fname)
#
os.unlink(commonSandbox)
# close log file
if logFile :
    logFile.write( "\n\t\t**********************************************\n\n" )
    logFile.close()
#
# ----------------------------- End of main ----------------------------------
