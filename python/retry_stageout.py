#! /usr/bin/env python

# issues:
# 
# - error checking on subprocesses
# - where to get remote stageout PFN
# - authentication?
# - tool for copying
# - correct ExitCode in rewritten fjr

import commands, re, os
import getopt, sys 
import xml.dom.minidom
from xml.dom.minidom import Node

class CopyError(Exception):
    type = -1

quiet = False
dryrun = False
remove_after_copy = False
copy_to_ui = False

prog_name = ""

def usage () :
    print prog_name + ":", "usage: retry_stageout.py -c <crab directory> [--help | -h] [--dry-run | -n] [--copy-to-ui | -l] [--quiet | -q] [--verbose | -v | -vv | -vvv]"

def help () :
    usage()
    print prog_name + ":"
    print prog_name + ":",  "-c\t\t\t (Mandatory) CRAB project directory to parse"
    print prog_name + ":",  "--help, -h\t\t Print this message"
    print prog_name + ":",  "--dry--run, -n\t Do not copy anything, only print a list of local PFN's that need to be copied"
    print prog_name + ":",  "--copy-to-ui, -l\t Copy output to local UI in the res dir of crab directory"
    print prog_name + ":",  "--quiet, -q\t\t Print only error messages or the list of PFN's produced by -n"
    print prog_name + ":",  "--verbose, -v, -vv, -vvv\t Be verbose."
    print prog_name + ":",  "\t The first level of verbosity prints what the program is doing and whether external commands succeeded"
    print prog_name + ":",  "\t second level also prints the output of external commands"
    print prog_name + ":",  "\t third level runs the external commands in verbose mode, if available"

# we use this function for popen calls so that we can control verbosity
def getstatusoutput (cmd):
    if verbosity > 0:
        print prog_name + ":", "executing command:", cmd
    (stat, output) = commands.getstatusoutput(cmd)
    if verbosity > 0:
        print prog_name + ":", "\tcommand:", cmd, "exit status:", stat
        if verbosity > 1:
            print prog_name + ":", "\tcommand:", cmd, "output:", output
    return stat, output

def get_fjrs (directory) :
    cmd = '/bin/ls ' + directory + '/res/*.xml'
    (stat, fjrs) = getstatusoutput(cmd)
    if stat != 0:
        print prog_name + ":", "aborting retrieval, error:",  fjrs
        raise CopyError
    return fjrs.split('\n')

def get_nodes () :
    cmd = "wget -O- -q http://cmsweb.cern.ch/phedex/datasvc/xml/prod/nodes"
    (stat, nodes) = getstatusoutput(cmd)
    if stat != 0:
        print prog_name + ":", "aborting retrieval, error:", nodes
        raise CopyError
    return nodes

def local_stageout (doc) :
    for node in doc.getElementsByTagName("FrameworkJobReport"):
        key =  node.attributes.keys()[0].encode('ascii')
        if node.attributes[key].value == "Failed":
            for node2 in doc.getElementsByTagName("FrameworkError"):
                exitStatus = node2.attributes["ExitStatus"].value
                type = node2.attributes["Type"].value
                # print prog_name + ":", type, exitStatus
                if exitStatus == "60308" and type == "WrapperExitCode":
                    node.attributes[key].value = "Success"
                    node2.attributes["ExitStatus"].value = "0"
                    return True
    return False

def local_stageout_filenames (doc) :
    lfn = ""
    for node in doc.getElementsByTagName("LFN"):
        if node.parentNode.tagName == "File":
            lfn = node.firstChild.nodeValue.strip()
    pfn = ""
    for node in doc.getElementsByTagName("PFN"):
        if node.parentNode.tagName == "File":
            pfn = node.firstChild.nodeValue.strip()
    return lfn, pfn

def local_stageout_filenames_from_datasvc (doc) :
    # convert SEName into node
    seName = ""
    for node in doc.getElementsByTagName("SEName"):
        if node.parentNode.tagName == "File":
            seName = node.firstChild.nodeValue.strip()
    if seName == "":
        print prog_name + ":", "could not find SEName in fjr, aborting retrieval"
        raise CopyError
    nodeName = ""
    for node in nodes.getElementsByTagName("node"):
        se = ""
        name = ""
        for key in node.attributes.keys():
            if key.encode("ascii") == "se":
                se = node.attributes[key].value
            if key.encode("ascii") == "name":
                name = node.attributes[key].value
        if se == seName:
            nodeName = name
            break
    if verbosity > 0:
        print prog_name + ":", "local stageout nodeName =", nodeName
    lfn = ""
    for node in doc.getElementsByTagName("LFN"):
        if node.parentNode.tagName == "File":
            lfn = node.firstChild.nodeValue.strip()
    cmd = "wget -O- -q \"http://cmsweb.cern.ch/phedex/datasvc/xml/prod/lfn2pfn?node=" + nodeName + "&lfn=" + lfn + "&protocol=srmv2\""
    (stat, pfnXml) = getstatusoutput(cmd)
    if stat != 0:
        print prog_name + ":", "aborting retrieval, error:", pfnXml
        raise CopyError
    try:
        pfnDoc = xml.dom.minidom.parseString(pfnXml)
    except:
        print prog_name + ":", "aborting retrieval, could not parse pfn xml for node/lfn:", nodeName, lfn
        raise CopyError
    pfn = ""
    for node in pfnDoc.getElementsByTagName("mapping"):
        for key in node.attributes.keys():
            if key.encode("ascii") == "pfn":
                pfn = node.attributes[key].value.encode("ascii")
    return lfn, pfn

def cp_target () :
    # this is a bit trickier; we need to parse CMSSW.sh to get $endpoint
    cmd = "grep 'export endpoint=' " + directory + "/job/CMSSW.sh"
    (stat, grep_output) = getstatusoutput(cmd)
    if stat != 0:
        print prog_name + ":", "aborting retrieval, error:", grep_output
        raise CopyError
    return grep_output.replace("export endpoint=", "")

def cp_ui_target():
    ### FEDE FOR COPY OF FILE WITH EXITCODE 60308 TO UI
    path =  os.getcwd() + '/' + directory + '/res/'
    endpoint = 'file:/' + path
    return path, endpoint

def copy_local_to_remote_pfn (fjr) :
    (local_lfn, local_pfn) = local_stageout_filenames_from_datasvc(fjr)
    remote_filename = os.path.split(local_lfn)[1]
    ### FEDE FOR COPY OF FILE WITH EXITCODE 60308 TO UI
    if copy_to_ui:
        path, endpoint = cp_ui_target()
        remote_pfn = endpoint + remote_filename
        remote_path = path + remote_filename 
    else:
        remote_pfn = cp_target() + remote_filename
    list_stageout.append(local_pfn)
    if not quiet:
        print prog_name + ":", "copying from local:", local_pfn, "to remote:", remote_pfn
    if dryrun:
        return
    # copy
    lcg_cp = "lcg-cp "
    if verbosity > 2:
        lcg_cp += "-v "
    (lcg_cp_stat, lcg_cp_output) = getstatusoutput(lcg_cp + "-D srmv2 " + local_pfn + " " + remote_pfn)
    if lcg_cp_stat != 0:
        print prog_name + ":", "aborting retrieval, copy error:", lcg_cp_output
        raise CopyError
    # check copied file
    (lcg_ls_source_stat, lcg_ls_source) = getstatusoutput("lcg-ls -l -D srmv2 " + local_pfn)
    if lcg_ls_source_stat != 0:
        print prog_name + ":", "aborting retrieval, size check error:", lcg_ls_source
        raise CopyError

    if copy_to_ui:
        (lcg_ls_dest_stat, lcg_ls_dest) = getstatusoutput("ls -l " + remote_path)
    else:
        (lcg_ls_dest_stat, lcg_ls_dest) = getstatusoutput("lcg-ls -l -D srmv2 " + remote_pfn)
    if lcg_ls_dest_stat != 0:
        print prog_name + ":", "aborting retrieval, size check error:", lcg_ls_dest
        raise CopyError
    source_size = lcg_ls_source.split()[4]
    dest_size = lcg_ls_dest.split()[4]
    if source_size != dest_size:
        print prog_name + ":", "aborting retrieval, copy error: source size", source_size, "dest size", dest_size
        raise CopyError
    # remove original
    if remove_after_copy:
        if not quiet:
            print prog_name + ":", "removing lcoal copy"
        (lcg_del_output_stat, lcg_del_output) = getstatusoutput("lcg-del -D srmv2 " + local_pfn)
        if lcg_del_output_stat != 0:
            print prog_name + ":", "warning: could not remove local copy:", lcg_del_output
            

def rewrite_fjr (file, doc) :
    if not quiet:
        print prog_name + ":", "rewriting fjr to indicate remote stageout success"
    (bkup_path, bkup_file) = os.path.split(file)
    bkup_path += "/retry_backup"
    if not quiet:
        print prog_name + ":", "backup path is", bkup_path
    try: 
        stat_result = os.stat(bkup_path)
    except OSError as err: 
        if err.errno == os.errno.ENOENT:
            if not quiet:
                print prog_name + ":", "backup directory does not exist, creating"
            os.mkdir(bkup_path)
        else:
            raise
    bkup_file = os.path.join(bkup_path, bkup_file)
    if not quiet:
        print prog_name + ":", "\told fjr will be backed up to", bkup_file
    (bkup_cp_output_stat, bkup_cp_output) = getstatusoutput("mv " + file + " " + bkup_file)
    if bkup_cp_output_stat != 0:
        print prog_name + ":", "could not back up fjr, error:", bkup_cp_output, "(fjr not rewritten)"
        raise CopyError
    out = open(file, "w")
    doc.writexml(out)

#################### get arguments ####################
(prog_path, prog_name) = os.path.split(sys.argv[0])

try:
    opts, argv = getopt.getopt(sys.argv[1:], "hc:vqnl", ["help", "verbose", "quiet", "dry-run", "copy-to-ui"])
except getopt.GetoptError, err:
    # print prog_name + ":", help information and exit:
    print prog_name + ": " + str(err) # will print prog_name + ":", something like "option -a not recognized"
    usage()
    sys.exit(2)

directory = ""
verbosity = 0
for o, a in opts:
    if o in ("-v", "--verbose"):
        verbosity += 1
    elif o in ("-n", "--dry-run"):
        dryrun = True
    elif o in ("-l", "--copy-to-ui"):
        copy_to_ui = True
    elif o in ("-q", "--quiet"):
        quiet = True
    elif o in ("-h", "--help"):
        help()
        sys.exit(1)
    elif o == "-c":
        directory = a
    else:
        print prog_name + ":", "unhandled option", o
        usage()
        sys.exit(1)

if directory == "":
    usage()
    sys.exit(2)

datasvc_nodes = get_nodes()
try:
    nodes = xml.dom.minidom.parseString(datasvc_nodes)
except:
    print prog_name + ":", "could not parse datasvc nodes, exiting"
    sys.exit(2)

list_stageout = []
list_not_stageout = []
    
fjrs = get_fjrs(directory)
for j in fjrs:
    if not quiet:
        print prog_name + ":", "processing fjr", j
    try:
        doc = xml.dom.minidom.parse(j) #read xml file to see if the job failed
    except:
        print prog_name + ":", 'FrameworkJobReport', j, 'could not be parsed and is skipped'
        continue
    try:
        if local_stageout(doc) : 
            if not quiet:
                print prog_name + ":", "fjr", j, "indicates remote stageout failure with local copy"
            copy_local_to_remote_pfn(doc)
            if not dryrun and not copy_to_ui:
                rewrite_fjr(j, doc)
        else:
            list_not_stageout.append(j)
            if not quiet:
                print prog_name + ":", "fjr", j, "does not require a local-to-remote copy"
    except CopyError:
        print prog_name + ":", "skipping fjr", j

if not quiet:
    print prog_name + ":", "all fjrs processed, exiting"
if dryrun:
    print prog_name + ":", "files that need to be copied:"
    if len(list_stageout) == 0:
       print "\tnone"
    for i in list_stageout:
        print "\t", i
