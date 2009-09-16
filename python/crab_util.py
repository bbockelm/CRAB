##########################################################################
#
#   C O N V E N I E N C E    F U N C T I O N S
#
###########################################################################

import string, sys, os, time
import ConfigParser, re, popen2, select, fcntl
import statvfs

import common
from crab_exceptions import CrabException
from ServerConfig import *

###########################################################################
def parseOptions(argv):
    """
    Parses command-line options.
    Returns a dictionary with specified options as keys:
    -opt1             --> 'opt1' : None
    -opt2 val         --> 'opt2' : 'val'
    -opt3=val         --> 'opt3' : 'val'
    Usually called as
    options = parseOptions(sys.argv[1:])
    """
    options = {}
    argc = len(argv)
    i = 0
    while ( i < argc ):
        if argv[i][0] != '-':
            i = i + 1
            continue
        eq = string.find(argv[i], '=')
        if eq > 0 :
            opt = argv[i][:eq]
            val = argv[i][eq+1:]
            pass
        else:
            opt = argv[i]
            val = None
            if ( i+1 < argc and argv[i+1][0] != '-' ):
                i = i + 1
                val = argv[i]
                pass
            pass
        options[opt] = val
        i = i + 1
        pass
    return options

def loadConfig(file, config):
    """
    returns a dictionary with keys of the form
    <section>.<option> and the corresponding values
    """
    #config={}
    cp = ConfigParser.ConfigParser()
    cp.read(file)
    for sec in cp.sections():
        for opt in cp.options(sec):
            ## temporary check. Allow compatibility
            new_sec = sec
            if sec == 'EDG':
                print ('\tWARNING: The [EDG] section is now deprecated.\n\tPlease remove it and use [GRID] instead.\n')
                new_sec = 'GRID'
            config[new_sec+'.'+opt] = string.strip(cp.get(sec,opt))
    return config

###########################################################################
def isInt(str):
    """ Is the given string an integer ?"""
    try: int(str)
    except ValueError: return 0
    return 1

###########################################################################
def isBool(str):
    """ Is the given string 0 or 1 ?"""
    if (str in ('0','1')): return 1
    return 0

###########################################################################
def parseRange(range):
    """
    Takes as the input a string with two integers separated by
    the minus sign and returns the tuple with these numbers:
    'n1-n2' -> (n1, n2)
    'n1'    -> (n1, n1)
    """
    start = None
    end   = None
    minus = string.find(range, '-')
    if ( minus < 0 ):
        if isInt(range):
            start = int(range)
            end   = start
            pass
        pass
    else:
        if isInt(range[:minus]) and isInt(range[minus+1:]):
            start = int(range[:minus])
            end   = int(range[minus+1:])
            pass
        pass
    return (start, end)

###########################################################################
def parseRange2(range):
    """
    Takes as the input a string in the form of a comma-separated
    numbers and ranges
    and returns a list with all specified numbers:
    'n1'          -> [n1]
    'n1-n2'       -> [n1, n1+1, ..., n2]
    'n1,n2-n3,n4' -> [n1, n2, n2+1, ..., n3, n4]
    """
    result = []
    if not range: return result

    comma = string.find(range, ',')
    if comma == -1: left = range
    else:           left = range[:comma]

    (n1, n2) = parseRange(left)
    while ( n1 <= n2 ):
        try:
            result.append(n1)
            n1 += 1
            pass
        except:
            msg = 'Syntax error in range <'+range+'>'
            raise CrabException(msg)

    if comma != -1:
        try:
            result.extend(parseRange2(range[comma+1:]))
            pass
        except:
            msg = 'Syntax error in range <'+range+'>'
            raise CrabException(msg)

    return result

###########################################################################
def findLastWorkDir(dir_prefix, where = None):

    if not where: where = os.getcwd() + '/'
    # dir_prefix usually has the form 'crab_0_'
    pattern = re.compile(dir_prefix)

    file_list = [f for f in os.listdir(where) if os.path.isdir(f) and pattern.match(f)]

    if len(file_list) == 0: return None

    file_list.sort()

    wdir = where + file_list[-1]
    return wdir

###########################################################################
def importName(module_name, name):
    """
    Import a named object from a Python module,
    i.e., it is an equivalent of 'from module_name import name'.
    """
    module = __import__(module_name, globals(), locals(), [name])
    return vars(module)[name]

###########################################################################
def readable(fd):
    return bool(select.select([fd], [], [], 0))

###########################################################################
def makeNonBlocking(fd):
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
	    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.FNDELAY)

###########################################################################
def runCommand(cmd, printout=0, timeout=-1):
    """
    Run command 'cmd'.
    Returns command stdoutput+stderror string on success,
    or None if an error occurred.
    Following recipe on http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/52296
    """

    if printout:
        common.logger.info(cmd)
    else:
        common.logger.log(10-1,cmd)
        pass

    child = popen2.Popen3(cmd, 1) # capture stdout and stderr from command
    child.tochild.close()             # don't need to talk to child
    outfile = child.fromchild
    outfd = outfile.fileno()
    errfile = child.childerr
    errfd = errfile.fileno()
    makeNonBlocking(outfd)            # don't deadlock!
    makeNonBlocking(errfd)
    outdata = []
    errdata = []
    outeof = erreof = 0

    if timeout > 0 :
        maxwaittime = time.time() + timeout

    err = -1
    while (timeout == -1 or time.time() < maxwaittime):
        ready = select.select([outfd,errfd],[],[]) # wait for input
        if outfd in ready[0]:
            outchunk = outfile.read()
            if outchunk == '': outeof = 1
            outdata.append(outchunk)
        if errfd in ready[0]:
            errchunk = errfile.read()
            if errchunk == '': erreof = 1
            errdata.append(errchunk)
        if outeof and erreof:
            err = child.wait()
            break
        select.select([],[],[],.01) # give a little time for buffers to fill
    if err == -1:
        # kill the pid
        common.logger.info('killing process '+(cmd)+' with timeout '+str(timeout))
        os.kill (child.pid, 9)
        err = child.wait()

    cmd_out = string.join(outdata,"")
    cmd_err = string.join(errdata,"")

    if err:
        common.logger.info('`'+cmd+'`\n   failed with exit code '
                           +`err`+'='+`(err&0xff)`+'(signal)+'
                           +`(err>>8)`+'(status)')
        common.logger.info(cmd_out)
        common.logger.info(cmd_err)
        return None

#    cmd_out = string.join(outdata,"")
#    cmd_err = string.join(errdata,"")
    cmd_out = cmd_out + cmd_err
    if printout:
        common.logger.info(cmd_out)
    else:
        common.logger.log(10-1,cmd_out)
        pass
    #print "<"+cmd_out+">"
    return cmd_out

####################################
def makeCksum(filename) :
    """
    make check sum using filename and content of file
    """

    from zlib import crc32
    hashString = filename

    inFile = open(filename, 'r')
    hashString += inFile.read()
    inFile.close()

    cksum = str(crc32(hashString))
    return cksum


def spanRanges(jobArray):
    """
    take array of job numbers and concatenate 1,2,3 to 1-3
    return string
    """

    output = ""
    jobArray.sort()

    previous = jobArray[0]-1
    for job in jobArray:
        if previous+1 == job:
            previous = job
            if len(output) > 0 :
                if output[-1] != "-":
                    output += "-"
            else :
                output += str(previous)
        else:
            output += str(previous) + "," + str(job)
            #output += "," + str(job)
            previous = job
    if len(jobArray) > 1 :
        output += str(previous)

    return output

def displayReport(self, header, lines, xml=''):

    counter = 0
    printline = ''
    printline+= header
    msg = '\n%s'%printline

    for i in range(len(lines)):
        if counter != 0 and counter%10 == 0 :
            msg += '-------------------------------------------------------------------------------------------------------\n'
        msg+=  '%s\n'%lines[i]
        counter += 1
    if xml != '' :
        fileName = common.work_space.shareDir() + xml
        task = common._db.getTask()
        taskXML = common._db.serializeTask(task)
        common.logger.log(10-1, taskXML)
        f = open(fileName, 'w')
        f.write(taskXML)
        f.close()
        pass
    common.logger.info(msg)

def CliServerParams(self):
    """
    Init client-server interactions
    """
    self.srvCfg = {}
    ## First I have to check if the decision has been already taken...
    task = common._db.getTask()
    if task['serverName']!=None and task['serverName']!="":
        self.cfg_params['CRAB.server_name']=task['serverName']

    if self.cfg_params.has_key('CRAB.server_name'):
        self.srvCfg = ServerConfig(self.cfg_params['CRAB.server_name']).config()
    elif self.cfg_params.has_key('CRAB.use_server'):
        serverName=self.cfg_params.get('CRAB.server_name','default')
        if self.cfg_params.has_key('CRAB.server_name'):
            serverName=self.cfg_params['CRAB.server_name']
        else:
            serverName='default'
        self.srvCfg = ServerConfig(serverName).config()
    else:
        msg = 'No server selected or port specified.\n'
        msg += 'Please specify a server in the crab cfg file'
        raise CrabException(msg)
        return
    # save the serverName for future use
    opsToBeSaved={}
    opsToBeSaved['serverName']=self.srvCfg['serverGenericName']
    common._db.updateTask_(opsToBeSaved)

    self.server_admin = str(self.srvCfg['serverAdmin'])
    self.server_dn = str(self.srvCfg['serverDN'])

    self.server_name = str(self.srvCfg['serverName'])
    self.server_port = int(self.srvCfg['serverPort'])

    self.storage_name = str(self.srvCfg['storageName'])
    self.storage_path = str(self.srvCfg['storagePath'])
    self.storage_proto = str(self.srvCfg['storageProtocol'])
    self.storage_port = str(self.srvCfg['storagePort'])

def bulkControl(self,list):
    """
    Check the BULK size and  reduce collection ...if needed
    """
    max_size = 400
    sub_bulk = []
    if len(list) > int(max_size):
        n_sub_bulk = int( int(len(list) ) / int(max_size) )
        for n in xrange(n_sub_bulk):
            first =n*int(max_size)
            last = (n+1)*int(max_size)
            sub_bulk.append(list[first:last])
        if len(list[last:-1]) < 50:
            for pp in list[last:-1]:
                sub_bulk[n_sub_bulk-1].append(pp)
        else:
            sub_bulk.append(list[last:])
    else:
        sub_bulk.append(list)

    return sub_bulk


def getUserName():
    """
    extract user name from either SiteDB or Unix
    """
    if common.scheduler.name().upper() in ['LSF', 'CAF', 'SGE']:
        common.logger.log(10-1, "Using as username the Unix user name")
        userName = unixUserName()
    else :
        userName = gethnUserNameFromSiteDB()

    return userName


def unixUserName():
    """
    extract username from whoami
    """
    try:
        userName = runCommand("whoami")
        userName = string.strip(userName)
    except:
        msg = "Error. Problem with whoami command"
        raise CrabException(msg)
    return userName


def getDN():
    """
    extract DN from user proxy's identity
    """
    try:
        userdn = runCommand("voms-proxy-info -identity")
        userdn = string.strip(userdn)
        #search for a / to avoid picking up warning messages
        userdn = userdn[userdn.find('/'):]
    except:
        msg = "Error. Problem with voms-proxy-info -identity command"
        raise CrabException(msg)
    return userdn.split('\n')[0]


def gethnUserNameFromSiteDB():
    """
    extract user name from SiteDB
    """
    from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
    hnUserName = None
    userdn = getDN()
    params = { 'cacheduration' : 24,
               'logger' : common.logger() }
    mySiteDB = SiteDBJSON(params)
    msg_ = "there is no user name associated to DN %s in SiteDB.\n" % userdn
    msg_ += "You need to register in SiteDB with the instructions at https://twiki.cern.ch/twiki/bin/view/CMS/SiteDBForCRAB"
    try:
        hnUserName = mySiteDB.dnUserName(dn=userdn)
    except Exception, text:
        msg = "Error extracting user name from SiteDB: %s\n" % text
        msg += " Check that you are registered in SiteDB, see https://twiki.cern.ch/twiki/bin/view/CMS/SiteDBForCRAB\n"
        msg += ' or %s' % msg_
        raise CrabException(msg)
    if not hnUserName:
        msg = "Error. %s" % msg_
        raise CrabException(msg)
    return hnUserName


def numberFile(file, txt):
    """
    append _'txt' before last extension of a file
    """
    txt=str(txt)
    p = string.split(file,".")
    # take away last extension
    name = p[0]
    for x in p[1:-1]:
        name=name+"."+x
    # add "_txt"
    if len(p)>1:
        ext = p[len(p)-1]
        result = name + '_' + txt + "." + ext
    else:
        result = name + '_' + txt

    return result

def readTXTfile(self,inFileName):
    """
    read file and return a list with the content
    """
    out_list=[]
    if os.path.exists(inFileName):
        f = open(inFileName, 'r')
        for line in  f.readlines():
            out_list.append(string.strip(line))
        f.close()
    else:
        msg = ' file '+str(inFileName)+' not found.'
        raise CrabException(msg)
    return out_list

def writeTXTfile(self, outFileName, args):
    """
    write a file with the given content ( args )
    """
    outFile = open(outFileName,"a")
    outFile.write(str(args))
    outFile.close()
    return

def readableList(self,rawList):
    """
    Turn a list of numbers into a string like 1-5,7,9,12-20
    """
    if not rawList:
        return ''

    listString = str(rawList[0])
    endRange = ''
    for i in range(1,len(rawList)):
        if rawList[i] == rawList[i-1]+1:
            endRange = str(rawList[i])
        else:
            if endRange:
                listString += '-' + endRange + ',' + str(rawList[i])
                endRange = ''
            else:
                listString += ',' + str(rawList[i])
    if endRange:
        listString += '-' + endRange
        endRange = ''

    return listString

def getLocalDomain(self):
    """
    Get local domain name
    """
    import socket
    tmp=socket.getfqdn()
    dot=string.find(tmp,'.')
    if (dot==-1):
        msg='Unkown domain name. Cannot use local scheduler'
        raise CrabException(msg)
    localDomainName = string.split(tmp,'.',1)[-1]
    return localDomainName

#######################################################
# Brian Bockelman bbockelm@cse.unl.edu
# Module to check the avaialble disk space on a specified directory.
#

def has_freespace(dir_name, needed_space_kilobytes):
     enough_unix_quota = False
     enough_quota = False
     enough_partition = False
     enough_mount = False
     try:
         enough_mount = check_mount(dir_name, need_space_kilobytes)
     except:
         enough_mount = True
     try:
         enough_quota = check_quota(dir_name, needed_space_kilobytes)
     except:
         raise
         enough_quota = True
     try:
         enough_partition = check_partition(dir_name,
             needed_space_kilobytes)
     except:
         enough_partition = True
     try:
         enough_unix_quota = check_unix_quota(dir_name,
             needed_space_kilobytes)
     except:
         enough_unix_quota = True
     return enough_mount and enough_quota and enough_partition \
         and enough_unix_quota

def check_mount(dir_name, needed_space_kilobytes):
     try:
         vfs = os.statvfs(dir_name)
     except:
         raise Exception("Unable to query VFS for %s." % dir_name)
     dev_free = vfs[statvfs.F_FRSIZE] * vfs[statvfs.F_BAVAIL]
     return dev_free/1024 > needed_space_kilobytes

def check_quota(dir_name, needed_space_kilobytes):
     _, fd, _ = os.popen3("/usr/bin/fs lq %s" % dir_name)
     results = fd.read()
     if fd.close():
         raise Exception("Unable to query the file system quota!")
     try:
         results = results.split('\n')[1].split()
         quota, used = results[1:3]
         avail = int(quota) - int(used)
         return avail > needed_space_kilobytes
     except:
         return Exception("Unable to parse AFS output.")

def check_partition(dir_name, needed_space_kilobytes):
     _, fd, _ = os.popen3("/usr/bin/fs diskfree %s" % dir_name)
     results = fd.read()
     if fd.close():
         raise Exception("Unable to query the file system quota!")
     try:
         results = results.split('\n')[1].split()
         avail = results[3]
         return int(avail) > needed_space_kilobytes
     except:
         raise Exception("Unable to parse AFS output.")

def check_unix_quota(dir_name, needed_space_kilobytes):
     _, fd, _ = os.popen3("df %s" % dir_name)
     results = fd.read()
     if fd.close():
         raise Exception("Unable to query the filesystem with df.")
     fs = results.split('\n')[1].split()[0]
     _, fd, _ = os.popen3("quota -Q -u -g")
     results = fd.read()
     if fd.close():
         raise Exception("Unable to query the quotas.")
     has_info = False
     for line in results.splitlines():
         info = line.split()
         if info[0] in ['Filesystem', 'Disk']:
             continue
         if len(info) == 1:
             filesystem = info[0]
             has_info = False
         if len(info) == 6:
             used, limit = info[0], max(info[1], info[2])
             has_info = True
         if len(info) == 7:
             filesystem, used, limit = info[0], info[1], max(info[2], info[3])
             has_info = True
         if has_info:
            if filesystem != fs:
                continue
            avail = int(limit) - int(used)
            if avail < needed_space_kilobytes:
                return False
     return True

def getGZSize(gzipfile):
    # return the uncompressed size of a gzipped file
    import struct
    f = open(gzipfile, "rb")
    if f.read(2) != "\x1f\x8b":
        raise IOError("not a gzip file")
    f.seek(-4, 2)
    return struct.unpack("<i", f.read())[0]

def showWebMon(server_name):
    taskName = common._db.queryTask('name')
    msg = ''
    msg +='You can also follow the status of this task on :\n'
    msg +='\tCMS Dashboard: http://dashb-cms-job-task.cern.ch/taskmon.html#task=%s\n'%(taskName)
    if server_name != '' :
        msg += '\tServer page: http://%s:8888/logginfo\n'%server_name
    msg += '\tYour task name is: %s \n'%taskName
    return msg

def SE2CMS(dests):
    """
    Trasnsform a list of SE grid name into a list SE according to CMS naming convention
    input: array of SE grid names
    output: array of SE CMS names
    """
    from ProdCommon.SiteDB.CmsSiteMapper import SECmsMap
    se_cms = SECmsMap()
    SEDestination = [se_cms[d] for d in dests]
    return SEDestination

def CE2CMS(dests):
    """
    Trasnsform a list of CE grid name into a list SE according to CMS naming convention
    input: array of CE grid names
    output: array of CE CMS names
    """
    from ProdCommon.SiteDB.CmsSiteMapper import CECmsMap
    ce_cms = CECmsMap()
    CEDestination = [ce_cms[d] for d in dests]
    return CEDestination

def checkLcgUtils( self ):
    """
    _checkLcgUtils_
    check the lcg-utils version and report
    """
    import commands
    cmd = "lcg-cp --version | grep lcg_util"
    status, output = commands.getstatusoutput( cmd )
    num_ver = -1
    if output.find("not found") == -1 or status == 0:
        temp = output.split("-")
        version = ""
        if len(temp) >= 2:
            version = output.split("-")[1]
            temp = version.split(".")
            if len(temp) >= 1:
                num_ver = int(temp[0])*10
                num_ver += int(temp[1])
    return num_ver

####################################
if __name__ == '__main__':
    print 'sys.argv[1] =',sys.argv[1]
    list = parseRange2(sys.argv[1])
    print list
    cksum = makeCksum("crab_util.py")
    print cksum

