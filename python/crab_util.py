###########################################################################
#
#   C O N V E N I E N C E    F U N C T I O N S
#
###########################################################################

import string, sys, os, time
import ConfigParser, re, popen2, select, fcntl

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
        # print 'Section',sec
        for opt in cp.options(sec):
            #print 'config['+sec+'.'+opt+'] = '+string.strip(cp.get(sec,opt))
            config[sec+'.'+opt] = string.strip(cp.get(sec,opt))
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
def crabJobStatusToString(crab_status):
    """
    Convert one-letter crab job status into more readable string.
    """
    status={
           'C':'Created',
           'D':'Done',
           'H':'Hold',
           'U':'Ready',
           'I':'Scheduled',
           'X':'Canceled',
           'W':'Created',
           'R':'Running',
           'SC':'Checkpointed',
           'SS':'Scheduled',
           'SR':'Ready',
           'RE':'Ready',
           'SW':'Waiting',
           'SU':'Submitted',
           'S' :'Submitted (Boss)',
           'UN':'Undefined',
           'SK':'Cancelled',
           'SD':'Done (Success)',
           'SA':'Aborted',
           'DA':'Done (Aborted)',
           'SE':'Cleared',
           'OR':'Done (Success)',
           'A?':'Aborted',
           'K':'Killed',
           'E':'Cleared',
           'Z':'Cleared (Corrupt)',
           'NA':'Unknown',
           'I?':'Idle',
           'O?':'Done',
           'R?':'Running'
           }
    return status[crab_status]

###########################################################################
def findLastWorkDir(dir_prefix, where = None):

    if not where: where = os.getcwd() + '/'
    # dir_prefix usually has the form 'crab_0_'
    pattern = re.compile(dir_prefix)

    file_list = []
    for fl in os.listdir(where):
        if pattern.match(fl):
            file_list.append(fl)
            pass
        pass

    if len(file_list) == 0: return None

    file_list.sort()

    wdir = where + file_list[len(file_list)-1]
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

### WARNING This Function become USELESS after Boss API implementation
def runBossCommand(cmd, printout=0, timeout=3600):
    """
    Cd to correct directory before running a boss command
    """
    cwd = os.getcwd()
    os.chdir(common.work_space.shareDir())
    out = runCommand(cmd, printout, timeout)
    os.chdir(cwd)
    return out

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
        common.logger.message(cmd)
    else:
        common.logger.debug(10,cmd)
        common.logger.write(cmd)
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
        common.logger.message('killing process '+(cmd)+' with timeout '+str(timeout))
        os.kill (child.pid, 9)
        err = child.wait()

    cmd_out = string.join(outdata,"")
    cmd_err = string.join(errdata,"")

    if err:
        common.logger.message('`'+cmd+'`\n   failed with exit code '
                           +`err`+'='+`(err&0xff)`+'(signal)+'
                           +`(err>>8)`+'(status)')
        common.logger.message(cmd_out)
        common.logger.message(cmd_err)
        return None

#    cmd_out = string.join(outdata,"")
#    cmd_err = string.join(errdata,"")
    cmd_out = cmd_out + cmd_err
    if printout:
        common.logger.message(cmd_out)
    else:
        common.logger.debug(10,cmd_out)
        common.logger.write(cmd_out)
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

    if xml == '' :
        counter = 0
        printline = ''
        printline+= header
        print printline
        print '---------------------------------------------------------------------------------------------------'

        for i in range(len(lines)):
            if counter != 0 and counter%10 == 0 :
                print '---------------------------------------------------------------------------------------------------'
            print lines[i]
            counter += 1
    else:
        fileName = common.work_space.shareDir() + xml
        task = common._db.getTask()
        taskXML = common._db.serializeTask(task)
        common.logger.debug(5, taskXML)
        f = open(fileName, 'w')
        f.write(taskXML)
        f.close()
        pass

def CliServerParams(self):
    """
    Init client-server interactions
    """
    self.srvCfg = {}
    try:
        self.srvCfg = ServerConfig(self.cfg_params['CRAB.server_name']).config()

        self.server_name = str(self.srvCfg['serverName'])
        self.server_port = int(self.srvCfg['serverPort'])

        self.storage_name = str(self.srvCfg['storageName'])
        self.storage_path = str(self.srvCfg['storagePath'])
        self.storage_proto = str(self.srvCfg['storageProtocol'])
        self.storage_port = str(self.srvCfg['storagePort'])
    except KeyError:
        msg = 'No server selected or port specified.'
        msg = msg + 'Please specify a server in the crab cfg file'
        raise CrabException(msg)
        return

def bulkControl(self,list):
    """
    Check the BULK size and  reduce collection ...if needed
    """
    max_size = 400
    sub_bulk = []
    if len(list) > int(max_size):
        n_sub_bulk = int( int(len(list) ) / int(max_size) )
        for n in range(n_sub_bulk):
            first =n*int(max_size)
            last = (n+1)*int(max_size)
            sub_bulk.append(list[first:last])
        if len(list[last:-1]) < 50:
            for pp in list[last:-1]:
                sub_bulk[n_sub_bulk-1].append(pp)
        else:
            sub_bulk.append(list[last:-1])
    else:
        sub_bulk.append(list)

    return sub_bulk

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

####################################
if __name__ == '__main__':
    print 'sys.argv[1] =',sys.argv[1]
    list = parseRange2(sys.argv[1])
    print list
    cksum = makeCksum("crab_util.py")
    print cksum

