###########################################################################
#
#   C O N V E N I E N C E    F U N C T I O N S
#
###########################################################################

import string, sys, os, time
import ConfigParser, re, popen2, select, fcntl

import common
from crab_exceptions import CrabException

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

def definePath(isOriginal):
    """
    read and cache path
    remove cmssw stuff from path
    return original/modified path   
    """
    bpath_original = os.getenv("PATH", None)
    try:
        cmssw = os.getenv("CMS_PATH", None)
        paths = bpath_original.split(':')
        bpath = ''
        for p in paths:
            if p.find( cmssw ) != -1 and p.find( 'python') != -1 and p.find( 'Crab') == -1:continue
            bpath+= p + ':'
    except:
        import traceback
        print traceback.format_exc()
        print "No CMSSW"
    if isOriginal == 'original':
        return bpath_original 
    else:
        return bpath[:-1]                                                                                                                                                
###########################################################################
def loadConfig(file):
    """
    returns a dictionary with keys of the form
    <section>.<option> and the corresponding values
    """
    config={}
    cp = ConfigParser.ConfigParser()
    cp.read(file)
    for sec in cp.sections():
        # print 'Section',sec
        for opt in cp.options(sec):
            #print 'config['+sec+'.'+opt+'] = '+string.strip(cp.get(sec,opt))
            config[sec+'.'+opt] = string.strip(cp.get(sec,opt))
    # marco. Try to prevent user from switching off Monalisa reporting
    config['USER.activate_monalisa'] = 1
    # marco
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
#    if   crab_status == 'C': status = 'Created'
#    elif crab_status == 'D': status = 'Done'
#    elif crab_status == 'R': status = 'Submitted'#Should be running? ds  
#    elif crab_status == 'S': status = 'Submitted'
#    elif crab_status == 'K': status = 'Killed'
#    elif crab_status == 'X': status = 'None'
#    elif crab_status == 'Y': status = 'Output retrieved'
#    elif crab_status == 'A': status = 'Aborted'
#    elif crab_status == 'RC': status = 'ReCreated'
#    else: status = '???'
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
    make chksum using filename and content of file
    """

    tobedeleted=0
    try:
        import tempfile
        tmpfile= tempfile.NamedTemporaryFile(mode='w')
        tmp_filename = tmpfile.name
    except:
        ## SL in case py2.2 is used, fall back to old solution
        tmp_filename = tempfile.mktemp()
        tmpfile= open(tmp_filename,'w')
        tobedeleted=1

    # add filename as first line
    tmpfile.write(filename+'\n')

    # fill input file in tmp file
    infile = open(filename)
    tmpfile.writelines(infile.readlines())
    tmpfile.flush()
    infile.close()

    cmd = 'cksum '+tmp_filename
    cmd_out = runCommand(cmd)

    cksum = cmd_out.split()[0]

    ## SL this is not needed if we use NamedTemporaryFile, which is not available in py2.2
    if (tobedeleted): os.remove(tmp_filename)

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
            previous = job
    if len(jobArray) > 1 :
        output += str(previous)

    return output

####################################
if __name__ == '__main__':
    print 'sys.argv[1] =',sys.argv[1]
    list = parseRange2(sys.argv[1])
    print list
    cksum = makeCksum("crab_util.py")
    print cksum
    
