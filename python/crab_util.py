###########################################################################
#
#   C O N V E N I E N C E    F U N C T I O N S
#
###########################################################################

import string, sys, os
import ConfigParser, re, popen2

from crab_logger import Logger

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
def runCommand(cmd, printout=1):
    """
    Run command 'cmd'.
    Returns command stdoutput+stderror string on success,
    or None if an error occurred.
    """
    log = Logger.getInstance()
    log.message(cmd)
    #child = os.popen(cmd)
    #(child,stdin) = popen2.popen4(cmd) # requires python2

    child = popen2.Popen3(cmd,1)
    err = child.wait()
    cmd_out = child.fromchild.read()
    cmd_err = child.childerr.read()
    if err:
        msg = ('`'+cmd+'`\n   failed with exit code '
               +`err`+'='+`(err&0xff)`+'(signal)+'
               +`(err>>8)`+'(status)'+'\n')
        msg += cmd_out
        msg += cmd_err
        log.message(msg)
        return None

    cmd_out = cmd_out + cmd_err
    if printout: log.message(cmd_out)
    #err = child_stdout.close()
    #if err:
    #    common.log.message('OUT`'+cmd+'`\n   failed with exit code '
    #                       +`err`+'='+`(err&0xff)`+'(signal)+'
    #                       +`(err>>8)`+'(status)')
    #    return None
    if cmd_out == '' : cmd_out = ' '
    return cmd_out
