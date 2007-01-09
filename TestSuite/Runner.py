# -*- coding: iso-8859-1 -*-
from subprocess import *
from logging import debug, warning


def runner(cmd, cwd = None):
    """ Executes a command.

    Executes cmd, which is a list containing as a first item a command followed by its parameters, in the working dir cwd (if None)
    runner uses the current working dir). It returns a tuple containing the returncode, a string containing the whole stdout and a
    string containing the whole stderr.
    """
    debug("Executing: "+str(cmd)+" in "+str(cwd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=False, cwd=cwd)
    returncode = p.wait()
    outdata = p.stdout.read()
    errdata = p.stderr.read()
    p.stdout.close()
    p.stderr.close()
    debug("----- Process stdout -----\n"+outdata+"\n--------------------------\n")
    debug("----- Process stderr -----\n"+errdata+"\n--------------------------\n")
    if returncode:
        warning("Process failed with exit code "+str(returncode) + ' = ' + str(returncode&0xff) + "+(signal)+" + str(returncode>>8) + "(status)")
    return (returncode, outdata, errdata)
    
    