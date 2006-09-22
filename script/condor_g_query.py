#!/usr/bin/env python2.2
import sys
import os
import time
import popen2,select,string

#output = open('oli_output','w')

for line in sys.stdin:
    identifier = line.split('\n')[0]
    #output.write(identifier)
    #output.write('\n')
    schedd = identifier.split('//')[0]
    id = identifier.split('//')[1]
    cmd = 'condor_q -name ' + schedd + ' ' + id

    # imported from crab_util
    child = popen2.Popen3(cmd, 1) # capture stdout and stderr from command
    child.tochild.close()             # don't need to talk to child
    outfile = child.fromchild 
    outfd = outfile.fileno()
    errfile = child.childerr
    errfd = errfile.fileno()
#    makeNonBlocking(outfd)            # don't deadlock!
#    makeNonBlocking(errfd)
    outdata = []
    errdata = []
    outeof = erreof = 0

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
    select.select([],[],[],.1) # give a little time for buffers to fill

    cmd_out = string.join(outdata,"")
    cmd_err = string.join(errdata,"")

    cmd_out = cmd_out + cmd_err

    #output.write(cmd_out)
    #output.write('\n\n')
    if cmd_out != None:
        status_flag = 0
        for line in cmd_out.splitlines() :
            if line.strip().startswith(id.strip()) :
                status = line.strip().split()[5]
                if ( status == 'I' ):
                    print identifier,' I'
                    msg = 'status: '+ identifier + ' RE\n\n'
                    #output.write(msg)
                    status_flag=1
                    break
                elif ( status == 'U' ) :
                    print identifier,' RE'
                    msg = 'status: '+ identifier + ' RE\n\n'
                    #output.write(msg)
                    status_flag=1
                    break
                elif ( status == 'H' ) :
                    print identifier,' SA'
                    msg = 'status: '+ identifier + ' SA\n\n'
                    #output.write(msg)
                    status_flag=1
                    break
                elif ( status == 'R' ) :
                    print identifier,' R'
                    msg = 'status: '+ identifier + ' R\n\n'
                    #output.write(msg)
                    status_flag=1
                    break
                elif ( status == 'X' ) :
                    print identifier,' SK'
                    msg = 'status: '+ identifier + ' SK\n\n'
                    #output.write(msg)
                    status_flag=1
                    break
                elif ( status == 'C' ) :
                    print identifier,' OR'
                    msg = 'status: '+ identifier + ' OR\n\n'
                    #output.write(msg)
                    status_flag=1
                    break
                else :
                    print identifier,' UN'
                    msg = 'status: '+ identifier + ' UN\n\n'
                    #output.write(msg)
                    status_flag=1
                    break
        if status_flag == 0 :
            print identifier,' OR'
            msg = 'status: ' + identifier + ' OR\n\n'
            #output.write(msg)

