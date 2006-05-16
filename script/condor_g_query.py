#!/usr/bin/env python
import sys
import os
import time
import popen2,select,string

output = open('oli_output','w')

for line in sys.stdin:
    id = line.split('\n')[0]
    user = os.environ['USER']
    cmd = 'condor_q -submitter ' + user + ' ' + id

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

    output.write(cmd_out)
    output.write('\n\n')
    if cmd_out != None:
        status_flag = 0
        for line in cmd_out.splitlines() :
            if line.strip().startswith(id.strip()) :
                status = line.strip().split()[5]
                if ( status == 'I' ):
                    print id,' I'
                    msg = 'status: '+ str(id) + ' RE\n\n'
                    output.write(msg)
                    status_flag=1
                    break
                elif ( status == 'U' ) :
                    print id,' RE'
                    msg = 'status: '+ str(id) + ' RE\n\n'
                    output.write(msg)
                    status_flag=1
                    break
                elif ( status == 'H' ) :
                    print id,' UN'
                    msg = 'status: '+ str(id) + ' UN\n\n'
                    output.write(msg)
                    status_flag=1
                    break
                elif ( status == 'R' ) :
                    print id,' R'
                    msg = 'status: '+ str(id) + ' R\n\n'
                    output.write(msg)
                    status_flag=1
                    break
                elif ( status == 'X' ) :
                    print id,' SK'
                    msg = 'status: '+ str(id) + ' SK\n\n'
                    output.write(msg)
                    status_flag=1
                    break
                elif ( status == 'C' ) :
                    print id,' OR'
                    msg = 'status: '+ str(id) + ' OR\n\n'
                    output.write(msg)
                    status_flag=1
                    break
                else :
                    print id,' UN'
                    msg = 'status: '+ str(id) + ' UN\n\n'
                    output.write(msg)
                    status_flag=1
                    break
        if status_flag == 0 :
            print id,' OR'
            msg = 'status: ' + str(id) + ' OR\n\n'
            output.write(msg)

