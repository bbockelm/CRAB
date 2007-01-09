import re
from logging import debug
from subprocess import Popen,PIPE

# timeleft: 167:58:19  (7.0 days)
myproxyRE = re.compile("timeleft: (?P<hours>[\\d]*):(?P<minutes>[\\d]*):(?P<seconds>[\\d]*)")

def shellRunner(cmd):
    debug("Executing: "+str(cmd))
    p = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=False)
    returncode = p.wait()
    outdata = p.stdout.read()
    errdata = p.stderr.read()
    p.stdout.close()
    p.stderr.close()
    debug("----- Process stdout -----\n"+outdata+"\n--------------------------\n")
    debug("----- Process stderr -----\n"+errdata+"\n--------------------------\n")
    return (returncode, outdata, errdata)


def isVomsProxyOk(minTime = 0):
    returncode, outdata, errdata = shellRunner(["voms-proxy-info", "-timeleft"])
    timeleft = -1
    for row in outdata.split("\n"):
        if row:
            timeleft = int(row)
            break
    debug("VomsProxy Timeleft: "+str(timeleft))
    return timeleft > minTime

def isMyProxyOk(minTime = 45*60):
    returncode, outdata, errdata = shellRunner(["myproxy-info", "-d"])
    timeleft = -1
    for row in outdata.split("\n"):
        g = myproxyRE.search(row)
        if g:
            timeleft = g.group("hours")*3600+g.group("minutes")*60+g.group("seconds")
    debug("MyProxy Timeleft: "+str(timeleft))
    return timeleft > minTime

def initVomsProxy():
    print ("Insert password to init voms-proxy: ")
    returncode, outdata, errdata = shellRunner(["voms-proxy-init", "-voms", "cms"])

def initMyProxy():
    print ("Insert password to init myproxy: ")
    returncode, outdata, errdata = shellRunner(["myproxy-init", "-d", "-s"])
    
def checkProxies():
    if not isVomsProxyOk():
        initVomsProxy()
        if not isVomsProxyOk():
            print >> stderr, "Can't init voms-proxy correctly!"
            return False
    if not isMyProxyOk():
        initMyProxy()
        if not isMyProxyOk():
            print >> stderr, "Can't init myproxy correctly!"
            return False
    return True
    
