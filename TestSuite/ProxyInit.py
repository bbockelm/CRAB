import logging, string, os, popen2, select, fcntl

class ProxyInit:
    """ Class taken from Mattia's work
    We need this class to be sure we have a correct certificate proxy before running threads or processes
    """
    def __init__(self, cfg):
        self.cfg = str(cfg)

    def makeNonBlocking(self,fd):
        fl = fcntl.fcntl(fd,fcntl.F_GETFL)
        try:
            fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
        except AttributeError:
                fcntl.fcntl(fd,fcntl.F_SETFL, fl | os.FNDELAY)


    def run( self, cmd):
        """
        Run command 'cmd'.
        Returns command stdoutput+stderror string on success,
        or None if an error occurred.
        Following recipe on http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/52296
        """

        timeout = -1

        logging.debug( "TestSuite: executing \"" + cmd + "\"") # prints the debug level

        child = popen2.Popen3(cmd, 1) # capture stdout and stderr from command
        child.tochild.close()             # don't need to talk to child
        outfile = child.fromchild 
        outfd = outfile.fileno()
        errfile = child.childerr
        errfd = errfile.fileno()
        self.makeNonBlocking(outfd)            # don't deadlock!
        self.makeNonBlocking(errfd)
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
            select.select([],[],[],.1) # give a little time for buffers to fill
        if err == -1:
            # kill the pid
            logging.warning('killing process '+(cmd)+' with timeout '+str(timeout))
            os.kill (child.pid, 9)
            err = child.wait()

        cmd_out = string.join(outdata,"")
        cmd_err = string.join(errdata,"")

        if err:
            logging.error('`'+cmd+'`\n   failed with exit code '
                  +`err`+'='+`(err&0xff)`+'(signal)+'
                              +`(err>>8)`+'(status)')
            logging.debug(cmd_out)
            logging.debug(cmd_err)
            return None

        cmd_out = cmd_out + cmd_err

        return cmd_out

    def loadField (self, field):
## jobtype
        fOut = open( self.cfg )

        while True:
            line = fOut.readline()
            if len(line) == 0: # lunghezza zero indica l'EOF
                               # (NdT. End Of File, fine del file)
                break
            else:
                stri = line.split(" ",1)
               ## if stri[0] == "proxy_server":
                if stri[0] == field:
                    stri = line.split(" ", 3)
                    proxyStr = stri[2]
                    break


        return proxyStr


    def scanMyProxy(self, txt):
        """
        username: /C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Mattia Cinquilli
        owner: /C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Mattia Cinquilli
          timeleft: 167:50:27  (7.0 days)
        """
        flag = 0
        n = 0
        n = txt.count("\n")
        stringa = txt.split("\n", n)
        i = 0
        h = ""
        m = ""
        s = ""
        while i < n:
            #print stringa[i]
            if self.findInside(stringa[i], "timeleft"):
                flag = 1
                stringa2 = stringa[i].split(":", 1)
                character = stringa2[1][0]
                count = 0
                j = 1
                h = ""
                m = ""
                s = ""
                tt = len(stringa2[1])
                while character != "\n" and j < tt:
                    if ord(character) >= 48 and ord(character) <= 57:
                        if count == 0:
                            h = h + character
                        elif count == 1:
                            m = m + character
                        elif count == 2:
                            s = s + character
                    elif ord(character) == 58: # ascii(":") = 58
                        count += 1
                    elif ord(character) == 32 and count > 0: # ascii(" ") = 32
                        i = n
                        break
                    character = stringa2[1][j]
                    j = j + 1
            i = i + 1
        if flag:
            s = s + stringa2[1][tt-1]
        ##print h,m,s
            hours = int(h)
            mins = int(m)
        ##secs = int(s)
            if hours > 0:
                return 1
            elif mins > 45:
                return 1
            else:
                return 0


    def checkProxy(self):
        """
        """
        ok = 0
        timeLeft = 0
      #  try:
        timeLeftLocal = self.run('voms-proxy-info -timeleft')
        
        #timeLeft = parser.scanVomsProxy(timeLeftLocal)
        #print "timeLeftLocal = ", timeLeftLocal

        if timeLeftLocal == None:
            timeLeft = 0
        else:
            tL = self.scanVomsProxy(timeLeftLocal)
            timeLeft = int(tL)
        try:
            #print "timeLeft = ", timeLeft
            if timeLeft <= 0: #or not timeLeft:
                cmd = 'voms-proxy-init -valid 24:00 -voms cms'
##            try:
                out = os.system(cmd)
                if out > 0:
                    msg = "Unable to create a valid proxy!\n"
                    logging.error('TestSuite: ' + msg)
                else:
                    ok = 1
  ##          except:
                #msg = "Unable to create a valid proxy!\n"
                #outStream.printStep('TestSuite: ' + msg)
                #print msg
            elif int(timeLeft) > 0:
                ok = 1

            proxy = self.loadField("proxy_server")
##            proxy = "myproxy.cern.ch"
        ##    print proxy
            if ok == 1:
                controlMyProxy = self.run('myproxy-info -d -s '+ proxy)
                flagError = 0
                if controlMyProxy == None:
                    flagError = 1
                    controlMyProxy = "timeleft: 0"
                if not self.scanMyProxy(controlMyProxy) or flagError:
                    ok = 0
                    logging.warning('TestSuite: No credential delegated to myproxy server '+proxy+' will do now')
                    print ("Enter pass phrase:")
                    setMyProxy = self.run('myproxy-init -d -n -s '+proxy)  ## Matt.  #, outStream)
                    controlMyProxy = self.run('myproxy-info -d -s '+proxy) ## Matt.  #, outStream)
                    if not self.scanMyProxy(controlMyProxy):
                        ok = 0
                    else:
                        ok = 1
        except AttributeError:
            outStream.printStep("TestSuite: Unable to create a valid proxy!\n")

        return ok

    def scanMyProxy(self, txt):
        """
        username: /C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Mattia Cinquilli
        owner: /C=IT/O=INFN/OU=Personal Certificate/L=Perugia/CN=Mattia Cinquilli
          timeleft: 167:50:27  (7.0 days)
        """
        flag = 0
        n = 0
        n = txt.count("\n")
        stringa = txt.split("\n", n)
        i = 0
        h = ""
        m = ""
        s = ""
        while i < n:
            #print stringa[i]
            if self.findInside(stringa[i], "timeleft"):
                flag = 1
                stringa2 = stringa[i].split(":", 1)
                character = stringa2[1][0]
                count = 0
                j = 1
                h = ""
                m = ""
                s = ""
                tt = len(stringa2[1])
                while character != "\n" and j < tt:
                    if ord(character) >= 48 and ord(character) <= 57:
                        if count == 0:
                            h = h + character
                        elif count == 1:
                            m = m + character
                        elif count == 2:
                            s = s + character
                    elif ord(character) == 58: # ascii(":") = 58
                        count += 1
                    elif ord(character) == 32 and count > 0: # ascii(" ") = 32
                        i = n
                        break
                    character = stringa2[1][j]
                    j = j + 1
            i = i + 1
        if flag:
            s = s + stringa2[1][tt-1]
        ##print h,m,s
            hours = int(h)
            mins = int(m)
        ##secs = int(s)
            if hours > 0:
                return 1
            elif mins > 45:
                return 1
            else:
                return 0

    def findInside(self, text, str):
        return text.find(str) != -1

    def scanVomsProxy(self, txt):
        """
        WARNING: Unable to verify signature! Server certificate possibly not installed.
        Error: VOMS extension not found!
        xxxxx
        """
        n = 0
        n = txt.count("\n")
        stringa = txt.split("\n", n)
        for line in stringa:
            #print "line: ", line
            if ord(line[0]) >= 48 and ord(line[0]) <= 57:
                #print line
                if int(line) > 180:
                    return line
                else:
                    break
        return 0


    #def checkProxy(self):
        #try:
            #logging.info('Checking the certificate proxy...')
            #err1 = subprocess.call(['voms-proxy-info', '-exists', '-valid', '0:45'])
            #err2 = subprocess.call(['voms-proxy-info', '-acexists', 'cms'])
            #if err1 > 0 or err2 > 0:
                #logging.warning('Need to create a valid proxy certificate!')
                #err = subprocess.call(['voms-proxy-init', '-voms', 'cms'])
                #if err > 1:
                    #logging.error('Not possible to create a valid proxy certificate!')
                    #sys.exit(1)
#            p = subprocess.Popen(['myproxy-info', '-d', '-s', 'proxy'], stdout=PIPE)
#            err = False
#            for line in p.stdout:
#                if 'timeleft: 0' in line:
#                    err = True
#                    break;
#            if err:
#                 outStream.printStep('TestSuite: No credential delegated to myproxy server '+proxy+' will do now')
#                        print ("Enter pass phrase:")
#                        setMyProxy = self.run('myproxy-init -d -n -s '+proxy, outStream)
#                        controlMyProxy = self.run('myproxy-info -d -s '+proxy, outStream)
#                        if not parser.scanMyProxy(controlMyProxy):
#                             ok = 0
#                        else:
#                             ok = 1
        #except OSError, msg:
            #logging.error('Not possible to use voms/proxy tools. Have you correctly setup your UI?')
            #sys.exit(1)
        #return True

