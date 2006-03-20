import apmon

class ApmonIf:
    """
    Provides an interface to the Monalisa Apmon python module
    """
    def __init__(self, address='http://lxgate35.cern.ch:40808/ApMonConf'):
        self._params = {}
        self._MLaddress = address
        self.apm = None
        self.apm = self.getApmonInstance()

    def fillDict(self, parr):
        """
        Used to fill the dictionary of key/value pair and instantiate the ML client
        """
        self._params = parr
    
    def sendToML(self):
        self.apm.sendParameters(self._params['taskId'], self._params['jobId'], self._params)
        
    def getApmonInstance(self):
        if self.apm is None :
            apmonUrl = 'http://lxgate35.cern.ch:40808/ApMonConf' 
            print "Creating ApMon with " + apmonUrl
            apmonInstance = apmon.ApMon(apmonUrl)
        return apmonInstance 
        
    def free(self):
        self.apm.free()
