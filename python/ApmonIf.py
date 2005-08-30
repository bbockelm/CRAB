import apmon

class ApmonIf:
    """
    Provides an interface to the Monalisa Apmon python module
    """
    def __init__(self, address='http://monalisa.cern.ch/ARDA/apmon.cms'):
        self._params = {}
        self._MLaddress = address

    def fillDict(self, parr):
        """
        Used to fill the dictionary of key/value pair and instantiate the ML client
        """
        self._params = parr
    
    def sendToML(self):
        apm = apmon.ApMon(self._MLaddress)
        apm.sendParameters(self._params['taskId'], self._params['jobId'], self._params)
