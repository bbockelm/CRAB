import apmon, sys, common

class ApmonIf:
    """
    Provides an interface to the Monalisa Apmon python module
    """
    def __init__(self, address='http://lxgate35.cern.ch:40808/ApMonConf'):
        self._params = {}
        self.fName = 'mlCommonInfo'
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
            try:
                apmonUrls = ["http://lxgate35.cern.ch:40808/ApMonConf", "http://monalisa.cacr.caltech.edu:40808/ApMonConf"]
                apmInstance = apmon.ApMon(apmonUrls, apmon.Logger.FATAL);
                if not apmInstance.initializedOK():
                    print "It seems that ApMon cannot read its configuration. Setting the default destination"
                apmInstance.setDestinations({'lxgate35.cern.ch:58884': {'sys_monitoring':0, 'general_info':0, 'job_monitoring':1, 'job_interval':300}});
            except:
                exctype, value = sys.exc_info()[:2]
                print "ApmonIf::getApmonInstance Exception raised %s Value: %s"%(exctype, value)
                common.logger.message("ApmonIf::getApmonInstance Exception raised: %s %s"%(exctype, value))
                return
        return apmInstance 
        
    def free(self):
        self.apm.free()
