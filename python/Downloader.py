from WMCore.Services.Service import Service
import common

class Downloader:

    def __init__(self, endpoint, cachepath, cacheduration = 0.5, timeout = 20, \
                 type = "txt/csv", logger = common.logger):
        self.wmcorecache = {}
        self.wmcorecache['logger'] = logger
        self.wmcorecache['cachepath'] = cachepath   ## cache area
        self.wmcorecache['cacheduration'] = 0.5     ## half an hour
        self.wmcorecache['timeout'] = 20            ## seconds
        self.wmcorecache['endpoint'] = endpoint

    def downloadConfig(self, cacheFile, type = "txt/csv"):
        self.wmcorecache['type'] = type
        common.logger.debug("Downloading file [%s] to [%s]." %(str(self.wmcorecache['endpoint']),(str(self.wmcorecache['cachepath'])+cacheFile)))
        servo = Service( self.wmcorecache )
        return servo.refreshCache( cacheFile, cacheFile )

    def config(self, fileName = "prova"):
        f = self.downloadConfig(fileName)
        l = ''.join( f.readlines() )
        f.close()

        value = None
        try:
            value = eval(l)
        except SyntaxError, se:
            common.logger.debug("Problem reading downloaded file %s "%str(fileName))

        return value
