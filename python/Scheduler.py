from crab_exceptions import *

class Scheduler :

    _instance = None

    def getInstance():
        if not Scheduler._instance :
            raise CrabException('Scheduler has no instance.')
        return Scheduler._instance
    
    getInstance = staticmethod(getInstance)

    def __init__(self, name):
        if Scheduler._instance:
            raise CrabException('Scheduler already exists.')
        Scheduler._instance = self

        self._name = name
        return

    def name(self):
        return self._name

    def configure(self, cfg_params):
        return

    def isInputReady(self, nj):
        msg = 'Internal ERROR. Pure virtual function called:\n'
        msg += self.__class__.__name__+'::isInputReady() from '+__file__
        raise CrabException(msg)

    def createJDL(self, nj):
        return
    
