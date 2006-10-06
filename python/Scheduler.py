from crab_exceptions import *

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#

class Scheduler :

    _instance = None

    def getInstance():
        if not Scheduler._instance :
            raise CrabException('Scheduler has no instance.')
        return Scheduler._instance
    
    getInstance = staticmethod(getInstance)

    def __init__(self, name):
        #if Scheduler._instance:
        #    raise CrabException('Scheduler already exists.')
        Scheduler._instance = self

        self._name = name
        return

    def name(self):
        return self._name

    def configure(self, cfg_params):
        return


    def sched_parameter(self):
        """
        Returns parameter scheduler-specific, to use with BOSS .
        """
        return 
    def wsSetupEnvironment(self):
        """
        Returns part of a job script which does scheduler-specific work.
        """
        return ''

