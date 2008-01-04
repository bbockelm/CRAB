from crab_exceptions import *
import string

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
        Scheduler._instance = self

        self._name = string.lower(name)
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

    def clean(self):
        """ destroy instance """
        return

    def checkProxy(self):
        """ check proxy """
        return

    def userName(self):
        """ return the user name """
        return

    def wsCopyInput(self):
        """
        Copy input data from SE to WN
        """
        return ""

    def wsCopyOutput(self):
        """
        Write a CopyResults part of a job script, e.g.
        to copy produced output into a storage element.
        """
        return ""

    def createXMLSchScript(self, nj, argsList):

        """
        Create a XML-file for BOSS4.
        """

        return

    def tOut(self, list):
        return 120
