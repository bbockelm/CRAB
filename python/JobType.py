from crab_exceptions import *

class JobType:
    def __init__(self, name):
        self._name = name
        return

    def name(self):
        return self._name

    def prepareSteeringCards(self):
        """
        Make initial modifications of the user's steering card file.
        These modifications are common for all jobs.
        """
        # The default is no action,
        # i.e. user's steering card file used as is.
        return
    
    def modifySteeringCards(self, nj):
        """
        Make individual modifications of the user's steering card file
        for one job.
        """
        msg = 'Internal ERROR. Pure virtual function called:\n'
        msg += self.__class__.__name__+'::modifySteeringCards() from '+__file__
        raise CrabException(msg)

    def executableName(self):
        msg = 'Internal ERROR. Pure virtual function called:\n'
        msg += self.__class__.__name__+'::executableName() from '+__file__
        raise CrabException(msg)
    
