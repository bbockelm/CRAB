from crab_exceptions import *

#
#  Naming convention:
#  methods starting with 'ws' are responsible to provide
#  corresponding part of the job script ('ws' stands for 'write script').
#


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

    def wsSetupCMSEnvironment_(self):
        """
        Returns part of a job script which is prepares
        the execution environment and which is common for all CMS jobs.
        """
        txt = '\n'
        txt += 'echo "### SETUP CMS ENVIRONMENT ###"\n'
        txt += '   echo "JOB_EXIT_STATUS = 0"\n'
        txt += 'if [ ! $VO_CMS_SW_DIR ] ;then\n'
        txt += '   echo "SET_CMS_ENV 1 ==> ERROR CMS software dir not found on WN `hostname`"\n'
        txt += '   echo "JOB_EXIT_STATUS = 1"\n' 
        txt += '   exit 1\n'
        txt += 'else\n'
        txt += '   echo "Sourcing environment... "\n'
        txt += '   if [ ! -s $VO_CMS_SW_DIR/cmsset_default.sh ] ;then\n'
        txt += '      echo "SET_CMS_ENV 1 ==> ERROR cmsset_default.sh file not found into dir $VO_CMS_SW_DIR"\n'
        txt += '      echo "JOB_EXIT_STATUS = 1"\n'
        txt += '      exit 1\n'
        txt += '   fi\n'
        txt += '   echo "sourcing $VO_CMS_SW_DIR/cmsset_default.sh"\n'
        txt += '   source $VO_CMS_SW_DIR/cmsset_default.sh\n'
        txt += '   result=$?\n'
        txt += '   if [ $result -ne 0 ]; then\n'
        txt += '       echo "SET_CMS_ENV 1 ==> ERROR problem sourcing $VO_CMS_SW_DIR/cmsset_default.sh"\n'
        txt += '       echo "JOB_EXIT_STATUS = 1"\n'  
        txt += '       exit 1\n'
        txt += '   fi\n'
        txt += 'fi\n'
        txt += '\n'
        txt += 'string=`cat /etc/redhat-release`\n'
        txt += 'echo $string\n'
        txt += 'if [[ $string = *alhalla* ]]; then\n'
        txt += '   echo "SCRAM_ARCH= $SCRAM_ARCH"\n'
        txt += 'elif [[ $string = *cientific* ]]; then\n'
        txt += '   export SCRAM_ARCH=slc3_ia32_gcc323\n'
        txt += '   echo "SCRAM_ARCH= $SCRAM_ARCH"\n'
        txt += 'else\n'
        txt += '   echo "SET_CMS_ENV 1 ==> ERROR OS unknown, LCG environment not initialized"\n'
        txt += '   echo "JOB_EXIT_STATUS = 1"\n'
        txt += '   exit 1\n'
        txt += 'fi\n'
        txt += 'echo "SET_CMS_ENV 0 ==> setup cms environment ok"\n'
        txt += 'echo "### END SETUP CMS ENVIRONMENT ###"\n'
        return txt
    
    def wsSetupEnvironment(self, nj):
        """
        Returns part of a job script which prepares
        the execution environment for the job 'nj'.
        """
        return ''

    def wsBuildExe(self, nj):
        """
        Returns part of a job script which builds the binary executable.
        """
        return ''

    def wsRenameOutput(self, nj):
        """
        Returns part of a job script which renames the produced files.
        """
        return ''

    def executableName(self):
        msg = 'Internal ERROR. Pure virtual function called:\n'
        msg += self.__class__.__name__+'::executableName() from '+__file__
        raise CrabException(msg)
