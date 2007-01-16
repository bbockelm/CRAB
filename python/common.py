#
#  This class is a collection of unrelated objects which should be
#  accessible from almost any place of the program.
#  The possible design alternative is to implement these objects
#  as singletons.
#

###########################################################################
#
#   General information about the program
#
###########################################################################

prog_name = 'crab'
prog_version = (1, 5, 0)
prog_version_str=`prog_version[0]`+'.'+`prog_version[1]`+'.'+`prog_version[2]`
prog_authors = [
    ['Stefano Lacaprara', 'Stefano.Lacaprara@pd.infn.it', 'INFN/Padova'],
    ['Federica Fanzago' , 'Federica.Fanzago@pd.infn.it' , 'INFN/Padova'],
    ['Daniele Spiga'  , 'Daniele.Spiga@pg.infn.it'  , 'INFN/Perugia'],
    ['Alessandra Fanfani'  , 'Alessandra.Fanfani@bo.infn.it'  , 'INFN/Bologna'],
    ['Marco Corvo'      , 'Marco.Corvo@pd.infn.it'      , 'INFN/Padova'],
    ['Oliver Gutsche'  , 'Oliver.Gutsche@fnal.gov'  , 'FNAL'],
    ['Nikolai Smirnov'  , 'Nikolai.Smirnov@pd.infn.it'  , 'INFN/Padova'],
    ]

###########################################################################
#
#   Objects accessible from almost any place of the program.
#
###########################################################################

logger     = None
work_space = None
scheduler  = None
job_list   = []
jobDB      = None
taskDB     = None
