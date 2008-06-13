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
prog_version = (2, 3, 0)
prog_version_str=`prog_version[0]`+'.'+`prog_version[1]`+'.'+`prog_version[2]`
prog_authors = [
    ['Stefano Lacaprara', 'Stefano.Lacaprara@pd.infn.it', 'INFN/LNL'],
    ['Daniele Spiga', 'Daniele.Spiga@pg.infn.it', 'INFN/Perugia'],
    ['Alvise Dorigo', 'Alvise.Dorigo@pd.infn.it', 'INFN/Padova'],
    ['Mattia Cinquilli', 'Mattia.Cinquilli@cern.ch', 'INFN/Perugia'],
    ['Marco Corvo', 'Marco.Corvo@cern.ch', 'CERN/CNAF'],
    ['Alessandra Fanfani', 'Alessandra.Fanfani@bo.infn.it', 'INFN/Bologna'],
    ['Federica Fanzago', 'Federica.Fanzago@cern.ch' , 'CERN/CNAF'],
    ['Fabio Farina', 'fabio.farina@cern.ch', 'INFN/Milano Bicocca'],
    ['Carlos Kavka', 'Carlos.Kavka@ts.infn.it', 'INFN/Trieste'],
    ['Matteo Merlo'  , 'merloma@gmail.com', 'INFN/Milano Bicocca'],
    ['Eric Vaandering', 'ewv@fnal.gov', 'FNAL'],
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
apmon      = None
