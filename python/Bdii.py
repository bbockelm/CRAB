#!/usr/bin/env python

try:
    from BdiiLdap import *
except ImportError:
    from BdiiLdapsearch import *

if __name__ == '__main__':
    from pprint import pprint

#    seList = listAllSEs('uscmsbd2.fnal.gov')

#    seList = ['ccsrm.in2p3.fr', 'cmssrm.hep.wisc.edu', 'pccms2.cmsfarm1.ba.infn.it', 'polgrid4.in2p3.fr', 'srm-disk.pic.es', 'srm.ciemat.es', 'srm.ihepa.ufl.edu', 't2data2.t2.ucsd.edu', 'srm.unl.edu', 'se01.cmsaf.mit.edu']

#    jmlist =  getJobManagerList(seList, "CMSSW_2_0_11", "slc4_ia32_gcc345", 'uscmsbd2.fnal.gov', False)
#    pprint(jmlist)

#    pprint(listAllCEs( "CMSSW_2_0_10", "slc4_ia32_gcc345", onlyOSG=False, bdii='uscmsbd2.fnal.gov'))
    pprint(listAllCEs( "CMSSW_2_0_10", "slc4_ia32_gcc345", onlyOSG=True, bdii='uscmsbd2.fnal.gov'))
#    pprint(listAllCEs(onlyOSG=False,bdii='is.grid.iu.edu'))
#    pprint(listAllCEs( "CMSSW_2_0_10", "slc4_ia32_gcc345", onlyOSG=True, bdii='uscmsbd2.fnal.gov'))
