#!/usr/bin/env python
"""
_SiteMapping_

To be automatized with SiteDB
"""

def SiteMap():

    T2 = {}
# crabas2
    T2['T2_US_Nebraska'] = ('unl.edu',)
    T2['T2_CH_CSCS'] = ('cscs.ch',)
    T2['T2_TW_Taiwan'] = ('sinica.edu.tw',)
    T2['T2_CN_Beijing'] = ('ac.cn',)
    T2['T2_ES_CIEMAT'] = ('ciemat',)
    T2['T2_ES_IFCA'] = ('ifca.es',)
    T2['T2_FR_CCIN2P3'] = ('in2p3',)
    T2['T2_HU_Budapest'] = ('.hu',)
    T2['T2_IT_Bari'] = ('ba.infn.it',)
    T2['T2_IT_Legnaro'] = ('lnl',)
    T2['T2_IT_Rome'] = ('roma',)
    T2['T2_US_Caltech'] = ('ultralight.org',)
    T2['T2_US_UCSD'] = ('ucsd.edu',)
    T2['T2_US_Purdue'] = ('purdue.edu',)
    T2['T2_US_Florida'] = ('ufl.edu',)
    T2['T2_IT_Pisa'] = ('pi.infn.it',)
    T2['T2_KR_KNU'] = ('knu.ac.kr',)
    T2['T2_UK_SGrid_Bristol'] = ('bris.ac.uk',)
    T2['T2_UK_SGrid_RALPP'] = ('rl.ac.uk',)
    T2['T2_US_MIT'] = ('mit.edu',)
    T2['T2_PL_Warsaw'] = ('polgrid.pl',)
    return T2


def SiteRegExp(site):
    if SiteMap().has_key(site):
        return SiteMap()[site]
    else:
        return 'all'

