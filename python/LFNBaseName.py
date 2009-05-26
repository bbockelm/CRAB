#!/usr/bin/env python
"""
_LFNBaseName_
"""

from crab_exceptions import *
from crab_util import runCommand, getUserName
import common
import os, string, time


def LFNBase(forced_path, PrimaryDataset='',ProcessedDataset='',merged=True,publish=False):
    """
    """
    if (PrimaryDataset == 'null'):
        PrimaryDataset = ProcessedDataset
    if PrimaryDataset != '':
        if ( PrimaryDataset[0] == '/' ):  PrimaryDataset=PrimaryDataset[1:]  
    lfnbase = os.path.join(forced_path, getUserName(), PrimaryDataset, ProcessedDataset)

    return lfnbase


if __name__ == '__main__' :
    """
    """
    import logging 
    common.logger = logging

    print "xx %s xx"%getUserName()
    baselfn = LFNBase("datasetstring")
    print baselfn

    unmergedlfn = LFNBase("datasetstring",merged=False)
    print unmergedlfn
    print PFNportion("datasetstring")
