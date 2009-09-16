#!/usr/bin/env python
"""
_ModifyJobReport.py

Example of how to use the FwkJobRep package to update a job report post processing


"""
import os, string
import sys
import popen2

from ProdCommon.FwkJobRep.ReportParser import readJobReport


def readCksum(filename):
    """
    _readCksum_

    Run a cksum command on a file an return the checksum value

    """
    pop = popen2.Popen4("cksum %s" % filename)
    while pop.poll() == -1:
        exitStatus = pop.poll()
    exitStatus = pop.poll()
    if exitStatus:
        return None
    content = pop.fromchild.read()
    value = content.strip()
    value = content.split()[0]
    print "checksum = ", value
    return value


def fileSize(filename):
    """
    _fileSize_

    Get size of file

    """
    print "(size) os.stat(filename)[6] = ", os.stat(filename)[6]
    return os.stat(filename)[6]    
    

def addFileStats(file):
    """
    _addFileStats_

    Add checksum and size info to each size

    """
    #if not os.path.exists(file['PFN']):
    #    print "Error: Cannot find file: %s " % file['PFN']
    #    return 1 
    file['Size'] = fileSize(file['PFN'])
    checkSum = readCksum(file['PFN'])
    file.addChecksum('cksum',checkSum)
    return


def modifyFile(file):
    """
    _modifyFile_
    
    Calls functions to modify PFN and LFN
    """

    str.split(str(file['PFN']), '.root')
    pref =  str.split(str(file['PFN']), '.root')[0]
    suff = str.split(str(file['PFN']), pref)[1]
    
    ### FEDE changing se_path with  the endpoint
    #newPfn = diz['se_name'] + diz['se_path'] + pref + '_' + diz['n_job'] + suff
    newPfn = diz['se_path'] + pref + '_' + diz['n_job'] + suff
    print "newPfn = ", newPfn
    #########################

    newLfn = diz['for_lfn'] + pref + '_' + diz['n_job'] + suff
    print "newLfn = ", newLfn

    updatePFN(file, file['LFN'], newPfn)

    updateLFN(file, file['LFN'], newLfn)

    return


def updatePFN(file, lfn, newPFN):
    """
    _updatePFN_

    Update a PFN for an LFN, based on a stage out to some SE.
    """
    if file['LFN'] != lfn:
        return

    file['PFN'] = newPFN
    file['SEName'] = diz['se_name']
    return


def updateLFN(file, lfn, newLFN):
    """
    _updateLFN_

    Update a LFN.
    """
    if file['LFN'] != lfn:
        return
    file['LFN'] = newLFN
    return


if __name__ == '__main__':

    # Example:  Load the report, update the file stats, pretend to do a stage out
    # and update the information for the stage out


    L = sys.argv[1:]
    if len(L) < 20:
        print "Error: wrong number of arguments passed to the ModifyJobreport. Please check your script"
        sys.exit(1)
    diz={}
    
    i = 0
    while i < len(L):
        diz[L[i]] = L[i+1]
        i = i + 2

    if diz.has_key('fjr'):
        inputReport = diz['fjr']
        reports = readJobReport(inputReport)
    
        # report is an instance of FwkJobRep.FwkJobReport class
        # can be N in a file, so a list is always returned
        # by for readJobReport, here I am assuming just one report per file for simplicity
        try:   
            report = reports[-1]
        except IndexError:
            print "Error: No file to publish in xml file"
            sys.exit(1)
    else:
        print "no crab fjr found"
        sys.exit(1)


    # ARGs parameters
    if diz.has_key('n_job'):
        n_job = diz['n_job'] 
    else:
        print "it is necessary to specify the job number" 
        sys.exit(1)
        
    if diz.has_key('UserProcessedDataset'): 
        UserProcessedDataset = diz['UserProcessedDataset']
    else:
        UserProcessedDataset=''
    print "UserProcessedDataset = ", UserProcessedDataset
    
    #### FEDE ####
    if (len(report.files) == 0) and (len(report.analysisFiles) == 0):
       print "no EDM_output file or NO_EDM_output to modify"
       sys.exit(1)
    ####
    if (len(report.files) != 0):
        for f in report.files:
            if (string.find(f['PFN'], ':') != -1):
                tmp_path = string.split(f['PFN'], ':')
                f['PFN'] = tmp_path[1]
            if not os.path.exists(f['PFN']):
                print "Error: Cannot find file: %s " % f['PFN']
                sys.exit(1)
            #Generate per file stats
            addFileStats(f)

            datasetinfo=f.newDataset()
            datasetinfo['PrimaryDataset'] = diz['PrimaryDataset'] 
            datasetinfo['DataTier'] = "USER" 
            datasetinfo['ProcessedDataset'] = UserProcessedDataset 
            datasetinfo['ApplicationFamily'] = diz['ApplicationFamily'] 
            datasetinfo['ApplicationName'] = diz['ApplicationName'] 
            datasetinfo['ApplicationVersion'] = diz['cmssw_version'] 
            datasetinfo['PSetHash'] = diz['psethash']
            datasetinfo['PSetContent'] = "TOBEADDED"
            
            ### to check if the job output is composed by more files
            modifyFile(f)    

    if (len(report.analysisFiles) != 0):
        for aFile in report.analysisFiles:
            aFile['PFN'] = os.path.basename(aFile['FileName'])
            modifyFile(aFile)
    # After modifying the report, you can then save it to a file.
    report.write("NewFrameworkJobReport.xml")
    


