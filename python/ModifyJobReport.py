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


def modifyFile(file, n_job, seName):
    """
    _modifyFile_
    
    Calls functions to modify PFN and LFN
    """

    str.split(str(file['PFN']), '.root')
    pref =  str.split(str(file['PFN']), '.root')[0]
    suff = str.split(str(file['PFN']), pref)[1]
    if (seName == ''):
         newPfn = pref + '_' + n_job + suff
    else:
         newPfn = seName + path + pref + '_' + n_job + suff
    print "newPfn = ", newPfn

    newLfn = for_lfn + pref + '_' + n_job + suff
    print "newLfn = ", newLfn

## remove parsing based on "."
#    str.split(str(file['PFN']), '.')
#    pref =  str.split(str(file['PFN']), '.')[0]
#    suff = str.split(str(file['PFN']), '.')[1]
#    if (seName == ''):
#        newPfn = pref + '_' + n_job + '.' + suff
#    else:    
#        newPfn = seName + path + pref + '_' + n_job + '.' + suff
#    print "newPfn = ", newPfn  
#
#    newLfn = for_lfn + pref + '_' + n_job + '.' + suff 
#    print "newLfn = ", newLfn

    updatePFN(file, file['LFN'], newPfn, seName)

    updateLFN(file, file['LFN'], newLfn)

    return


def updatePFN(file, lfn, newPFN, seName):
    """
    _updatePFN_

    Update a PFN for an LFN, based on a stage out to some SE.
    """
    if file['LFN'] != lfn:
        return

    file['PFN'] = newPFN
    file['SEName'] = seName
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

    inputReport = sys.argv[1]
    reports = readJobReport(inputReport)
    
    # report is an instance of FwkJobRep.FwkJobReport class
    # can be N in a file, so a list is always returned
    # by for readJobReport, here I am assuming just one report per file for simplicity
    try:   
        report = reports[-1]
    except IndexError:
        print "Error: No file to publish in xml file"
        sys.exit(1)

    #print "report.files[0] = ", report.files[0]     

    # ARGs parameters
    try:
        n_job = sys.argv[2]   # NJob
    except:
        print "it is necessary to specify the job number" 
        pass
    print "n_job = ", n_job

    try: 
        for_lfn = sys.argv[3]   # User_dataset_cmssw
    except:
        for_lfn=''
        pass 
    print "for_lfn = ", for_lfn
    try: 
        PrimaryDataset = sys.argv[4]
    except:
        PrimaryDataset=''
        pass 
    try: 
        #DataTier = sys.argv[5]
        DataTier = 'USER'
    except:
        Datatier=''
        pass 
    try: 
        ProcessedDataset = sys.argv[6]
    except:
        ProcessedDataset=''
        pass 
    try: 
        ApplicationFamily = sys.argv[7]
    except:
        ApplicationFamily=''
        pass 
    try: 
        ApplicationName = sys.argv[8]
    except:
        ApplicationName=''
        pass 
    try: 
        CMSSW_VERSION = sys.argv[9]
    except:
        CMSSW_VERSION=''
        pass 
    try: 
        PSETHASH = sys.argv[10]
    except:
        PSETHASH=''
        pass 
    try:  
        seName = sys.argv[11] # LCG SE name
    except:
        seName=''
        pass
    print "seName = ", seName

    try: 
        path = sys.argv[12]   # LCG SE path
    except:
        path=''
        pass 
    print "path = ", path

    if (len(report.files) == 0):
       print "no output file to modify"
       sys.exit(1)
    else:
        for f in report.files:
            if (string.find(f['PFN'], ':') != -1):
                tmp_path = string.split(f['PFN'], ':')
                f['PFN'] = tmp_path[1]
                #print f['PFN']
            if not os.path.exists(f['PFN']):
                print "Error: Cannot find file: %s " % f['PFN']
                sys.exit(1)
                #continue
            #Generate per file stats
            addFileStats(f)

            datasetinfo=f.newDataset()
            datasetinfo['PrimaryDataset'] = PrimaryDataset 
            datasetinfo['DataTier'] = DataTier 
            datasetinfo['ProcessedDataset'] = ProcessedDataset 
            datasetinfo['ApplicationFamily'] = ApplicationFamily 
            datasetinfo['ApplicationName'] = ApplicationName 
            datasetinfo['ApplicationVersion'] = CMSSW_VERSION 
            datasetinfo['PSetHash'] = PSETHASH
            datasetinfo['PSetContent'] = "TOBEADDED"
            #  //
            # // Fake stage out to somese.host.com/path 
            #//

            ### to check if the job output is composed by more files
            modifyFile(f, n_job, seName)    

    # After modifying the report, you can then save it to a file.
    report.write("NewFrameworkJobReport.xml")


