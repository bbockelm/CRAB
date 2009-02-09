import getopt, string
import common
import time, glob
from Actor import *
from crab_util import *
from crab_logger import Logger
from crab_exceptions import *
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.FwkJobRep.ReportState import checkSuccess
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx,DBSReaderError
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter,DBSWriterObjects
import sys

class Publisher(Actor):
    def __init__(self, cfg_params):
        """
        Publisher class: 

        - parses CRAB FrameworkJobReport on UI
        - returns <file> section of xml in dictionary format for each xml file in crab_0_xxxx/res directory
        - publishes output data on DBS and DLS
        """

        try:
            self.userprocessedData = cfg_params['USER.publish_data_name'] 
            self.processedData = None
        except KeyError:
            raise CrabException('Cannot publish output data, because you did not specify USER.publish_data_name parameter in the crab.cfg file')

        try:
            if (int(cfg_params['USER.copy_data']) != 1):
                raise KeyError
        except KeyError:
            msg  = 'You can not publish data because you did not selected \n'
            msg += '\t*** copy_data = 1 or publish_data = 1  *** in the crab.cfg file'
            raise CrabException(msg)
        try:
            self.pset = cfg_params['CMSSW.pset']
        except KeyError:
            raise CrabException('Cannot publish output data, because you did not specify the psetname in [CMSSW] of your crab.cfg file')
        try:
            self.globalDBS=cfg_params['CMSSW.dbs_url']
        except KeyError:
            self.globalDBS="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
        try:
            self.DBSURL=cfg_params['USER.dbs_url_for_publication']
            common.logger.message('<dbs_url_for_publication> = '+self.DBSURL)
            if (self.DBSURL == "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet") or (self.DBSURL == "https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet"):
                msg = "You can not publish your data in the globalDBS = " + self.DBSURL + "\n" 
                msg = msg + "Please write your local one in the [USER] section 'dbs_url_for_publication'"
                raise CrabException(msg)
        except KeyError:
            msg = "Warning. The [USER] section does not have 'dbs_url_for_publication'"
            msg = msg + " entry, necessary to publish the data.\n"
            msg = msg + "Use the command **crab -publish -USER.dbs_url_for_publication=dbs_url_for_publication*** \nwhere dbs_url_for_publication is your local dbs instance."
            raise CrabException(msg)
            
        self.content=file(self.pset).read()
        self.resDir = common.work_space.resDir()
        
        self.dataset_to_import=[]
        
        self.datasetpath=cfg_params['CMSSW.datasetpath']
        if (self.datasetpath.upper() != 'NONE'):
            self.dataset_to_import.append(self.datasetpath)
        
        ### Added PU dataset
        tmp = cfg_params.get('CMSSW.dataset_pu',None)
        if tmp :
            datasets = tmp.split(',')
            for dataset in datasets:
                dataset=string.strip(dataset)
                self.dataset_to_import.append(dataset)
        ###        
                
        self.SEName=''
        self.CMSSW_VERSION=''
        self.exit_status=''
        self.time = time.strftime('%y%m%d_%H%M%S',time.localtime(time.time()))
        self.problemFiles=[]  
        self.noEventsFiles=[]
        self.noLFN=[]
        
    def importParentDataset(self,globalDBS, datasetpath):
        """
        """ 
        dbsWriter = DBSWriter(self.DBSURL,level='ERROR')
        
        try:
            dbsWriter.importDatasetWithoutParentage(globalDBS, self.datasetpath, self.DBSURL) 
        except DBSWriterError, ex:
            msg = "Error importing dataset to be processed into local DBS\n"
            msg += "Source Dataset: %s\n" % datasetpath
            msg += "Source DBS: %s\n" % globalDBS
            msg += "Destination DBS: %s\n" % self.DBSURL
            common.logger.message(msg)
            common.logger.write(str(ex))
            return 1
        return 0
          
    def publishDataset(self,file):
        """
        """
        try:
            jobReport = readJobReport(file)[0]
            self.exit_status = '0'
        except IndexError:
            self.exit_status = '1'
            msg = "Error: Problem with "+file+" file"  
            common.logger.message(msg)
            return self.exit_status

        if (len(self.dataset_to_import) != 0):
           for dataset in self.dataset_to_import:
               common.logger.message("--->>> Importing parent dataset in the dbs: " +dataset)
               status_import=self.importParentDataset(self.globalDBS, dataset)
               if (status_import == 1):
                   common.logger.message('Problem with parent '+ dataset +' import from the global DBS '+self.globalDBS+ 'to the local one '+self.DBSURL)
                   self.exit_status='1'
                   return self.exit_status
               else:    
                   common.logger.message('Import ok of dataset '+dataset)
            
        #// DBS to contact
        dbswriter = DBSWriter(self.DBSURL)                        
        try:   
            fileinfo= jobReport.files[0]
            self.exit_status = '0'
        except IndexError:
            self.exit_status = '1'
            msg = "Error: No file to publish in xml file"+file+" file"  
            common.logger.message(msg)
            return self.exit_status

        datasets=fileinfo.dataset 
        common.logger.debug(6,"FileInfo = " + str(fileinfo))
        common.logger.debug(6,"DatasetInfo = " + str(datasets))
        if len(datasets)<=0:
           self.exit_status = '1'
           msg = "Error: No info about dataset in the xml file "+file
           common.logger.message(msg)
           return self.exit_status
        for dataset in datasets:
            #### for production data
            self.processedData = dataset['ProcessedDataset']
            if (dataset['PrimaryDataset'] == 'null'):
                #dataset['PrimaryDataset'] = dataset['ProcessedDataset']
                dataset['PrimaryDataset'] = self.userprocessedData
            #else: # add parentage from input dataset
            elif self.datasetpath.upper() != 'NONE':
                dataset['ParentDataset']= self.datasetpath
    
            dataset['PSetContent']=self.content
            cfgMeta = {'name' : self.pset , 'Type' : 'user' , 'annotation': 'user cfg', 'version' : 'private version'} # add real name of user cfg
            common.logger.message("PrimaryDataset = %s"%dataset['PrimaryDataset'])
            common.logger.message("ProcessedDataset = %s"%dataset['ProcessedDataset'])
            common.logger.message("<User Dataset Name> = /"+dataset['PrimaryDataset']+"/"+dataset['ProcessedDataset']+"/USER")
            
            common.logger.debug(6,"--->>> Inserting primary: %s processed : %s"%(dataset['PrimaryDataset'],dataset['ProcessedDataset']))
            
            primary = DBSWriterObjects.createPrimaryDataset( dataset, dbswriter.dbs)
            common.logger.debug(6,"Primary:  %s "%primary)
            
            algo = DBSWriterObjects.createAlgorithm(dataset, cfgMeta, dbswriter.dbs)
            common.logger.debug(6,"Algo:  %s "%algo)

            processed = DBSWriterObjects.createProcessedDataset(primary, algo, dataset, dbswriter.dbs)
            common.logger.debug(6,"Processed:  %s "%processed)
            
            common.logger.debug(6,"Inserted primary %s processed %s"%(primary,processed))
            
        common.logger.debug(6,"exit_status = %s "%self.exit_status)
        return self.exit_status    

    def publishAJobReport(self,file,procdataset):
        """
           input:  xml file, processedDataset
        """
        try:
            jobReport = readJobReport(file)[0]
            self.exit_status = '0'
        except IndexError:
            self.exit_status = '1'
            msg = "Error: Problem with "+file+" file"
            raise CrabException(msg)
        ### overwrite ProcessedDataset with user defined value
        ### overwrite lumisections with no value
        ### skip publication for 0 events files
        filestopublish=[]
        for file in jobReport.files:
            #### added check for problem with copy to SE and empty lfn
            if (string.find(file['LFN'], 'copy_problems') != -1):
                self.problemFiles.append(file['LFN'])
            elif (file['LFN'] == ''):
                self.noLFN.append(file['PFN'])
            else:
                ### Fede removed check about number of events
                #if int(file['TotalEvents']) != 0 :
                    # lumi info are now in run hash
                file.runs = {}
                for ds in file.dataset:
                    ### Fede for production
                    if (ds['PrimaryDataset'] == 'null'):
                        ds['PrimaryDataset']=self.userprocessedData
                filestopublish.append(file)
                #else:
                #    self.noEventsFiles.append(file['LFN'])
        jobReport.files = filestopublish
        ### if all files of FJR have number of events = 0
        if (len(filestopublish) == 0):
           return None
           
        #// DBS to contact
        dbswriter = DBSWriter(self.DBSURL)
        # insert files
        Blocks=None
        try:
            Blocks=dbswriter.insertFiles(jobReport)
            common.logger.message("Inserting file in blocks = %s"%Blocks)
        except DBSWriterError, ex:
            common.logger.error("Insert file error: %s"%ex)
        return Blocks

    def run(self):
        """
        parse of all xml file on res dir and creation of distionary
        """
        
        file_list = glob.glob(self.resDir+"crab_fjr*.xml")
        ## Select only those fjr that are succesfull
        good_list=[]
        for fjr in file_list:
            reports = readJobReport(fjr)
            if len(reports)>0:
               if reports[0].status == "Success":
                  good_list.append(fjr)
        file_list=good_list
        ##
        common.logger.debug(6, "file_list = "+str(file_list))
        common.logger.debug(6, "len(file_list) = "+str(len(file_list)))
            
        if (len(file_list)>0):
            BlocksList=[]
            common.logger.message("--->>> Start dataset publication")
            self.exit_status=self.publishDataset(file_list[0])
            if (self.exit_status == '1'):
                return self.exit_status 
            common.logger.message("--->>> End dataset publication")


            common.logger.message("--->>> Start files publication")
            for file in file_list:
                common.logger.debug(1, "file = "+file)
                Blocks=self.publishAJobReport(file,self.processedData)
                if Blocks:
                    for x in Blocks: # do not allow multiple entries of the same block
                        if x not in BlocksList:
                           BlocksList.append(x)
                    
            # close the blocks
            common.logger.debug(6, "BlocksList = %s"%BlocksList)
            # dbswriter = DBSWriter(self.DBSURL,level='ERROR')
            dbswriter = DBSWriter(self.DBSURL)
            
            for BlockName in BlocksList:
                try:   
                    closeBlock=dbswriter.manageFileBlock(BlockName,maxFiles= 1)
                    common.logger.debug(6, "closeBlock %s"%closeBlock)
                    #dbswriter.dbs.closeBlock(BlockName)
                except DBSWriterError, ex:
                    common.logger.message("Close block error %s"%ex)

            if (len(self.noEventsFiles)>0):
                common.logger.message("--->>> WARNING: "+str(len(self.noEventsFiles))+" files not published because they contain 0 events are:")
                for lfn in self.noEventsFiles:
                    common.logger.message("------ LFN: %s"%lfn)
            if (len(self.noLFN)>0):
                common.logger.message("--->>> WARNING: there are "+str(len(self.noLFN))+" files not published because they have empty LFN")
                for pfn in self.noLFN:
                    common.logger.message("------ pfn: %s"%pfn)
            if (len(self.problemFiles)>0):
                common.logger.message("--->>> WARNING: "+str(len(self.problemFiles))+" files not published because they had problem with copy to SE")
                for lfn in self.problemFiles:
                    common.logger.message("------ LFN: %s"%lfn)
            common.logger.message("--->>> End files publication")
            common.logger.message("--->>> To check data publication please use: InspectDBS2.py --DBSURL=<dbs_url_for_publication> --datasetPath=<User Dataset Name>")
            return self.exit_status

        else:
            common.logger.message("--->>> "+self.resDir+" empty: no file to publish on DBS")
            self.exit_status = '1'
            return self.exit_status
    
