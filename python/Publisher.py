import getopt, string
import common
import time, glob
from Actor import *
from crab_util import *
from crab_logger import Logger
from crab_exceptions import *
from FwkJobRep.ReportParser import readJobReport
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
            self.processedData = cfg_params['USER.publish_data_name']
        except KeyError:
            raise CrabException('Cannot publish output data, because you did not specify USER.publish_data_name parameter in the crab.cfg file')

        try:
            if (int(cfg_params['USER.copy_data']) != 1): raise KeyError
        except KeyError:
            raise CrabException('You can not publish data because you did not selected *** copy_data = 1  *** in the crab.cfg file')

        #common.logger.message('processedData = '+self.processedData)
        self.resDir = common.work_space.resDir()
        #common.logger.message('resDir = '+self.resDir)
        try:
            self.DBSURL=cfg_params['USER.dbs_url_for_publication']
            common.logger.message('dbs url = '+self.DBSURL)
        except KeyError:
            msg = "Error. The [USER] section does not have 'dbs_url_for_publication'"
            msg = msg + " entry, necessary to publish the data"
            raise CrabException(msg)
        self.datasetpath=cfg_params['CMSSW.datasetpath']
        #common.logger.message('datasetpath = '+self.datasetpath)
        self.SEName=''
        self.CMSSW_VERSION=''
        self.exit_status=''
        self.time = time.strftime('%y%m%d_%H%M%S',time.localtime(time.time()))
        
    def importParentDataset(self,globalDBS, datasetpath):
        """
        """ 
        dbsWriter = DBSWriter(self.DBSURL,level='ERROR')
        
        try:
            dbsWriter.importDataset(globalDBS, self.datasetpath, self.DBSURL)
        except DBSWriterError, ex:
            msg = "Error importing dataset to be processed into local DBS\n"
            msg += "Source Dataset: %s\n" % datasetpath
            msg += "Source DBS: %s\n" % globalDBS
            msg += "Destination DBS: %s\n" % self.DBSURL
            common.logger.message(msg)
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
            
        #### the globalDBS has to be written in the crab cfg file!!!!! ###############
        if (self.datasetpath != 'None'):
            globalDBS="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
            common.logger.message("--->>> Importing parent dataset in the dbs")
            status_import=self.importParentDataset(globalDBS, self.datasetpath)
            if (status_import == 1):
                common.logger.message('Problem with parent import from the global DBS '+globalDBS+ 'to the local one '+self.DBSURL)
                self.exit_status='1'
                ##  ___ >>>>>>>  comment out the next line, if you have problem with the import
                return self.exit_status
            common.logger.message("Parent import ok")
            
        #// DBS to contact
        dbswriter = DBSWriter(self.DBSURL)                        
        try:   
            fileinfo= jobReport.files[0]
            #fileinfo.lumisections = {}
            self.exit_status = '0'
        except IndexError:
            self.exit_status = '1'
            msg = "Error: No file to publish in xml file"+file+" file"  
            common.logger.message(msg)
            return self.exit_status

        #common.logger.message("FileInfo = " + str(fileinfo))
        datasets=fileinfo.dataset 
        #common.logger.message("DatasetInfo = " + str(datasets))
        for dataset in datasets:
            #### to understand how to fill cfgMeta info ###############
            cfgMeta = {'name' : 'usercfg' , 'Type' : 'user' , 'annotation': 'user cfg', 'version' : 'private version'} # add real name of user cfg
            common.logger.message("PrimaryDataset = %s"%dataset['PrimaryDataset'])
            common.logger.message("ProcessedDataset = %s"%dataset['ProcessedDataset'])
            common.logger.message("--->>> Inserting primary: %s processed : %s"%(dataset['PrimaryDataset'],dataset['ProcessedDataset']))
            #common.logger.message("dataset: %s"%dataset)
            
            primary = DBSWriterObjects.createPrimaryDataset( dataset, dbswriter.dbs)
            common.logger.message("Primary:  %s "%primary)
            
            algo = DBSWriterObjects.createAlgorithm(dataset, cfgMeta, dbswriter.dbs)
            common.logger.message("Algo:  %s "%algo)

            processed = DBSWriterObjects.createProcessedDataset(primary, algo, dataset, dbswriter.dbs)
            common.logger.message("Processed:  %s "%processed)
            
            common.logger.message("Inserted primary %s processed %s"%(primary,processed))
            
        #common.logger.message("exit_status = %s "%self.exit_status)
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
        # overwrite ProcessedDataset with user defined value
        # overwrite lumisections with no value
        for file in jobReport.files:
            file.lumisections = {}
            for ds in file.dataset:
                ds['ProcessedDataset']=procdataset
        #// DBS to contact
        dbswriter = DBSWriter(self.DBSURL)
        # insert files
        Blocks=None
        try:
            Blocks=dbswriter.insertFiles(jobReport)
            common.logger.message("Blocks = %s"%Blocks)
        except DBSWriterError, ex:
            common.logger.message("Insert file error: %s"%ex)
        return Blocks

    def run(self):
        """
        parse of all xml file on res dir and creation of distionary
        """
        
        file_list = glob.glob(self.resDir+"crab_fjr*.xml")
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
                common.logger.message("file = "+file)
                Blocks=self.publishAJobReport(file,self.processedData)
                if Blocks:
                    [BlocksList.append(x) for x in Blocks]
                    
            # close the blocks
            common.logger.message("BlocksList = %s"%BlocksList)
            # dbswriter = DBSWriter(self.DBSURL,level='ERROR')
            dbswriter = DBSWriter(self.DBSURL)
            
            for BlockName in BlocksList:
                try:   
                    closeBlock=dbswriter.manageFileBlock(BlockName,maxFiles= 1)
                    common.logger.message("closeBlock %s"%closeBlock)
                    #dbswriter.dbs.closeBlock(BlockName)
                except DBSWriterError, ex:
                    common.logger.message("Close block error %s"%ex)
            common.logger.message("--->>> End files publication")
            return self.exit_status

        else:
            common.logger.message("--->>> "+self.resDir+" empty: no file to publish on DBS")
            self.exit_status = '1'
            return self.exit_status
    
