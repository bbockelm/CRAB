import sys, getopt, string
import common
import time, glob
from Actor import *
from FwkJobRep.ReportParser import readJobReport
from crab_util import *
from crab_logger import Logger
from crab_exceptions import *
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.DataMgmt.DBS.DBSWriter import DBSWriter
from ProdCommon.DataMgmt.DBS.DBSErrors import DBSWriterError, formatEx,DBSReaderError
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
import ProdCommon.DataMgmt.DBS.DBSWriterObjects as DBSWriterObjects



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

        common.logger.message('self.processedData = '+self.processedData)
        self.resDir = common.work_space.resDir()
        common.logger.message('self.resDir = '+self.resDir)
        #old api self.DBSURL='http://cmssrv18.fnal.gov:8989/DBS/servlet/DBSServlet'
        self.DBSURL=cfg_params['USER.dbs_url_for_publication']
        #self.DBSURL='http://cmssrv17.fnal.gov:8989/DBS_1_0_4_pre2/servlet/DBSServlet'
        common.logger.message('self.DBSURL = '+self.DBSURL)
        self.datasetpath=cfg_params['CMSSW.datasetpath']
        common.logger.message('self.datasetpath = '+self.datasetpath)
        self.SEName=''
        self.CMSSW_VERSION=''
        self.exit_status=''
        self.time = time.strftime('%y%m%d_%H%M%S',time.localtime(time.time()))
        
    
    def importParentDataset(self,globalDBS, datasetpath):
        """
        """ 
        dbsWriter = DBSWriter(self.DBSURL,level='ERROR')
        
        try:
            common.logger.message("----->>>>importing parent dataset in the local dbs")
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
            msg = "--->>> reading "+file+" file"  
            common.logger.message(msg)
            self.exit_status = '0'
        except IndexError:
            self.exit_status = '1'
            msg = "Error: Problem with "+file+" file"  
            common.logger.message(msg)
            return self.exit_status
        #####for parents information import ######################################### 
        #### the globalDBS has to be written in the crab cfg file!!!!! ###############
        globalDBS="http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"
        if (self.datasetpath != 'None'):
            status_import=self.importParentDataset(globalDBS, self.datasetpath)
            if (status_import == 1):
                common.logger.message('Problem with parent import from the global DBS '+globalDBS+ 'to the local one '+self.DBSURL)
                self.exit_status='1'
                ###############################################################################
                ##  ___ >>>>>>>  comment out the next line, if you have problem with the import
                ###############################################################################
                return self.exit_status
            pass
        #// DBS to contact
        dbswriter = DBSWriter(self.DBSURL,level='ERROR')                        
        # publish a dataset : it should be done only once for the task
        #                     and not for all the JobReport
        try:   
            fileinfo= jobReport.files[0]
            self.exit_status = '0'
        except IndexError:
            self.exit_status = '1'
            msg = "Error: No file to publish in xml file"+file+" file"  
            common.logger.message(msg)
            return self.exit_status

        common.logger.message("FileInfo = " + str(fileinfo))
        datasets=fileinfo.dataset 
        common.logger.message("DatasetInfo = " + str(datasets))
        for dataset in datasets:
            #### to understand how to fill cfgMeta info ###############
            cfgMeta = {'name' : 'usercfg' , 'Type' : 'user' , 'annotation': 'user cfg', 'version' : 'private version'} # add real name of user cfg
            common.logger.message("PrimaryDataset = %s"%dataset['PrimaryDataset'])
            common.logger.message("ProcessedDataset = %s"%dataset['ProcessedDataset'])
            common.logger.message("Inserting primary: %s processed : %s"%(dataset['PrimaryDataset'],dataset['ProcessedDataset']))
            
            primary = DBSWriterObjects.createPrimaryDataset( dataset, dbswriter.dbs)
            
            algo = DBSWriterObjects.createAlgorithm(dataset, cfgMeta, dbswriter.dbs)

            processed = DBSWriterObjects.createProcessedDataset(primary, algo, dataset, dbswriter.dbs)
            
            common.logger.message("Inserted primary %s processed %s"%(primary,processed))
        return self.exit_status    

    def publishAJobReport(self,file,procdataset):
        """
        """
        try:
            jobReport = readJobReport(file)[0]
            self.exit_status = '0'
        except IndexError:
            self.exit_status = '1'
            msg = "Error: Problem with "+file+" file"
            raise CrabException(msg)

        # overwite ProcessedDataset with user defined value
        for file in jobReport.files:
            for ds in file.dataset:
                ds['ProcessedDataset']=procdataset
        #// DBS to contact
        dbswriter = DBSWriter(self.DBSURL,level='ERROR')
        # insert files
        Blocks=None
        try:
            Blocks=dbswriter.insertFiles(jobReport)
            common.logger.message("------>>>> Blocks = %s"%Blocks)
        except DBSWriterError, ex:
            common.logger.message("insert file error: %s"%ex)
        return Blocks

    def run(self):
        """
        parse of all xml file on res dir and creation of distionary
        """
        common.logger.message("Starting data publish")
        file_list = glob.glob(self.resDir+"crab_fjr*.xml")
        common.logger.debug(6, "file_list = "+str(file_list))
        common.logger.debug(6, "len(file_list) = "+str(len(file_list)))
        # FIXME: 
        #  do the dataset publication self.publishDataset here
        #
        if (len(file_list)>0):
            BlocksList=[]
            for file in file_list:
                common.logger.message("file = "+file)
                common.logger.message("Publishing dataset")
                self.exit_status=self.publishDataset(file)
                if (self.exit_status == '1'):
                    return self.exit_status 
                common.logger.message("Publishing files")
                Blocks=self.publishAJobReport(file,self.processedData)
                if Blocks:
                    [BlocksList.append(x) for x in Blocks]
            # close the blocks
            common.logger.message("------>>>> BlocksList = %s"%BlocksList)
            dbswriter = DBSWriter(self.DBSURL,level='ERROR')
            for BlockName in BlocksList:
                try:   
                    closeBlock=dbswriter.manageFileBlock(BlockName,maxFiles= 1)
                    common.logger.message("------>>>> closeBlock %s"%closeBlock)
                    #dbswriter.dbs.closeBlock(BlockName)
                except DBSWriterError, ex:
                    common.logger.message("------>>>> close block error %s"%ex)

            return self.exit_status

        else:
            common.logger.message(self.resDir+" empty --> No file to publish on DBS/DLS")
            self.exit_status = '1'
            return self.exit_status
    
