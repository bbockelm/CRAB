#!/usr/bin/env python
"""
_AnalysisTools_

Installer for crabserver monitoring tools

"""
import os
import logging
from ProdAgentCore.Configuration import prodAgentName
from ProdAgentCore.Configuration import loadProdAgentConfiguration

import cherrypy
from cherrypy.lib.static import serve_file

from Plugins.HTTPFrontend.TaskMon import TaskMonitor,TaskGraph, CumulativeTaskGraph,DatasetInfos,DatasetDetails,UserGraph
from Plugins.HTTPFrontend.JobMon import JobMonitor,CumulativeJobStatGraph, DestinationSitesMonitor, StatusPerDest, PlotByWMS
from Plugins.HTTPFrontend.ComponentServicesMonitor import CompServMonitor, ShowCompStatus, ShowCompLogs, WriteLog, MsgByComponent, MsgBalance
from Plugins.HTTPFrontend.UserMonitoring import TaskLogVisualizer,TaskLogMonitor, ListTaskForUser
from  Plugins.HTTPFrontend.ComponentCpuPlot import ComponentCpuPlot 

class Root:
    """
    _Root_

    Main index page for the component, will appear as the index page
    of the toplevel address

    """
    def __init__(self, myUrl):
        self.myUrl = myUrl

    def index(self):
        html = """<html><body><h2>CrabServer Instance: %s </h2>\n """ % (

            prodAgentName(), )

        html += "<table>\n"
        html += "<tr><th>Service</th><th>Description</th></tr>\n"

        html += "<tr><td><a href=\"tasks\">Tasks</a></td>\n"
        html += "<td>Tasks entities data in this CrabServer</td></td>\n"

        html += "<tr><td><a href=\"jobs\">Jobs</a></td>\n"
        html += "<td>Jobs entities data in this CrabServer</td></td>\n"

        html += "<tr><td><a href=\"compsermon\">Component Monitor</a></td>\n" 
        html += "<td>Component and Sevice status in this CrabServer</td></td>\n"

        html += "<tr><td><a href=\"logginfo\">User Monitoring</a></td>\n" 
        html += "<td>User task and job log information</td></tr>\n"

        html += """</table></body></html>"""
        return html
    index.exposed = True


class Downloader:
    """
    _Downloader_

    Serve files from the JobCreator Cache via HTTP

    """
    def __init__(self, rootDir):
        self.rootDir = rootDir

    def index(self, filepath):
        """
        _index_

        index response to download URL, serves the file
        requested

        """
        pathInCache = os.path.join(self.rootDir, filepath)
        logging.debug("Download Agent serving file: %s" % pathInCache)
        return serve_file(pathInCache, "application/x-download", "attachment")
    index.exposed = True


class ImageServer:

    def __init__(self, rootDir):
        self.rootDir = rootDir

    def index(self, filepath):
        pathInCache = os.path.join(self.rootDir, filepath)
        logging.debug("ImageServer serving file: %s" % pathInCache)
        return serve_file(pathInCache, content_type="image/png")
    index.exposed = True


def installer(**args):
    """
    _installer_
    """

    baseUrl = args['BaseUrl']

    root = Root(baseUrl)
    root.images = ImageServer(args['StaticDir'])

    root.datasetdetails = DatasetDetails(
#        "%s/images" % baseUrl,
        "images", 
        args["StaticDir"] ) 
    root.datasetinfos = DatasetInfos(
#        "%s/images" % baseUrl,
        "images",
        args["StaticDir"],
        "datasetdetails")
#        "%s/datasetdetails" % baseUrl ) 
    root.cumulativetaskgraph = CumulativeTaskGraph(
#        "%s/images" % baseUrl,
        "images",
        args["StaticDir"])
    root.taskgraph = TaskGraph(
#        "%s/images" % baseUrl,
        "images",
        args["StaticDir"])
    root.usergraph = UserGraph(
#        "%s/images" % baseUrl,
        "images",
        args['StaticDir'])
    root.tasks = TaskMonitor(
        "../taskgraph" ,
        "../cumulativetaskgraph",
        "../datasetinfos",
        "../usergraph" 
#        "%s/taskgraph" % baseUrl,
#        "%s/cumulativetaskgraph" % baseUrl,
#        "%s/datasetinfos" % baseUrl,
#        "%s/usergraph" % baseUrl
        )
 
    root.graphdest = DestinationSitesMonitor(
#        "%s/images" % baseUrl,
        "images",
        args['StaticDir']
        )
    root.graphstatus = StatusPerDest(
#        "%s/images" % baseUrl,
        "images" ,
        args['StaticDir'])
    root.graphjobstcum = CumulativeJobStatGraph(
#        "%s/images" % baseUrl,
        "images" ,
        args["StaticDir"]
        )


    root.jobwms = PlotByWMS(
#        "%s/images" % baseUrl,
        "images" ,
        args['StaticDir']
        )


    root.jobs = JobMonitor(
#        "%s/graphjobstcum" % baseUrl,
        "graphjobstcum" ,
#        "%s/graphdest" % baseUrl,
        "graphdest" ,
#        "%s/graphstatus" % baseUrl,
        "graphstatus" ,
#        "%s/jobwms" % baseUrl
        "jobwms" 
        )
    root.writelog = WriteLog()
    root.compstatus = ShowCompStatus(
#        "%s/compcpu" % baseUrl
        "../compcpu"
        )
    root.compmsg = MsgByComponent()
    root.msgblnc = MsgBalance(
        "images" ,
        args['StaticDir']
        )
    root.complog = ShowCompLogs( 
#        "%s/writelog" % baseUrl)
        "writelog"  )
    root.compcpu = ComponentCpuPlot(
#        "%s/images" % baseUrl,
        "images"  ,
        args["StaticDir"],
        args["ComponentDir"],
#        "%s/compcpu" % baseUrl
        "compcpu"  
        )
    root.compsermon = CompServMonitor(
#        "%s/compstatus" % baseUrl,
        "compstatus"  ,
#        "%s/complog" % baseUrl,
        "complog"  ,
#        "%s/compcpu" % baseUrl,
        "compcpu"  ,
#        "%s/compmsg" % baseUrl,
        "compmsg"  ,
#        "%s/msgblnc" % baseUrl
        "msgblnc"  
        )

    root.visualog = TaskLogVisualizer()
    root.usertask = ListTaskForUser(
#        "%s/visualog" % baseUrl
        "../visualog"  
        )
    root.logginfo = TaskLogMonitor(
#        "%s/visualog" % baseUrl,
        "visualog"  ,
#        "%s/usertask" % baseUrl
        "usertask"  
        )

    return root
