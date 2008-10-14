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

from Plugins.HTTPFrontend.TaskMon import TaskMonitor,TaskGraph, CumulativeTaskGraph,DatasetInfos
from Plugins.HTTPFrontend.JobMon import JobMonitor,CumulativeJobStatGraph
from Plugins.HTTPFrontend.ComponentServicesMonitor import CompServMonitor, ShowCompStatus, ShowCompLogs, WriteLog
from Plugins.HTTPFrontend.OverallMonitor import OverallMonitor, UserGraph, DestinationSitesMonitor, StatusPerDest
from Plugins.HTTPFrontend.UserMonitoring import TaskLogVisualizer,TaskLogMonitor, ListTaskForUser

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

        html += "<tr><td><a href=\"%s/tasks\">Tasks</a></td>\n" % (
            self.myUrl,)
        html += "<td>Tasks entities data in this CrabServer</td></td>\n"

        html += "<tr><td><a href=\"%s/jobs\">Jobs</a></td>\n" % (
            self.myUrl,)
        html += "<td>Jobs entities data in this CrabServer</td></td>\n"

        html += "<tr><td><a href=\"%s/compsermon\">Component Monitor</a></td>\n" % (
            self.myUrl,)
        html += "<td>Component and Sevice status in this CrabServer</td></td>\n"

        html += "<tr><td><a href=\"%s/overall\">Overall Statistics</a></td>\n" % (
            self.myUrl,)
        html += "<td>Destination, Users.... </td></td>\n"

        html += "<tr><td><a href=\"%s/logginfo\">User Monitoring</a></td>\n" % (
            self.myUrl,)
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

    root.datasetinfos = DatasetInfos(
        "%s/images" % baseUrl,
        args["StaticDir"] 
        ) 
    root.cumulativetaskgraph = CumulativeTaskGraph(
        "%s/images" % baseUrl,
        args["StaticDir"])
    root.taskgraph = TaskGraph(
        "%s/images" % baseUrl,
        args["StaticDir"])
    root.tasks = TaskMonitor(
        "%s/taskgraph" % baseUrl,
        "%s/cumulativetaskgraph" % baseUrl,
        "%s/datasetinfos" % baseUrl
        )
 
    root.graphjobstcum = CumulativeJobStatGraph(
        "%s/images" % baseUrl,
        args["StaticDir"])
    root.jobs = JobMonitor(
        "%s/graphjobstcum" % baseUrl
        )
    root.writelog = WriteLog()
    root.compstatus = ShowCompStatus()
    root.complog = ShowCompLogs( 
        "%s/writelog" % baseUrl)
    root.compsermon = CompServMonitor(
        "%s/compstatus" % baseUrl,
        "%s/complog" % baseUrl
        )
   
    root.graphdest = DestinationSitesMonitor(
        "%s/images" % baseUrl,
        args['StaticDir'])

    root.usergraph = UserGraph(
        "%s/images" % baseUrl,
        args['StaticDir'])
    root.graphstatus = StatusPerDest(
        "%s/images" % baseUrl,
        args['StaticDir'])

    root.overall = OverallMonitor(
        "%s/usergraph" % baseUrl,
        "%s/graphdest" % baseUrl,
        "%s/graphstatus" % baseUrl
        )

    root.visualog = TaskLogVisualizer()
    root.usertask = ListTaskForUser(
        "%s/visualog" % baseUrl
        )
    root.logginfo = TaskLogMonitor(
        "%s/visualog" % baseUrl,
        "%s/usertask" % baseUrl
        )

    return root
