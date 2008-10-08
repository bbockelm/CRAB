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

from Plugins.HTTPFrontend.Task import TaskMonitor,TaskGraph
from Plugins.HTTPFrontend.ComponentServicesMonitor import CompServMonitor
from Plugins.HTTPFrontend.OverallMonitor import OverallMonitor, UserGraph, DestinationSitesMonitor, StatusPerDest

class Root:
    """
    _Root_

    Main index page for the component, will appear as the index page
    of the toplevel address

    """
    def __init__(self, myUrl):
        self.myUrl = myUrl

    def index(self):
        html = """<html><body><h2>ProdAgent Instance: %s </h2>\n """ % (

            prodAgentName(), )

        html += "<table>\n"
        html += "<tr><th>Service</th><th>Description</th></tr>\n"

        html += "<tr><td><a href=\"%s/tasks\">Tasks</a></td>\n" % (
            self.myUrl,)
        html += "<td>Tasks entities data in this CrabServer</td></td>\n"

        html += "<tr><td><a href=\"%s/compsermon\">Component Monitor</a></td>\n" % (
            self.myUrl,)
        html += "<td>Component and Sevice status in this CrabServer</td></td>\n"

        html += "<tr><td><a href=\"%s/overall\">Overall Statistics</a></td>\n" % (
            self.myUrl,)
        html += "<td>Destination, Users.... </td></td>\n"
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

    root.taskgraph = TaskGraph(
        "%s/images" % baseUrl,
        args["StaticDir"])
    root.tasks = TaskMonitor(
        "%s/taskgraph" % baseUrl
        )
    root.compsermon = CompServMonitor()
   
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


    return root