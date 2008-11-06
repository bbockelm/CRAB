#!/usr/bin/env python
"""
_ComponentCpuPlot_

CherryPy handler for displaying the plot of Component CPU usage

"""

from TaskTracking.TaskStateAPI import *
from pylab import *
import cherrypy
from cherrypy import tools
from numpy import *
import time, os, random, datetime
from matplotlib.font_manager import FontProperties
from matplotlib.backends.backend_agg import RendererAgg
from matplotlib.transforms import Value
from graphtool.graphs.common_graphs import PieGraph, BarGraph, CumulativeGraph
from graphtool.graphs.graph import TimeGraph
import API
from ProdAgentCore.Configuration import loadProdAgentConfiguration
import Sites
from ComponentServicesMonitor import status


def gatherHWData(logFilename,Nbins):
        user = {}
        system = {}
        total = {}
        logLines = open(logFilename,'r').readlines()
        end = len(logLines);
        start = end - Nbins;
        if start < 0:
                start = 0;
        begin_time = int(logLines[start].split()[1]);
        end_time = int(logLines[end-1].split()[1]);
        logLines = logLines[start:end]
        for line in logLines:
                time = int(line.split()[1]);
                user[time]  = float(line.split()[2]);
                system[time]  = float(line.split()[3]);
                total[time] = user[time]+system[time]
        return begin_time, end_time, user, system, total

def draw_TimeComponentCpuGraph(Component,sensorsDir,file,length,span,size='big'):
        if span == "days":
                span = 24*60
        else:
                span = 60
        dataFilename = os.path.join(sensorsDir,"%s-pidstat.dat" % Component)
        begin_time, end_time, user, system, total = gatherHWData(dataFilename,int(length)*span)
        metadata = {'title':'History plot of CPU usage for '+Component, 'starttime':begin_time, 'endtime':end_time, 'span':span, 'is_cumulative':True }
        if size == 'small':
                metadata['height'] = 250
                metadata['width'] = 400
                metadata['legend'] = False
                metadata['title'] = Component
                metadata['title_size'] = 22
                metadata['text_size'] = 18
                
        data = {'user (%)':user, 'system (%)':system}
        Graph = CumulativeGraph()
        Graph( data, file, metadata )

class ComponentCpuPlot:

    def __init__(self, imageUrl, imageDir, compDir, compcpu):
        self.imageServer = imageUrl
        self.workingDir = imageDir
        self.sensorsDir =  os.path.join(compDir,'sensors')
        self.compcpu = compcpu

    def index(self, length, span, Component):
        
	_header = """
                                <html>
                                <head>
                                <title>"""+os.environ['HOSTNAME']+""" - """+Component+""" Monitor</title>
                                </head>
                                <body>
                                <div class="container">"""
        _footer = """
                                </div>
                                </body>
                                </html>"""

        cherrypy.response.headers['Content-Type']= 'text/html'
        
        page = [_header]

        if int(length) <= 0:
                page.append("Unable to process request for %s %s<br/>Please ask for a strictly positive period of time..."%(length,span))
        elif Component == 'All':
                for Comp in status(True):
                        pngfile = os.path.join(self.workingDir, "%s-%s-%s_pidstat.png" % (Comp,length,span))
                        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
                        page += "<a href=\"%s?Component=%s&length=%s&span=%s\"><img border=0 src=\"%s\"></a>" % (self.compcpu,Comp,length,span,pngfileUrl)
                        draw_TimeComponentCpuGraph(Comp,self.sensorsDir,pngfile,length,span,'small')
        else:
                pngfile = os.path.join(self.workingDir, "%s-%s-%s_pidstat.png" % (Component,length,span))
                pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
                page += "<img src=\"%s\">" % pngfileUrl
                draw_TimeComponentCpuGraph(Component,self.sensorsDir,pngfile,length,span)
                        
                
        page.append(_footer)
        return page
       
    index.exposed = True
