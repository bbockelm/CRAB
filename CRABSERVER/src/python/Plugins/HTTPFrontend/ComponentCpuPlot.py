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

AllServices = {'GridFTP':'globus-gridftp-server','mySQL':'mysqld'}
AllResourcesPlot = {
        'CPU'     : [ 'CPU',
                      ['user (%)','nice (%)','system (%)','iowait (%)' ]
                      ],
        'LOAD'    : [ 'LOAD',
                      ['1m-average','5m-average', '15m-average']
                      ],
        'MEM'     : [ 'MEM',
                      ['buffered (kBi)','cached (kBi)', 'used (kBi)']
                      ],
        'SWAP'    : [ 'MEM',
                      [None,None,None,'used swap (%)']
                      ],
        'SWAPPING': [ 'SWAP',
                      ['swap pages brought in (Hz)','swap pages brought out (Hz)']
                      ]
        }
                                                                                                                                                                        

def gatherHWData(logFilename,Nbins):
        logLines = open(logFilename,'r').readlines()
        end = len(logLines);
        start = end - Nbins;
        if start < 0:
                start = 0;
        begin_time = int(logLines[start].split()[1]);
        end_time = int(logLines[end-1].split()[1]);
        logLines = logLines[start:end]
        series = {}
        for Jd in range(len(logLines[0].split()[2:])):
                series[Jd] = {}
        for line in logLines:
                time = int(line.split()[1]);
                columns = line.split()[2:]
                for Jd,datum in enumerate(columns):
                        series[Jd][time] = float(datum)
        return begin_time, end_time, series
#         for line in logLines:
#                 time = int(line.split()[1]);
#                 user[time]  = float(line.split()[2]);
#                 system[time]  = float(line.split()[3]);
#                 total[time] = user[time]+system[time]
#         return begin_time, end_time, user, system, total

def draw_TimeComponentCpuGraph(Component,sensorsDir,file,length,span,size,labels,title):
        if span == "days":
                span = 24*60
        else:
                span = 60
        dataFilename = os.path.join(sensorsDir,"%s-pidstat.dat" % Component)
        begin_time, end_time, series = gatherHWData(dataFilename,int(length)*span)
        metadata = {'title':title, 'starttime':begin_time, 'endtime':end_time, 'span':span, 'is_cumulative':True }
        if size == 'small':
                metadata['height'] = 250
                metadata['width'] = 400
                metadata['legend'] = False
                metadata['title_size'] = 22
                metadata['text_size'] = 18
        data = {}
        for Js, label in enumerate(labels):
                if label != None:
                        data[label] = series[Js]
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
        labels = ['user (%)', 'system (%)']  # default labels for components and services
        
        if int(length) <= 0:
                page.append("Unable to process request for %s %s<br/>Please ask for a strictly positive period of time..."%(length,span))
        elif Component == 'All components':
                for Comp in status(True):
                        pngfile = os.path.join(self.workingDir, "%s-%s-%s-%s_pidstat.png" % (Comp,length,span,'small'))
                        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
                        page += "<a href=\"%s?Component=%s&length=%s&span=%s\"><img border=0 src=\"%s\"></a>" % (self.compcpu,Comp,length,span,pngfileUrl)
                        title = Comp+' CPU usage'
                        draw_TimeComponentCpuGraph(Comp,self.sensorsDir,pngfile,length,span,'small',labels,title)
        elif Component == 'All services':
                for Serv in AllServices.keys():
                        pngfile = os.path.join(self.workingDir, "%s-%s-%s-%s_pidstat.png" % (Serv,length,span,'small'))
                        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
                        page += "<a href=\"%s?Component=%s&length=%s&span=%s\"><img border=0 src=\"%s\"></a>" % (self.compcpu,Serv,length,span,pngfileUrl)
                        title = Serv+' CPU usage' # ,'History plot of CPU usage for '+Serv]
                        draw_TimeComponentCpuGraph(Serv,self.sensorsDir,pngfile,length,span,'small',labels,title)
        elif Component == 'All resources':
                sk=AllResourcesPlot.keys(); sk.sort()
                for Res in sk:
                        pngfile = os.path.join(self.workingDir, "%s-%s-%s-%s_pidstat.png" % (Res,length,span,'small'))
                        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
                        page += "<a href=\"%s?Component=%s&length=%s&span=%s\"><img border=0 src=\"%s\"></a>" % (self.compcpu,Res,length,span,pngfileUrl)
                        title = Res
                        draw_TimeComponentCpuGraph(AllResourcesPlot[Res][0],self.sensorsDir,pngfile,length,span,'small',AllResourcesPlot[Res][1],title)
        else:
                if Component in AllResourcesPlot.keys():
                        labels = AllResourcesPlot[Component][1]
                        title = 'History plot for '+Component
                        Component = AllResourcesPlot[Component][0]
                else:
                        title = 'History plot of CPU usage for '+Component
                pngfile = os.path.join(self.workingDir, "%s-%s-%s-%s_pidstat.png" % (Component,length,span,'big'))
                pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
                page += "<img src=\"%s\">" % pngfileUrl
                draw_TimeComponentCpuGraph(Component,self.sensorsDir,pngfile,length,span,'big',labels,title)
                
        page.append(_footer)
        return page
       
    index.exposed = True
