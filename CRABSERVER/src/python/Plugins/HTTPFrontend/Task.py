#!/usr/bin/env python
"""
_TaskMonitor_

CherryPy handler for displaying the list of task state in the CS instance

"""
import os
import API

_States = ["arrived", "submitting", "not submitted", "submitted", 
           "killed", "ended", "partially submitted", "partially killed"]

_GraphType = [ 'FinishedNotFinished', 'All']

class TaskMonitor:

    def __init__(self, graphMonUrl = None):
        self.graphmon = graphMonUrl
    
    def index(self):
        dictStat = statusTasks()
        html = """<html><body><h2>CrabServer Tasks </h2>\n """
        html += "<table>\n"
        html += " <tr><th>Components </th><th> Status</th></tr>\n"
        for st in dictStat.keys():
            html += '<tr><td align="left">'+str(st)+': </td><td><b> : '+str(dictStat[st])+'</b></td></tr>\n'
        html += "</table>\n"

        html += "<table>\n"
        html += " <tr><th>Graphics...</th></tr>\n"
        html += "</table>\n"
        for type in _GraphType:
            html += "<li><a href=\"%s?tasktype=%s\">%s</a></li>\n" % (
                self.graphmon, type, type)
        html += "</ul>\n"
        html += """</body></html>"""

        return html
    index.exposed = True


class TaskGraph:

    def __init__(self, imageUrl, imageDir):
        self.imageServer = imageUrl
        self.workingDir = imageDir

    def index( self, tasktype ):

        errHtml = "<html><body><h2>No Graph Tools installed!!!</h2>\n "
        errHtml += "</body></html>"
        try:
            from graphtool.graphs.common_graphs import PieGraph
        except ImportError:
            return errHtml
        total, dictStat = statusTasks(all=True)

        pngfile = os.path.join(self.workingDir, "%s-Task.png" % tasktype)
        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
        data={}
        data[_GraphType[1]] =  dictStat
        data[_GraphType[0]] =  { "Finished" : dictStat['ended'], "NotFinished" : (total-dictStat['ended']) }

        metadata = {"title" : "Task States %s : " % tasktype }
        pie = PieGraph()
        coords = pie.run( data[tasktype], pngfile, metadata )

        html = "<html><body><img src=\"%s\"></body></html>" % pngfileUrl
        return html
        
    index.exposed = True

def statusTasks(all=False):
    procStatus = {}
    total = 0
    for state in _States:
        procStatus[state] = API.getNumTask( state, False )
        total += procStatus[state]
    if all : return total, procStatus
    return procStatus 
