#!/usr/bin/env python
"""
_TaskMonitor_

CherryPy handler for displaying the list of task state in the CS instance

"""
import os, time
import API

_tasktype = {'All' : '', 'Archived' : True , 'NotArchived' : False }

class TaskMonitor:

    def __init__(self, graphTask = None, graphTaskCumulative = None):
        self.graphtask = graphTask
        self.graphtaskcumulative = graphTaskCumulative
    
    def index(self):

        html = """<html><body><h2>CrabServer Tasks Entities</h2>\n """
        html += "<table>\n"
        html += "<i>a time-window of 0 (zero) means all available statistics:</i><br/><br/><br/>"
        html += "<i> </i><br/><br/>"
        html += '<form action=\"%s"\ method="get" >' % (self.graphtask)
        html += 'Status of  '
        html += ' <select name="tasktype" style="width:80px"><option>All</option><option>Archived</option><option>NotArchived</option></select>'
        html += '  tasks for last  '
        html += '<input type="text" name="length" size=4 value=0>'
        html += '<select name="span" style="width:80px"><option>hours</option><option>days</option></select>'
        html += '<select name="type" style="width:80px"><option>list</option><option>plot</option></select>'
        html += '<input type="submit" value="Show Summary"/>'
        html += '</select>'
        html += '</form>'
        html += "</table>\n"

        html += "<table>\n"
        html += "<i> </i><br/><br/>"
        html += '<form action=\"%s"\ method="get">' % (self.graphtaskcumulative)
        html += 'Cumulative plot of tasks status '
        html += ' for last  '
        html += '<input type="text" name="length" size=4 value=0>'
        html += '<select name="span" style="width:80px"><option>hours</option><option>days</option></select>'
        html += '<input type="submit" value="Show Summary"/>'
        html += '</select>'
        html += '</form>'
        html += "</table>\n"

        html += """</body></html>"""

        return html
    index.exposed = True


class TaskGraph:

    def __init__(self, imageUrl, imageDir):
        self.imageServer = imageUrl
        self.workingDir = imageDir

    def index( self, length, span, tasktype, type ):

        _span=3600 
        if span == 'days': _span=24*3600

        query_time = int(length)*_span
        end_time = time.time() - time.altzone
        start_time = end_time - query_time
       
        tasks = API.getNumTask( query_time, _tasktype[tasktype] )
        
        data={}
        for num, state in tasks:
            data[state]= num 

        if type == 'plot':
            html = self.drawPlot(span, length, data, tasktype)
        else:
           html = """<html><body><h2>%s Tasks per status for """% (tasktype)
           html += "Last %s %s</h2>\n " % ( length, span )
           html += "<table>\n"
           html += "<tr>"
           html += self.writeList(data)

        return html
        
    index.exposed = True

    def writeList(self, data):

        html = "<th>Task status</th><th>Number of tasks </th>"
        html += "</tr>\n"
  
        taskCount = 0
        for status, num in data.items():
            html += "<tr>"
            html += "<td>%s</td><td>%s</td>" % ( status, num )
            html += "</tr>\n"
            taskCount += num
        
        html += "</table>\n"
 
        html += "<h4>Total number of tasks: %s </h4>\n" % taskCount
        html += """</body></html>"""
        return html


    def drawPlot(self, span, length, data, tasktype) :
  
        errHtml = "<html><body><h2>No Graph Tools installed!!!</h2>\n "
        errHtml += "</body></html>"
        try:
            from graphtool.graphs.common_graphs import PieGraph
        except ImportError:
            return errHtml

        pngfile = os.path.join(self.workingDir, "%s-Task.png" % tasktype)
        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)

        metadata = {"title" : "%s Task  : " % tasktype }
        pie = PieGraph()
        coords = pie.run( data, pngfile, metadata )

        html = "<html><body><img src=\"%s\"></body></html>" % pngfileUrl
 
        return html

class CumulativeTaskGraph:

    def __init__(self, imageUrl, imageDir):
        self.imageServer = imageUrl
        self.workingDir = imageDir

    def index( self, length, span ):

        _span=3600 
        if span == 'days': _span=24*3600

        query_time = int(length)*_span
        end_time = time.time() - time.altzone
        start_time = end_time - query_time
       
        querydata = API.getTimeStatusTask( query_time )
  
        errHtml = "<html><body><h2>No Graph Tools installed!!!</h2>\n "
        errHtml += "</body></html>"
        try:
            from graphtool.graphs.common_graphs import CumulativeGraph
        except ImportError:
            return errHtml

        interval = range(int(start_time),int(end_time),int(_span))
        temp_dictOfList = {}
        cnt=0
        list_status=[]
        for i in interval :
            temp_list = []
            for a,b in querydata :
                list_status.append(a)
                t = time.mktime(b.timetuple())
                if t > i and t < i+_span :
                    temp_list.append(a)
            temp_dictOfList[cnt]=temp_list
            cnt +=1
        binning={} 
        num_stat =  0
        for ii in set(list_status):
            c=0
            dict_for_binning={}
            for i in temp_dictOfList.values():
                cc=0
                for  stat in i:
                    if stat == ii: cc+=1
                dict_for_binning[interval[c]]=cc
                c += 1
            binning[ii] = dict_for_binning
            num_stat += 1 

        pngfile = os.path.join(self.workingDir, "%s-TaskCumulative.png" % (length) )
        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
       
        data = binning  

        metadata = {'title':' Cumulative of Task per Status ', 'starttime':start_time, 'endtime':end_time, 'span':_span, 'is_cumulative':False }
        cum = CumulativeGraph()
        coords = cum.run( data, pngfile, metadata )

        html = "<html><body><img src=\"%s\"></body></html>" % pngfileUrl
 
        return html
        
    index.exposed = True
