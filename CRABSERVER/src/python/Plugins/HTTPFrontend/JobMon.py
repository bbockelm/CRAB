#!/usr/bin/env python
"""
_TaskMonitor_

CherryPy handler for displaying the list of task state in the CS instance

"""
import os, time
import API


class JobMonitor:

    def __init__(self,  graphJobStCum = None):
        self.graphjobstcum  =graphJobStCum
    
    def index(self):

        html = """<html><body><h2>CrabServer Jobs</h2>\n """

        html += "<table>\n"
        html += "<i> </i><br/><br/>"
        html += '<form action=\"%s"\ method="get">' % (self.graphjobstcum)
        html += 'Cumulative plot of jobs status '
        html += ' for last  '
        html += '<input type="text" name="length" size=4 value=0>'
        html += '<select name="span" style="width:80px"><option>hours</option><option>days</option></select>'
        html += '<input type="submit" value="Show Plot"/>'
        html += '</select>'
        html += '</form>'
        html += "</table>\n"

        html += """</body></html>"""

        return html
    index.exposed = True

class CumulativeJobStatGraph:

    def __init__(self, imageUrl, imageDir):
        self.imageServer = imageUrl
        self.workingDir = imageDir

    def index( self, length, span ):

        _span=3600 
        if span == 'days': _span=24*3600

        query_time = int(length)*_span
        end_time = time.time() - time.altzone
        start_time = end_time - query_time
       
        querydata = API.getTimeStatusJob( query_time )
  
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

        pngfile = os.path.join(self.workingDir, "%s-JobCumulative.png" % (length) )
        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
       
        data = binning  

        metadata = {'title':' Cumulative of jobs per Status ', 'starttime':start_time, 'endtime':end_time, 'span':_span, 'is_cumulative':False }
        cum = CumulativeGraph()
        coords = cum.run( data, pngfile, metadata )

        html = "<html><body><img src=\"%s\"></body></html>" % pngfileUrl
 
        return html
        
    index.exposed = True
