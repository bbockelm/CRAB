#!/usr/bin/env python
"""
_TaskMonitor_

CherryPy handler for displaying the list of task state in the CS instance

"""
import os, time
import API, Sites


class JobMonitor:

    def __init__(self,  graphJobStCum = None, graphdestination = None, graphstatusdest = None, jobwms = None  ):
        self.graphjobstcum  =graphJobStCum
        self.graphdestination = graphdestination
        self.graphstatusdest = graphstatusdest
        self.jobwms = jobwms
    
    def index(self):


        template = ' for last '
        template += ' <input type="text" name="length" size=4 value=0> '
        template += ' <select name="span" style="width:80px"><option>hours</option><option>days</option></select> '
        template += ' <select name="type" style="width:80px"><option>list</option><option>plot</option></select> '
        template += ' <input type="submit" value="Show Summary"/> '
        
        template1 = ' for '
        template1 += '<select name="site">'
        template1 += '<option>all</option>'
        sitesnames = Sites.SiteMap().keys()
        sitesnames.sort()
        for sitename in sitesnames:
            template1 += '<option>'+sitename+'</option>'
        template1 += '</select>'
        template1 += ' site(s) '

        html = """<html><body><h2>CrabServer Jobs</h2>\n """

        html += "<table>\n"
        html += "<br/><br/>"
        html += '<form action=\"%s"\ method="get">' % (self.graphjobstcum)
        html += 'Cumulative plot of jobs status '
        html += ' for last  '
        html += ' <input type="text" name="length" size=4 value=0> '
        html += ' <select name="span" style="width:80px"><option>hours</option><option>days</option></select> '
        html += ' <input type="submit" value="Show Plot"/> '
        html += '</select>'
        html += '</form>'
        html += "</table>\n"

        html += "<table>\n"
        html += "<br/><br/>"
        html += "<i>Current job destination pie for job submitted in the last period:</i><br/>"
        html += '<form action=\"%s"\ method="get">' % (self.graphdestination)
        html += 'Job Destionation sites '
        html += template
        html += template1
        html += '</form>'
        html += "</table>\n"
        
        html += "<table>\n"
        html += "<br/><br/>"
        html += "<i>Current per-site job status distribution for jobs submitted in the last period:</i><br/>"
        html += '<form action=\"%s"\ method="get">' % (self.graphstatusdest)
        html += 'Status per Destination Sites '
        html += template
        html += template1
        html += '</select>'
        html += '</form>'
        html += "</table>\n"

        from time import gmtime, strftime
        curryear  = strftime("%Y", gmtime())
        currmonth = strftime("%m", gmtime())
        currday   = strftime("%d", gmtime())

        days   = "".join(["<option>%s</option>"%(str(obj)) for obj in xrange(1,int(currday))])
        days  += "<option selected='selected'>%s</option>"%str(currday)
        days  += "".join(["<option>%s</option>"%(str(obj)) for obj in xrange(int(currday)+1,32)])

        months = "".join(["<option>%s</option>"%(str(obj)) for obj in xrange(1,int(currmonth))])
        months+= "<option selected='selected'>%s</option>"%str(currmonth)
        months+= "".join(["<option>%s</option>"%(str(obj)) for obj in xrange(int(currmonth)+1,13)])

        years  = "<option selected='selected'>%s</option><option>%s</option>"%(str(curryear),str(int(curryear)-1))

        html += "<table>\n"
        html += "<br/><br/>"
        html += '<form action=\"%s"\ method="get">' % (self.jobwms)
        html += '<i>Status per Destination Sites</i>'
        html += '&nbsp from &nbsp<select name="fromy" style="width:80px">%s</select>'%years
        html += '<select name="fromm" style="width:80px">%s</select>'%months
        html += '<select name="fromd" style="width:80px">%s</select>'%days
        html += '&nbsp to &nbsp<select name="toy" style="width:80px">%s</select>'%years
        html += '<select name="tom" style="width:80px">%s</select>'%months
        html += '<select name="tod" style="width:80px">%s</select>'%days
        html += '&nbsp<input type="submit" value="Show Plot"/> '
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
                t = time.mktime(b.timetuple()) - time.altzone
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


class DestinationSitesMonitor:

    def __init__(self, imageUrl, imageDir):
        self.imageServer = imageUrl
        self.workingDir = imageDir


    def TypePlot(self,span, length, site, sites):
        """
        """ 
        errHtml = "<html><body><h2>No Graph Tools installed!!!</h2>\n "
        errHtml += "</body></html>"
        try:
            from graphtool.graphs.common_graphs import PieGraph
        except ImportError:
            return errHtml
 
        pngfile = os.path.join(self.workingDir, "%s-%s-%s-Site.png" % (span, length, site))
        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
        data = sites
        metadata = {'title':'Destination Sites Distribution'}
        Graph = PieGraph()
        coords = Graph( data, pngfile, metadata )
 
        html = "<html><body><img src=\"%s\"></body></html>" % pngfileUrl

        return html

    def TypeList(self, sites):
        """
        preare list site-#jobs
        """
        html = "<h4>List of sites...</h4>\n"
        html += "<table>\n"
        
        html += "<tr>"
        html += "<th>Destination Site</th><th>Number of Jobs</th>"
        html += "</tr>\n"
  
        JobCount = 0 
        lensite=len(sites)
        for dest,num in sites.items():
            html += "<tr>"
            html += "<td>%s</td><td>%s</td>" % ( dest, num )
            html += "</tr>\n"
            JobCount += num 
        html += "</table>\n"
 
        html += "<h4>Total number of jobs : %s </h4>\n" % JobCount
        html += "<h4>Total number of sites : %s </h4>\n" % lensite
        html += """</body></html>"""
         
        return html

    def index(self, length, span, type, site ):

        _span=3600 
        if span == 'days': _span=24*3600

        query_time = int(length)*_span
        end_time = time.time() - time.altzone
        start_time = end_time - query_time
 
        import Sites
        to_query = site 
        if site != 'all' : to_query = Sites.SiteMap()[site]
        
        sites = API.getSites( query_time, to_query ) 
        total = len(sites)
 
        if (total == 0):
            html = "<html><body>No job for %s Site,  during last %s %s </body></html>" % (
                site,length,span)
            return html

        if type == 'plot':
            html = self.TypePlot(span, length, site, sites)
        else:
            html = """<html><body><h2>List of Users for """
            html += "Last %s %s</h2>\n " % ( length, span )
            html += self.TypeList(sites)
 
        return html
        
    index.exposed = True

class StatusPerDest:

    def __init__(self, imageUrl, imageDir):
        self.imageServer = imageUrl
        self.workingDir = imageDir


    def TypePlot(self,span, length, site, data):
        """
        """ 
        errHtml = "<html><body><h2>No Graph Tools installed!!!</h2>\n "
        errHtml += "</body></html>"
        try:
            from graphtool.graphs.common_graphs import StackedBarGraph
        except ImportError:
            return errHtml
        
        pngfile = os.path.join(self.workingDir, "%s-%s-%s-Status.png" % (span, length, site))
        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
        metadata = {'title':'Job status per Sites Distribution'}
        Graph = StackedBarGraph()
        coords = Graph( data, pngfile, metadata )
 
        html = "<html><body><img src=\"%s\"></body></html>" % pngfileUrl

        return html

    def TypeList(self, sites):
        """
        preare list site-#jobs
        """
        html = "<h4>List of sites...</h4>\n"
        html += "<table>\n"
        
        html += "<tr>"
        html += "<th>Destination Site</th><th>Number of Jobs</th>"
        html += "</tr>\n"
  
        JobCount = 0 
        lensite=len(sites)
        for dest,num in sites.items():
            html += "<tr>"
            html += "<td>%s</td><td>%s</td>" % ( dest, num )
            html += "</tr>\n"
            JobCount += num 
        html += "</table>\n"
 
        html += "<h4>Total number of jobs : %s </h4>\n" % JobCount
        html += "<h4>Total number of sites : %s </h4>\n" % lensite
        html += """</body></html>"""
         
        return html

    def index(self, length, span, type, site ):

        _span=3600 
        if span == 'days': _span=24*3600

        query_time = int(length)*_span
        end_time = time.time() - time.altzone
        start_time = end_time - query_time
 
        status_site_dict = {} 
        import Sites
        if site == 'all' :
            for st1,st2 in Sites.SiteMap().items():
                query =  API.getKeyNum( 'status_scheduler', st2, query_time ) 
                for a,b in query:
                    status_site_dict[b] = {str(st1).split('_')[2]:a}         
        else:
            site_to_query = Sites.SiteMap()[site]
            query = API.getKeyNum('status_scheduler', site_to_query, query_time ) 
            for a,b in query:
                status_site_dict[b] = {site:a}         
         
        if type == 'plot':
            html = self.TypePlot(span, length, site, status_site_dict)
        else:
            html = """<html><body><h2>NOT YET IMPLEMENTED """
         #   html += "Last %s %s</h2>\n " % ( length, span )
         #   html += self.TypeList(sites)
 
        return html
        
    index.exposed = True


class PlotByWMS:

    def __init__(self, imageUrl, imageDir):
        self.imageServer = imageUrl
        self.workingDir = imageDir


    def TypePlot(self, data, wms):
        """
        """
        errHtml = "<html><body><h2>No Graph Tools installed!!!</h2>\n "
        errHtml += "</body></html>"
        try:
            from graphtool.graphs.common_graphs import PieGraph
        except ImportError:
            return errHtml

        pngfile = os.path.join(self.workingDir, "JobStatusByWMS_%s_.png"%str(wms))
        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
        metadata = {'title':'Job Status for [%s] WMS'%str(wms)}
        Graph = PieGraph()
        coords = Graph( data, pngfile, metadata )

        html = "<img src=\"%s\">" % pngfileUrl

        return html

    def index(self, fromy = "", fromm ="", fromd = "", toy = "", tom = "", tod = ""):

        fromdata = fromy+"-"+fromm+"-"+fromd
        todata   = toy+"-"+tom+"-"+tod
        data = API.jobsByWMS(fromdata, todata)
        dictofdict = {}
        for wms,status,count in data:
            if dictofdict.has_key(str(wms)):
                dictofdict[str(wms)].setdefault(str(status), int(count))
            else:
                dictofdict.setdefault(wms,{str(status):int(count)})

        html = "<html><body>"
        for key, val in dictofdict.iteritems():
            if str(key).find("https://") != -1:
                wms = str(key).split("https://")[1].split(":")[0]
                html += self.TypePlot(val, wms) + "<br><br>"
            else:
                html += self.TypePlot(val, str(key))

        html += "</body></html>"
            

        return html

    index.exposed = True

