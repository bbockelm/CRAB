#!/usr/bin/env python
"""
_Overall Statiscs_
CherryPy handler for displaying the Statics for:
- users
- sites
- exit code 
"""
import os, time
import API, Sites

class OverallMonitor:
    """
    _OverallMonitor_

    Generate a list of possible queries for user/site/exitcodes

    """
    def __init__(self, graphuser = None, graphdestination = None, graphstatusdest = None ):
        self.graphuser = graphuser
        self.graphdestination = graphdestination
        self.graphstatusdest = graphstatusdest
        
    
    def index(self):

        template = ' for last '
        template += '<input type="text" name="length" size=4 value=0>'
        template += '<select name="span" style="width:80px"><option>hours</option><option>days</option></select>'
        template += '<select name="type" style="width:80px"><option>list</option><option>plot</option></select>'
        template += '<input type="submit" value="Show Summary"/>'
        
        template1 = ' for '
        template1 += '<select name="site">'
        template1 += '<option>all</option>'
        sitesnames = Sites.SiteMap().keys()
        sitesnames.sort()
        for sitename in sitesnames:
            template1 += '<option>'+sitename+'</option>'
        template1 += '</select>'
        template1 += ' site(s) '

        html = """<html><body><h2> Overall Statistics </h2>\n """
        html += "<table>\n"
        html += "<i>a time-window of 0 (zero) means all available statistics:</i><br/><br/><br/>"
        html += "<i>History plot of CrabServer usage by different users for last 24 hours, 7 days or month:</i><br/><br/>"
        html += '<form action=\"%s"\ method="get">' % (self.graphuser)
        html += 'Users submitting jobs to this CrabServer  '
        html += template
        html += '</select>'
        html += '</form>'
        html += "<br/><br/>"
        html += "</table>\n"
          
        html += "<table>\n"
        html += "<i>Current job destination pie for job submitted in the last period:</i><br/><br/>"
        html += '<form action=\"%s"\ method="get">' % (self.graphdestination)
        html += 'Job Destionation sites '
        html += template
        html += template1
        html += '</select>'
        html += '</form>'
        html += "<br/><br/>"
        html += "</table>\n"
        
        html += "<table>\n"
        html += "<i>Current per-site job status distribution for jobs submitted in the last period:</i><br/><br/>"
        html += '<form action=\"%s"\ method="get">' % (self.graphstatusdest)
        html += 'Status per Destination Sites '
        html += template
        html += template1
        html += '</select>'
        html += '</form>'
        html += "<br/><br/>"
        html += "</table>\n"
        
        html += """</body></html>"""
        return html
    index.exposed = True


class UserGraph:

    def __init__(self, imageUrl, imageDir):
        self.imageServer = imageUrl
        self.workingDir = imageDir


    def index(self, length, span, type ):

        _span=3600 
        if span == 'days': _span=24*3600

        query_time = int(length)*_span
        end_time = time.time() #- time.altzone
        start_time = end_time - query_time

        users = API.getUserName( query_time ) 
        total = len(users)
 
        if (total == 0):
            html = "<html><body>No Users for last: %s %s</body></html>" % (
                length,span)
            return html

        if type == 'plot':
            errHtml = "<html><body><h2>No Graph Tools installed!!!</h2>\n "
            errHtml += "</body></html>"
            try:
                from graphtool.graphs.common_graphs import CumulativeGraph
            except ImportError:
                return errHtml
 
            land_time = []
            for name,land in users:
                land_time.append( time.mktime(land.timetuple()))
 
            range_time= range(int(start_time),int(end_time),_span)
            binning = {}
            for i in range_time:
                count = 0
                for t in land_time:
                    if t > i and t < i+_span:
                        count += 1 
                binning[i]= count
 
            pngfile = os.path.join(self.workingDir, "%s-%s-User.png" % (span, length))
            pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
 
            data={'User': binning }

            metadata = {'title':'User Statistics', 'starttime':start_time, 'endtime':end_time, 'span':_span, 'is_cumulative':False }
            Graph = CumulativeGraph()
            coords = Graph( data, pngfile, metadata )
 
            html = "<html><body><img src=\"%s\"></body></html>" % pngfileUrl
        else:
            html = """<html><body><h2>List of Users for """
            html += "Last %s %s</h2>\n " % ( length, span )
 
            html += "<table>\n"
            
            html += "<tr>"
            html += "<th>Arrived Time</th><th>User Name</th>"
            html += "</tr>\n"
  
            userCount = len(users)
            for landed, name in users:
                html += "<tr>"
                html += "<td>%s</td><td>%s</td>" % ( name, landed )
                html += "</tr>\n"
            
            html += "</table>\n"
 
            html += "<h4>Total number of users: %s </h4>\n" % userCount
            html += """</body></html>"""
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


