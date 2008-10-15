#!/usr/bin/env python
"""
_TaskMonitor_

CherryPy handler for displaying the list of task state in the CS instance

"""
import os, time
import API

_tasktype = {'All' : '', 'Archived' : True , 'NotArchived' : False }

class TaskMonitor:

    def __init__(self, graphTask = None, graphTaskCumulative = None, datasetRelated= None,graphuser=None ):
        self.graphtask = graphTask
        self.graphtaskcumulative = graphTaskCumulative
        self.datasetrelated = datasetRelated
        self.graphuser = graphuser
    
    def index(self):

        html = """<html><body><h2>CrabServer Tasks Entities</h2>\n """
        html += "<table>\n"
        html += "<br/><br/>"
        html += '<form action=\"%s"\ method="get" >' % (self.graphtask)
        html += 'Status of  '
        html += ' <select name="tasktype" style="width:80px"><option>All</option><option>Archived</option><option>NotArchived</option></select>'
        html += '  tasks for last  '
        html += ' <input type="text" name="length" size=4 value=0> '
        html += ' <select name="span" style="width:80px"><option>hours</option><option>days</option></select> '
        html += ' <select name="type" style="width:80px"><option>list</option><option>plot</option></select> '
        html += ' <input type="submit" value="Show Summary"/> '
        html += '</select>'
        html += '</form>'
        html += "</table>\n"

        html += "<table>\n"
        html += "<i> </i><br/><br/>"
        html += '<form action=\"%s"\ method="get">' % (self.graphtaskcumulative)
        html += 'Cumulative plot of tasks status '
        html += ' for last  '
        html += ' <input type="text" name="length" size=4 value=0> '
        html += ' <select name="span" style="width:80px"><option>hours</option><option>days</option></select> '
        html += ' <input type="submit" value="Show Summary"/> '
        html += '</select>'
        html += '</form>'
        html += "</table>\n"

        html += "<table>\n"
        html += "<br/><br/>"
        html += "<i>History plot of CrabServer usage by different users for last 24 hours, 7 days or month:</i><br/>"
        html += '<form action=\"%s"\ method="get">' % (self.graphuser)
        html += 'Users submitting jobs to this CrabServer  '
        html += ' for last '
        html += ' <input type="text" name="length" size=4 value=0> '
        html += ' <select name="span" style="width:80px"><option>hours</option><option>days</option></select> '
        html += ' <select name="type" style="width:80px"><option>list</option><option>plot</option></select> '
        html += ' <input type="submit" value="Show Summary"/> '
        html += '</form>'
        html += "<br/><br/>"
        html += "</table>\n"

        html += "<table>\n"
        html += "<br/><br/>"
        html += "<b>Dataset related infos </b><br/><br/>"
        html += '<form action=\"%s"\ method="get" >' % (self.datasetrelated)
        html += ' Distinct datasets accessed'
        html += ' for last  '
        html += ' <input type="text" name="length" size=4 value=0> '
        html += ' <select name="span" style="width:80px"><option>hours</option><option>days</option></select> '
        html += ' <select name="type" style="width:80px"><option>list</option><option>plot</option></select> '
        html += ' <input type="submit" value="Show Summary"/> ' 
        html += '</select>'
        html += '</form>'

        """
        html += "<table>\n"
        html += "<b>Users related infos </b><br/><br/>"
        ## numb task
        ## numb dataset
        html += '<form action=\"%s"\ method="get" >' % (self.usersrelated)
        html += 'Cumulative plot of tasks status '
        html += ' for last  '
        html += '<input type="text" name="length" size=4 value=0>'
        html += '<select name="span" style="width:80px"><option>hours</option><option>days</option></select>'
        html += '<input type="submit" value="Show Summary"/>'
        html += '</select>'
        html += '</form>'
        html += "</table>\n"
        html += "</table>\n"
        """

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

        pngfile = os.path.join(self.workingDir, "%s-TaskCumulative.png" % (length) )
        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
       
        data = binning  

        metadata = {'title':' Cumulative of Task per Status ', 'starttime':start_time, 'endtime':end_time, 'span':_span, 'is_cumulative':False }
        cum = CumulativeGraph()
        coords = cum.run( data, pngfile, metadata )

        html = "<html><body><img src=\"%s\"></body></html>" % pngfileUrl
 
        return html
        
    index.exposed = True

class DatasetInfos:
    def __init__(self, imageUrl, imageDir, baseDDUrl):
        self.imageServer = imageUrl
        self.workingDir = imageDir
        self.baseDDUrl = baseDDUrl
        return

    def DatasetList(self, data,query_time):


          #                                           <td align="left"><a href=\"%s?dataset+%s\">%s</a></td>\
          #                                           self.baseDDUrl,'job::%s'%dataset,jobs,\
        
        html = "<html><body><h2>List of Dataset</h2>\n "
        html += '<table cellspacing="10" cellpadding=5>\n'
    
        st = ['Dataset name','Numeber of users','Number of tasks','Total Number of jobs','Efficiency']
        html += '<tr>'  
        for s in st:        
            html += '<th align="left"> %s</th>\n'%s
        html += '</tr>'  
        for dataset in data.keys():
            if dataset:
                html += '<tr>'  
                users  = API.countUsers(dataset,query_time)
                tasks  = API.countTasks(dataset,query_time)
                jobs  = API.countJobs(dataset,query_time)
                exitcodes=API.getJobExit(dataset,query_time) 
                if not len(exitcodes):
                    TotEff = 'Not jet available' 
                    eff = 'eff::%s::%s'%('None',dataset)
                else:  
                    tot = len(exitcodes) 
                    countSucc = 0
                    for appl, wrapp in exitcodes:
                        if wrapp == 0: countSucc += 1
                    TotEff = countSucc*1./tot
                    eff = 'eff::%s::%s'%(query_time,dataset)
                
                user = 'user::%s::%s'%(query_time,dataset)
                task = 'task::%s::%s'%(query_time,dataset)
                if dataset == 'None': dataset='User Private MC Production'
                html += '<td align="left">%s</td><td align="left"><a href=\"%s?user=%s\">%s</a></td>\
                                                     <td align="left"><a href=\"%s?task=%s\">%s</a></td>\
                                                     <td align="left">%s</td>\
                                                     <td align="left"><a href=\"%s?eff=%s\">%s</a></td>\n'\
                                                    %(str(dataset),self.baseDDUrl,user,users,\
                                                     self.baseDDUrl,task,tasks,\
                                                     jobs,\
                                                     self.baseDDUrl,eff,TotEff)
                html += '</tr>'  
        html += "</table>\n"
        html += """</body></html>"""
        return  html

    def DatasetGraph(self, span, length, data):

        errHtml = "<html><body><h2>No Graph Tools installed!!!</h2>\n "
        errHtml += "</body></html>"
        try:
            from graphtool.graphs.common_graphs import PieGraph
        except ImportError:
            return errHtml
        
        pngfile = os.path.join(self.workingDir, "%s-dataset.png" % length)
        pngfileUrl = "%s?filepath=%s" % (self.imageServer, pngfile)
        
        metadata = {"title" : "Datests"}
        pie = PieGraph()
        coords = pie.run( data, pngfile, metadata )

        html = "<html><body><img src=\"%s\"></body></html>" % pngfileUrl
        return html
       
    def index(self, length , span, type):

        _span=3600
        if span == 'days': _span=24*3600

        query_time = int(length)*_span
        end_time = time.time() - time.altzone
        start_time = end_time - query_time

        dataset = API.getKeyNum_task('dataset',from_time=query_time)

        if not len(dataset): 
           html = """<html><body><h2> No dataset accessed  </h2>\n """
           html += """</body></html>"""
           return html

        data={}
        for num,dat in dataset:
            data[dat]= num

        if type == 'plot':
            html = self.DatasetGraph(span, length, data)
        else:
            html = self.DatasetList(data,query_time)

        return html

    index.exposed = True

class DatasetDetails:

    def __init__(self, imageUrl, imageDir):
        self.imageServer = imageUrl
        self.workingDir = imageDir
        return

    def writeList(self,data):
 
        html = """<html><body>"""
        html += "<table>\n"
        html += "<tr>"
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


    def index(self,user=None,task=None,eff=None):

        if user : string = user 
        elif task: string=task
        else: string=eff
        action = string.split('::')[0]
        query_time = string.split('::')[1]
        dataset = string.split('::')[2]
        if action == 'task':
            tasklist=API.getTaskNameList(dataset,query_time) 
            html = """<html><body>"""
            html += "<table>\n"
            html += "<tr>"
            html += "<th>List of Task</th>"
            html += "</tr>\n"
            for t_name, nn in tasklist:
                html += "<tr>"
                html += "<td>%s</td>" % ( t_name )
                html += "</tr>\n"
            html += "</table>\n"
            html += """</body></html>"""
        elif action == 'user':
            userlist=API.getUserNameList(dataset,query_time) 
            html = """<html><body>"""
            html += "<table>\n"
            html += "<tr>"
            html += "<th>List of Users</th><th>Number of Tasks </th>"
            html += "</tr>\n"
            for num,user in userlist:
                html += "<tr>"
                html += "<td>%s</td><td>%s</td>" % ( user,num )
                html += "</tr>\n"
            html += "</table>\n"
            html += """</body></html>"""
        elif action == 'eff':
            if query_time == 'None':
                html = "<html><body><h2>No codes available</h2>\n "
                html += "</body></html>"
                return html 
            else: 
                onlyWrap = API.getWrapExit(dataset,query_time)
                onlyExec = API.getApplExit(dataset,query_time)
                data_wrap = {}
                data_exec = {} 
                for i,code in onlyWrap:
                    data_wrap[str(code)]=i 
                    #data_wrap['code']=i 
                for i,code in onlyExec:
                    data_exec[str(code)]=i 
                 
                errHtml = "<html><body><h2>No Graph Tools installed!!!</h2>\n "
                errHtml += "</body></html>"
                try:
                    from graphtool.graphs.common_graphs import PieGraph
                except ImportError:
                    return errHtml
                pngfile_wrap = os.path.join(self.workingDir, "wrappCode.png" )
                pngfileUrl_wrap = "%s?filepath=%s" % (self.imageServer, pngfile_wrap)
                pngfile_exec = os.path.join(self.workingDir, "applicationCode.png" )
                pngfileUrl_exec = "%s?filepath=%s" % (self.imageServer, pngfile_exec)
                
                metadata_wrap = {"title" : "Wrapper Error Codes"}
                metadata_exec = {"title" : "Executable Error Codes"}
          
                pie = PieGraph()
                coords_wrap = pie.run( data_wrap, pngfile_wrap, metadata_wrap )
                pie1 = PieGraph()
                coords_exec = pie1.run( data_exec, pngfile_exec, metadata_exec )
                    
                html =  '<html><body>'
                html += '<td align="left"><img src=\"%s\"></td>'% pngfileUrl_exec
                html += '<td align="left"><img src=\"%s\"></td>'% pngfileUrl_wrap
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
    
