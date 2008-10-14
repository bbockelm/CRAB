from CrabServer.XmlFramework import *
import os, time
import API

_tasktype = {'All' : '', 'Archived' : True , 'NotArchived' : False }

class TaskLogMonitor:

    def __init__(self, showtasklog = None, showlisttask = None, showusertask = None):
        self.showtasks = showlisttask
        self.visualize = showtasklog
        self.usertasks = showusertask

    def __prepareSelect(self, tuple):
        html = ""
        for data in tuple:
            html += "<option>%s</option>"%(str(data[0]))
        return html

    def index(self):
        users = API.getAllUserName()

        html = """<html><body><h2>CrabServer Tasks: internal server logging</h2>\n """

        html += "<table>\n"
        html += "<i>Filling the field with the string resulting from 'crab -printId' allow to check both staus and logging: </i><br/><br/>"
        html += '<form action=\"%s"\ method="get">' % (self.visualize)
        html += 'Task unique name&nbsp;'
        html += ' <input type="text" name="taskname" size=50>&nbsp; '
        html += ' <select name="logtype" style="width:80px"><option>Status</option><option>Logging</option></select>&nbsp;'
        html += '<input type="submit" value="Show"/>'
        html += '</form>'
        html += "</table>\n"

        html += "<br><br><table>\n"
        html += "<i>Select the user name to see all his tasks on the server</i><br/><br/>"
        html += '<form action=\"%s"\ method="get" >' % (self.usertasks)
        html += 'User&nbsp;'
        html += ' <select name="username" style="width:150px">%s</select>&nbsp;'%(self.__prepareSelect(users))
        html += '<input type="submit" value="Show Tasks"/>'
        html += '</select>'
        html += '</form>'
        html += "</table>\n"

        html += "<br><br><table>\n"
        html += '<form action=\"%s"\ method="get">' % (self.showtasks)
        html += 'Status of&nbsp;'
        html += ' <select name="tasktype" style="width:80px"><option>All</option><option>Archived</option><option>NotArchived</option></select>&nbsp;'
        html += '&nbsp;tasks for last&nbsp;'
        html += '<input type="text" name="length" size=4 value=0>&nbsp;'
        html += '<select name="span" style="width:80px"><option>hours</option><option>days</option></select>&nbsp;'
        html += '<input type="submit" value="Show List"/>'
        html += '</select>'
        html += '</form>'
        html += "</table>\n"

        html += """</body></html>"""

        return html

    index.exposed = True


class ListTaskForLog:

    def __init__(self, showlogtask = None):
        self.visualize = showlogtask

    def index( self, length, span, tasktype ):
        _span=3600
        if span == 'days': _span=24*3600

        query_time = int(length)*_span
        end_time = time.time() - time.altzone
        start_time = end_time - query_time

        tasks = API.getTasks( query_time, _tasktype[tasktype] )

        html = """<html><body><h2>List of %s tasks"""% (tasktype)
        html += "Last %s %s</h2>\n " % ( length, span )
        html += "<table>\n"
        html += "<tr>"
        html += self.writeList(tasks)

        return html

    index.exposed = True

    def writeList(self, data):
        html = "<th align='left'>Task name</th><th align='left'>Task status</th>"
        html += "<th align='left' ='2'>Show</th>"
        html += "</tr>\n"

        for taskname, status in data:
            html += "<tr>"
            html += "<td>%s</td><td align='left'>%s</td>" \
                    % ( taskname, status )
            baselink = self.visualize + "/?taskname=" + taskname + "&logtype="
            html += "<td><a href='%s'>Logging</a></td><td><a href='%s'>Status</a></td>" \
                    % ((baselink + "Logging"), (baselink + "Status"))
            html += "</tr>\n"

        html += "</table>\n"

        html += "<h4>Total number of tasks: %s </h4>\n" % len(data)
        html += """</body></html>"""
        return html

class ListTaskForUser:

    def __init__(self, showusertask = None):
        self.visualize = showusertask

    def index( self, username ):
        tasks = API.getUserTasks(username)

        html = """<html><body><h2>List of %s's tasks"""% (username)
        html += "<table>\n"
        html += "<tr>"
        html += self.writeList(tasks)

        return html

    index.exposed = True

    def writeList(self, data):

        html = "<th align='left'>Task name</th><th align='left'>Task status</th>"
        html += "<th align='left' ='2'>Show</th>"
        html += "</tr>\n"

        for taskname, status in data:
            html += "<tr>"
            html += "<td>%s</td><td align='left'>%s</td>" \
                    % ( taskname, status )
            baselink = self.visualize + "/?taskname=" + taskname + "&logtype="
            html += "<td><a href='%s'>Logging</a></td><td><a href='%s'>Status</a></td>" \
                    % ((baselink + "Logging"), (baselink + "Status"))
            html += "</tr>\n"

        html += "</table>\n"

        html += "<h4>Total number of tasks: %s </h4>\n" % len(data)
        html += """</body></html>"""
        return html



class TaskLogVisualizer:

    def __init__(self):
        return

    def getDbox(self):

        from ProdAgentCore.Configuration import loadProdAgentConfiguration
        dbox = None
        try:
            config  = loadProdAgentConfiguration()
            compCfg = config.getConfig("CrabServerConfigurations")
            dbox    = compCfg["dropBoxPath"]
        except Exception, exc:
            logging.error( str(exc) )
            raise Exception("Problem loading server configuration info " + str(exc))
        return dbox

    def index(self, taskname, logtype):
        self.taskname = taskname 
        dbox = self.getDbox()

        if taskname == 'ListAllTaskDir':
            return  self.listAllDir()

        if logtype == 'Status': filename = "xmlReportFile.xml"         
        elif logtype == 'Logging': filename =  "internalog.xml" 
       
        filepath = os.path.join(dbox, (self.taskname + "_spec"),filename)
        
        
        errhtml = "<html><body><h2>Internal server logging for task: " + str(self.taskname) + "</h2>\n "
        if not os.path.exists( filepath ):
            errhtml += "<tr> %s info for task :   ' %s '</tr></br>\n"%(logtype,self.taskname)
            errhtml += "<tr>  not existing in this CrabServer. Please check the Task ID. </tr>\n"
            return errhtml
           # raise Exception("Logging info file %s not existing"% filepath)
  
        html = self.showHtml( logtype ,filepath ) 
            
        html += "</table>\n"
        html += """</body></html>"""
        return html

    index.exposed = True

    def listAllDir(self):
        dbox = self.getDbox()
        html = "<html><body><h2>List Of All Task Dir</h2>\n "
        for f in os.listdir( dbox ):
            html += "<tr> Task Dir Name :   ' %s '</tr></br>\n"%(f)
        return html

    def showHtml(self, logtype, filepath):
         
        if logtype == 'Status': html = self.showStatus(filepath)
        elif logtype == 'Logging': html = self.showLogging(filepath)

        return html

    def showLogging( self, filepath):

        c = XmlFramework()
        c.fromFile( filepath )

        html = "<html><body><h2>Internal server logging for task: " + str(self.taskname) + "</h2>\n "
        html += '<table cellspacing="10">\n'

        for key, value in c.getEventValues().iteritems():
            html += '<tr><td colspan="2">Event: <b>'+str(value['ev'])+'</b></td></tr>\n'
            for key2, value2 in value.iteritems():
                if len(value2) > 0 and key2 != "ev": 
                    html += '<tr> <td align="right">'+str(key2)+': </td><td>'+str(value2)+'</td></tr>\n'
        return  html
    
    def showStatus( self, filepath ):

        c = XmlFramework("TaskTracking")
        c.fromFile( filepath )

        html = "<html><body><h2>Staus of task : " + str(self.taskname) + "</h2>\n "
        html += '<table cellspacing="10">\n'

        #st = ['Job','Status','Destination','Job_exit_code','Exe_exit_code','Submission Number']
        st = ['Status','Destination','Job_exit_code','Exe_exit_code']
        for s in st:        
            html += '<td align="left"> <td><b>%s</b></td>\n'%s
        for i in c.getJobValues().values():
            html += '<tr></tr>\n'
            for key2, value2 in i.items():
                if len(value2) > 0 and key2 not in  ['id','resubmit',"sched_id","cleared"]:
                    if value2 != 'None':
                        html += '<td align="left"> <td>%s</td>\n'%str(value2)
                    else:
                        html += '<td align="left"> <td>&nbsp</td>\n'
        return  html

if __name__=="__main__":
    tlv = TaskLogVisualizer()
    tlv.index( "mcinquil_crab_0_081009_230531_262e6cb8-b0d1-4947-be06-c57153273b12" )

