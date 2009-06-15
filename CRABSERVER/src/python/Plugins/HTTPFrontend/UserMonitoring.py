import os, time
import API

_tasktype = {'All' : '', 'Archived' : True , 'NotArchived' : False }

class TaskLogMonitor:

    def __init__(self, showtasklog = None, showusertask = None):
        self.visualize = showtasklog
        self.usertasks = showusertask

    def __prepareSelect(self, tuple):
        html = ""
        for data in tuple:
            html += "<option>%s</option>"%(str(data[0]))
        return html

    def index(self, **rest):
        users = API.getAllUserName()

        html = """<html><body><h2>CrabServer Tasks: internal server logging</h2>\n """

        html += "<table>\n"
        html += "<i>Filling the field with the string resulting from 'crab -printId' allow to check both staus and logging: </i><br/><br/>"
        html += '<form action=\"../%s"\ method="get">' % (self.visualize)
        html += 'Task unique name&nbsp;'
        html += ' <input type="text" name="taskname" size=50>&nbsp; '
        html += ' <select name="logtype" style="width:80px"><option>Status</option><option>Logging</option></select>&nbsp;'
        html += '<input type="submit" value="Show"/>'
        html += '</form>'
        html += "</table>\n"

        html += "<br><br><table>\n"
        html += "<i>Select the user name to see all his tasks on the server</i><br/><br/>"
        html += '<form action=\"../%s"\ method="get" >' % (self.usertasks)
        html += 'User&nbsp;'
        html += ' <select name="username" style="width:150px"><option>All</option>%s</select>&nbsp;'%(self.__prepareSelect(users))
        html += '&nbsp;status of&nbsp;'
        html += ' <select name="tasktype" style="width:120px"><option>All</option><option>Archived</option><option>NotArchived</option></select>&nbsp;'
        html += '&nbsp;tasks for last&nbsp;'
        html += '<input type="text" name="length" size=4 value=0>&nbsp;'
        html += '<select name="span" style="width:80px"><option>hours</option><option>days</option></select>&nbsp;'
        html += '<input type="submit" value="Show Tasks"/>'
        html += '</select>'
        html += '</form>'
        html += "</table>\n"

        html += """</body></html>"""

        return html

    index.exposed = True


class ListTaskForUser:

    def __init__(self, showusertask = None):
        self.visualize = showusertask

    def index( self, username, length, span, tasktype, **rest ):
        try:
            _span=3600
            if span == 'days': _span=24*3600

            query_time = int(length)*_span
            end_time = time.time() - time.altzone
            start_time = end_time - query_time

            tasks = API.getUserTasks(username, query_time, _tasktype[tasktype])

            html = """<html><body><h2>List of %s's tasks"""% (username)
            html += "<table>\n"
            html += "<tr>"
            html += self.writeList(tasks)

            return html
        except ValueError, ve:
            return "<html><body><h2>Problem</h2>Not a valid time format provided ['%s']!</body></html>"%(str(length))

    index.exposed = True

    def writeList(self, data):

        html = "<th align='left'>Task name</th><th align='left'>Task status</th><th align='left'>Completeness </th>"
        html += "<th align='left' ='2'>Show</th>"
        html += "</tr>\n"

        for taskname, status, percent in data:
            html += "<tr>"
            html += "<td>%s</td><td align='left'>%s</td><td>%s</td>" \
                    % ( taskname, status, percent )
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
            dbox    = compCfg["CacheDir"]
        except Exception, exc:
            logging.error( str(exc) )
            raise Exception("Problem loading server configuration info " + str(exc))
        return dbox

    def index(self, taskname, logtype, **rest):
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

    def showLogging( self, filepath ):
        from IMProv.IMProvLoader import loadIMProvFile
        from IMProv.IMProvQuery import IMProvQuery

        rootag  = "InternalLogInfo"
        keystag = "Keys"
        eventag = "Event"

        gotit = loadIMProvFile( filepath )
        k4v   = IMProvQuery( keystag )
        nodiparam = k4v( gotit )

        tags = []
        for item in nodiparam:
            tags = eval( item.attrs.get("tags") )

        param = IMProvQuery( eventag)
        nodiparam = param( gotit )

        html = "<html><body><h2>Internal server logging for task: " + str(self.taskname) + "</h2>\n "
        html += '<table cellspacing="10">\n'

        textcolor = ""
        for item in nodiparam:
            if item.attrs.has_key('ev'):
                html += '<tr><td colspan="2">Event: <b>'+item.attrs.get('ev')+'</b></td></tr>\n'
            else:
                html += '<tr><td colspan="2">Event: <b>Unknown</b></td></tr>\n'
            for key in tags:
                value = item.attrs.get(key)
                if key is not 'ev' and value is not None and len(str(value)) > 0:
                    if key in ['exc', 'error']:
                        textcolor = "<font color='FF0000'>%s<font>"%(value)
                    else:
                        textcolor = value
                    html += '<tr> <td align="right">%s: </td><td>%s</td></tr>\n'%(key,textcolor)

        html += "</table>"
        html += "</body>"

        return  html
    
    def showStatus( self, filepath ):
        from IMProv.IMProvLoader import loadIMProvFile
        from IMProv.IMProvQuery import IMProvQuery

        rootag  = "TaskTracking"
        headtag = "TaskReport"
        eventag = "Job"

        gotit = loadIMProvFile( filepath )
        param = IMProvQuery( eventag)
        nodiparam = param( gotit )

        html = "<html><body><h2>Staus of task : " + str(self.taskname) + "</h2>\n "
        html += '<table cellspacing="10">\n'

        st = ['Job','Status','Action','Destination','Job_exit_code','Exe_exit_code','Ended']
        html += "<tr>"
        for s in st:
            html += '<th align="left">%s</b></th>\n'%s
        html += "</tr>"

        for item in nodiparam:
            status   = item.attrs.get("status")
            action   = item.attrs.get("action")
            site     = item.attrs.get("site")
            ended    = item.attrs.get("ended")
            jec      = item.attrs.get("job_exit")
            eec      = item.attrs.get("exe_exit")
            jobid    = item.attrs.get("id")
            if ended == 'Y':
                ended = 'Yes'
            elif ended == 'N':
                ended = 'No'
            ## temporary workaround 
            if action == 'Terminated': status = 'Done'
            if action == 'SubRequested' : status = 'Submitting'

            html += "<tr>"
            html += "<td align='left'>%s</td>"%(jobid)
            html += "<td align='left'>%s</td>"%(status)
            html += "<td align='left'>%s</td>"%(action)
            html += "<td align='left'>%s</td>"%(site)
            if jec != 'None':
                html += "<td align='left'>%s</td>"%(jec)
            else: 
                html += "<td align='left'>&nbsp</td>"
            if eec != 'None':
                html += "<td align='left'>%s</td>"%(eec)
            else:
                html += "<td align='left'>&nbsp</td>"
            html += "<td align='left'>%s</td>"%(ended)
            html += "</tr>"

        html += "</table>"
        html += "</body>"

        return  html

if __name__=="__main__":
    tlv = TaskLogVisualizer()
    tlv.index( "mcinquil_crab_0_081009_230531_262e6cb8-b0d1-4947-be06-c57153273b12" )

