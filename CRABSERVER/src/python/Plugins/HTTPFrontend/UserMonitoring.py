from CrabServer.XmlFramework import *
import os


class TaskLogMonitor:

    def __init__(self, showtasklog = None):
        self.visualize  = showtasklog

    def index(self):

        html = """<html><body><h2>CrabServer Tasks: internal server logging</h2>\n """

        html += "<table>\n"
        html += "<i>Filling the field with the string resulting from 'crab -printId' allow to check both staus and logging: </i><br/><br/>"
        html += '<form action=\"%s"\ method="get">' % (self.visualize)
        html += 'Task unique name '
        html += ' <input type="text" name="taskname" size=50> '
        html += ' <select name="logtype" style="width:80px"><option>Status</option><option>Logging</option></select>'
        html += '<input type="submit" value="Show"/>'
        html += '</form>'
        html += "</table>\n"

        html += """</body></html>"""

        return html

    index.exposed = True


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
                    html += '<td align="left"> <td>%s</td>\n'%str(value2)
        return  html

if __name__=="__main__":
    tlv = TaskLogVisualizer()
    tlv.index( "mcinquil_crab_0_081009_230531_262e6cb8-b0d1-4947-be06-c57153273b12" )

