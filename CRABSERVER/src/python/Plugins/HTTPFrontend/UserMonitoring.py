from CrabServer.XmlFramework import *
import os


class TaskLogMonitor:

    def __init__(self, showtasklog = None):
        self.visualize  = showtasklog

    def index(self):

        html = """<html><body><h2>CrabServer Tasks: internal server logging</h2>\n """

        html += "<table>\n"
        html += "<i> </i><br/><br/>"
        html += '<form action=\"%s"\ method="get" target="_blank">' % (self.visualize)
        html += 'Task unique name '
        html += '<input type="text" name="taskname" size=50>'
        html += '<input type="submit" value="Show logging info"/>'
        html += '</select>'
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
            raise Exception("Proca Puttana " + str(exc))
        return dbox

    def index(self, taskname):
        dbox = self.getDbox()
        filepath = os.path.join(dbox, (taskname + "_spec"), "internalog.xml") 
        c = XmlFramework()

        if not os.path.exists( filepath ):
            raise Exception("Proca Puttana " + str(exc))

        c.fromFile( filepath )

        html = "<html><body><h2>Internal server logging for task: " + str(taskname) + "</h2>\n "

        html += '<table cellspacing="10">\n'

        for key, value in c.getEventValues().iteritems():
            html += '<tr><td colspan="2">Event: <b>'+str(value['ev'])+'</b></td></tr>\n'
            for key2, value2 in value.iteritems():
                if len(value2) > 0 and key2 != "ev": 
                    html += '<tr> <td align="right">'+str(key2)+': </td><td>'+str(value2)+'</td></tr>\n'
            
        html += "</table>\n"
        html += """</body></html>"""
        return html

    index.exposed = True


if __name__=="__main__":
    tlv = TaskLogVisualizer()
    tlv.index( "mcinquil_crab_0_081009_230531_262e6cb8-b0d1-4947-be06-c57153273b12" )

