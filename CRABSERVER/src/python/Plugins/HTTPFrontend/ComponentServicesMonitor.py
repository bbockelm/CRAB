#!/usr/bin/env python
"""

CherryPy handler for displaying the status of server components

"""

import os
import API
from ProdAgentCore.Configuration import ProdAgentConfiguration
from ProdAgentCore.DaemonDetails import DaemonDetails


class CompServMonitor:

    def __init__(self, compStatus = None, compLog = None):
        self.compstatus = compStatus
        self.complog = compLog
    
    def index(self):

 
        html = """<html><body><h2>CrabServer Components and Services Monitoring</h2>\n """
        html += "<table>\n"
        html += "<i>a time-window of 0 (zero) means all available statistics:</i><br/><br/><br/>"
        html += "<i> Diplay the status of components and active service in this CrabServer: </i><br/><br/>"
        html += '<form action=\"%s"\ method="get" >' % (self.compstatus)
        html += ' Status of all components and external services of this CrabServer '
        html += ' <input type="submit" value="Show report"/>'
        html += '</form>'
        html += "</table>\n"

        html += "<table>\n"
        html += "<br/><br/><i> Allow to access components logs through web:</i><br/><br/>"
        html += ' <form action=\"%s"\ method="get"> ' % (self.complog)
        html += 'Show logs for  '
        html += ' <select name="comp_name" style="width:140px">'
        for components in status(True):
            html += '<option>'+components+'</option>'
        html += '<input type="submit" value="Show logs"/>'
        html += '</select>'
        html += '</form>'
        html += "</table>\n"


        html += """</body></html>"""

        return html
    index.exposed = True


class ShowCompStatus:
  
    def init(self) :
        return

    def index(self):
        delegation = API.getpidof("delegation", "Delegation Service")
        gridftp = API.getpidof("gridftp-server","Globus GridFtp")

        run , not_run = status()

        html = """<html><body><h2>Components and Services State </h2>\n """

        html += "<table>\n"
        html += " <tr><th>Components </th><th> Status</th></tr>\n"
        for r in run:
            html += '<tr><td align="left">'+str(r[0])+': </td><td><b>PID : '+str(r[1])+'</b></td></tr>\n'
        for n in not_run:
            html  += '<tr><td align="left">'+str(n)+': </td><td><b>Not Running </b></td></tr>\n'
        html += "</table>\n"
        html += "<table>\n"
        html += " <tr><th>Services </th><th> Status</th></tr>\n"
        html+= "<tr><td align=\"left\">"+str(gridftp[0])+": </td><td><b>"+str(gridftp[1])+"</b></td></tr>\n"
        html += "<tr><td align=\"left\">"+str(delegation[0])+": </td><td><b>"+str(delegation[1])+"</b></td></tr>\n"
        html += "</table>\n"
        html += """</body></html>"""

        return html
    index.exposed = True

def status(compList=False):
    """
    _status_

    Print status of all components in config file

    """
    config = os.environ.get("PRODAGENT_CONFIG", None)
    cfgObject = ProdAgentConfiguration()
    cfgObject.loadFromFile(config)

    components = cfgObject.listComponents()
    if compList: return components
    else:
        component_run = []
        component_down = []
        for component in components:
            compCfg = cfgObject.getConfig(component)
            compDir = compCfg['ComponentDir']
            compDir = os.path.expandvars(compDir)
            daemonXml = os.path.join(compDir, "Daemon.xml")
            if not os.path.exists(daemonXml):
                continue
            daemon = DaemonDetails(daemonXml)
            if not daemon.isAlive():
 
                component_down.append(component)
            else:
                tmp=[component, daemon['ProcessID']]
                component_run.append(tmp)
        return component_run, component_down


class ShowCompLogs:

    def __init__(self, writeComp ):
        self.writecomp = writeComp

    def index(self, comp_name):
        html = """<html><body><h2>List of Available Components logs </h2>\n """

        config = os.environ.get("PRODAGENT_CONFIG", None)
        cfgObject = ProdAgentConfiguration()
        cfgObject.loadFromFile(config)

        compCfg = cfgObject.getConfig(comp_name)
        compDir = compCfg['ComponentDir']
        LogFiles=[] 
        list_file = os.listdir(compDir)
        for file in list_file:
            if file.find('Component')>-1: LogFiles.append(file)

        html += "<table>\n"
        html += " <tr><th> list of logs for Components %s</th>\n"% comp_name
        html += "<table>\n"
        html += "<table>\n"
        for f in LogFiles:
            to_read=os.path.join(compDir,f)
            html += "<li><a href=\"%s?to_read=%s\">%s</a></li>\n" % (
                self.writecomp, to_read,f )
        html += "</ul>\n"

        html += "<table>\n"
        html += """</body></html>"""


        return html
    index.exposed = True

class WriteLog:

    def __init__(self):
        return

    def index(self, to_read):

        html = """<html><body><h2> %s </h2>\n """%os.path.basename(to_read)

        html += "<table>\n"
        html += " <tr><th> Log content </th>\n"
        html += "<table>\n"
        html += "<table>\n"
        componentLog = open(to_read).read().replace('\n','<br>')
        html += componentLog
        html += "<table>\n"
        html += """</body></html>"""


        return html
    index.exposed = True


