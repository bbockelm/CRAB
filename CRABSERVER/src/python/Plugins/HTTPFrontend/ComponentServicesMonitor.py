#!/usr/bin/env python
"""

CherryPy handler for displaying the status of server components

"""

import os
import API
from ProdAgentCore.Configuration import ProdAgentConfiguration
from ProdAgentCore.DaemonDetails import DaemonDetails


class CompServMonitor:

    def __init__(self, compStatus = None, compLog = None, compCpu = None):
        self.compstatus = compStatus
        self.complog = compLog
        self.compcpu = compCpu
    
    def index(self):

 
        html = """<html><body><h2>CrabServer Components and Services Monitoring</h2>\n """
        html += "<table>\n"
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

        html += "<table>\n"
        html += "<br/><br/><i> Display components CPU usage:</i><br/><br/>"
        html += ' <form action=\"%s"\ method="get"> ' % (self.compcpu)
        html += 'Show CPU plot for  '
        html += ' <select name="Component" style="width:140px">'
        html += '<option>All</option>'
        for components in status(True):
            html += '<option>'+components+'</option>'
        html += '</select>'
        html += ' for last  '
        html += ' <input type="text" name="length" size=4 value=0> '
        html += ' <select name="span" style="width:80px"><option>hours</option><option>days</option></select> '
        html += ' <input type="submit" value="Show Plot"/> '
        html += '</select>'
        html += '</form>'
        html += "</table>\n"


        html += """</body></html>"""

        return html
    index.exposed = True


class ShowCompStatus:
  
    def __init__(self, compCpu = None):
        self.compcpu = compCpu
        return

    def index(self):
        delegation = API.getpidof("delegation", "Delegation Service")
        gridftp = API.getpidof("gridftp-server","Globus GridFtp")

        run , not_run = status()

        html = "<html><body><h2>Components and Services State </h2>\n"

        html += "<table>\n"
        html += " <tr><th>Components </th><th> Status</th></tr>\n"
        for r in run:
            html += "<tr><td align=\"left\">%s:</td><td><a href=\"%s/?Component=%s&length=12&span=hours\"><b>PID : %s</b></a></td></tr>\n'"%(
                str(r[0]),self.compcpu,str(r[0]),str(r[1])
                )
        for n in not_run:
            html  += "<tr><td align=\"left\">%s: </td><td><b>Not Running </b></td></tr>\n"%str(n)
        html += "</table>\n"
        html += "<table>\n"
        html += " <tr><th>Services </th><th> Status</th></tr>\n"
        html += "<tr><td align=\"left\">%s: </td><td><b>%s</b></td></tr>\n"%(str(gridftp[0]),str(gridftp[1]))
        html += "<tr><td align=\"left\">%s: </td><td><b>%s</b></td></tr>\n"%(str(delegation[0]),str(delegation[1]))
        html += "</table>\n"
        html += "</body></html>"

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
        compDir=CompDIR(comp_name)

        LogFiles=[] 
        list_file = os.listdir(compDir)
        for file in list_file:
            if file.find('Component')>-1: LogFiles.append(file)

        html += "<table>\n"
        html += " <tr><th> list of logs for Components %s</th>\n"% comp_name
        html += "<table>\n"
        html += "<table>\n"
        for f in LogFiles:
            to_read=os.path.join(comp_name,f) 
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

        compDir = CompDIR(str(to_read).split('/')[0])
        html = """<html><body><h2> %s </h2>\n """%os.path.basename(to_read)

        html += "<table>\n"
        html += " <tr><th> Log content </th>\n"
        html += "<table>\n"
        html += "<table>\n"
        componentLog = open(compDir+'/'+to_read.split('/')[1]).read().replace('\n','<br>')
        html += componentLog
        html += "<table>\n"
        html += """</body></html>"""


        return html
    index.exposed = True

def CompDIR(comp_name):

    config = os.environ.get("PRODAGENT_CONFIG", None)
    cfgObject = ProdAgentConfiguration()
    cfgObject.loadFromFile(config)
    compCfg = cfgObject.getConfig(comp_name)
    return compCfg['ComponentDir']
