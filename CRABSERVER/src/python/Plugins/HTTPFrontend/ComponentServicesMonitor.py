#!/usr/bin/env python
"""

CherryPy handler for displaying the status of server components

"""

import os
import API
from ProdAgentCore.Configuration import ProdAgentConfiguration
from ProdAgentCore.DaemonDetails import DaemonDetails


class CompServMonitor:

    def status(self):
        """
        _status_
 
        Print status of all components in config file
 
        """
        config = os.environ.get("PRODAGENT_CONFIG", None)
        cfgObject = ProdAgentConfiguration()
        cfgObject.loadFromFile(config)
 
        components = cfgObject.listComponents()
 
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


    def index(self):

        delegation = API.getpidof("delegation", "Delegation Service")
        gridftp = API.getpidof("gridftp-server","Globus GridFtp")

        run , not_run = self.status()

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


