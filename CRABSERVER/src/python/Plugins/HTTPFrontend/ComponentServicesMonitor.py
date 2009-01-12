#!/usr/bin/env python
"""

CherryPy handler for displaying the status of server components

"""

import os
import API
from ProdAgentCore.Configuration import ProdAgentConfiguration
from ProdAgentCore.DaemonDetails import DaemonDetails

AllServices = {'GridFTP':'globus-gridftp-server','mySQL':'mysqld'}
AllResources = ['CPU','LOAD','LOAD-5m','LOAD-15m','MEM','SWAP','SWAPPING']
disks = os.popen('ls -1 /dev/?d[a-z]').readlines()
disks = map(lambda x:x.split('/')[2].rstrip(),disks)
for disk in disks:
        AllResources.append(disk)

class CompServMonitor:

    def __init__(self, compStatus = None, compLog = None, compCpu = None, compMsg = None, msgBalance = None):
        self.compstatus = compStatus
        self.complog = compLog
        self.compcpu = compCpu
        self.compmsg = compMsg
        self.msgblnc = msgBalance
    
    def index(self):
 
        html = """<html><body><h2>CrabServer Components and Services Monitoring</h2>\n """
        html += "<table>\n"
        html += "<i> Diplay the status of components and active service in this CrabServer: </i><br/><br/>"
        html += '<form action=\"../%s"\ method="get" >' % (self.compstatus)
        html += ' Status of all components and external services of this CrabServer '
        html += ' <input type="submit" value="Show report"/>'
        html += '</form>'
        html += "</table>\n"
        
        html += "<table>\n"
        html += "<br/><br/><i> Allow to access components logs through web:</i><br/><br/>"
        html += ' <form action=\"../%s"\ method="get"> ' % (self.complog)
        html += 'Show logs for  '
        html += ' <select name="comp_name" style="width:140px">'
        for component in status(True):
            html += '<option>'+component+'</option>'
        html += '</select>'
        html += '<input type="submit" value="Show logs"/>'
        html += '</form>'
        html += "</table>\n"

        html += "<table>\n"
        html += "<br/><br/><i> Watch message queue by component:</i><br/><br/>"
        html += ' <form action=\"../%s"\ method="get"> ' % (self.compmsg)
        html += 'Show message queue for  '
        html += ' <select name="comp_name" style="width:140px">'
        for components in status(True):
            html += '<option>'+components+'</option>'
        html += '</select>'
        html += '<input type="submit" value="Show"/>'
        html += '</form>'
        html += "</table>\n"

        html += "<table>\n"
        html += "<br><i>or whatch message balance between all the component:</i>&nbsp"
        html += ' <form action=\"../%s"\ method="get"> ' % (self.msgblnc)
        html += '<input type="submit" value="Show"/>'
        html += '</form>'
        html += "</table>\n"

        html += "<table><br/>\n"
        html += "<small style=\"color:red\">(Work in progress)</small><br/><br/>\n"
        html += "<i> Display components CPU usage:</i><br/>"
        html += ' <form action=\"../%s"\ method="get"> ' % (self.compcpu)
        html += 'Show CPU plot for  '
        html += ' <select name="Component" style="width:140px">'
        html += '<option>All components</option>'
        for component in status(True):
            html += '<option>'+component+'</option>'
        html += '</select>'
        html += ' for last  '
        html += ' <input type="text" name="length" size=4 value=0> '
        html += ' <select name="span" style="width:80px"><option>hours</option><option>days</option></select> '
        html += ' <input type="submit" value="Show Plot"/> '
        html += '</select>'
        html += '</form>'
        html += "</table>\n"

        html += "<table><br/>\n"
        html += "<i> Display services CPU usage:</i><br/>"
        html += ' <form action=\"../%s"\ method="get"> ' % (self.compcpu)
        html += 'Show CPU plot for  '
        html += ' <select name="Component" style="width:140px">'
        html += '<option>All services</option>'
        for service in AllServices.keys():
            html += '<option>'+service+'</option>'
        html += '</select>'
        html += ' for last  '
        html += ' <input type="text" name="length" size=4 value=0> '
        html += ' <select name="span" style="width:80px"><option>hours</option><option>days</option></select> '
        html += ' <input type="submit" value="Show Plot"/> '
        html += '</select>'
        html += '</form>'
        html += "</table>\n"

        html += "<table><br/>\n"
        html += "<i> Display resources usage:</i><br/>"
        html += ' <form action=\"../%s"\ method="get"> ' % (self.compcpu)
        html += 'Show plot for '
        html += ' <select name="Component" style="width:140px">'
        html += '<option>All resources</option>'
#        ks = AllResources.keys(); ks.sort()
        for resource in AllResources:
            html += '<option>'+resource+'</option>'
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
        run , not_run = status()
        
        tableHeader = "<tr><th>%s</th><th>Process ID</th><th>Plots<span style=\"color:red\">*</span></th><th>CPU sensor status<span style=\"color:red\">*</span></th></tr>\n"
        plotLink = "<td><small><a href=\"%s/?Component=%s&length=12&span=hours\">last 12h</a></small></td>"
        nameNpid = "<tr><td>%s</td><td><b>%s</b></td>\n"
        sensorFound =  "sensor %s is attached to %s"
        sensorReady = "sensor %s is going to attach %s %s%s"
        sensorMissing = "<b>no CPU sensor found for %s %s...!</b>"
        
        html = """
        <html><head><style type=\"text/css\">
        th, td { text-indent:16px; text-align:left}
        th:first-child, td:first-child {text-indent:0px !important; }
        </style>
        </head>
        """
        html += "<body><h2>Components and Services State<br/>\n"
        html += "<small style=\"color:red; font-weight:normal;\">* Work in progress</small><br/></h2>\n"

        html += "<table>\n"
#        html += "<tr><th></th><th></th><th colspan=2><small style=\"color:red;\">work in progress</th></tr>\n"
        html += tableHeader%"Components"

        for r in run:
            comp = str(r[0])
            cpid = str(r[1])
            html += nameNpid%(comp,cpid)
            html += plotLink%(self.compcpu,comp)
            html += "<td><small>"
            sensorOn, spid, cpid = API.isSensorRunning(comp)
            if sensorOn:
                html += sensorFound%(spid,cpid)
            else:
                sensorDaemonOn, spid = API.isSensorDaemonRunning(comp)
                if sensorDaemonOn:
                    html += sensorReady%(spid,"component",comp,"... retry in a minute.")
                else:
                    html += sensorMissing%("component",comp)
            html += "</small></td></tr>\n"

        for n in not_run:
            html  += nameNpid%(str(n),"Not Running")
            html += plotLink%(self.compcpu,str(n))
            html += "<td><small>"
            sensorDaemonOn, spid = API.isSensorDaemonRunning(n)
            if sensorDaemonOn:
                html += sensorReady%(spid,"component",n,"when it will be (re)started")
            else:
                html += sensorMissing%("component",n)
            html += "</small></td></tr>\n"

        html += " <tr><th>&nbsp; </th><th>&nbsp;</th></tr>\n"
        html += tableHeader%"Services"

        for serv in AllServices.keys():
            cpid = API.getPIDof(AllServices[serv])
            spid = API.isSensorRunning(AllServices[serv])
            html += nameNpid%(serv,cpid)
            html += plotLink%(self.compcpu,serv)
            html += "<td><small>"
            sensorOn, spid, cpid = API.isSensorRunning(AllServices[serv])
            if sensorOn:
                html += sensorFound%(spid,cpid)
            else:
                sensorDaemonOn, spid = API.isSensorDaemonRunning(serv)
                if sensorDaemonOn:
                    html += sensorReady%(spid,"service",serv,"... retry in a minute.")
                else:
                    html += sensorMissing%("service",serv)
            html += "</small></td></tr>\n"

        cpid = API.getPIDof("delegation-server")
        html += nameNpid%("Delegation",cpid)
        html += "<td></td><td></td>"
        
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
            html += "<li><a href=\"../%s?to_read=%s\">%s</a></li>\n" % (
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


class MsgByComponent:

    def __init__(self):
        pass

    def index(self, comp_name):

        query, data = API.messageListing(comp_name)
        dictofdict = {}
        for id, ev, fro, to, time, delay in data:
            if dictofdict.has_key(str(to)):
                listval = [str(ev), str(fro), str(time), str(delay)]
                dictofdict[str(to)].setdefault(id, listval)
            else:
                tempdict = {id: [str(ev), str(fro), str(time), str(delay)]}
                dictofdict.setdefault(str(to), tempdict)

        if dictofdict.has_key(comp_name):
            html = "<html><body><h2> %s </h2>\n "%("Showing %s message in queue for %s"%(str(len(dictofdict[comp_name])),str(comp_name)))
            html += "<table border='2' cellspacing='2' cellpadding='5'>"
            html += "<tr><th>Message</th><th>From</th><th>Time</th><th>Delay</th></tr>"
            for key, val in dictofdict.iteritems():
                for key2, val2 in val.iteritems():
                    html += "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"%(val2[0],val2[1],val2[2],val2[3])
            html += """</body></html>"""
        else:
            html = "<html><body><h2> %s </h2>\n "%("%s has no message in queue"%(comp_name))
        return html
    index.exposed = True

class MsgBalance:

    def __init__(self, imageUrl, imageDir):
        self.imageServer = imageUrl
        self.workingDir = imageDir

    def index(self):

        query, data = API.messageListing("")
        dictofdict = {}
        for id, ev, fro, to, time, delay in data:
            if dictofdict.has_key(str(to)):
                listval = [str(ev), str(fro), str(time), str(delay)]
                dictofdict[str(to)].setdefault(id, listval)
            else:
                tempdict = {id: [str(ev), str(fro), str(time), str(delay)]}
                dictofdict.setdefault(str(to), tempdict)

        goodone = {}

        for key, val in dictofdict.iteritems():
            goodone.setdefault(key, len(val))

        html = "<html><body><h2> %s </h2>\n "%("Showing message pie chart balance")

        errHtml = "<html><body><h2>No Graph Tools installed!!!</h2>\n "
        errHtml += "</body></html>"
        try:
            from graphtool.graphs.common_graphs import PieGraph
        except ImportError:
            return errHtml

        pngfile = os.path.join(self.workingDir, "MsgBalance.png")
        pngfileUrl = "../%s?filepath=%s" % (self.imageServer, pngfile)
        metadata = {'title':'Message queue balance'}
        Graph = PieGraph()
        coords = Graph( goodone, pngfile, metadata )

        html += "<img src=\"%s\">" % pngfileUrl

        html += """</body></html>"""
        return html

    index.exposed = True

