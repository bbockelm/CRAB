
"""
 * ApMon - Application Monitoring Tool
 * Version: 2.0.4
 *
 * Copyright (C) 2005 California Institute of Technology
 *
 * Permission is hereby granted, free of charge, to use, copy and modify 
 * this software and its documentation (the "Software") for any
 * purpose, provided that existing copyright notices are retained in 
 * all copies and that this notice is included verbatim in any distributions
 * or substantial portions of the Software. 
 * This software is a part of the MonALISA framework (http://monalisa.cacr.caltech.edu).
 * Users of the Software are asked to feed back problems, benefits,
 * and/or suggestions about the software to the MonALISA Development Team
 * (developers@monalisa.cern.ch). Support for this software - fixing of bugs,
 * incorporation of new features - is done on a best effort basis. All bug
 * fixes and enhancements will be made available under the same terms and
 * conditions as the original software,

 * IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES ARISING OUT
 * OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY DERIVATIVES THEREOF,
 * EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 * THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE, AND NON-INFRINGEMENT. THIS SOFTWARE IS
 * PROVIDED ON AN "AS IS" BASIS, AND THE AUTHORS AND DISTRIBUTORS HAVE NO
 * OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
 * MODIFICATIONS.
"""

"""
apmon.py

This is a python implementation for the ApMon API for sending
data to the MonALISA service.

For further details about ApMon please see the C/C++ or Java documentation
You can find a sample usage of this module in apmTest.py.

Note that the parameters must be either integers(32 bits) or doubles(64 bits).
Sending strings is supported, but they will not be stored in the
farm's store nor shown in the farm's window in the MonALISA client.
"""

import re
import xdrlib
import socket
import urllib2
import threading
import time
import Logger
import ProcInfo
import random

#__all__ = ["ApMon"]

#__debug = False   # set this to True to be verbose

class ApMon:
	"""
	Main class for sending monitoring data to a MonaLisa module.
	One or more destinations can be chosen for the data. See constructor.
	
	The data is packed in UDP datagrams, using XDR. The following fields are sent:
	- version & password (string)
	- cluster name (string)
	- node name (string)
	- number of parameters (int)
	- for each parameter:
		- name (string)
		- value type (int)
		- value
	- optionally a (int) with the given timestamp

	Attributes (public):
	- destinations - a list containing (ip, port, password) tuples
	- configAddresses - list with files and urls from where the config is read
	- configRecheckInterval - period, in seconds, to check for changes 
	  in the configAddresses list
	- configRecheck - boolean - whether to recheck periodically for changes 
	  in the configAddresses list
	"""

	__defaultOptions = {
		'job_monitoring': True,       # perform (or not) job monitoring
		'job_interval'	: 10,         # at this interval (in seconds)
		'job_data_sent' : 0,          # time from Epoch when job information was sent; don't touch!

		'job_cpu_time'  : True,       # elapsed time from the start of this job in seconds
		'job_run_time'  : True,       # processor time spent running this job in seconds
		'job_cpu_usage' : True,       # current percent of the processor used for this job, as reported by ps
		'job_virtualmem': True,       # size in JB of the virtual memory occupied by the job, as reported by ps
		'job_rss'       : True,       # size in KB of the resident image size of the job, as reported by ps
		'job_mem_usage' : True,       # percent of the memory occupied by the job, as reported by ps
		'job_workdir_size': True,     # size in MB of the working directory of the job
		'job_disk_total': True,       # size in MB of the total size of the disk partition containing the working directory
		'job_disk_used' : True,       # size in MB of the used disk partition containing the working directory
		'job_disk_free' : True,       # size in MB of the free disk partition containing the working directory
		'job_disk_usage': True,       # percent of the used disk partition containing the working directory
		'job_open_files': True,       # number of open file descriptors

		'sys_monitoring': True,       # perform (or not) system monitoring
		'sys_interval'  : 10,         # at this interval (in seconds)
		'sys_data_sent' : 0,          # time from Epoch when system information was sent; don't touch!

		'sys_cpu_usr'   : False,      # cpu-usage information
		'sys_cpu_sys'   : False,      # all these will produce coresponding paramas without "sys_"
		'sys_cpu_nice'  : False,
		'sys_cpu_idle'  : False,
		'sys_cpu_usage' : True,
		'sys_load1'     : True,       # system load information
		'sys_load5'     : True,
		'sys_load15'    : True,
		'sys_mem_used'  : False,      # memory usage information
		'sys_mem_free'  : False,
		'sys_mem_usage' : True,
		'sys_pages_in'  : False,
		'sys_pages_out' : False,
		'sys_swap_used' : True,       # swap usage information
		'sys_swap_free' : False,
		'sys_swap_usage': True,
		'sys_swap_in'   : False,
		'sys_swap_out'  : False,
		'sys_net_in'    : True,       # network transfer in kBps
		'sys_net_out'   : True,       # these will produce params called ethX_in, ethX_out, ethX_errs
		'sys_net_errs'  : False,      # for each eth interface
		'sys_processes' : True,
		'sys_uptime'    : True,       # uptime of the machine, in days (float number)
		
		'general_info'  : True,       # send (or not) general host information once every 2 $sys_interval seconds
		'general_data_sent': 0,       # time from Epoch when general information was sent; don't touch!

		'hostname'      : True,
		'ip'            : True,       # will produce ethX_ip params for each interface
		'cpu_MHz'       : True,
		'no_CPUs'       : True,       # number of CPUs
		'total_mem'     : True,
		'total_swap'    : True,
		'cpu_vendor_id'	: True,
		'cpu_family'	: True,
		'cpu_model'	: True,
		'cpu_model_name': True,
		'bogomips'	: True};

	def __init__ (self, initValue):
		"""
		Class constructor:
		- if initValue is a string, put it in configAddresses and load destinations
		  from the file named like that. if it starts with "http://", the configuration 
		  is loaded from that URL. For background monitoring, given parameters will overwrite defaults

		- if initValue is a list, put its contents in configAddresses and create 
		  the list of destinations from all those sources. For background monitoring, 
		  given parameters will overwrite defaults (see __defaultOptions)
		
		- if initValue is a tuple (of strings), initialize destinations with that values.
		  Strings in this tuple have this form: "{hostname|ip}[:port][ passwd]", the
		  default port being 8884 and the default password being "". Background monitoring will be
		  enabled sending the parameters active from __defaultOptions (see end of file)

		- if initValue is a hash (key = string(hostname|ip[:port][ passwd]),
		  val = hash{'param_name': True/False, ...}) the given options for each destination 
		  will overwrite the default parameters (see __defaultOptions)
		"""
		self.destinations = {}              # empty, by default; key = tuple (host, port, pass) ; val = hash {"param_mame" : True/False, ...}
		self.configAddresses = []           # empty, by default; list of files/urls from where we read config
		self.configRecheckInterval = 120    # 2 minutes
		self.configRecheck = True           # enabled by default
		self.performBgMonitoring = True     # by default, perform background monitoring
		self.monitoredJobs = {}	            # Monitored jobs; key = pid; value = hash with
		self.maxMsgRate = 50;		    # Maximum number of messages allowed to be sent per second
		self.__defaultUserCluster = "ApMon_UserSend";
		self.__defaultUserNode = socket.getfqdn();
		self.__defaultSysMonCluster = "ApMon_SysMon";
		self.__defaultSysMonNode = socket.getfqdn();
		# don't touch these:
		self.__initializedOK = True
		self.__udpSocket = None
		self.__configUpdateLock = threading.Lock()
		self.__configUpdateEvent = threading.Event()
		self.__bgMonitorLock = threading.Lock()
		self.__bgMonitorEvent = threading.Event()
		# don't allow a user to send more than MAX_MSG messages per second, in average
		self.__crtTime = 0;
		self.__prvTime = 0;
		self.__prvSent = 0;
		self.__prvDrop = 0;
		self.__crtSent = 0;
		self.__crtDrop = 0;
		self.__hWeight = 0.92;
	
		self.logger = Logger.Logger(self.__defaultLogLevel)
		if type(initValue) == type("string"):
			self.configAddresses.append(initValue)
			self.__reloadAddresses()
		elif type(initValue) == type([]):
			self.configAddresses = initValue
			self.__reloadAddresses()
		elif type(initValue) == type(()):
			for dest in initValue:
				self.__addDestination (dest, self.destinations)
		elif type(initValue) == type({}):
			for dest, opts in initValue.items():
				self.__addDestination (dest, self.destinations, opts)		
		
		self.__initializedOK = (len (self.destinations) > 0)
		if not self.__initializedOK:
			self.logger.log(Logger.ERROR, "Failed to initialize. No destination defined.");
		#self.__defaultClusterName = None
		#self.__defaultNodeName = self.getMyHo
		if self.__initializedOK:
			self.__udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			if len(self.configAddresses) > 0:
				# if there are addresses that need to be monitored,
				# start config checking and reloading thread
				th = threading.Thread(target=self.__configLoader)
				th.setDaemon(True)  # this is a daemon thread
				th.start()
			# create the ProcInfo instance
			self.procInfo = ProcInfo.ProcInfo(self.logger);
			self.procInfo.update();
			# start the background monitoring thread
			th = threading.Thread(target=self.__bgMonitor);
			th.setDaemon(True);
			th.start();


	def sendParams (self, params):
		"""
		Send multiple parameters to MonALISA, with default (last given) cluser and node names.
		"""
		self.sendTimedParams (-1, params)
	
	def sendTimedParams (self, timeStamp, params):
		"""
		Send multiple parameters, specifying the time for them, with default (last given) cluster and node names. 
		(See sendTimedParameters for more details)
		"""
		self.sendTimedParameters (None, None, timeStamp, params);
	
	def sendParameter (self, clusterName, nodeName, paramName, paramValue):
		"""
		Send a single parameter to MonALISA.
		"""
		self.sendTimedParameter(clusterName, nodeName, -1, paramName, paramValue);
	
	def sendTimedParameter (self, clusterName, nodeName, timeStamp, paramName, paramValue):
		"""
		Send a single parameter, with a given time.
		"""
		self.sendTimedParameters (clusterName, nodeName, timeStamp, {paramName:paramValue})
	
	def sendParameters (self, clusterName, nodeName, params):
		"""
		Send multiple parameters specifying cluster and node name for them
		"""
		self.sendTimedParameters (clusterName, nodeName, -1, params);
	
	def sendTimedParameters (self, clusterName, nodeName, timeStamp, params):
		"""
		Send multiple monitored parameters to MonALISA. 
		
		- clusterName is the name of the cluster being monitored. The first
		  time this function is called, this paramenter must not be None. Then,
		  it can be None; last given clusterName will be used instead.
		- nodeName is the name of the node for which are the parameters. If this
		  is None, the full hostname of this machine will be sent instead.
		- timeStamp, if > 0, is given time for the parameters. This is in seconds from Epoch.
		  Note that this option should be used only if you are sure about the time for the result.
		  Otherwize, the parameters will be assigned a correct time (obtained from NTP servers)
		  in MonALISA service. This option can be usefull when parsing logs, for example.
		- params is a dictionary containing pairs with:
			- key: parameter name
			- value: parameter value, either int or float.
		  or params is a vector of tuples (key, value). This version can be used
		  in case you want to send the parameters in a given order.
		
		NOTE that python doesn't know about 32-bit floats (only 64-bit floats!)
		"""
		if not self.__initializedOK:
			self.logger.log(Logger.WARNING, "Not initialized correctly. Message NOT sent!");
			return
		if clusterName == None:
			clusterName = self.__defaultUserCluster
		else:
			self.__defaultUserCluster = clusterName
		if nodeName == None:
			nodeName = self.__defaultUserNode
		else:
			self.__defaultUserNode = nodeName
		self.__configUpdateLock.acquire()
		for dest in self.destinations.keys():
			self.__directSendParams(dest, clusterName, nodeName, timeStamp, params);
		self.__configUpdateLock.release()
	
	def addJobToMonitor (self, pid, workDir, clusterName, nodeName):
		"""
		Add a new job to monitor.
		"""
		self.__bgMonitorLock.acquire();
		self.monitoredJobs[pid] = {};
		self.monitoredJobs[pid]['CLUSTER_NAME'] = clusterName;
		self.monitoredJobs[pid]['NODE_NAME'] = nodeName;
		self.procInfo.addJobToMonitor(pid, workDir);
		self.__bgMonitorLock.release();
		
	def removeJobToMonitor (self, pid):
		"""
		Remove a job from being monitored.
		"""
		self.__bgMonitorLock.acquire();
		self.procInfo.removeJobToMonitor(pid);
		del self.monitoredJobs[pid];
		self.__bgMonitorLock.release();
	
	def setMonitorClusterNode (self, clusterName, nodeName):
		"""
		Set the cluster and node names where to send system related information.
		"""
		self.__bgMonitorLock.acquire();
		self.__defaultSysMonCluster = clusterName;
		self.__defaultSysMonNode = nodeName;
		self.__bgMonitorLock.release();
	
	def enableBgMonitoring (self, onOff):
		"""
		Enable or disable background monitoring. Note that background monitoring information
		can still be sent if user calls the sendBgMonitoring method.
		"""
		self.performBgMonitoring = onOff;
	
	def sendBgMonitoring (self):
		"""
		Send now background monitoring about system and jobs to all interested destinations.
		"""
		self.__bgMonitorLock.acquire();
		self.procInfo.update();
		now = int(time.time());
		for destination, options in self.destinations.items():
			sysParams = [];
			jobParams = [];
			# for each destination and its options, check if we have to report any background monitoring data
			if(options['sys_monitoring'] and options['sys_data_sent'] + options['sys_interval'] < now):
				for param, active in options.items():
					m = re.match("sys_(.+)", param);
					if(m != None and active):
						param = m.group(1);
						if not (param == 'monitoring' or param == 'interval' or param == 'data_sent'):
							sysParams.append(param)
				options['sys_data_sent'] = now;
			if(options['job_monitoring'] and options['job_data_sent'] + options['job_interval'] < now):
				for param, active in options.items():
					m = re.match("job_(.+)", param);
					if(m != None and active):
						param = m.group(1);
						if not (param == 'monitoring' or param == 'interval' or param == 'data_sent'):
							jobParams.append(param);
				options['job_data_sent'] = now;
			if(options['general_info'] and options['general_data_sent'] + 2 * int(options['sys_interval']) < now):
				for param, active in options.items():
					if not (param.startswith("sys_") or param.startswith("job_")) and active:
						if not (param == 'general_info' or param == 'general_data_sent'):
							sysParams.append(param);
			sysResults = {}
			if(len(sysParams) > 0):
				sysResults = self.procInfo.getSystemData(sysParams);
			if(len(sysResults.keys()) > 0):
				self.__directSendParams(destination, self.__defaultSysMonCluster, self.__defaultSysMonNode, -1, sysResults);
			for pid, props in self.monitoredJobs.items():
				jobResults = {};
				if(len(jobParams) > 0):
					jobResults = self.procInfo.getJobData(pid, jobParams);
				if(len(jobResults) > 0):
					self.__directSendParams(destination, props['CLUSTER_NAME'], props['NODE_NAME'], -1, jobResults);
		self.__bgMonitorLock.release();
	
	def setLogLevel (self, strLevel):
		"""
		Change the log level. Given level is a string, one of 'FATAL', 'ERROR', 'WARNING', 
		'INFO', 'NOTICE', 'DEBUG'.
		"""
		self.logger.setLogLevel(strLevel);
	
	def setMaxMsgRate(self, rate):
		"""
		Set the maximum number of messages that can be sent, per second.
		"""
		self.maxMsgRate = rate;
		self.logger.log(Logger.DEBUG, "Setting maxMsgRate to: " + str(rate));
	
	def free(self):
		"""
		Stop background threands, close opened sockets. You have to use this function if you want to
		free all the resources that ApMon takes, and allow it to be garbage-collected.
		"""
		self.__configUpdateEvent.set();
		self.__bgMonitorEvent.set();
		time.sleep(0.01);
		if self.__udpSocket != None:
			self.logger.log(Logger.DEBUG, "Closing UDP socket on ApMon object destroy.");
			self.__udpSocket.close();


	#########################################################################################
	# Internal functions - Config reloader thread
	#########################################################################################
	
	def __configLoader(self):
		"""
		Main loop of the thread that checks for changes and reloads the configuration
		"""
		while not self.__configUpdateEvent.isSet():
			self.__configUpdateEvent.wait(self.configRecheckInterval);
			if self.__configUpdateEvent.isSet():
				return;
			if self.configRecheck:
				self.__reloadAddresses()
				self.logger.log(Logger.DEBUG, "Config reloaded. Seleeping for "+`self.configRecheckInterval`+" sec.");

	def __reloadAddresses(self):
		"""
		Refresh destinations hash, by loading data from all sources in configAddresses
		"""
		newDestinations = {}
		for src in self.configAddresses:
			self.__initializeFromFile(src, newDestinations)
		# avoid changing config in the middle of sending packets to previous destinations
		self.__configUpdateLock.acquire()
		self.destinations = newDestinations
		self.__configUpdateLock.release()

	def __addDestination (self, aDestination, tempDestinations, options = __defaultOptions):
		"""
		Add a destination to the list.
		
		aDestination is a string of the form "{hostname|ip}[:port] [passwd]" without quotes.
		If the port is not given, it will be used the default port (8884)
		If the password is missing, it will be considered an empty string
		"""
		aDestination = aDestination.strip().replace('\t', ' ')
		while aDestination != aDestination.replace('  ', ' '):
			aDestination = aDestination.replace('  ', ' ')
		sepPort = aDestination.find (':')
		sepPasswd = aDestination.rfind (' ')
		if sepPort >= 0:
			host = aDestination[0:sepPort].strip()
			if sepPasswd > sepPort + 1:
				port = aDestination[sepPort+1:sepPasswd].strip()
				passwd = aDestination[sepPasswd:].strip()
			else:
				port = aDestination[sepPort+1:].strip()
				passwd = ""
		else:
			port = str(self.__defaultPort)
			if sepPasswd >= 0:
				host = aDestination[0:sepPasswd].strip()
				passwd = aDestination[sepPasswd:].strip()
			else:
				host = aDestination.strip()
				passwd = ""
		if (not port.isdigit()):
			self.logger.log(Logger.WARNING, "Bad value for port number "+`port`+" in "+aDestination+" destination");
			return
		alreadyAdded = False
		port = int(port)
		host = socket.gethostbyname(host) # convert hostnames to IP addresses to avoid suffocating DNSs
		for h, p, w in tempDestinations.keys():
			if (h == host) and (p == port):
				alreadyAdded = True
				break
		if not alreadyAdded:
			self.logger.log(Logger.INFO, "Adding destination "+host+':'+`port`+' '+passwd);
			tempDestinations[(host, port, passwd)] = self.__defaultOptions;
			if options != self.__defaultOptions:
				# we have to overwrite defaults with given options
				for key, value in options.items():
					self.logger.log(Logger.NOTICE, "Overwritting option: "+key+" = "+`value`);
					tempDestinations[(host, port, passwd)][key] = value;
		else:
			self.logger.log(Logger.NOTICE, "Destination "+host+":"+str(port)+" "+passwd+" already added. Skipping it");

	def __initializeFromFile (self, confFileName, tempDestinations):
		"""
		Load destinations from confFileName file. If it's an URL (starts with "http://")
		load configuration from there. Put all destinations in tempDestinations hash.
		
		Calls addDestination for each line that doesn't start with # and 
		has non-whitespace characters on it
		"""
		try:
			if confFileName.find ("http://") == 0:
				confFile = urllib2.urlopen (confFileName)
			else:
				confFile = open (confFileName)
		except urllib2.HTTPError, e:
			self.logger.log(Logger.ERROR, "Cannot open "+confFileName);
			if e.code == 401:
				self.logger.log(Logger.ERROR, 'HTTPError: not authorized.');
			elif e.code == 404:
				self.logger.log(Logger.ERROR, 'HTTPError: not found.');
			elif e.code == 503:
				self.logger.log(Logger.ERROR, 'HTTPError: service unavailable.');
			else:
				self.logger.log(Logger.ERROR, 'HTTPError: unknown error.');
			return
		except urllib2.URLError, ex:
			self.logger.log(Logger.ERROR, "Cannot open "+confFileName);
			self.logger.log(Logger.ERROR, "URL Error: "+str(ex.reason[1]));
			return
		except IOError, ex:
			self.logger.log(Logger.ERROR, "Cannot open "+confFileName);
			self.logger.log(Logger.ERROR, "IOError: "+str(ex));
			return
		self.logger.log(Logger.INFO, "Adding destinations from "+confFileName);
		dests = []
		opts = {}
		while(True):
			line = confFile.readline();
			if line == '':
				break;
			line = line.strip()
			self.logger.log(Logger.DEBUG, "Reading line "+line);
			if (len(line) == 0) or (line[0] == '#'):
				continue
			elif line.startswith("xApMon_"):
				m = re.match("xApMon_(.*)", line);
				if m != None:
					m = re.match("(\S+)\s*=\s*(\S+)", m.group(1));
					if m != None:
						param = m.group(1); value = m.group(2);
						if(value.upper() == "ON"):
							value = True;
						elif(value.upper() == "OFF"):
							value = False;
						elif(param.endswith("_interval")):
							value = int(value);
						if param == "loglevel":
							self.logger.setLogLevel(value);
						elif param == "maxMsgRate":
							self.setMaxMsgRate(int(value));
						elif param == "conf_recheck":
							self.configRecheck = value;
						elif param == "recheck_interval":
							self.configRecheckInterval = value;
						elif param.endswith("_data_sent"):
							pass;	 # don't reset time in sys/job/general/_data_sent
						else:
							opts[param] = value;
			else:
				dests.append(line);
		confFile.close ()
		for line in dests:
			self.__addDestination(line, tempDestinations, opts)
	
	###############################################################################################
	# Internal functions - Background monitor thread
	###############################################################################################

	def __bgMonitor (self):
		while not self.__bgMonitorEvent.isSet():
			self.__bgMonitorEvent.wait(10);
			if self.__bgMonitorEvent.isSet():
				return;
			if self.performBgMonitoring:
				self.sendBgMonitoring();

	###############################################################################################
	# Internal helper functions
	###############################################################################################
	
	def __directSendParams (self, destination, clusterName, nodeName, timeStamp, params):
		
		if self.__shouldSend() == False:
			return;
		
		xdrPacker = xdrlib.Packer ()
		host, port, passwd = destination
		self.logger.log(Logger.DEBUG, "Building XDR packet for ["+str(clusterName)+"] <"+str(nodeName)+"> len:"+str(len(params)));
		xdrPacker.pack_string ("v:"+self.__version+"p:"+passwd)
		xdrPacker.pack_string (clusterName)
		xdrPacker.pack_string (nodeName)
		xdrPacker.pack_int (len(params))
		if type(params) == type( {} ):
			for name, value in params.iteritems():
				self.__packParameter(xdrPacker, name, value)
		elif type(params) == type( [] ):
			for name, value in params:
				self.logger.log(Logger.DEBUG, "Adding parameter "+name+" = "+str(value));
				self.__packParameter(xdrPacker, name, value)
		else:
			self.logger.log(Logger.WARNING, "Unsupported params type in sendParameters: " + str(type(params)));
		if(timeStamp > 0):
			xdrPacker.pack_int(timeStamp);
		buffer = xdrPacker.get_buffer();
		# send this buffer to the destination, using udp datagrams
		try:
			self.__udpSocket.sendto(buffer, (host, port))
			self.logger.log(Logger.DEBUG, "Packet sent");
		except socket.error, msg:
			self.logger.log(Logger.ERROR, "Cannot send packet to "+host+":"+str(port)+" "+passwd+": "+str(msg[1]));
		xdrPacker.reset()
	
	def __packParameter(self, xdrPacker, name, value):
		try: 
			typeValue = self.__valueTypes[type(value)]
			xdrPacker.pack_string (name)
			xdrPacker.pack_int (typeValue)
			self.__packFunctions[typeValue] (xdrPacker, value)
			self.logger.log(Logger.DEBUG, "Adding parameter "+str(name)+" = "+str(value));
		except Exception, ex:
			print "ApMon: error packing %s = %s; got %s" % (name, str(value), ex)
	
	# Destructor
	def __del__(self):
		self.free();

	# Decide if the current datagram should be sent.
	# This decision is based on the number of messages previously sent.
	def __shouldSend(self):
		now = long(time.time());
		if now != self.__crtTime :
			# new time
			# update previous counters;
			self.__prvSent = self.__hWeight * self.__prvSent + (1.0 - self.__hWeight) * self.__crtSent / (now - self.__crtTime); 
			self.__prvTime = self.__crtTime;
			self.logger.log(Logger.DEBUG, "previously sent: " + str(self.__crtSent) + "; dropped: " + str(self.__crtDrop));
			# reset current counter
			self.__crtTime = now;
			self.__crtSent = 0;
			self.__crtDrop = 0;
		
		# compute the history
		valSent = self.__prvSent * self.__hWeight + self.__crtSent * (1 - self.__hWeight);
		
		doSend = True;
			
		# when we should start dropping messages
		level = self.maxMsgRate - self.maxMsgRate / 10; 
	
		if valSent > (self.maxMsgRate - level) :
			if random.randint(0,self.maxMsgRate / 10) >= (self.maxMsgRate - valSent):
				doSend = False;
	
		# counting sent and dropped messages
		if doSend:
			self.__crtSent+=1;
		else:
			self.__crtDrop+=1;

		return doSend;

	
	################################################################################################
	# Private variables. Don't touch
	################################################################################################

	__valueTypes = {
		type("string"): 0,	# XDR_STRING (see ApMon.h from C/C++ ApMon version)
		type(1): 2, 		# XDR_INT32
		type(1.0): 5}; 		# XDR_REAL64
	
	__packFunctions = {
		0: xdrlib.Packer.pack_string,
		2: xdrlib.Packer.pack_int,
		5: xdrlib.Packer.pack_double }
	
	__defaultPort = 8884
	__defaultLogLevel = Logger.INFO
	__version = "2.0.6-py"			# apMon version number

