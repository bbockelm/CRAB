
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


import os
import re
import time
import socket
import Logger

"""
Class ProcInfo
extracts information from the proc/ filesystem for system and job monitoring
"""
class ProcInfo:
	# ProcInfo constructor
	def __init__ (this, logger):
		this.DATA = {};             # monitored data that is going to be reported
		this.OLD_RAW = {};          # helper hashes from which is derived the
		this.NEW_RAW = {};          # information in DATA for some of the measurements.
		this.LAST_UPDATE_TIME = 0;  # when the last measurement was done
		this.JOBS = {};             # jobs that will be monitored
		this.logger = logger	    # use the given logger
	
	# This should be called from time to time to update the monitored data,
	# but not more often than once a second because of the resolution of time()
	def update (this):
		if this.LAST_UPDATE_TIME == int(time.time()):
			this.logger.log(Logger.NOTICE, "ProcInfo: update() called too often!");
			return;
		this.readStat();
		this.readMemInfo();
		this.readLoadAvg();
		this.countProcesses();
		this.readGenericInfo();
		this.readNetworkInfo();
		for pid in this.JOBS.keys():
			this.readJobInfo(pid);
			this.readJobDiskUsage(pid);
		this.LAST_UPDATE_TIME = int(time.time());

	# Call this to add another PID to be monitored
	def addJobToMonitor (this, pid, workDir):
		this.JOBS[pid] = {};
		this.JOBS[pid]['WORKDIR'] = workDir;
		this.JOBS[pid]['DATA'] = {};
		print this.JOBS;
	
	# Call this to stop monitoring a PID
	def removeJobToMonitor (this, pid):
		if this.JOBS.has_key(pid):
			del this.JOBS[pid];

	# Return a filtered hash containting the system-related parameters and values
	def getSystemData (this, params):
		return this.getFilteredData(this.DATA, params);
	
	# Return a filtered hash containing the job-related parameters and values
	def getJobData (this, pid, params):
		if not this.JOBS.has_key(pid):
			return [];
		return this.getFilteredData(this.JOBS[pid]['DATA'], params);

	############################################################################################
	# internal functions for system monitoring
	############################################################################################
	
	# this has to be run twice (with the $lastUpdateTime updated) to get some useful results
	# the information about pages_in/out and swap_in/out isn't available for 2.6 kernels (yet)
	def readStat (this):
		this.OLD_RAW = this.NEW_RAW.copy();
		try:
			FSTAT = open('/proc/stat');
			line = FSTAT.readline();
        	        while(line != ''):
				if(line.startswith("cpu ")):
					elem = re.split("\s+", line);
					this.NEW_RAW['cpu_usr'] = float(elem[1]);
					this.NEW_RAW['cpu_nice'] = float(elem[2]);
					this.NEW_RAW['cpu_sys'] = float(elem[3]);
					this.NEW_RAW['cpu_idle'] = float(elem[4]);
				if(line.startswith("page")):
					elem = line.split();
					this.NEW_RAW['pages_in'] = float(elem[1]);
					this.NEW_RAW['pages_out'] = float(elem[2]);
				if(line.startswith('swap')):
					elem = line.split();
					this.NEW_RAW['swap_in'] = float(elem[1]);
					this.NEW_RAW['swap_out'] = float(elem[2]);
				line = FSTAT.readline();
			FSTAT.close();
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/stat");
			return;
		if(len(this.OLD_RAW.keys()) == 0):
			return;
		diff = {};
		cpu_sum = 0;
		for key in (this.NEW_RAW.keys()):
			#this.logger.log(Logger.DEBUG, "key = " + key);
			if key.startswith('cpu_') or key.startswith('pages_') or key.startswith('swap_'):
				#this.logger.log(Logger.DEBUG, "old = "+`this.OLD_RAW[key]`+" new = "+`this.NEW_RAW[key]`);
				diff[key] = this.NEW_RAW[key] - this.OLD_RAW[key];
			if key.startswith('cpu_'):
				#this.logger.log(Logger.DEBUG, "diff=" + `diff[key]`);
				cpu_sum += diff[key];
		for key in ('cpu_usr', 'cpu_nice', 'cpu_sys', 'cpu_idle'):
			this.DATA[key] = 100.0 * diff[key] / cpu_sum;
		this.DATA['cpu_usage'] = 100.0 * (diff['cpu_usr'] + diff['cpu_sys'] + diff['cpu_nice']) / cpu_sum;
		
		if this.NEW_RAW.has_key('pages_in'):
			now = int(time.time());
			for key in ('pages_in', 'pages_out', 'swap_in', 'swap_out'):
				this.DATA[key] = diff[key] / (now - this.LAST_UPDATE_TIME);
	
	# sizes are reported in MB (except _usage that is in percent).
	def readMemInfo (this):
		try:
			FMEM = open('/proc/meminfo');
			line = FMEM.readline();
        	        while(line != ''):
				elem = re.split("\s+", line);
				if(line.startswith("MemFree:")):
					this.DATA['mem_free'] = float(elem[1]) / 1024.0;
				if(line.startswith("MemTotal:")):
					this.DATA['total_mem'] = float(elem[1]) / 1024.0;
				if(line.startswith("SwapFree:")):
					this.DATA['swap_free'] = float(elem[1]) / 1024.0;
				if(line.startswith("SwapTotal:")):
					this.DATA['total_swap'] = float(elem[1]) / 1024.0;
				line = FMEM.readline();
			FMEM.close();
			this.DATA['mem_used'] = this.DATA['total_mem'] - this.DATA['mem_free'];
			this.DATA['swap_used'] = this.DATA['total_swap'] - this.DATA['swap_free'];
			this.DATA['mem_usage'] = 100.0 * this.DATA['mem_used'] / this.DATA['total_mem'];
			this.DATA['swap_usage'] = 100.0 * this.DATA['swap_used'] / this.DATA['total_swap'];
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/meminfo");
			return;

	# read system load average
	def readLoadAvg (this):
		try:
			FAVG = open('/proc/loadavg');
			line = FAVG.readline();
			FAVG.close();
			elem = re.split("\s+", line);
			this.DATA['load1'] = float(elem[0]);
			this.DATA['load5'] = float(elem[1]);
			this.DATA['load15'] = float(elem[2]);
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/meminfo");
			return;

	# read the number of processes currently running on the system
	def countProcesses (this):
		nr = 0;
		try:
			for file in os.listdir("/proc"):
				if re.match("\d+", file):
					nr += 1;
			this.DATA['processes'] = nr;
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc to count processes");
			return;
	
	# reads the IP, hostname, cpu_MHz, uptime
	def readGenericInfo (this):
		this.DATA['hostname'] = socket.getfqdn();
		try:
			output = os.popen('/sbin/ifconfig -a')
			eth, ip = '', '';
			line = output.readline();
			while(line != ''):
				line = line.strip();
				if line.startswith("eth"):
					elem = line.split();
					eth = elem[0];
					ip = '';
				if len(eth) > 0 and line.startswith("inet addr:"):
					ip = re.match("inet addr:(\d+\.\d+\.\d+\.\d+)", line).group(1);
					this.DATA[eth + '_ip'] = ip;
					eth = '';
				line = output.readline();
			output.close();
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot get output from /sbin/ifconfig -a");
			return;
		try:
			no_cpus = 0;
			FCPU = open('/proc/cpuinfo');
			line = FCPU.readline();
        	        while(line != ''):
				if line.startswith("cpu MHz"):
					this.DATA['cpu_MHz'] = float(re.match("cpu MHz\s+:\s+(\d+\.?\d*)", line).group(1));
					no_cpus += 1;
				line = FCPU.readline();
			FCPU.close();
			this.DATA['no_CPUs'] = no_cpus;
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/cpuinfo");
			return;
		try:
			FUPT = open('/proc/uptime');
        	        line = FUPT.readline();
			FUPT.close();
			elem = line.split();
			this.DATA['uptime'] = float(elem[0]) / (24.0 * 3600);
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/uptime");
			return;
	
	# read network information like transfered kBps and nr. of errors on each interface
	def readNetworkInfo (this):
		this.OLD_RAW = this.NEW_RAW;
		interval = int(time.time()) - this.LAST_UPDATE_TIME;
		try:
			FNET = open('/proc/net/dev');
			line = FNET.readline();
			while(line != ''):
				m = re.match("\s*eth(\d):(\d+)\s+\d+\s+(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+\d+\s+(\d+)", line);
				if m != None:
					this.NEW_RAW['eth'+m.group(1)+'_in'] = float(m.group(2));
					this.NEW_RAW['eth'+m.group(1)+'_out'] = float(m.group(4));
					this.DATA['eth'+m.group(1)+'_errs'] = int(m.group(3)) + int(m.group(5));
					if(this.OLD_RAW.has_key('eth'+m.group(1)+"_in")):
						#this.logger.log(Logger.DEBUG, "Net I/O eth"+m.group(1)+" interval = "+ `interval`);
						Bps = (this.NEW_RAW['eth'+m.group(1)+'_in'] - this.OLD_RAW['eth'+m.group(1)+'_in']) / interval;
						this.DATA['eth'+m.group(1)+'_in'] = Bps / 1024.0;
						Bps = (this.NEW_RAW['eth'+m.group(1)+'_out'] - this.OLD_RAW['eth'+m.group(1)+'_out']) / interval;
						this.DATA['eth'+m.group(1)+'_out'] = Bps / 1024.0;
				line = FNET.readline();
			FNET.close();
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot open /proc/net/dev");
			return;
	
	##############################################################################################
	# job monitoring related functions
	##############################################################################################
	
	# internal function that gets the full list of children (pids) for a process (pid)
	def getChildren (this, parent):
		pidmap = {};
		try:
			output = os.popen('ps --no-headers -eo "pid ppid"');
			line = output.readline();
			while(line != ''):
				line = line.strip();
				elem = re.split("\s+", line);
				pidmap[elem[0]] = elem[1];
				line = output.readline();
			output.close();
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot execute ps --no-headers -eo \"pid ppid\"");

		if not pidmap.has_key(parent):
			this.logger.log(Logger.INFO, 'ProcInfo: No job with pid='+str(parent));
			this.removeJobToMonitor(parent);
			return [];

		children = [parent];				
		i = 0;
		while(i < len(children)):
			prnt = children[i];
			for (pid, ppid) in pidmap.items():
				if ppid == prnt:
					children.append(pid);
        	i += 1;
		return children;

	# internal function that parses a time formatted like "days-hours:min:sec" and returns the corresponding
	# number of seconds.
	def parsePSTime (this, my_time):
		my_time = my_time.strip();
		if m != None:
			m = re.match("(\d+)-(\d+):(\d+):(\d+)", my_time);
			return int(m.group(1)) * 24 * 3600 + int(m.group(2)) * 3600 + int(m.group(3)) * 60 + int(m.group(4));
		else:
			m = re.match("(\d+):(\d+):(\d+)", my_time);
			if(m != None):
				return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3));
			else:
				m = re.match("(\d+):(\d+)", my_time);
				if(m != None):
					return int(m.group(1)) * 60 + int(m.group(2));
				else:
					return 0;

	# read information about this the JOB_PID process
	# memory sizes are given in KB
	def readJobInfo (this, pid):
		if (pid == '') or this.JOBS.has_key(pid):
			return;
		children = this.getChildren(pid);
		if(len(children) == 0):
			this.logger.log(Logger.INFO, "ProcInfo: Job with pid="+str(pid)+" terminated; removing it from monitored jobs.");
			this.removeJobToMonitor(pid);
			return;
		try:
			JSTATUS = os.popen("ps --no-headers --pid " + ",".join([`child` for child in  children]) + " -o etime,time,%cpu,%mem,rsz,vsz,comm");
			mem_cmd_map = {};
			etime, cputime, pcpu, pmem, rsz, vsz, comm = 0, 0, 0, 0, 0, 0, 0;
			line = JSTATUS.readline();
			while(line != ''):
				line = line.strip();
				m = re.match("(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(.+)", line);
				if m != None:
					etime1, cputime1, pcpu1, pmem1, rsz1, vsz1, comm1 = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7);
					sec = this.parsePSTime(etime1);
					if sec > etime: 	# the elapsed time is the maximum of all elapsed
						etime = sec;
					sec = this.parsePSTime(cputime1); # times corespornding to all child processes.
					cputime += sec;	# total cputime is the sum of cputimes for all processes.
					pcpu += float(pcpu1); # total %cpu is the sum of all children %cpu.
					if not mem_cmd_map.has_key(`pmem1`+" "+`rsz1`+" "+`vsz1`+" "+`comm1`):
						# it's the first thread/process with this memory footprint; add it.
						mem_cmd_map[`pmem1`+" "+`rsz1`+" "+`vsz1`+" "+`comm1`] = 1;
						pmem += float(pmem1); rsz += int(rsz1); vsz += int(vsz1);
					# else not adding memory usage
				line = JSTATUS.readline();
			JSTATUS.close();
			this.JOBS[pid]['DATA']['run_time'] = etime;
			this.JOBS[pid]['DATA']['cpu_time'] = cputime;
			this.JOBS[pid]['DATA']['cpu_usage'] = pcpu;
			this.JOBS[pid]['DATA']['mem_usage'] = pmem;
			this.JOBS[pid]['DATA']['rss'] = rsz;
			this.JOBS[pid]['DATA']['virtualmem'] = vsz;
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ProcInfo: cannot execute ps --no-headers -eo \"pid ppid\"");

	# if there is an work directory defined, then compute the used space in that directory
	# and the free disk space on the partition to which that directory belongs
	# sizes are given in MB
	def readJobDiskUsage (this, pid):
		if (pid == '') or this.JOBS.has_key(pid):
			return;
		workDir = this.JOBS[pid]['WORKDIR'];
		if workDir == '':
			return;
		try:
			DU = os.popen("du -Lscm "+workDir+" | tail -1 | cut -f 1");
			line = DU.readline();
			this.JOBS[pid]['DATA']['workdir_size'] = int(line);
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ERROR", "ProcInfo: cannot run du to get job's disk usage for job "+`pid`);
		try:
			DF = os.popen("df -m "+workDir+" | tail -1");
			line = DF.readline().strip();
			m = re.match("\S+\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)%", line);
			if m != None:
				this.JOBS[pid]['DATA']['disk_total'] = m.group(1);
				this.JOBS[pid]['DATA']['disk_used'] = m.group(2);
				this.JOBS[pid]['DATA']['disk_free'] = m.group(3);
				this.JOBS[pid]['DATA']['disk_usage'] = m.group(4);
			DF.close();
		except IOError, ex:
			this.logger.log(Logger.ERROR, "ERROR", "ProcInfo: cannot run df to get job's disk usage for job "+`pid`);

	# Return a hash containing (param,value) pairs with existing values from the requested ones
	def getFilteredData (this, dataHash, paramsList):
		result = {};
		for param in paramsList:
			m = re.match("^net_(.*)$", param);
			if m == None:
				m = re.match("^(ip)$", param);
			if m != None:
				net_param = m.group(1);
				#this.logger.log(Logger.DEBUG, "Querying param "+net_param);
				for key, value in dataHash.items():
					m = re.match("eth\d_"+net_param, key);
					if m != None:
						result[key] = value;
			else:
				if dataHash.has_key(param):
					result[param] = dataHash[param];
		return result;

######################################################################################
# self test

if __name__ == '__main__':
	logger = this.logger(Logger.DEBUG);
	pi = ProcInfo(logger);
	print "first update";
	pi.update();
	print "Sleeping to accumulate";
	time.sleep(1);
	pi.update();
	print "System Monitoring:";
	
	sys_cpu_params = ['cpu_usr', 'cpu_sys', 'cpu_idle', 'cpu_nice', 'cpu_usage'];
	sys_2_4_params = ['pages_in', 'pages_out', 'swap_in', 'swap_out'];
	sys_mem_params = ['mem_used', 'mem_free', 'total_mem', 'mem_usage'];
	sys_swap_params = ['swap_used', 'swap_free', 'total_swap', 'swap_usage'];
	sys_load_params = ['load1', 'load5', 'load15', 'processes', 'uptime'];
	sys_gen_params = ['hostname', 'cpu_MHz', 'no_CPUs'];
	sys_net_params = ['net_in', 'net_out', 'net_errs', 'ip'];
	
	print "sys_cpu_params", pi.getSystemData(sys_cpu_params);
	print "sys_2_4_params", pi.getSystemData(sys_2_4_params);
	print "sys_mem_params", pi.getSystemData(sys_mem_params);
	print "sys_swap_params", pi.getSystemData(sys_swap_params);
	print "sys_load_params", pi.getSystemData(sys_load_params);
	print "sys_gen_params", pi.getSystemData(sys_gen_params);
	print "sys_net_params", pi.getSystemData(sys_net_params);
	
	print "Job (mysefl) monitoring:";
	pi.addJobToMonitor(os.getpid(), os.getcwd());
	print "Sleep another second";
	time.sleep(1);
	pi.update();
	
	job_cpu_params = ['run_time', 'cpu_time', 'cpu_usage'];
	job_mem_params = ['mem_usage', 'rss', 'virtualmem'];
	job_disk_params = ['workdir_size', 'disk_used', 'disk_free', 'disk_total', 'disk_usage'];
	
	print "job_cpu_params", pi.getJobData(os.getpid(), job_cpu_params);
	print "job_mem_params", pi.getJobData(os.getpid(), job_mem_params);
	print "job_disk_params", pi.getJobData(os.getpid(), job_disk_params);

	pi.removeJobToMonitor(os.getpid());

