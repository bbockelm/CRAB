#!/usr/bin/env python
import re, popen2, sys, copy

CR = '\r'
LF = '\n'
CRLF = CR+LF

def unwraplines(wrapped_list):
    r = re.compile('^ (.*)$')
    unwrapped_list = []
    for l in wrapped_list:
        m = r.match(l)
        if m:
            unwrapped_list[-1] += m.groups()[0]
        else:
            unwrapped_list.append(l.rstrip())

    return unwrapped_list


def runldapquery(filter, attribute, bdii):
    command = 'ldapsearch -xLLL -p 2170 -h ' + bdii + ' -b o=grid '
    command += filter + ' ' + attribute

    pout,pin,perr = popen2.popen3(command)

    pout = pout.readlines()
    p = perr.readlines()

    pout = unwraplines(pout)
    if (p):
        for l in p: print l
        raise RuntimeError('ldapsearch call failed')

    return pout

def jm_from_se_bdii(se, bdii='exp-bdii.cern.ch'):
    se = '\'' + se + '\''
    pout = runldapquery(''' '(GlueCESEBindGroupSEUniqueID=''' + se + ''')' ''', 'GlueCESEBindGroupCEUniqueID', bdii)

    r = re.compile('^GlueCESEBindGroupCEUniqueID: (.*:.*/jobmanager-.*)-cms')
    jm = []
    for l in pout:
        m = r.match(l)
        if m:
            item = m.groups()[0]
            if (jm.count(item) == 0):
                jm.append(m.groups()[0])

    return jm


def cestate_from_se_bdii(se, bdii='exp-bdii.cern.ch' ):
    status = []
    jmlist = jm_from_se_bdii(se)

    for jm in jmlist:
        jm += "-cms"

        pout = runldapquery(''' '(&(objectClass=GlueCEState)(GlueCEUniqueID=''' + jm + '''))' ''', 'GlueCEStateStatus', bdii)

        r = re.compile('^GlueCEStateStatus: (.*)')
        for l in pout:
            m = r.match(l)
            if m:
                status.append(m.groups()[0])

    return status

def cestate_from_ce_bdii(ce, bdii='exp-bdii.cern.ch'):
    pout = runldapquery(''' '(&(objectClass=GlueCEState)(GlueCEInfoHostName=''' + ce + ''')(GlueCEAccessControlBaseRule=VO:cms))' ''', 'GlueCEStateStatus', bdii)

    status = ''
    r = re.compile('^GlueCEStateStatus: (.*)')
    for l in pout:
        m = r.match(l)
        if m:
            status = m.groups()[0]

    return status

def concatoutput(pout):
    output = ''
    for l in pout:
        if l == '':
            output = output + LF
        output = output + l + LF

    return output

def getJMListFromSEList(selist, bdii='exp-bdii.cern.ch'):
    # Get the Jobmanager list
    jmlist = []

    query = ''' '(|'''
    for se in selist:
        query = query + '''(GlueCESEBindGroupSEUniqueID=''' + se + ''')'''
    query = query + ''')' '''

    pout = runldapquery(query, 'GlueCESEBindGroupCEUniqueID', bdii)
    r = re.compile('^GlueCESEBindGroupCEUniqueID: (.*:.*/jobmanager-.*)-cms')
    for l in pout:
        m = r.match(l)
        if m:
            item = m.groups()[0]
            if (jmlist.count(item) == 0):
                jmlist.append(m.groups()[0])

    return jmlist

def isOSGSite(host_list, bdii='exp-bdii.cern.ch'):
    results_list = []
    r = re.compile('^GlueSiteDescription: (OSG.*)')
    s = re.compile('^GlueSiteUniqueID: (.*)')

    query = ''' '(|'''
    for h in host_list:
        query = query + '''(GlueSiteUniqueID=''' + h + ''')'''
    query = query + ''')' GlueSiteDescription'''

    pout = runldapquery(query, 'GlueSubClusterUniqueID GlueSiteUniqueID', bdii)
    output = concatoutput(pout)

    stanzas = output.split(LF + LF)
    for stanza in stanzas:
        osg = 0
        host = ""
        details = stanza.split(LF)
        for detail in details:
            m = r.match(detail)
            n = s.match(detail)
            if m:
                osg = 1
            if n:
                host = n.groups()[0]
        if (osg == 1):
            results_list.append(host)

    return results_list


def getSoftwareAndArch(host_list, software, arch, bdii='exp-bdii.cern.ch'):
    results_list = []

    # Find installed CMSSW versions and Architecture
    software = 'VO-cms-' + software
    arch = 'VO-cms-' + arch

    query = ''' '(|'''
    for h in host_list:
        query = query + '''(GlueChunkKey='GlueClusterUniqueID=''' + h + '''\')'''
    query = query + ''')' '''

    pout = runldapquery(query, 'GlueHostApplicationSoftwareRunTimeEnvironment GlueSubClusterUniqueID GlueChunkKey', bdii)
    output = concatoutput(pout)

    r = re.compile('^GlueHostApplicationSoftwareRunTimeEnvironment: (.*)')
    s = re.compile('^GlueChunkKey: GlueClusterUniqueID=(.*)')
    stanzas = output.split(LF + LF)
    for stanza in stanzas:
        software_installed = 0
        architecture = 0
        host = ''
        details = stanza.split(LF)
        for detail in details:
            m = r.match(detail)
            if m:
                if (m.groups()[0] == software):
                    software_installed = 1
                elif (m.groups()[0] == arch):
                    architecture = 1
            m2 = s.match(detail)
            if m2:
                host = m2.groups()[0]

        if ((software_installed == 1) and (architecture == 1)):
            results_list.append(host)

    return results_list

def getJMInfo(selist, software, arch, bdii='exp-bdii.cern.ch'):
    jminfo_list = []
    host_list = []

    stat = re.compile('^GlueCEStateStatus: (.*)')
    host = re.compile('^GlueCEInfoHostName: (.*)')
    wait = re.compile('^GlueCEStateWaitingJobs: (.*)')
    name = re.compile('^GlueCEUniqueID: (.*)')

    jmlist = getJMListFromSEList(selist)
    query = ''' '(&(objectClass=GlueCEState)(|'''
    for jm in jmlist:
        query = query + '''(GlueCEUniqueID=''' + jm + '''-cms)'''
    query = query + '''))' '''

    pout = runldapquery(query, 'GlueCEUniqueID GlueCEStateStatus GlueCEInfoHostName GlueCEStateWaitingJobs GlueCEStateFreeJobSlots', bdii)
    output = concatoutput(pout)

    stanza_list = output.split(LF+LF)
    for stanza in stanza_list:
        if len(stanza) > 1:
            status = 1
            wait_jobs = 0
            jmname = ''
            hostname = 0
            jminfo = {}

            details = stanza.split(LF)
            for det in details:
                mhost = host.match(det)
                if mhost: # hostname
                    host_list.append(mhost.groups()[0])
                    hostname = mhost.groups()[0]
                mstat = stat.match(det)
                if mstat: # e.g. Production
                    if not ((mstat.groups()[0] == 'Production') and (status == 1)):
                        status = 0
                mwait = wait.match(det)
                if mwait: # Waiting jobs
                    if (mwait.groups()[0] > wait_jobs):
                        wait_jobs = mwait.groups()[0]
                mname = name.match(det)
                if mname: # jm name
                    jmname = mname.groups()[0]

            jminfo["name"] = jmname
            jminfo["status"] = status
            jminfo["waiting_jobs"] = wait_jobs
            jminfo["host"] = hostname

            jminfo_list.append(copy.deepcopy(jminfo))

    # Narrow the list of host to include only OSG sites
    osg_list = isOSGSite(host_list)

    # Narrow the OSG host list to include only those with the specified software and architecture
    softarch_list = getSoftwareAndArch(osg_list, software, arch)

    # remove any non-OSG sites from the list
    for item in jminfo_list:
        found = 0
        for narrowed_item in softarch_list:
            if (item["host"] == narrowed_item):
                found = 1
                break
        if (found == 0):
            jminfo_list.remove(item)

    return jminfo_list

# This function is used to sort lists of dictionaries
def compare_by (fieldname):
    def compare_two_dicts (a, b):
        return cmp(int(a[fieldname]), int(b[fieldname]))
    return compare_two_dicts

def getJobManagerList(selist, software, arch, bdii='exp-bdii.cern.ch'):
    jms = getJMInfo(selist, software, arch)
    # Sort by waiting_jobs field and return the jobmanager with the least waiting jobs
    jms.sort(compare_by('waiting_jobs'))
    jmlist = []
    for jm in jms:
        jmlist.append(jm["name"][:-4])

    return jmlist

if __name__ == '__main__':
    seList = ['ccsrm.in2p3.fr', 'cmssrm.hep.wisc.edu', 'pccms2.cmsfarm1.ba.infn.it', 'polgrid4.in2p3.fr', 'srm-disk.pic.es', 'srm.ciemat.es', 'srm.ihepa.ufl.edu', 't2data2.t2.ucsd.edu']
    jmlist =  getJobManagerList(seList, "CMSSW_2_0_0", "slc4_ia32_gcc345")
    for jm in jmlist:
        print jm
#   print jm_from_se_bdii(sys.argv[1])

