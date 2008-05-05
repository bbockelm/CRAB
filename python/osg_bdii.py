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
    pout = runldapquery(" '(GlueCESEBindGroupSEUniqueID=" + se + ")' ", 'GlueCESEBindGroupCEUniqueID', bdii)

#    r = re.compile('^GlueCESEBindGroupCEUniqueID: (.*:.*/jobmanager-.*)-cms')
    r = re.compile('^GlueCESEBindGroupCEUniqueID: (.*:.*/jobmanager-.*?)-(.*)')
    jm = []
    for l in pout:
        m = r.match(l)
        if m:
            item = m.groups()[0]
            if (jm.count(item) == 0):
                jm.append(item)

    return jm


def cestate_from_se_bdii(se, bdii='exp-bdii.cern.ch' ):
    status = []
    jmlist = jm_from_se_bdii(se)

    for jm in jmlist:
        jm += "-cms"

        pout = runldapquery(" '(&(objectClass=GlueCEState)(GlueCEUniqueID=" + jm + "))' ", 'GlueCEStateStatus', bdii)

        r = re.compile('^GlueCEStateStatus: (.*)')
        for l in pout:
            m = r.match(l)
            if m:
                status.append(m.groups()[0])

    return status

def cestate_from_ce_bdii(ce, bdii='exp-bdii.cern.ch'):
    pout = runldapquery(" '(&(objectClass=GlueCEState)(GlueCEInfoHostName=" + ce + ")(GlueCEAccessControlBaseRule=VO:cms))' ", 'GlueCEStateStatus', bdii)

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

    query = " '(|"
    for se in selist:
        query = query + "(GlueCESEBindGroupSEUniqueID=" + se + ")"
    query = query + ")' "

    pout = runldapquery(query, 'GlueCESEBindGroupCEUniqueID', bdii)
    r = re.compile('^GlueCESEBindGroupCEUniqueID: (.*:.*/jobmanager-.*?)-(.*)')

    for l in pout:
        m = r.match(l)
        if m:
            item = m.groups()[0]
            if (jmlist.count(item) == 0):
                jmlist.append(item)


    query = " '(&(GlueCEAccessControlBaseRule=VO:cms)(|"
    for l in jmlist:
        query += "(GlueCEInfoContactString=" + l + "-*)"

    query += "))' "

    pout = runldapquery(query, 'GlueCEInfoContactString', bdii)

    r = re.compile('^GlueCEInfoContactString: (.*:.*/jobmanager-.*)')
    for l in pout:
        m = r.match(l)
        if m:
            item = m.groups()[0]
            if (jmlist.count(item) == 0):
                jmlist.append(item)

    return jmlist

def isOSGSite(host_list, bdii='exp-bdii.cern.ch'):
    results_list = []
    r = re.compile('^GlueSiteDescription: (OSG.*)')
    s = re.compile('^GlueSiteUniqueID: (.*)')

    query = " '(|"
    for h in host_list:
        query = query + "(GlueSiteUniqueID=" + h + ")"
    query = query + ")' GlueSiteDescription"

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

def getSoftwareAndArch2(host_list, software, arch, bdii='exp-bdii.cern.ch'):
    results_list = []

    # Find installed CMSSW versions and Architecture
    software = 'VO-cms-' + software
    arch = 'VO-cms-' + arch

    query = "'(|"

    for h in host_list:
        query += "(GlueCEInfoContactString=" + h + ")"
    query += ")'"

    pout = runldapquery(query, 'GlueForeignKey GlueCEInfoContactString', bdii)
    r = re.compile('GlueForeignKey: GlueClusterUniqueID=(.*)')
    s = re.compile('GlueCEInfoContactString: (.*)')

    ClusterMap =  {}
    ClusterUniqueID = None
    CEInfoContact = None

    for l in pout:
        m = r.match(l)
        if m:
            ClusterUniqueID = m.groups()[0]
        m = s.match(l)
        if m:
            CEInfoContact = m.groups()[0]

        if (ClusterUniqueID and CEInfoContact):
            ClusterMap[ClusterUniqueID] = CEInfoContact
            ClusterUniqueID = None
            CEInfoContact = None

    query = "'(|"
    for c in ClusterMap.keys():
        query += "(GlueChunkKey=GlueClusterUniqueID="+c+")"
    query += ")'"

    pout = runldapquery(query, 'GlueHostApplicationSoftwareRunTimeEnvironment GlueChunkKey', bdii)
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
                ClusterUniqueID = m2.groups()[0]
                host = ClusterMap[ClusterUniqueID]

        if ((software_installed == 1) and (architecture == 1)):
            results_list.append(host)

    return results_list

def getSoftwareAndArch(host_list, software, arch, bdii='exp-bdii.cern.ch'):
    results_list = []

    # Find installed CMSSW versions and Architecture
    software = 'VO-cms-' + software
    arch = 'VO-cms-' + arch

    query = " '(|"
    for h in host_list:
        query = query + "(GlueChunkKey='GlueClusterUniqueID=" + h + "\')"
    query = query + ")' "

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

def getJMInfo(selist, software, arch, bdii='exp-bdii.cern.ch', onlyOSG=True):
    jminfo_list = []
    host_list = []

    stat = re.compile('^GlueCEStateStatus: (.*)')
    host = re.compile('^GlueCEInfoHostName: (.*)')
    wait = re.compile('^GlueCEStateWaitingJobs: (.*)')
    name = re.compile('^GlueCEUniqueID: (.*)')

    jmlist = getJMListFromSEList(selist)

    query = " '(&(objectClass=GlueCEState)(|"
    for jm in jmlist:
        query = query + "(GlueCEUniqueID=" + jm + ")"
    query = query + "))' "

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

    # Narrow the list of host to include only OSG sites if requested
    osg_list = isOSGSite([x['host'] for x in jminfo_list])
    if not onlyOSG:
        CElist = [x['name'] for x in jminfo_list]
    else:
        CElist = [x['name'] for x in jminfo_list if osg_list.count(x['host'])]

    # Narrow the OSG host list to include only those with the specified software and architecture
#    softarch_list = getSoftwareAndArch(osg_list, software, arch)
    softarch_list = getSoftwareAndArch2(CElist, software, arch)

    # remove any non-OSG sites from the list
    jminfo_newlist = []

    for item in jminfo_list:
        for narrowed_item in softarch_list:
            if (item['name'] == narrowed_item):
                if (jminfo_newlist.count(item) == 0):
                    jminfo_newlist.append(item)

    return jminfo_newlist

# This function is used to sort lists of dictionaries
def compare_by (fieldname):
    def compare_two_dicts (a, b):
        return cmp(int(a[fieldname]), int(b[fieldname]))
    return compare_two_dicts

def getJobManagerList(selist, software, arch, bdii='exp-bdii.cern.ch', onlyOSG=True):
    jms = getJMInfo(selist, software, arch, bdii, onlyOSG)
    # Sort by waiting_jobs field and return the jobmanager with the least waiting jobs
    jms.sort(compare_by('waiting_jobs'))
    jmlist = []
    r = re.compile('^(.*:.*/jobmanager-.*?)-(.*)')
    for jm in jms:
        fullname = jm['name']
        m = r.match(fullname)
        if m:
            name = m.groups()[0]
            if (jmlist.count(name) == 0): jmlist.append(name)

    return jmlist

if __name__ == '__main__':
    seList = ['ccsrm.in2p3.fr', 'cmssrm.hep.wisc.edu', 'pccms2.cmsfarm1.ba.infn.it', 'polgrid4.in2p3.fr', 'srm-disk.pic.es', 'srm.ciemat.es', 'srm.ihepa.ufl.edu', 't2data2.t2.ucsd.edu']
#    seList = ['ccsrm.in2p3.fr', 'storm.ifca.es']
    jmlist =  getJobManagerList(seList, "CMSSW_1_6_11", "slc4_ia32_gcc345", 'uscmsbd2.fnal.gov', True)
    for jm in jmlist:
        print jm
#   print jm_from_se_bdii(sys.argv[1])
