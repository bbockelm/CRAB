#!/usr/bin/env python

import re
import sys
import copy
import sets
import popen2

CR = '\r'
LF = '\n'
CRLF = CR+LF
DEBUG = 0

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
    if DEBUG:
        print "runldapquery ["+bdii+"]", filter[0:100], attribute[0:100]
    command = 'ldapsearch -xLLL -p 2170 -h ' + bdii + ' -b o=grid '
    command += filter + ' ' + attribute
    #print "Command",command
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
    """
    Determine which hosts in the host_list are OSG sites.

    Take a list of CE host names, and find all the associated clusters.
    Take the resulting clusters and map them to sites.  Any such site whose
    description includes "OSG" is labeled an OSG site.

    @param host_list: A list of host names which are CEs we want to consider.
    @keyword bdii: The BDII instance to query
    @return: A list of host names filtered from host_list which are OSG sites.
    """
    siteUniqueID_CE_map = {}
    results = sets.Set()
    description_re = re.compile('^GlueSiteDescription: (OSG.*)')
    siteid_re = re.compile('GlueForeignKey: GlueSiteUniqueID=(.*)')
    siteid_site_re = re.compile('^GlueSiteUniqueID: (.*)')
    ce_re = re.compile('^GlueForeignKey: GlueCEUniqueID=(.*):')

    # Build the BDII query for all the hosts.
    # This asks for all GlueClusters which are associated with one of the
    # host names.
    query = " '(&(objectClass=GlueCluster)(|"
    for h in host_list:
        query += "(GlueForeignKey=GlueCEUniqueID=%s:*)" % h
    query += "))' "

    pout = runldapquery(query, 'GlueForeignKey', bdii)
    output = concatoutput(pout)

    # Now, we build a mapping from host to SiteUniqueID
    stanzas = output.split(LF + LF)
    for stanza in stanzas:
        details = stanza.split(LF)
        ces_tmp = sets.Set()
        siteUniqueID = None
        # Find out all the matching CEs and Sites with this cluster.
        for detail in details:
            m = ce_re.match(detail)
            if m:
                ces_tmp.add(m.groups()[0])
            m = siteid_re.match(detail)
            if m:
                siteUniqueID = m.groups()[0]
        if siteUniqueID:
            siteUniqueID_CE_map[siteUniqueID] = ces_tmp

    # Build a new query, this time for SiteUniqueIDs
    query = " '(|"
    for site in siteUniqueID_CE_map:
        query += "(GlueSiteUniqueID=%s)" % site
    query += ")' "
    pout = runldapquery(query, "GlueSiteUniqueID GlueSiteDescription", bdii)
    output = concatoutput(pout)

    # See which resulting sites are OSG sites, and then add the
    # corresponding CEs into the results set.
    stanzas = output.split(LF + LF)
    for stanza in stanzas:
        isOsgSite = False
        siteUniqueID = None
        details = stanza.split(LF)
        # We need to find the stanza's siteUniqueID and if the description
        # is a "OSG Site".  If it is, afterward add it to the results.
        for detail in details:
            m = siteid_site_re.search(detail)
            if m:
                siteUniqueID = m.groups()[0]
            m = description_re.match(detail)
            if m:
                isOsgSite = True
        if siteUniqueID and isOsgSite:
            results.update(siteUniqueID_CE_map[siteUniqueID])
    return list(results)

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

    jmlist = getJMListFromSEList(selist, bdii)

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
    osg_list = isOSGSite([x['host'] for x in jminfo_list], bdii)
    if not onlyOSG:
        CElist = [x['name'] for x in jminfo_list]
    else:
        CElist = [x['name'] for x in jminfo_list if osg_list.count(x['host'])]

    # Narrow the OSG host list to include only those with the specified software and architecture
#    softarch_list = getSoftwareAndArch(osg_list, software, arch)
    softarch_list = getSoftwareAndArch2(CElist, software, arch, bdii)

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

def listAllCEs(software, arch, bdii='exp-bdii.cern.ch',onlyOSG=True):
    ''' List all GlueCEUniqueIDs that advertise support for CMS '''

#    RE_cename = re.compile('^GlueCEUniqueID: (.*:.*/jobmanager-.*?)-(.*)', re.IGNORECASE)
    RE_cename = re.compile('^GlueCEUniqueID: (.*)', re.IGNORECASE)
    hostSplit  = re.compile(r'[^\w\.\-]')
    filt = "'(&(GlueCEUniqueID=*)(GlueCEAccessControlBaseRule=VO:cms))'"
    pout = runldapquery(filt, 'GlueCEUniqueID', bdii)
    ceList   = []
    hostList = []
    for l in pout:
        m = RE_cename.match(l)
        if m:
            item = m.groups()[0]
            hostname = hostSplit.split(item)[0]
            if (ceList.count(item) == 0):       ceList.append(item)
            if (hostList.count(hostname) == 0): hostList.append(hostname)

    if onlyOSG:
      osgCEs = []
      osgList =  isOSGSite(hostList, bdii)
      for ce in ceList:
        hostname = hostSplit.split(ce)[0]
        if hostname in osgList:
          osgCEs.append(ce)
    else:
      osgCEs = ceList

    softarch_list = getSoftwareAndArch2(osgCEs, software, arch)

    shortCeList   = [] # Convert to CE without queue
    RE_short = re.compile('^(.*:.*/jobmanager-.*?)-(.*)', re.IGNORECASE)
    for ce in softarch_list:
        m = RE_short.match(ce)
        if m:
            item = m.groups()[0]
            if (shortCeList.count(item) == 0):       shortCeList.append(item)

    return shortCeList

def listAllSEs(bdii='exp-bdii.cern.ch'):
    ''' List all SEs that are bound to (CEs that advertise support for CMS) '''

    RE_sename = re.compile('^GlueCESEBindGroupSEUniqueID: (.*)', re.IGNORECASE)
    seList = []
    filt = "'(|"
    ceList = listAllCEs(bdii)
    for ce in ceList:
        filtstring = '(GlueCESEBindGroupCEUniqueID=' + ce + ')'
        filt += filtstring
    filt += ")'"

    pout = runldapquery(filt, 'GlueCESEBindGroupSEUniqueID', bdii)
    for l in pout:
        m = RE_sename.match(l)
        if m:
            item = m.groups()[0]
            if (seList.count(item) == 0): seList.append(item)

    return seList
