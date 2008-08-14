#!/usr/bin/env python
import re
import sys
import ldap

DEBUG = 0
map_source = {'ceList': [], 'bdii': ''}
ce_to_cluster_map = {}
cluster_to_site_map = {}

def runldapquery(filter, attribute, bdii):
    if DEBUG:
        print "runldapquery ["+bdii+"]", filter[0:100], attribute[0:100]
    attribute = attribute.split(' ')
    filter = filter.strip()
    filter = filter.lstrip("'").rstrip("'")

    bdiiuri = 'ldap://' + bdii + ':2170'
    l = ldap.initialize(bdiiuri)

    l.simple_bind_s('', '')

    base = "o=grid"
    scope = ldap.SCOPE_SUBTREE
    timeout = 0
    result_set = []
    filter = filter.strip("'")

    try:
        result_id = l.search(base, scope, filter, attribute)
        while 1:
            result_type, result_data = l.result(result_id, timeout)
            if (result_data == []):
                break
            else:
                if result_type == ldap.RES_SEARCH_ENTRY:
                    result_set.append(result_data)

    except ldap.LDAPError, error_message:
        print error_message

    return result_set

def getJMListFromSEList(selist, bdii='exp-bdii.cern.ch'):
    """
    Given a list of SE FQDNs, return list of CEUniqueIDs that advertise CMS
    support and are in Production,
    sorted by number of waiting jobs in descending order
    """
    jmlist = []

    query = buildOrQuery('GlueCESEBindGroupSEUniqueID', selist)
    pout = runldapquery(query, 'GlueCESEBindGroupCEUniqueID', bdii)

    query = "(&(GlueCEAccessControlBaseRule=VO:cms)(GlueCEStateStatus=Production)"
    query += buildOrQuery('GlueCEUniqueID', [l[0][1]['GlueCESEBindGroupCEUniqueID'][0] for l in pout])
    query += ")"
    pout = runldapquery(query, 'GlueCEUniqueID GlueCEStateWaitingJobs', bdii)

    jminfo_list = []
    for x in pout:
        jminfo = {}
        ce = x[0][1]['GlueCEUniqueID'][0]
        waiting_jobs = x[0][1]['GlueCEStateWaitingJobs'][0]

        jminfo['ce'] = ce
        jminfo['waiting_jobs'] = waiting_jobs
        jminfo_list.append(jminfo)

    def compare_by (fieldname):
        """ Comparison function for sorting dicts """
        def compare_two_dicts (a, b):
            return cmp(int(a[fieldname]), int(b[fieldname]))
        return compare_two_dicts

    jminfo_list.sort(compare_by('waiting_jobs'))
    jmlist = [x['ce'] for x in jminfo_list]
    return jmlist

def generateMaps(ceList, bdii='exp-bdii.cern.ch'):
    """
    Generate maps of CE to Cluster and Cluster to Site as the globals
    ce_to_cluster_map, cluster_to_site_map

    ceList: list of GlueCEUniqueIDs
    bdii: BDII instance to query
    """
    if (ceList == map_source['ceList']
        and bdii == map_source['bdii']): return

    query = buildOrQuery('GlueCEUniqueID', ceList)

    pout = runldapquery(query, 'GlueCEUniqueID GlueForeignKey', bdii)

    r = re.compile('^GlueClusterUniqueID\s*=\s*(.*)')
    for x in pout:
        host = x[0][1]['GlueCEUniqueID'][0]
        clusterid = x[0][1]['GlueForeignKey'][0]
        m = r.match(clusterid)
        if m: ce_to_cluster_map[host] = m.groups()[0]

    query = "(&(objectClass=GlueCluster)"
    query += buildOrQuery('GlueClusterUniqueID', ce_to_cluster_map.values())
    query += ")"

    pout = runldapquery(query, 'GlueClusterUniqueID GlueForeignKey', bdii)
    r = re.compile('^GlueSiteUniqueID=(.*)')
    for x in pout:
        cluster = x[0][1]['GlueClusterUniqueID'][0]
        foreign_keys = x[0][1]['GlueForeignKey']
        for foreign_key in foreign_keys:
            m = r.match(foreign_key)
            if m:
                site = m.groups()[0]
                cluster_to_site_map[cluster] = site

    # cache the list sources
    map_source['ceList'] = ceList
    map_source['bdii'] = bdii

    if (DEBUG): print 40*'*', 'exit generateMaps', 40*'*'
def buildOrQuery(gluekey, list):
    """
    Returns a nugget of LDAP requesting the OR of all items
    of the list equal to the gluekey
    """

    query = "(|"
    for x in list:
        query += "(%s=%s)" % (gluekey, x)
    query += ")"
    return query

def isOSGSite(host_list, bdii='exp-bdii.cern.ch'):
    """
    Given a list of CEs, return only the ones which belong to OSG sites
    """
    generateMaps(host_list, bdii)

    query = buildOrQuery('GlueSiteUniqueID', cluster_to_site_map.values())
    pout = runldapquery(query, 'GlueSiteUniqueID GlueSiteDescription', bdii)
    osg_site_list = []
    for x in pout:
        site_descr = x[0][1]['GlueSiteDescription'][0]

        if (site_descr.find('OSG') != -1):
            osg_site_list.append(x[0][1]['GlueSiteUniqueID'][0])

    osg_host_list = []

    for host in host_list:
        cluster = ce_to_cluster_map[host]
        site = cluster_to_site_map[cluster]

        if (osg_site_list.count(site)):
            osg_host_list.append(host)

    return osg_host_list

def getSoftwareAndArch(host_list, software, arch, bdii='exp-bdii.cern.ch'):
    """
    Given a list of CEs, return only those that match a given software
    and architecture tag
    """
    generateMaps(host_list, bdii)

    results_list = []
    software = 'VO-cms-' + software
    arch = 'VO-cms-' + arch
    query = "'(&(GlueHostApplicationSoftwareRunTimeEnvironment="+software+ ")"
    query +=   "(GlueHostApplicationSoftwareRunTimeEnvironment="+arch+")"

    query += buildOrQuery('GlueChunkKey=GlueClusterUniqueID', [ce_to_cluster_map[h] for h in host_list])
    query += ")"

    pout = runldapquery(query, 'GlueHostApplicationSoftwareRunTimeEnvironment GlueChunkKey', bdii)
    clusterlist = [x[0][1]['GlueChunkKey'][0] for x in pout]

    results_list = []
    for jm in host_list:
        cluster = "GlueClusterUniqueID=" + ce_to_cluster_map[jm]
        if (clusterlist.count(cluster) != 0):
            results_list.append(jm)

    return results_list


def getJobManagerList(selist, software, arch, bdii='exp-bdii.cern.ch', onlyOSG=True):
    """
    Given a list of SE FQDNs, return list of CEUniqueIDs that advertise CMS
    support and are in Production, sorted by number of waiting jobs in
    descending order that have a given software and arch.

    If OnlyOSG is True, return only OSG Sites.
    """

    jmlist = getJMListFromSEList(selist, bdii)
    jmlist = filterCE(jmlist, software, arch, bdii, onlyOSG)

def filterCE(ceList, software, arch, bdii, onlyOSG):
    """
    Given a list of CEUniqueIDs, filter out only the ones with given
    software, arch, and if it belongs to an OSG site.
    """
    generateMaps(ceList, bdii)
    if (onlyOSG): ceList = isOSGSite(ceList, bdii)
    ceList = getSoftwareAndArch(ceList, software, arch, bdii)
    res = removeQueues(ceList)
    return res

def removeQueues(celist):
    """
    Given a list of CEUniqueIDs, return a list of jobmanager contact
    strings.
    """
    r = re.compile('^(.*:.*/jobmanager-.*?)-(.*)')
    jmlist = []
    for x in celist:
        m = r.match(x)
        if m:
            item = m.groups()[0]
            if (jmlist.count(item) == 0):
                jmlist.append(item)
    return jmlist

def listAllCEs(software='', arch='', onlyOSG=False, bdii='exp-bdii.cern.ch'):
    ''' List all GlueCEUniqueIDs that advertise support for CMS '''

    RE_cename = re.compile('^GlueCEUniqueID: (.*)', re.IGNORECASE)
    filt = "'(&(GlueCEUniqueID=*)(GlueCEAccessControlBaseRule=VO:cms))'"
    res = runldapquery(filt, 'GlueCEUniqueID', bdii)
    ceList = [x[0][1]['GlueCEUniqueID'][0] for x in res]

    if (software or arch or onlyOSG):
        ceList = filterCE(ceList, software, arch, bdii, onlyOSG)

    return ceList

def listAllSEs(bdii='exp-bdii.cern.ch'):
    ''' List all SEs that are bound to CEs that advertise support for CMS '''

    RE_sename = re.compile('^GlueCESEBindGroupSEUniqueID: (.*)', re.IGNORECASE)
    seList = []
    ceList = listAllCEs(bdii=bdii)

    query = buildOrQuery('GlueCESEBindGroupCEUniqueID', ceList)
    res = runldapquery(query, 'GlueCESEBindGroupSEUniqueID', bdii)

    for x in res:
        try:
            item = x[0][1]['GlueCESEBindGroupSEUniqueID'][0]
        except KeyError:
            # Sometimes we publish a CESE BindGroup without an SE attached
            pass

        if (seList.count(item) == 0): seList.append(item)
    return seList
