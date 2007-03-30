#!/usr/bin/python
import re, popen2, sys

def runldapquery(filter, attribute):
    bdii = 'exp-bdii.cern.ch'
    command = 'ldapsearch -xLLL -p 2170 -h '+bdii+' -b o=grid '
    command += filter+' '+attribute

    pout,pin,perr = popen2.popen3(command)

    pout = pout.readlines()
    p = perr.readlines()

    if (p):
        for l in p: print l
        raise RuntimeError('ldapsearch call failed')

    return pout

def jm_from_se_bdii(se):
    se = '\''+se+'\''
    pout = runldapquery(''' '(GlueCESEBindGroupSEUniqueID='''+se+''')' ''',
                        'GlueCESEBindGroupCEUniqueID')

    r = re.compile('^GlueCESEBindGroupCEUniqueID: (.*:.*/jobmanager-.*)-cms')
    jm = []
    for l in pout:
        m = r.match(l)
        if m:
            item = m.groups()[0]
            if (jm.count(item) == 0):
                jm.append(m.groups()[0])

    return jm

    
def cestate_from_se_bdii(se):
    status = []
    jmlist = jm_from_se_bdii(se)

    for jm in jmlist:
        jm += "-cms"

        pout = runldapquery(''' '(&(objectClass=GlueCEState)(GlueCEUniqueID='''+
        jm+'''))' ''', 'GlueCEStateStatus')

        r = re.compile('^GlueCEStateStatus: (.*)')
        for l in pout:
            m = r.match(l)
            if m:
                status.append(m.groups()[0])

    return status            

def cestate_from_ce_bdii(ce):
    pout = runldapquery(''' '(&(objectClass=GlueCEState)(GlueCEInfoHostName='''+
    ce+''')(GlueCEAccessControlBaseRule=VO:cms))' ''', 'GlueCEStateStatus')

    status = ''
    r = re.compile('^GlueCEStateStatus: (.*)')
    for l in pout:
        m = r.match(l)
        if m:
            status = m.groups()[0]

    return status            
