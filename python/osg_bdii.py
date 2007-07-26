#!/usr/bin/python
import re, popen2, sys
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
#    bdii = 'exp-bdii.cern.ch'
    command = 'ldapsearch -xLLL -p 2170 -h '+bdii+' -b o=grid '
    command += filter+' '+attribute

    pout,pin,perr = popen2.popen3(command)

    pout = pout.readlines()
    p = perr.readlines()

    pout = unwraplines(pout)
    if (p):
        for l in p: print l
        raise RuntimeError('ldapsearch call failed')

    return pout

def jm_from_se_bdii(se, bdii='exp-bdii.cern.ch'):
    se = '\''+se+'\''
    pout = runldapquery(''' '(GlueCESEBindGroupSEUniqueID='''+se+''')' ''',
                        'GlueCESEBindGroupCEUniqueID', bdii)

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

        pout = runldapquery(''' '(&(objectClass=GlueCEState)(GlueCEUniqueID='''+
        jm+'''))' ''', 'GlueCEStateStatus', bdii)

        r = re.compile('^GlueCEStateStatus: (.*)')
        for l in pout:
            m = r.match(l)
            if m:
                status.append(m.groups()[0])

    return status            

def cestate_from_ce_bdii(ce, bdii='exp-bdii.cern.ch'):
    pout = runldapquery(''' '(&(objectClass=GlueCEState)(GlueCEInfoHostName='''+
    ce+''')(GlueCEAccessControlBaseRule=VO:cms))' ''', 'GlueCEStateStatus', bdii)

    status = ''
    r = re.compile('^GlueCEStateStatus: (.*)')
    for l in pout:
        m = r.match(l)
        if m:
            status = m.groups()[0]

    return status            

if __name__ == '__main__':
    print jm_from_se_bdii(sys.argv[1])

