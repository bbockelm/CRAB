import os, sys, popen2

# env to be preserved
preserveList = ['SCRAMRT_LOCALRT', 'LOCALRT', 'BASE_PATH', 'CMSSW_SEARCH_PATH', 'CMSSW_VERSION', 
     'CMSSW_BASE','CMSSW_RELEASE_BASE', 'CMSSW_DATA_PATH']

reverseList = ['LD_LIBRARY_PATH', 'PATH', 'PYTHONPATH']

# init
if 'CRAB_UNFOLD_ENV' in os.environ:
     sys.exit(0) 

if 'CMSSW_BASE' not in os.environ:
     # CMSSW env likely not set, nothing to do
     print 'echo "CMSSW_BASE not set"' 
     sys.exit(-1)

# identify the shell kind
sout, sin, serr = popen2.popen3('echo $BASH')
l = sout.readlines()[0]
sout.close()
sin.close()
serr.close()

if len(l)!=0: #'BASH' in os.environ:
    shKind = 'sh'
else:
    shKind = 'csh'

# -create -submit special case: first part # Fabio
cachedPath = os.environ['PATH']
cachedPyPath = os.environ['PYTHONPATH']

if '-create' in sys.argv or '-publish' in sys.argv:
    reverseList.remove('PATH')
    reverseList.remove('PYTHONPATH')
    preserveList = preserveList + ['PATH', 'PYTHONPATH']  
#
#

# extract the env setting introduced by scram
curDir = os.getcwd() 
os.chdir(str(os.environ['CMSSW_BASE'])+'/src')
sout, sin, serr = popen2.popen3('echo `scramv1 runtime -%s`'%str(shKind))
os.chdir(curDir) 
out = sout.readlines()[0]
err = serr.readlines()
sout.close()
sin.close()
serr.close()

if len(err) != 0:
     print 'echo "Error while summoning scramv1: %s"'%out
     sys.exit(stat)

out = out.replace('export ','').replace('=',' ').replace('setenv ', '')
outl = out.split(';')[:-1]

pre_env = {}
drop_out_cmd = ''
fakePath = '' # needed for source ordering in -create # Fabio

for e in outl:
     vl = e.strip()
     k = str(vl.split(' ')[0])
     v = str(vl.split(' ')[1].replace('"','')).split(':')

     if k == 'PATH':
         fakePath = vl.split(' ')[1].replace('"','')

     if 'SCRAMRT' not in k:
         pre_env[k] = []+v
     else:
         drop_out_cmd = drop_out_cmd + 'unset %s;\n'%k

# unfold the current env from the scram attributes
unfold_cmd = 'setenv CRAB_UNFOLD_ENV "1";\n'
if shKind == 'sh':
     unfold_cmd = 'export CRAB_UNFOLD_ENV="1";\n'

## cache to reuse   at creation Level .DS 
for v in os.environ:
     if v in preserveList:
          continue
     if 'SCRAMRT' in v:
          unfold_cmd = unfold_cmd + 'unset %s;\n'%v
          continue
     if v not in pre_env:
          continue

     entry = ''+str(os.environ[v])
     purgedList = [ i for i in entry.split(':') if i not in pre_env[v] ]
     # manage reverseList special cases (ie move entries at the end of the env var)
     if v in reverseList:
          purgedList = purgedList + pre_env[v] 

     if len(purgedList)==0:
          unfold_cmd = unfold_cmd + 'unset %s;\n'%v
          continue
     entry = str(purgedList).replace('[','').replace(']','')
     entry = entry.replace('\'','').replace(', ',':')

     if shKind == 'sh':
          unfold_cmd = unfold_cmd + 'export %s="%s";\n'%(v, entry) 
     else:
          unfold_cmd = unfold_cmd + 'setenv %s "%s";\n'%(v, entry)

# export the environment
print drop_out_cmd + unfold_cmd

#
# -create special case: second part 
# reverse and export as AUXs both the PATH and the PYTHONPATH
# this value will be used by crab.py whenever both -create and -submit are used 
#
if '-create' in sys.argv or '-publish' in sys.argv:
     # source order bug # Fabio
     # reproduce a PATH as lcg->cmssw would produce and compare it with the cachedPath
     # if they differs then override the cached path with the prepared one (brute force ordering)
     #
     fakePath = fakePath[:-len(':$SCRAMRT_PATH')]
     newPath = fakePath + cachedPath.replace(fakePath,'')
     if shKind == 'sh':
         print 'export PATH="%s";\n'%newPath
     else:
         print 'setenv PATH "%s";\n'%newPath

     # PATH
     purgedList = [ i for i in cachedPath.split(':') if i not in pre_env['PATH'] ]
     purgedList = purgedList + pre_env['PATH'] 
     entry = str(purgedList).replace('[','').replace(']','').replace('\'','').replace(', ',':')
     if shKind == 'sh':
          print 'export AUX_SUBM_PATH="%s";\n'%entry
     else:
          print 'setenv AUX_SUBM_PATH "%s";\n'%entry
     # PYTHONPATH
     purgedList = [ i for i in cachedPyPath.split(':') if i not in pre_env['PYTHONPATH'] ]
     purgedList = purgedList + pre_env['PYTHONPATH']
     entry = str(purgedList).replace('[','').replace(']','').replace('\'','').replace(', ',':')
     if shKind == 'sh':
          print 'export AUX_SUBM_PY="%s";\n'%entry
     else:
          print 'setenv AUX_SUBM_PY "%s";\n'%entry
     
#
#
sys.exit(0)

 
