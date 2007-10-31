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

shKind = 'sh' # fix for wrapper shell, temporary #'csh' #Fabio
if 'BASH' in os.environ:
    shKind = 'sh'

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
for e in outl:
     v = e.strip()
     k = str(v.split(' ')[0])
     v = str(v.split(' ')[1].replace('"','')).split(':')
     if 'SCRAMRT' not in k:
         pre_env[k] = []+v
     else:
         drop_out_cmd = drop_out_cmd + 'unset %s;\n'%k

# unfold the current env from the scram attributes
unfold_cmd = 'setenv CRAB_UNFOLD_ENV "1";\n'
if shKind == 'sh':
     unfold_cmd = 'export CRAB_UNFOLD_ENV="1";\n'

if '-create' in sys.argv[1:]:
     for ek in ['PATH', 'PYTHONPATH']:
         entry = ''+str(os.environ[ek])
         purgedList = [ i for i in entry.split(':') if i not in pre_env[ek] ]
         purgedList = pre_env[ek] + purgedList
         entry = str(purgedList).replace('[','').replace(']','')
         entry = entry.replace('\'','').replace(', ',':')
         if shKind == 'sh':
             print 'export %s="%s";\n'%(ek, entry)
         else:
             print 'setenv %s "%s";\n'%(ek, entry)
     # auxiliary export for the -create -submit option
     if '-submit' in sys.argv[1:]:
         if shKind == 'sh':
             print 'export AUX_SCRAMPATH="%s";\n'%str(pre_env['PATH'])
         else:
             print 'setenv AUX_SCRAMPATH "%s";\n'%str(pre_env['PATH'])
     sys.exit(0)

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
sys.exit(0)

 
