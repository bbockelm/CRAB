#!/usr/bin/env python
import sys, os, string

debug = 1
if debug :
    output = open('queryLog','w')

# save bossId in dictionary with empty status
boss_ids = {}
for line in sys.stdin:
    boss_ids[line.strip()] = ''


# get schedd/jobids from boss_ids and save them in dictionary { 'schedd' : [id,id,..] , ... }
job_ids = {}
for id in boss_ids.keys():
    # extract schedd and id from bossId
    schedd = id.split('//')[0]
    id     = id.split('//')[1]
    # fill dictionary
    if schedd in job_ids.keys() :
        job_ids[schedd].append(id)
    else :
        job_ids[schedd] = [id]

# call condor_q for each schedd in job_ids, parsing output and storing status in boss_ids
for schedd in job_ids.keys() :

    if debug :
        output.write(schedd+'\n')

    # call condor_q
    cmd = 'condor_q -name ' + schedd + ' ' + os.environ['USER']
    (input_file,output_file) = os.popen4(cmd)

    # parse output and store status in dictionary { 'id' : 'status' , ... }
    condor_status = {}
    for line in output_file.readlines() :
        line = line.strip()
        if debug :
            output.write(line+'\n')
        try:
            line_array = line.split()
            if line_array[1].strip() == os.environ['USER'] :
                condor_status[line_array[0].strip()] = line_array[5].strip()
        except:
            pass

    # go through job_ids[schedd] and save status in boss_ids
    for id in job_ids[schedd] :
        for condor_id in condor_status.keys() :
            if condor_id.find(id) != -1 :
                status = condor_status[condor_id]
                output.write(status+'\n')
                if ( status == 'I' ):
                    boss_ids[schedd+'//'+id] = 'I'
                elif ( status == 'U' ) :
                    boss_ids[schedd+'//'+id] = 'RE'
                elif ( status == 'H' ) :
                    boss_ids[schedd+'//'+id] = 'SA'
                elif ( status == 'R' ) :
                    boss_ids[schedd+'//'+id] = 'R'
                elif ( status == 'X' ) :
                    boss_ids[schedd+'//'+id] = 'SK'
                elif ( status == 'C' ) :
                    boss_ids[schedd+'//'+id] = 'SD'
                else :
                    boss_ids[schedd+'//'+id] = 'UN'

# print status output using boss_ids
# if no status was filled (job already removed from queue, set status to SD
for id in boss_ids.keys() :
    status = boss_ids[id]
    if status == '' :
        status_output = str(id)+' SD'
    else :
        status_output = str(id)+' '+str(status)
    if debug :
        output.write(status_output+'\n')
    print status_output
        




