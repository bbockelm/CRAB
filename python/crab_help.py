
###########################################################################
#
#   H E L P   F U N C T I O N S
#
###########################################################################

import common

import sys, os, string
import tempfile

###########################################################################
def usage():
    usa_string = common.prog_name + """ [options]
  The most useful general options (use '-h' to get complete help):
  -create n -- Create only n jobs. Default is 'all'. bunch_creation will become obsolete
  -submit n -- Submit only n jobs. Default is 0. bunch_submission will become obsolete   
  -mon | -monitor | -autoretrieve [value in secs] -- Retrieve the output at the end of the job, plus simple monitoring. No value means default to 60 seconds. autoretrieve will become obsolete
  -continue [dir] | -c [dir]     -- Continue creation and submission of jobs from <dir>.
  -h [format]         -- Detailed help. Formats: man (default), tex, html.
  -cfg fname          -- Configuration file name. Default is 'crab.cfg'.
  -use_boss flag      -- If flag = 1 then BOSS will be used. Default is 0.
  -use_jam  flag      -- If flag = 1 the JAM monitoring is used. Default is 0
  -v                  -- Print version and exit.

Example:
  crab.py -create 1 -submit 1
"""
    print usa_string
    sys.exit(2)

###########################################################################
def help(option='man'):
    help_string = """
=pod

=head1 NAME

B<CRAB>:  B<C>ms B<R>emote B<A>nalysis B<B>uilder

"""+common.prog_name+""" version: """+common.prog_version_str+""" to use with PubDB_V3_1

This tool _must_ be used from an User Interface and the user is supposed to
have a valid GRID certificate and an active proxy.

B<WARNING: THIS HELP IS OBSOLETE !!!>

=head1 SYNOPSIS

B<"""+common.prog_name+""".py> [I<options>]


=head1 DESCRIPTION

CRAB is a Python program intended to simplify the process of creation and submission into GRID environment of CMS analysis jobs.

Parameters and card-files for analysis are to by provided by user changing the configuration file crab.cfg.

CRAB generates scripts and additional data files for each job. The produced scripts are submitted directly to the Grid.


=head1 BEFORE STARTING

A) Develop your code in your ORCA working area, build with the usual `scram project ORCA ORCA_X_Y_Z`
   From user scram area (which can be anywhere eg  /home/fanzago/ORCA_8_2_0/), issue the usual command
   > eval `scram runtime -sh|csh`

B) Move to your CRAB working area (that is UserTools/src) and modify the configuration file "crab.cfg" (into UserTools/src).
   The most important section is called [USER] where the user declares:

   Mandatory!
      *) dataset and owner to analyze
      *) the ORCA executable name (e.g. EXDigiStatistics).
         CRAB finds the executable into the user scram area (e.g. /home/fanzago/ORCA_8_2_0/bin/Linux__2.4/here!).
      *) the name of output produced by ORCA executable. Empty entry means no output produced
      *) the total number of events to analyze, the number of events for each job and the number of the first event to analyze.
      *) the orcarc card to use. This card will be modified by crab according with the job splitting.
         Use the very same cars you used in your interactive test: CRAB will modify what is needed.

   Might be useful:
      *) additional_input_files (from 005)
         Comma separated list of files to be submitted via input sandbox. 
         The files will be put in the working directory on WN. It's user responsibility to actually use them!
      *) data_tiers (new from ver 004)
         The possible choices are "DST,Digi,Hit" (comma separated list, mind the case!)
         If set, the job will be able to access not only the data tier corresponding to the dataset/owner asked, but also
         to its "parents". This requires that the parents are actually published in the same site of the primary dataset/owner.
         If not set, only the primary data tier will be accessible
      *) output_dir e log_dir, path of directory where crab will put the std_error and std_output of job.
         If these parameters are commented, error and output will be put into the directory where sh and jdl script are (crab_0_date_time).
         These parameter will be use only with the automatic retrieve of output option (-autoretrieve)

   Optional:
      *) how to pack the ORCA code provided by the user  (up to now is possible only as tgz)
      *) the name of tgz archive ( called by default "default.tgz")
      *) run_jam e output_jam are parameter for JAM monitoring (used with option use_jam)
      *) the name of UI directory where crab will create jobs. By default the name is "crab_data_time"

C) Before submitting jobs, user needs to create a proxy with the command:
     grid-proxy-init 

   At CERN, you can use "lxplus" as a UI by sourcing the file
     source /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.csh
                                                                                                                                                             
   WARNING:
     Since the LCG working nodes actually installed on different site still use RedHat7.3, you can only submit jobs from a UI based on RH7.3. 
     At CERN, this is possible using "lxplus7".

=head1 HOW TO RUN CRAB FOR THE IMPATIENT USER

Please, read all anyway!
                                                                                                                                                             
>  ./crab.py -create 2
  create 2 jobs (no submission!)
                                                                                                                                                             
>  ./crab.py -bunch_create 0 -submit 2  -continue [ui_working_dir]
  create 0, submit 2, the ones already created (-continue)
                                                                                                                                                             
>  ./crab.py -create 2 -submit 2
  create _and_ submit 2 jobs
                                                                                                                                                             
>  ./crab.py -create 2 -submit 2 -autoretrieve
  create, submit 2 jobs and retrieve the output at the end, plus simple monitoring


=head1 HOW TO RUN CRAB

The executable file is crab.py
                                                                                                                                                             
I<If you want only create jobs (NO submission):>
  >  ./crab.py -create 2

Crab creates a directory called crab_0_"data"_"time" where you can find 4 subdirectories
 job:    contains sh, jdl and card
 share:  contains the "file_to_send", that provides the informations retrieved by local_pubdb
 log:    there are the log of crab and the grid ID of submitted jobs
 res     empty...
                                                                                                                                                             
The option "register_data" allows to copy and register the ORCA output (e.g.the .root file) into a 
Storage element and RLS catalog ( .root file). To do it, put register_data 1 (see information about this option)

I<If you to submit the previously created jobs:>
  >  ./crab.py -create 0 -submit 2  -continue [ui_working_dir]
 (the submission is done using edg-job-submit command).

To see the status of job, the user needs to run
  > edg-job-status -i crab_data_time/log/submission_id.log

To retrieve the output
  > edg-job-get-output -i crab_data_time/log/submission_id.log

If you want to use the automatic retrieve of output, add the option "-autoretrieve"
  >  ./crab.py -create 0 -submit 2  -autoretrieve -continue [ui_working_dir]
                                                                                                                                                             
In this case the monitoring (status) and the get-output will be done I<automatically>.
You can find some information about the status into the log of crab (directory crab_0_data_time/log).

The job monitoring and output retrieval runs asynchronously, that is you can submit your jobs and only afterwards
start the retrieval of output.
  >  ./crab.py -create 0 -submit 2 -continue [ui_working_dir]
  >  ./crab.py -create 0 -submit 0  -autoretrieve -continue [ui_working_dir]
                                                                                                                                                             
If you want to use the JAM monitornig, add the option -use_jam (0 by default)
  > ./crab.py -create 1  -submit 1 -use_jam 1 -autoretrieve


=head1 SOME OTHER INFO:
                                                                                                                                                           
You can find a useful file into directory "ui_working_dir"/share/script.list.
 Here are written the name of job (with jobsplitting) that are to be created and submitted.
 Near the name a letter that means:
                                                                                                                                                             
 X = job to create
 C = job created but not submitted
 S = job submitted
 M = job being monitored


=head1 KNOWN PROBLEMS:
                                                                                                                                                             
1) It is possible to read a warning messagge when crab start to run, depending on ORCA version (e.g. ORCA_8_6_0):
  .../src/scram.py:13: DeprecationWarning: Non-ASCII character '\xa7' in file /opt/edg/bin/UIutils.py on line 225, but no encoding declared; see http://www.python.org/peps/pep-0263.html for details 
  import UIutils
  /opt/edg/lib/python/edg_wl_userinterface_common_NsWrapper.py:4: RuntimeWarning: Python C API version mismatch for module _edg_wl_userinterface_common_NsWrapper: This Python has API version 1012, module _edg_wl_userinterface_common_NsWrapper has version 1011.
  import _edg_wl_userinterface_common_NsWrapper
  /opt/edg/lib/python/edg_wl_userinterface_common_LbWrapper.py:4: RuntimeWarning: Python C API version mismatch for module _edg_wl_userinterface_common_LbWrapper:
  This Python has API version 1012,
  module _edg_wl_userinterface_common_LbWrapper has version 1011.
  import _edg_wl_userinterface_common_LbWrapper
  /opt/edg/lib/python/edg_wl_userinterface_common_AdWrapper.py:4: RuntimeWarning: Python  C API version mismatch for module  _edg_wl_userinterface_common_AdWrapper: This Python has API version 1012,
                                                                                                                                                             
It seems to depend on a mismatch between the version of python used by
ORCA_8_6_0 and the version used to "compile" /opt/edg/etc/bin/UIutils
                                                                                                                                                             
Not critical !
                                                                                                                                                             
                                                                                                                                                             
2) If you are using the option -autoretrieve, when the submission step
finishes, the shell prompt doesn't retun. Just press enter!

3) If you use -monitor and then exit the shell, the autoretrieve thread are killed...


=head1 WORK IN PROGRESS:

 Implementing BOSS monitoring.
 Changing monitor function.
 Final merging


=head1 OPTIONS

=over 4

=item B<-bunch_creation n | -create n>

Create n jobs maximum. 'n' is either positive integer or 'all'.
Default is 'all'.
See also I<-continue>.

=item B<-bunch_size n>

The same as '-bunch_creation n' and '-bunch_submission n'.

=item B<-bunch_submission n | -submit n>

Submit n jobs maximum. 'n' is either positive integer or 'all'.
Default is 0.
See also I<-continue>.

=item B<-mon | -monitor | -autoretrieve>

With this option the monitoring (status) and the get-output of jobs will be done I<automatically>.
You can find some information about the status into the log of crab (directory crab_0_data_time/log).

=item B<-continue [dir]>

Continue submission of batch jobs from 'dir'. 'dir' is a top level directory
created when scripts were generated.
By default the name of the dir is I<crab_0_date_time>. 
If the name of dir is different (selected by the user, changing in crab.cfg file the ui_working_dir parameter),
it is necessary to specify it in -continue "ui_working_dir"

Examples:
   1) Into the cfg file the line "ui_working_dir" is commented:
      the command
      > ./crab.py  -create 1 -submit 1 -register_data 0
      creates and submit 1 job. The name of directory where the job is creates, is ".../UserTools/src/crab_data_time"
                                                                                                                                                             
      If you want to create and submitt an other jobs:
      > ./crab.py -create 1 -submit 1 -register_data 0 -continue
      the job will be created into the same directory  ".../UserTools/src/crab_data_time"
   
   2) Into the cfg file the line "ui_working_dir" is uncommented:
      the command
      > ./crab.py -create 1 -submit 1 -register_data 0
      creates and submit 1 job. The directory where the job is creates, is ".../UserTools/src/'ui_working_dir'"
                                                                                                                                                             
      If you want to create and submitt 1 other jobs:
      > ./crab.py -create 1 -submit 1 -register_data 0 -continue 'ui_working_dir'
      In this case you need to specified the name of directory
                                                                                                                                                             
                                                                                                                                                             
Another way to modify the value of parameter into the cfg file, without change the cfg file, is to write like option the parameter that you want to change.

Example:
   > ./crab.py -create 1 -submit 1 -register_data 0  -USER.ui_working_dir name_that_you_want
   and to continue                                                                                                                      
   > ./crab.py -create 1 -submit 1 -register_data 0 -continue name_that_you_want

=item B<-h [format]>

Detailed help. Formats: man (default), tex, html.

=item B<-ini fname>

Configuration file name. Default is 'crab.cfg'.
I<'none'> is a special value used to ignore the default file.

=item B<-Q>

Quiet mode, i.e. no output on the screen.

=item B<-register_data flag>

register_data 1  allows to copy and register the output of ORCA executable into
the Storage Element "close" to the Worker node where the job is running, or, if
the close has problem, into a storage element provided by the user into the
configuration file.

Into crab.cfg:
   [EDG]
   ...
   storage_element = gridit002.pd.infn.it   <--- name of "backup storage element" (to use if the CloseSE isn\'t available)
   storage_path = /flatfiles/SE00/cms/      <--- directory into the SE where a cms user can write
   ...

   [USER]
   output_storage_subdir = fede/orca/25_11_2004/   <--- subdirectory of cms area where the output will be stored
   Example: we can found the output stored in
      1) closeSE/mountpoint_cms/[USER].output_storage_subdir/[USER].output_file
      or (if close has problem)
      2) [EDG].storage_element/[EDG].storage_path/[USER].output_storage_subdir/[USER].output_file
                                                                                                                                                             
      into RLS the lfn = [USER].output_storage_subdir/[USER].output_file will be registered

The value of "register_data" parameter can be written into the cfg file into the section
   [CRAB]
   ...                                                                                                                                             
   register_data = 0    or
   register_data = 1                                                                                                                                                             
in order to avoid to write it like command line option.
 Default is 0

=item B<-return_data flag>

If flag = 0 then produced data will not be returned to user.
Default is 0 for 'edg' and always 1 for local schedulers.

=item B<-use_boss flag>

If flag = 1 then the BOSS metascheduler will be used.
Default is 0, i.e. BOSS is not used.

=item B<-usecloseCE>

If flag = 1 then in jdl and classad files are written InputData that
contains LFN of input_data, ReplicaCatalog that contains RLS URL (for
example rls://datatag2.cnaf.infn.it) and DataAccessProtocol that contains
protocol used to data access (for example gsiftp).
In this case the Resource Broker selects a CE closest to SE where input_data
are stored, in order to run jobs.

=item B<-V>

Verbose, i.e. produce more output.

=item B<-v>

Print version.

=item B<->I<any_key value>

Any unrecognized option is treated as a configuration parameter with
specified value. Can be used for the command-line redefinition
of configuration parameters from an ini-file. For example, a user wants
to submit jobs into EDG but he does not like the default User
Interface configuration file in which a location of a Resource Broker is
specified. One possibility is to edit the ini-file
changing the value of the 'rb_config' parameter in the 'EDG' section.
The second possibility is to provide this value as a command-line
option: I<-EDG.rb_config my_ui_config>.

Can be used also for specification of private production parameters, e.g.
I<-Private.executablename myjob> (Note all lowercase letters in the second part
of the option, i.e. after the dot).

=back


=head1 FILES

I<crab> uses initialization file I<crab.cfg> which contains
configuration parameters. This file is written in the Windows INI-style.
The default filename can be changed by the I<-cfg> option.

I<crab> creates by default a working directory
'crab_0_E<lt>dateE<gt>_E<lt>timeE<gt>'

I<crab> saves all command lines in the file I<crab.history>.


=head1 HISTORY

B<crab> is a tool for the CMS analysis on the grid environment.
It is based on the ideas from CMSprod, a production tools
implemented by Nikolai Smirnov.

=head1 AUTHORS

"""
    author_string = '\n'
    for auth in common.prog_authors:
        #author = auth[0] + ' (' + auth[2] + ')' + ' E<lt>'+auth[1]+'E<gt>,\n'
        author = auth[0] + ' E<lt>' + auth[1] +'E<gt>,\n'
        author_string = author_string + author
        pass
    help_string = help_string + author_string[:-2] + '.'\
"""

=cut
    """

    pod = tempfile.mktemp()+'.pod'
    pod_file = open(pod, 'w')
    pod_file.write(help_string)
    pod_file.close()

    if option == 'man':
        man = tempfile.mktemp()
        pod2man = 'pod2man --center=" " --release=" " '+pod+' >'+man
        os.system(pod2man)
        os.system('man '+man)
        pass
    elif option == 'tex':
        fname = common.prog_name+'-v'+common.prog_version_str
        tex0 = tempfile.mktemp()+'.tex'
        pod2tex = 'pod2latex -full -out '+tex0+' '+pod
        os.system(pod2tex)
        tex = fname+'.tex'
        tex_old = open(tex0, 'r')
        tex_new = open(tex,  'w')
        for s in tex_old.readlines():
            if string.find(s, '\\begin{document}') >= 0:
                tex_new.write('\\title{'+common.prog_name+'\\\\'+
                              '(Version '+common.prog_version_str+')}\n')
                tex_new.write('\\author{\n')
                for auth in common.prog_authors:
                    tex_new.write('   '+auth[0]+
                                  '\\thanks{'+auth[1]+'} \\\\\n')
                tex_new.write('}\n')
                tex_new.write('\\date{}\n')
            elif string.find(s, '\\tableofcontents') >= 0:
                tex_new.write('\\maketitle\n')
                continue
            elif string.find(s, '\\clearpage') >= 0:
                continue
            tex_new.write(s)
        tex_old.close()
        tex_new.close()
        print 'See '+tex
        pass
    elif option == 'html':
        fname = common.prog_name+'-v'+common.prog_version_str+'.html'
        pod2html = 'pod2html --title='+common.prog_name+\
                   ' --infile='+pod+' --outfile='+fname
        os.system(pod2html)
        print 'See '+fname
        pass

    sys.exit(0)
