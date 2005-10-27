
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

  -create n           -- Create only n jobs. Default is 'all'. bunch_creation will become obsolete
  -submit n           -- Submit only n jobs. Default is 0. bunch_submission will become obsolete   
  -status [range]     -- check status of all jobs: if range is defined, only of selected jobs
  -getoutput [range]  -- get back the output of all jobs: if range is defined, only of selected jobs
  -kill [range]       -- kill submitted jobs
  -clean              -- gracefully cleanup the idrectory of a task
  -testJdl [range]    -- check if resources exist which are compatible with jdl
  -postMortem [range] -- provide a file with information useful for post-mortem analysis of the jobs
  -continue [dir] | -c [dir]     -- Apply command to task stored in [dir].
  -h [format]         -- Detailed help. Formats: man (default), tex, html.
  -cfg fname          -- Configuration file name. Default is 'crab.cfg'.
  -use_boss flag      -- If flag = 1 then BOSS will be used. Default is 0.
  -debug N            -- set the verbosity level to N
  -v                  -- Print version and exit.

  "range" has syntax "n,m,l-p" which correspond to [n,m,l,l+1,...,p-1,p] and all possible combination

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

"""+common.prog_name+""" version: """+common.prog_version_str+"""

This tool _must_ be used from an User Interface and the user is supposed to
have a valid GRID certificate.

=head1 SYNOPSIS

B<"""+common.prog_name+""".py> [I<options>]

=head1 DESCRIPTION

CRAB is a Python program intended to simplify the process of creation and submission into grid environment of CMS analysis jobs.

Parameters for CRAB usage and configuration are provided by the user changing the configuration file B<crab.cfg>.

CRAB generates scripts and additional data files for each job. The produced scripts are submitted directly to the Grid. CRAB makes use of BOSS to interface to the grid scheduler, as well as for logging and bookkeeping and eventually real time monitoring.

CRAB supports any ORCA based executable, including the user provided one, and deals with the output produced by the executable. CRAB provides an interface with CMS data discovery services (today RefDB and PubDB), which are completely hidden to the final user. It also splits a task (such as analyzing a whole dataset) into smaller jobs, according with user requirements.

=head1 BEFORE STARTING

=over 4

=item B<A)>

Develop your code in your ORCA working area (both I<scram> and I<scramv1> based ORCA are supported).
Does anything which is needed to run interactively your executable, including the setup or run time environment (I<eval `scram(v1) runtime -sh|csh`>), a suitable I<.orcarc>, etc.

=item B<B)> 

Source B<crab.(c)sh> from the CRAB installation area, which have been setup either by you or by someone else for you.
Modify the CRAB configuration file B<crab.cfg> according to your need: see below for a complete list. The most important parameters are the following:

=over 4

=item B<Mandatory!>

=item o dataset, owner and data tiers to be accessed

=item o the ORCA executable name (e.g. ExDigiStatistics): can be a user built executable or one available from the standard release (such as ExDigiStatistics)

=item o the name of output file(s) produced by ORCA executable.

=item o job splitting directives: the total number of events to analyze, the number of events for each job or the number of jobs and eventually the first event

=item o the B<.orcarc> card to be used. This card will be modified by crab for data access and according to the job splitting. Use the very same cars you used in your interactive test: CRAB will modify what is needed.

=item B<Might be useful:>

=item o Comma separated list of files to be submitted via input sandbox. The files will be put in the working directory on WN. It's user responsibility to actually use them!

=item o output_dir e log_dir, path of directory where crab will put the std_error and std_output of job.  If these parameters are commented, error and output will be put into the directory where sh and jdl script are (crab_0_date_time).

=item B<Optional:>

=item o the name of UI directory where crab will create jobs. By default the name is "crab_data_time"

=back

=item B<C)> 

As stated before, you need to have a valid grid certificate (see CRAB web page for instruction) to submit to the grid. You need also a valid proxy (obtained via B<grid-proxy-init>): if you don't have it (or if it is too short), CRAB will issues that command for you.

At CERN, you can use "lxplus" as a UI by sourcing the file B</afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.(c)sh>

=back

=head1 HOW TO RUN CRAB FOR THE IMPATIENT USER

Please, read all anyway!

~>crab.py -create 2
  create 2 jobs (no submission!)

~>crab.py -submit 2 -continue [ui_working_dir]
  submit 2 jobs, the ones already created (-continue)

~>crab.py -create 2 -submit 2
  create _and_ submit 2 jobs

~>crab.py -status
  check the status of all jobs

~>crab.py -getoutput
  get back the output of all jobs

=head1 COMMAND

=over 4

=item B<-create n>

Create n jobs: 'n' is either a positive integer or 'all' (default).
The maximum number of jobs depens on dataset and splittig directives: if more are asked for, a warning is issued and job are created up to the maximum possible. This set of identical jobs accessing the same dataset are defined as a task.
This command create a directory with default name is I<crab_0_date_time> (can be changed via ui_working_dir parameter, see below). Inside this directory it is placed whatever is needed to submit your jobs. Also the output of your jobs (once finished) will be place there (see after). Do not cancel by hand this directory: rather use -clean (see).
See also I<-continue>.

=item B<-submit n>

Submit n jobs: 'n' is either a positive integer or 'all'. Default is all.
This option must be used in conjunction with -create (to create and submit immediately) or with -continue, to submit previously created jobs. Failure to do so will stop CRAB and generate an error message.
See also I<-continue>.

=item B<-continue [dir] | -c [dir]>

Apply the action on the task stored on directory [dir]. If the task directory is the standard one (crab_0_date_time), the more recent in time is taken. Any other directory must be specified.
Basically all commands (but -create) need -continue, so it is automatically assumed, with exception of -submit, where it must be explicitly used. Of course, the standard task directory is used in this case.

=item B<-status [range]>

Check the status of the jobs, in all states. If BOSS real time monitor is enabled, also some real time information are available, otherwise all the info will be available only after the output retrieval. See I<range> below for syntax.

=item B<-getoutput [range]>

Retrieve the output declared by the user via the output sandbox. By default the output will be put in task working dir under I<res> subdirectory. This can be changed via config parameters. See I<range> below for syntax.

=item B<-resubmit [range]>

Resubmit jobs which have been previously submitted and have been either I<killed> or are I<aborted>. See I<range> below for syntax. 

=item B<-kill [range]>

Kill (cancel) jobs which have been submitted to the scheduler. A range B<must> be used in all cases, no default value is set.

=item B<-testJdl [range]>

Check if the job can find compatible resources. It's equivalent of doing I<edg-job-list-match> on edg.

=item B<-postMortem [range]>

Produce a file (via I<edg-job-logging-info -v 2>) which might help in undertanding grid related problem for a job.

=item B<-clean [dir]>

Clean up (i.e. erase) the task working directory after a check whether there are still running jobs. In case, you are notified and asked to kill them or retrieve their output. B<Warning> this will eventually delete also the output produced by the task (if any)!

=item B<-help [format] | -h [format]>

This help. It can be produced in three different I<format>: I<man> (default), I<tex> and I<html>.

=item B<-v>

Print the version and exit.

=item B<range>

The range to be used in many of the above command has the following syntax. It is a comma separated list of jobs ranges, each of which may be a job number, or a job range of the form first-last.
Example: 1,3-5,8 = {1,3,4,5,8}

=head1 OPTION

=item B<-cfg [file]>

Configuration file name. Default is B<crab.cfg>.

=item B<-debug [level]>

Set the debug level.

=head1 CONFIGURATION PARAMETERS

All the parameter describe in this section can be defined in the CRAB configuration file. The configuration file has different sections: [CRAB], [USER], etc. Each parameter must be defined in its proper section. An alternative way to pass a config parameter to CRAB is to to it via command line interface; the syntax is: crab.py -section.key value .
The parameters passed to CRAB at the creation step are stored, so they cannot be changed by changing the original crab.cfg . On the other hand the task is protected from any accidental change. If you want to change any parameters, this require the creation of a new task.

B<[CRAB]>
=over 2

=item B<jobtype>

=item B<scheduler>

=item B<use_boss>

=back

B<[USER]>
B<[EDG]>

Examples:
   1) In the cfg file the line "ui_working_dir" is commented:
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
