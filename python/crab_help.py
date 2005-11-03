
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

B<"""+common.prog_name+""".py> [I<options>] [I<command>]

=head1 DESCRIPTION

CRAB is a Python program intended to simplify the process of creation and submission into grid environment of CMS analysis jobs.

Parameters for CRAB usage and configuration are provided by the user changing the configuration file B<crab.cfg>.

CRAB generates scripts and additional data files for each job. The produced scripts are submitted directly to the Grid. CRAB makes use of BOSS to interface to the grid scheduler, as well as for logging and bookkeeping and eventually real time monitoring.

CRAB supports any ORCA based executable, including the user provided one, and deals with the output produced by the executable. CRAB provides an interface with CMS data discovery services (today RefDB and PubDB), which are completely hidden to the final user. It also splits a task (such as analyzing a whole dataset) into smaller jobs, according with user requirements.

CRAB web page is available at I<http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab/>

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

=item o dataset, owner to be accessed: also data_tiers if you want more than the one pointed by owner

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

=back 

=head1 OPTION

=over 4

=item B<-cfg [file]>

Configuration file name. Default is B<crab.cfg>.

=item B<-debug [level]>

Set the debug level.

=back 

=head1 CONFIGURATION PARAMETERS

All the parameter describe in this section can be defined in the CRAB configuration file. The configuration file has different sections: [CRAB], [USER], etc. Each parameter must be defined in its proper section. An alternative way to pass a config parameter to CRAB is to to it via command line interface; the syntax is: crab.py -SECTION.key value . For example I<crab.py -USER.outputdir MyDirWithFullPath> .
The parameters passed to CRAB at the creation step are stored, so they cannot be changed by changing the original crab.cfg . On the other hand the task is protected from any accidental change. If you want to change any parameters, this require the creation of a new task.
Mandatory parameters are flagged with a *.

B<[CRAB]>

=over 2

=item B<jobtype *>

The type of the job to be executed: for the time being only I<orca> jobtype are supported

=item B<scheduler *>

The scheduler to be used: I<edg> is the grid one. In later version also other scheduler (local and grid) will be possible, including eg glite, condor-g, lsf, pbs, etc...

=item B<use_boss>

Flag to enable the use of BOSS for submission, monitoring etc.

=back

B<[USER]>

=over 2

=item B<dataset *>

The dataset the user want to analyze.

=item B<owner *>

The owner name whcih the user want to access. These two parameter can be found using data discovery tool: for the time being, RefDB/PubDB. See production page (linked also from CRAB web page) for access to the list of available dataset/owner

=item B<data_tier>

The data tiers the user want to access: by deafult, only the tier corresponding to the given owner will be provided. If user needs more, he B<must> specify the full list.
Syntax: comma separated list, known tiers are I<Hits>,I<Digi>, I<DST>

=item B<order_catalogs>

Define the order of the catalogs which will be put in the generated .orcarc fragment: for expert use only.

=item B<executable *>

The ORCA executable the user want to run on remote site. This must be on the I<path> of the user at the time of the creation of the jobs, so it's mandatory to issue the usual I<eval `scramv1 runtim -(c)sh`> from user ORCA working area before creating jobs. A warning will be prompted if the executable is not found in the path.

=item B<script_exe>

Name of a script that the user want to execute on remote site: full path must be provided. The ORCA executable whcih is executed by the script must be declared in any case, since CRAB must ship it to the remote site. The script can do anything, but change directory before the ORCA executable start. On the remote WN, the full scram-based environment will be found.

=item B<output_file *>

Output files as produced by the executable: comma separated list of all the files. These are the file names which are produced by the executable also in the interactive environment. The output files will be modified by CRAB when returned in order to cope with the job splitting, by adding a "_N" to each file.

=item B<additional_input_files>

Any additional input file you want to ship to WN: comma separated list. These are the files which might be needed by your executable: they will be placed in the WN working dir. Please note that the full I<Data> directory of your private ORCA working are will be send to WN (if present). 

=item B<orcarc_file *>

User I<.orcarc> file: if it is not in the current directory, full path is needed. Use the very same file you used for your interactive test: CRAB will modify it according to data requested and splitting directives.

=item B<first_event>

The first event the user want to analyze in the dataset. Default is 0.

=item B<total_number_of_events *>

The total number of events the user want to analyze. I<-1> means all available events. If first even is set, a gran total of (total available events)-(first event) will be analyzed.

=item B<job_number_of_events *>

Numer of event for each job. Either this or I<total_number_of_jobs> must be defined.

=item B<total_number_of_jobs>

Total numer of jobs in which the task will be splitted. It is incompatible with previous I<job_number_of_events>. If both are set, this card will be ignored and a warning message issued.

=item B<ui_working_dir>

Name of the working directory for the current task. By default, a name I<crab_0_(date)_(time)> will be used. If this card is set, any CRAB command which require I<-continue> need to specify also the name of the working directory. A special syntax is also possible, to reuse the name of the dataset provided before: I<ui_working_dir : %(dataset)s> . In this case, if eg the dataset is SingleMuon, the ui_working_dir will be set to SingleMuon as well.

=item B<return_data *>

The output produced by the ORCA executable on WN is returned (via output sandbox) to the UI, by issuing the I<-getoutput> command. B<Warning>: this option should be used only for I<small> output, say less than 10MB, since the sandbox cannot accomodate big files. Depending on Resource Broker used, a size limit on output sandbox can be applied: bigger files will be truncated. To be used in alternative to I<copy_data>.

=item B<outputdir>

To be used together with I<return_data>. Directory on user interface where to store the ORCA output. Full path is mandatory, "~/" is not allowed: the defaul location of returned output is ui_working_dir/res .

=item B<logdir>

To be used together with I<return_data>. Directory on user interface where to store the ORCA standard output and error. Full path is mandatory, "~/" is not allowed: the defaul location of returned output is ui_working_dir/res .

=item B<copy_data *>

The output (only the ORCA one, not the std-out and err) is copied to a Storage Element of your choice (see below). To be used as an alternative to I<return_data> and recomended in case of large output.

=item B<storage_element>

To be used together with I<copy_data>. Storage Element name.

=item B<storage_path>

To be used together with I<copy_data>. Path where to put output files on Storage Element. Full path is needed, and the directory must be writeable by all.

=item B<register_data>

To be used together with I<copy_data>. Register output files to RLS catalog: for advanced LCG users.

=item B<lfn_dir>

To be used together with I<register_data>. Path for the Logical File Name.

=item B<activate_MonALisa>

Activate MonaLisa monitoring of running jobs on WN.

=item B<use_central_bossDB>

Use central BOSS DB instead of one for each task: the DB must be already been setup. See installation istruction for more details.

=item B<use_boss_rt>

Use BOSS real time monitoring.

=back

B<[EDG]>

=over 2

=item B<lcg_version>

Version of LCG middleware to be used.

=item B<config>

Configuration file to change the resource broker to be used (download files from CRAB web page)

=item B<config_vo>

Configuration file to change the resource broker to be used (download files from CRAB web page). Both I<conig> and I<config_vo> must be set.

=item B<requirements>

Any other requirements to be add to JDL. Must be written in compliance with JDL syntax (see LCG user manual for further info). No requirement on Computing element must be set.

=item B<max_cpu_time>

Maximum CPU time needed to finish one job. It will be used to select a suitable queue on the CE. Time in minutes.

=item B<max_wall_clock_time>

Same as previous, but with real time, and not CPU one.

=item B<CE_black_list>

All the CE whose name contains the following strings (comma separated list) will not be considered for submission.  Use the dns domain (eg fnal, cern, ifae, fzk, cnaf, lnl,....)

=item B<CE_white_list>

Only the CE whose name contains the following strings (comma separated list) will be considered for submission.  Use the dns domain (eg fnal, cern, ifae, fzk, cnaf, lnl,....)

=item B<virtual_organization>

You don't want to change this: it's cms!

=item B<retry_count>

Number of time the grid will try to resubmit your job in case of grid related problem.

=back

=head1 FILES

I<crab> uses a configuration file I<crab.cfg> which contains configuration parameters. This file is written in the INI-style.  The default filename can be changed by the I<-cfg> option.

I<crab> creates by default a working directory 'crab_0_E<lt>dateE<gt>_E<lt>timeE<gt>'

I<crab> saves all command lines in the file I<crab.history>.


=head1 HISTORY

B<crab> is a tool for the CMS analysis on the grid environment. It is based on the ideas from CMSprod, a production tools implemented originally by Nikolai Smirnov.

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
