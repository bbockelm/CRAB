
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
  -getoutput|-get [range]   -- get back the output of all jobs: if range is defined, only of selected jobs
  -kill [range]       -- kill submitted jobs
  -cancelAndResubmit [range]  -- kill and resubmit submitted jobs
  -clean              -- gracefully cleanup the idrectory of a task
  -testJdl [range]    -- check if resources exist which are compatible with jdl
  -list n or range    -- show technical job details
  -postMortem [range] -- provide a file with information useful for post-mortem analysis of the jobs
  -printId [range]    -- print the job SID
  -continue|-c [dir]  -- Apply command to task stored in [dir].
  -h [format]         -- Detailed help. Formats: man (default), tex, html.
  -cfg fname          -- Configuration file name. Default is 'crab.cfg'.
  -debug N            -- set the verbosity level to N
  -v                  -- Print version and exit.

  "range" has syntax "n,m,l-p" which correspond to [n,m,l,l+1,...,p-1,p] and all possible combination

Example:
  crab.py -create 1 -submit 1
"""
    print 
    sys.exit(2)

###########################################################################
def help(option='man'):
    help_string = """
=pod

=head1 NAME

B<CRAB>:  B<C>ms B<R>emote B<A>nalysis B<B>uilder

"""+common.prog_name+""" version: """+common.prog_version_str+"""

This tool B<must> be used from an User Interface and the user is supposed to
have a valid GRID certificate.

=head1 SYNOPSIS

B<"""+common.prog_name+"""> [I<options>] [I<command>]

=head1 DESCRIPTION

CRAB is a Python program intended to simplify the process of creation and submission into grid environment of CMS analysis jobs.

Parameters for CRAB usage and configuration are provided by the user changing the configuration file B<crab.cfg>.

CRAB generates scripts and additional data files for each job. The produced scripts are submitted directly to the Grid. CRAB makes use of BOSS to interface to the grid scheduler, as well as for logging and bookkeeping and eventually real time monitoring.

CRAB supports any ORCA based executable, including the user provided one, and deals with the output produced by the executable. CRAB provides an interface with CMS data discovery services (today RefDB and PubDB), which are completely hidden to the final user. It also splits a task (such as analyzing a whole dataset) into smaller jobs, according with user requirements.  CRAB support also FAMOS based jobs.

CRAB web page is available at

I<http://cmsdoc.cern.ch/cms/ccs/wm/www/Crab/>

=head1 HOW TO RUN CRAB FOR THE IMPATIENT USER

Please, read all anyway!

Source B<crab.(c)sh> from the CRAB installation area, which have been setup either by you or by someone else for you.

Modify the CRAB configuration file B<crab.cfg> according to your need: see below for a complete list: in particular set your jobtype (orca or famos) and fill the corresponding section. A template and commented B<crab.cfg> can be found on B<$CRABDIR/python/crab.cfg>

~>crab.py -create 
  create all jobs (no submission!)

~>crab.py -submit 2 -continue [ui_working_dir]
  submit 2 jobs, the ones already created (-continue)

~>crab.py -create 2 -submit 2
  create _and_ submit 2 jobs

~>crab.py -status
  check the status of all jobs

~>crab.py -getoutput
  get back the output of all jobs

=head1 RUNNING CMSSW WITH CRAB

=over 4

=item B<A)>

Develop your code in your CMSSW working area.  Do anything which is needed to run interactively your executable, including the setup or run time environment (I<eval `scramv1 runtime -sh|csh`>), a suitable I<ParameterSet>, etc.It seems silly, but B<be extra sure that you actaully did compile your code> 

=item B<B)> 

Source B<crab.(c)sh> from the CRAB installation area, which have been setup either by you or by someone else for you.  Modify the CRAB configuration file B<crab.cfg> according to your need: see below for a complete list.

The most important parameters are the following (see below for complete description of each parameter):

=item B<Mandatory!>

=over 6

=item B<[CMSSW]> section: datasetpath, pset, splitting parameters, output_file

=item B<[USER]> section: output handling parameters, such as return_data, copy_data etc...

=back

=item B<Run it!>

You must have a valid voms-enabled grid proxy. See CRAB web page for details.

=back

=head1 RUNNING ORCA WITH CRAB

=over 4

=item B<A)>

The preliminary steps are the same as CMSSW (both I<scram> and I<scramv1> are supported). The most important parameters in I<crab.cfg> are the following:

=item B<Mandatory!>

=over 6

=item o dataset, owner to be accessed: also data_tiers if you want more than the one pointed by owner

=item o the ORCA executable name (e.g. ExDigiStatistics): can be a user built executable or one available from the standard release (such as ExDigiStatistics)

=item o the name of output file(s) produced by ORCA executable.

=item o job splitting directives: the total number of events to analyze, the number of events for each job or the number of jobs and maybe the first event. B<Note that from CRAB 1_2_0 the job-splitting parameters have been moved into [ORCA] section>

=item o the B<.orcarc> card to be used. This card will be modified by crab for data access and according to the job splitting. Use the very same cars you used in your interactive test: CRAB will modify what is needed.

=back

=item B<Might be useful:>

=over 6

=item o Comma separated list of files to be submitted via input sandbox. The files will be put in the working directory on WN. It's user responsibility to actually use them!

=item o output_dir e log_dir, path of directory where crab will put the std_error and std_output of job.  If these parameters are commented, error and output will be put into the directory where sh and jdl script are (crab_0_date_time).

=back

=item B<Optional:>

=over 6

=item o the name of UI directory where crab will create jobs. By default the name is "crab_data_time"

=back

=item B<C)> 

As stated before, you need to have a valid grid certificate (see CRAB web page for instruction) to submit to the grid. You need also a valid proxy: if you don't have it (or if it is too short), CRAB will issues that command for you. From version 1_1_0 CRAB uses I<voms> and I<myproxy> server to renew the proxy length, so if your proxy expires while your job is on the grid, the proxy will be extended by the myproxy server, to which you have delegated, and your job will continue. The voms proxy lenght is I<24> hours, while the myproxy delegation extends for I<7> days.

At CERN, you can use "lxplus" as a UI by sourcing the file B</afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.(c)sh>

=back

=head1 RUNNING FAMOS WITH CRAB

=over 2

=item B<Registering files to LFC:>

input ntuples to FAMOS should be first registered as Grid files on the LFC File Catalog (LFC) (RLS is not supported anymore): to select the proper LFC, set the following environmental variables:

I<LCG_CATALOG_TYPE=lfc> and I<LFC_HOST=lfc-cms-test.cern.ch>

LFC uses a directory structure (was not the case for RLS), so user has to create a directory under /grid/cms/ (strongly suggested something like I<lfc-mkdir /grid/cms/user>). User will have read/write permissions under this dir.  Then user can use the common lcg- commands:

lcg-rf --vo cms -l /grid/cms/georgia/inputfile \ sfn://$SE/$SE_PATH/inputfile

where SE = Storage element name (e.g. castorgrid.cern.ch)

SE_PATH = storage element path  (e.g. /castor/cern.ch/user/u/user/)

=item B<crab.cfg:>

jobtype must be set to I<famos>. A [FAMOS] session is included in addition to the [ORCA] one. In there you will find some additional parameters introduced:

=over 2

=item "input_lfn" 

stands for the general logical file name (LFN) used to register the ntuples (e.g. if ntuples named as su05_pyt_lm6_i.ntpl , you must put input_lfn = user/su05_pyt_lm6.ntpl);

=item "events_per_ntuple" 

the number of entries in each input ntuple;  

=item "input_pu_lfn"

stands for the general LFN used to register the pile-up ntuples (e.g. if pile-up ntuples are named mu05b_MBforPU_20200000i.ntpl, you must put input_pu_lfn = user/mu05b_MBforPU_20200000.ntpl);

=item "number_pu_ntuples"

the number of your pile-up ntuples you wish to access per job.

=back

=item B<Warning>:

=item B<input ntuples>: the number of events per job should correspond to an integer multiple of the number of events in ntuple!     

=item B<Pile-up ntuples>: you should leave all parameters, concerning pile-up reading in .orcarc, ON exactly as you do when you run locally.

=back

Run CRAB exactly as you run with ORCA jobtype.

=head1 HOW TO RUN ON CONDOR-G

The B<Condor-G> mode for B<CRAB> is a special submission mode next to the standard Resource Broker submission. It is designed to submit jobs directly to a site and not using the Resource Broker.

Due to the nature of this submission possibility, the B<Condor-G> mode is restricted to OSG sites within the CMS grid, currently the 7 US T2: Florida(ufl.edu), Nebraska(unl.edu), San Diego(ucsd.edu), Purdue(purdue.edu), Wisconsin(wisc.edu), Caltech(ultralight.org), MIT(mit.edu). 

=head2 B<Requirements:>

=over 2

=item installed and running local Condor scheduler

(either installed by the local Sysadmin or self-installed using the VDT user interface: http://www.uscms.org/SoftwareComputing/UserComputing/Tutorials/vdt.html)

=item locally available LCG or OSG UI installation

for authentication via grid certificate proxies ("voms-proxy-init -voms cms" should result in valid proxy) 

=item set of the environment variable EDG_WL_LOCATION to the edg directory of the local LCG or OSG UI installation 

=back

=head2 B<What the Condor-G mode can do:>

=over 2

=item submission directly to a single OSG site,

the requested dataset has to be published correctly by the site in the local and global services 

=back

=head2 B<What the Condor-G mode cannot do:>

=over 2

=item submit jobs if no condor scheduler is running on the submission machine

=item submit jobs if the local condor installation does not provide Condor-G capabilities

=item submit jobs to more than one site in parallel

=item submit jobs to a LCG site

=item support grid certificate proxy renewal via the myproxy service

=back

=head2 B<CRAB configuration for Condor-G mode:>

The CRAB configuration for the Condor-G mode only requires changes in crab.cfg:

=over 2

=item select condor_g Scheduler:

scheduler = condor_g

=item select the domain for a single OSG site: 

CE_white_list = "one of unl.edu,ufl.edu,ucsd.edu,wisc.edu,purdue.edu,ultralight.org,mit.edu"

=back

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

=item B<-getoutput|-get [range]>

Retrieve the output declared by the user via the output sandbox. By default the output will be put in task working dir under I<res> subdirectory. This can be changed via config parameters. B<Be extra sure that you have enough free space>. See I<range> below for syntax.

=item B<-resubmit [range]>

Resubmit jobs which have been previously submitted and have been either I<killed> or are I<aborted>. See I<range> below for syntax. 

=item B<-kill [range]>

Kill (cancel) jobs which have been submitted to the scheduler. A range B<must> be used in all cases, no default value is set.

=item B<-testJdl [range]>

Check if the job can find compatible resources. It's equivalent of doing I<edg-job-list-match> on edg.

=item B<-printId [range]>

Just print the SID of the job(s).

=item B<-postMortem [range]>

Produce a file (via I<edg-job-logging-info -v 2>) which might help in understanding grid related problem for a job.

=item B<-list [range]>

Dump technical informations about jobs: for developers only.

=item B<-clean [dir]>

Clean up (i.e. erase) the task working directory after a check whether there are still running jobs. In case, you are notified and asked to kill them or retrieve their output. B<Warning> this will eventually delete also the output produced by the task (if any)!

=item B<-help [format] | -h [format]>

This help. It can be produced in three different I<format>: I<man> (default), I<tex> and I<html>.

=item B<-v>

Print the version and exit.

=item B<range>

The range to be used in many of the above commands has the following syntax. It is a comma separated list of jobs ranges, each of which may be a job number, or a job range of the form first-last.
Example: 1,3-5,8 = {1,3,4,5,8}

=back 

=head1 OPTION

=over 4

=item B<-cfg [file]>

Configuration file name. Default is B<crab.cfg>.

=item B<-debug [level]>

Set the debug level: high number for high verbosity.

=back 

=head1 CONFIGURATION PARAMETERS

All the parameter describe in this section can be defined in the CRAB configuration file. The configuration file has different sections: [CRAB], [USER], etc. Each parameter must be defined in its proper section. An alternative way to pass a config parameter to CRAB is via command line interface; the syntax is: crab.py -SECTION.key value . For example I<crab.py -USER.outputdir MyDirWithFullPath> .
The parameters passed to CRAB at the creation step are stored, so they cannot be changed by changing the original crab.cfg . On the other hand the task is protected from any accidental change. If you want to change any parameters, this require the creation of a new task.
Mandatory parameters are flagged with a *.

B<[CRAB]>

=over 4

=item B<jobtype *>

The type of the job to be executed: I<cmssw> I<orca> I<famos> jobtypes are supported

=item B<scheduler *>

The scheduler to be used: I<edg> is the grid one. In later version also other scheduler (local and grid) will be possible, including eg glite, condor-g, lsf, pbs, etc...

=back

B<[CMSSW]>

=over 4

=item B<datasetpath *>

the path of processed dataset as defined on the DBS. It comes with the format I</PrimaryDataset/DataTier/Process> . In case no input is needed I<None> must be specified.

=item B<pset *>

the ParameterSet to be used

=item I<the following three parameter are mutually exclusive.>

=item B<total_number_of_events *>

the number of events to be processed. To access all available events, use I<-1>. Of course, the latter option is not viable in caso of no input. In this case, the total number of events will be used to split the task in jobs, together with I<event_per_job>.

=item B<files_per_jobs *>

number of files (EventCollection) to be accessed by each job. The DBS provide a list of EvC available for a given datasetpath. Cannot be used with no input.

=item B<events_per_job *>

Define the number of events to be run for each job of the task. The actual number of events will be rounded matching the number of events per file, since each job accesses a integer number of files (or EventCollections). It can be used also with No input.

=item B<number_of_jobs *>

Define the number of job to be run for the task. The number of event for each job is computed taking into account the total number of events required as well as the granularity of EventCollections. Can be used also with No input.

=item B<output_file *>

the output files produced by your application (comma separated list).

=item B<pythia_seed>

If the job is pythia based, and has I<untracked uint32 sourceSeed = x> in the ParameterSet, the seed value can be changed using this parameter. Each job will have a different seed, of the type I<pythia_seed>I<$job_number> .

=item B<vtx_seed>

Seed for random number generation used for vertex smearing: to be used only if PSet has I<untracked uint32 VtxSmeared = x>. It is modified if and only if also I<pythia_seed> is set. As for I<pythia_seed> the actual seed will be of the type I<vtx_seed>I<$job_number>.

=back

B<[ORCA]>

=over 4

=item B<dataset *>

The dataset the user want to analyze.

=item B<owner *>

The owner name which the user want to access. These two parameter can be found using data discovery tool: for the time being, RefDB/PubDB. See production page (linked also from CRAB web page) for access to the list of available dataset/owner

=item B<data_tier>

The data tiers the user want to access: by deafult, only the tier corresponding to the given owner will be provided. If user needs more, he B<must> specify the full list.
Syntax: comma separated list, known tiers are I<Hits>,I<Digi>, I<DST>

=item B<order_catalogs>

Define the order of the catalogs which will be put in the generated .orcarc fragment: for expert use only.

=item B<executable *>

The ORCA executable the user want to run on remote site. This must be on the I<path> of the user at the time of the creation of the jobs, so it is mandatory to issue the usual I<eval `scramv1 runtim -(c)sh`> from user ORCA working area before creating jobs. A warning will be prompted if the executable is not found in the path.

=item B<script_exe>

Name of a script that the user want to execute on remote site: full path must be provided. The ORCA executable whcih is executed by the script must be declared in any case, since CRAB must ship it to the remote site. The script can do anything, but change directory before the ORCA executable start. On the remote WN, the full scram-based environment will be found.

=item B<first_event>

The first event the user want to analyze in the dataset. Default is 0.

=item B<total_number_of_events *>

The total number of events the user want to analyze. I<-1> means all available events. If first even is set, a gran total of (total available events)-(first event) will be analyzed.

=item B<job_number_of_events *>

Numer of event for each job. Either this or I<total_number_of_jobs> must be defined.

=item B<total_number_of_jobs>

Total numer of jobs in which the task will be splitted. It is incompatible with previous I<job_number_of_events>. If both are set, this card will be ignored and a warning message issued.

=item B<output_file *>

Output files as produced by the executable: comma separated list of all the files. These are the file names which are produced by the executable also in the interactive environment. The output files will be modified by CRAB when returned in order to cope with the job splitting, by adding a "_N" to each file.

=item B<orcarc_file *>

User I<.orcarc> file: if it is not in the current directory, full path is needed. Use the very same file you used for your interactive test: CRAB will modify it according to data requested and splitting directives.

=back

B<[FAMOS]>

User needs to define jobtype = famos.
A [FAMOS] session is included in addition to the [ORCA] one. Inside you can find some additional parameters introduced:

All parameters (in .orcarc) concerning pile-up ntuples should be left exactly as they are in interactive usage.

=over 4

=item B<input_lfn>

LFN of the input file registered into the LFC catalog. It is stands for the general logical file name (LFN) used to register the ntuples (e.g. if ntuples named as su05_pyt_lm6_i.ntpl , you must put input_lfn = user/su05_pyt_lm6.ntpl)

=item B<events_per_ntuple>

number of events per ntuple

=item B<input_pu_lfn>

LFN for the input pile-up ntuples (already registered to LFC)
It is the general LFN used to register the pile-up ntuples (e.g. if pile-up ntuples are named mu05b_MBforPU_20200000i.ntpl, you must put input_pu_lfn = user/mu05b_MBforPU_20200000.ntpl)

=item B<number_pu_ntuples>

It is the number of your pile-up ntuples you wish to access per job.   

=item B<executable>

FAMOS executable: user must provide the executable into his personal scram area.

=item B<script_exe>

or the script that calls the executable...

=item B<first_event>

The first event the user want to analyze in the dataset. Default is 0.

=item B<total_number_of_events *>

The total number of events the user want to analyze. I<-1> means all available events. If first even is set, a gran total of (total available events)-(first event) will be analyzed.

=item B<job_number_of_events *>

Numer of event for each job. Either this or I<total_number_of_jobs> must be defined.

=item B<total_number_of_jobs>

Total numer of jobs in which the task will be splitted. It is incompatible with previous I<job_number_of_events>. If both are set, this card will be ignored and a warning message issued.

=item B<output_file>

Output files produced by executable: comma separated list, can be empty but mut be present!

=item B<orcarc_file>

orcarc card provided by user (if not in current directory, the full path must to be provided) NB the file must exist (could be empty)

=back

B<[USER]>

=over 4

=item B<additional_input_files>

Any additional input file you want to ship to WN: comma separated list. These are the files which might be needed by your executable: they will be placed in the WN working dir. Please note that the full I<Data> directory of your private ORCA working are will be send to WN (if present). 

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

=item B<use_central_bossDB>

Use central BOSS DB instead of one for each task: the DB must be already been setup. See installation istruction for more details.

=item B<use_boss_rt>

Use BOSS real time monitoring.

=back

B<[EDG]>

=over 4

=item B<lcg_version>

Version of LCG middleware to be used.

=item B<RB>

Which RB you want to use instead of the default one. The ones available for CMS are I<CERN> and I<CNAF>: the configuration files needed to change the broker will be automatically downloaded from CRAB web page and used. If the files are already present on the working directory they will be used.

=item B<proxy_server>

The proxy server to which you delegate the responsibility to renew your proxy once expired. The default is I<myproxy.cern.ch> : change only if you B<really> know what you are doing.

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

B<CRAB> is a tool for the CMS analysis on the grid environment. It is based on the ideas from CMSprod, a production tools implemented originally by Nikolai Smirnov.

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
