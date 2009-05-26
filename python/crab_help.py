
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
    print 'in usage()'
    usa_string = common.prog_name + """ [options]

The most useful general options (use '-h' to get complete help):

  -create                    -- Create all the jobs.
  -submit n                  -- Submit the first n available jobs. Default is all.
  -status [range]            -- check status of all jobs.
  -getoutput|-get [range]    -- get back the output of all jobs: if range is defined, only of selected jobs.
  -extend                    -- Extend an existing task to run on new fileblocks if there.
  -publish [dbs_url]         -- after the getouput, publish the data user in a local DBS instance.
  -kill [range]              -- kill submitted jobs.
  -resubmit [range]          -- resubmit killed/aborted/retrieved jobs.
  -copyData [range]          -- copy locally the output stored on remote SE.
  -renewCredential           -- renew credential on the server.
  -clean                     -- gracefully cleanup the directory of a task.
  -match|-testJdl [range]    -- check if resources exist which are compatible with jdl.
  -report                    -- print a short report about the task
  -list [range]              -- show technical job details.
  -postMortem [range]        -- provide a file with information useful for post-mortem analysis of the jobs.
  -printId [range]           -- print the job SID or Task Unique ID while using the server.
  -createJdl [range]         -- provide files with a complete Job Description (JDL).
  -validateCfg [fname]       -- parse the ParameterSet using the framework's Python API.
  -continue|-c [dir]         -- Apply command to task stored in [dir].
  -h [format]                -- Detailed help. Formats: man (default), tex, html, txt.
  -cfg fname                 -- Configuration file name. Default is 'crab.cfg'.
  -debug N                   -- set the verbosity level to N.
  -v                         -- Print version and exit.

  "range" has syntax "n,m,l-p" which correspond to [n,m,l,l+1,...,p-1,p] and all possible combination

Example:
  crab -create -submit 1
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

This tool B<must> be used from an User Interface and the user is supposed to
have a valid Grid certificate.

=head1 SYNOPSIS

B<"""+common.prog_name+"""> [I<options>] [I<command>]

=head1 DESCRIPTION

CRAB is a Python program intended to simplify the process of creation and submission of CMS analysis jobs to the Grid environment .

Parameters for CRAB usage and configuration are provided by the user changing the configuration file B<crab.cfg>.

CRAB generates scripts and additional data files for each job. The produced scripts are submitted directly to the Grid. CRAB makes use of BossLite to interface to the Grid scheduler, as well as for logging and bookkeeping.

CRAB supports any CMSSW based executable, with any modules/libraries, including user provided ones, and deals with the output produced by the executable. CRAB provides an interface to CMS data discovery services (DBS and DLS), which are completely hidden to the final user. It also splits a task (such as analyzing a whole dataset) into smaller jobs, according to user requirements.

CRAB can be used in two ways: StandAlone and with a Server.
The StandAlone mode is suited for small task, of the order of O(100) jobs: it submits the jobs directly to the scheduler, and these jobs are under user responsibility.
In the Server mode, suited for larger tasks, the jobs are prepared locally and then passed to a dedicated CRAB server, which then interacts with the scheduler on behalf of the user, including additional services, such as automatic resubmission, status caching, output retrieval, and more.
The CRAB commands are exactly the same in both cases.

CRAB web page is available at

I<https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideCrab>

=head1 HOW TO RUN CRAB FOR THE IMPATIENT USER

Please, read all the way through in any case!

Source B<crab.(c)sh> from the CRAB installation area, which have been setup either by you or by someone else for you.

Modify the CRAB configuration file B<crab.cfg> according to your need: see below for a complete list. A template and commented B<crab.cfg> can be found on B<$CRABDIR/python/crab.cfg>

~>crab -create
  create all jobs (no submission!)

~>crab -submit 2 -continue [ui_working_dir]
  submit 2 jobs, the ones already created (-continue)

~>crab -create -submit 2
  create _and_ submit 2 jobs

~>crab -status
  check the status of all jobs

~>crab -getoutput
  get back the output of all jobs

~>crab -publish
  publish all user outputs in the DBS specified in the crab.cfg (dbs_url_for_publication) or written as argument of this option

=head1 RUNNING CMSSW WITH CRAB

=over 4

=item B<A)>

Develop your code in your CMSSW working area.  Do anything which is needed to run interactively your executable, including the setup of run time environment (I<eval `scramv1 runtime -sh|csh`>), a suitable I<ParameterSet>, etc. It seems silly, but B<be extra sure that you actually did compile your code> I<scramv1 b>.

=item B<B)>

Source B<crab.(c)sh> from the CRAB installation area, which have been setup either by you or by someone else for you.  Modify the CRAB configuration file B<crab.cfg> according to your need: see below for a complete list.

The most important parameters are the following (see below for complete description of each parameter):

=item B<Mandatory!>

=over 6

=item B<[CMSSW]> section: datasetpath, pset, splitting parameters, output_file

=item B<[USER]> section: output handling parameters, such as return_data, copy_data etc...

=back

=item B<Run it!>

You must have a valid voms-enabled Grid proxy. See CRAB web page for details.

=back

=head1 RUNNING MULTICRAB

MultiCRAB is a CRAB extension to submit the same job to multiple datasets in one go. 

The use case for multicrab is when you have your analysis code that you want to run on several datasets, typically some signals plus some backgrounds (for MC studies) 
or on different streams/configuration/runs for real data taking. You want to run exactly the same code, and also the crab.cfg are different only for few keys:
for sure datasetpath but also other keys, such as eg total_number_of_events, in case you want to run on all signals but only a fraction of background, or anything else. 
So far, you would have to create a set of crab.cfg, one for each dataset you want to access, and submit several instances of CRAB, saving the output to different locations.
Multicrab is meant to automatize this procedure.
In addition to the usual crab.cfg, there is a new configuration file called multicrab.cfg. The syntax is very similar to that of crab.cfg, namely
[SECTION]   <crab.cfg Section>.Key=Value

Please note that it is mandatory to add explicitly the crab.cfg [SECTION] in front of [KEY].
The role of multicrab.cfg is to apply modification to the template crab.cfg, some which are common to all tasks, and some which are task specific.

=head2  So there are two sections:

=over 2

=item B<[COMMON]> 

section: which applies to all task, and which is fully equivalent to modify directly the template crab.cfg

=item B<[DATASET]> 

section: there could be an arbitrary number of sections, one for each dataset you want to run. The names are free (but COMMON and MULTICRAB), and they will be used as ui_working_dir for the task as well as an appendix to the user_remote_dir in case of output copy to remote SE. So, the task corresponding to section, say [SIGNAL] will be placed in directory SIGNAL, and the output will be put on /SIGNAL/, so SIGNAL will be added as last subdir in the user_remote_dir. 

=back

For further details please visit

I<https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideMultiCrab>

=head1 HOW TO RUN ON CONDOR-G

The B<Condor-G> mode for B<CRAB> is a special submission mode next to the standard Resource Broker submission. It is designed to submit jobs directly to a site and not using the Resource Broker.

Due to the nature of B<Condor-G> submission, the B<Condor-G> mode is restricted to OSG sites within the CMS Grid, currently the 7 US T2: Florida(ufl.edu), Nebraska(unl.edu), San Diego(ucsd.edu), Purdue(purdue.edu), Wisconsin(wisc.edu), Caltech(ultralight.org), MIT(mit.edu).

=head2 B<Requirements:>

=over 2

=item installed and running local Condor scheduler

(either installed by the local Sysadmin or self-installed using the VDT user interface: http://www.uscms.org/SoftwareComputing/UserComputing/Tutorials/vdt.html)

=item locally available LCG or OSG UI installation

for authentication via Grid certificate proxies ("voms-proxy-init -voms cms" should result in valid proxy)

=item set the environment variable GRID_WL_LOCATION to the edg directory of the local LCG or OSG UI installation

=back

=head2 B<What the Condor-G mode can do:>

=over 2

=item submission directly to multiple OSG sites,

the requested dataset must be published correctly by the site in the local and global services.
Previous restrictions on submitting only to a single site have been removed. SE and CE whitelisting
and blacklisting work as in the other modes.

=back

=head2 B<What the Condor-G mode cannot do:>

=over 2

=item submit jobs if no condor scheduler is running on the submission machine

=item submit jobs if the local condor installation does not provide Condor-G capabilities

=item submit jobs to an LCG site

=item support Grid certificate proxy renewal via the myproxy service

=back

=head2 B<CRAB configuration for Condor-G mode:>

The CRAB configuration for the Condor-G mode only requires one change in crab.cfg:

=over 2

=item select condor_g Scheduler:

scheduler = condor_g

=back

=head1 COMMANDS

=over 4

=item B<-create>

Create the jobs: from version 1_3_0 it is only possible to create all jobs.
The maximum number of jobs depends on dataset and splitting directives. This set of identical jobs accessing the same dataset are defined as a task.
This command create a directory with default name is I<crab_0_date_time> (can be changed via ui_working_dir parameter, see below). Inside this directory it is placed whatever is needed to submit your jobs. Also the output of your jobs (once finished) will be place there (see after). Do not cancel by hand this directory: rather use -clean (see).
See also I<-continue>.

=item B<-submit [range]>

Submit n jobs: 'n' is either a positive integer or 'all' or a [range]. Default is all.
If 'n' is passed as argument, the first 'n' suitable jobs will be submitted. Please note that this is behaviour is different from other commands, where -command N means act the command to the job N, and not to the first N jobs. If a [range] is passed, the selected jobs will be submitted.
This option must be used in conjunction with -create (to create and submit immediately) or with -continue (which is assumed by default), to submit previously created jobs. Failure to do so will stop CRAB and generate an error message.  See also I<-continue>.

=item B<-continue [dir] | -c [dir]>

Apply the action on the task stored on directory [dir]. If the task directory is the standard one (crab_0_date_time), the more recent in time is taken. Any other directory must be specified.
Basically all commands (but -create) need -continue, so it is automatically assumed. Of course, the standard task directory is used in this case.

=item B<-status>

Check the status of the jobs, in all states. All the info (e.g. application and wrapper exit codes)  will be available only after the output retrieval.

=item B<-getoutput|-get [range]>

Retrieve the output declared by the user via the output sandbox. By default the output will be put in task working dir under I<res> subdirectory. This can be changed via config parameters. B<Be extra sure that you have enough free space>. See I<range> below for syntax.

=item B<-publish [dbs_url]>

Publish user output in a local DBS instance after retrieving of output. By default the publish uses the dbs_url_for_publication specified in the crab.cfg file, otherwise you can write it as argument of this option.

=item B<-resubmit [range]>

Resubmit jobs which have been previously submitted and have been either I<killed> or are I<aborted>. See I<range> below for syntax.

=item B<-extend>

Create new jobs for an existing task, checking if new blocks are available for the given dataset.

=item B<-kill [range]>

Kill (cancel) jobs which have been submitted to the scheduler. A range B<must> be used in all cases, no default value is set.

=item B<-copyData [range]>

Copy locally (on current working directory) the output previously stored on remote SE by the jobs. Of course, only if copy_data option has been set.

=item B<-renewCredential >

If using the server modality, this command allows to delegate a valid credential (proxy/token) to the server associated with the task.

=item B<-match|-testJdl [range]>

Check if the job can find compatible resources. It is equivalent of doing I<edg-job-list-match> on edg.

=item B<-printId [range]>

Just print the job identifier, which can be the SID (Grid job identifier) of the job(s) or the taskId if you are using CRAB with the server or local scheduler Id. If [range] is "full", the the SID of all the jobs are printed, also in the case of submission with server.

=item B<-printJdl [range]>

Collect the full Job Description in a file located under share directory. The file base name is File- .

=item B<-postMortem [range]>

Try to collect more information of the job from the scheduler point of view.

=item B<-list [range]>

Dump technical information about jobs: for developers only.

=item B<-report>

Print a short report about the task, namely the total number of events and files processed/requested/available, the name of the datasetpath, a summary of the status of the jobs, the list of runs and lumi sections, and so on. In principle it should contain all the info needed for analysis. Work in progress.

=item B<-clean [dir]>

Clean up (i.e. erase) the task working directory after a check whether there are still running jobs. In case, you are notified and asked to kill them or retrieve their output. B<Warning> this will possibly delete also the output produced by the task (if any)!

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

All the parameter describe in this section can be defined in the CRAB configuration file. The configuration file has different sections: [CRAB], [USER], etc. Each parameter must be defined in its proper section. An alternative way to pass a config parameter to CRAB is via command line interface; the syntax is: crab -SECTION.key value . For example I<crab -USER.outputdir MyDirWithFullPath> .
The parameters passed to CRAB at the creation step are stored, so they cannot be changed by changing the original crab.cfg . On the other hand the task is protected from any accidental change. If you want to change any parameters, this require the creation of a new task.
Mandatory parameters are flagged with a *.

B<[CRAB]>

=over 4

=item B<jobtype *>

The type of the job to be executed: I<cmssw> jobtypes are supported

=item B<scheduler *>

The scheduler to be used: I<glitecoll> is the more efficient grid scheduler and should be used. Other choice are I<glite>, same as I<glitecoll> but without bulk submission (and so slower) or I<condor_g> (see specific paragraph) or I<edg> which is the former Grid scheduler, which will be dismissed in some future
From version 210, also local scheduler are supported, for the time being only at CERN. I<LSF> is the standard CERN local scheduler or I<CAF> which is LSF dedicated to CERN Analysis Facilities.

=item B<use_server>

To use the server for job handling (recommended) 0=no (default), 1=true. The server to be used will be found automatically from a list of available ones: it can also be specified explicitly by using I<server_name> (see below)

=item B<server_name>

To use the CRAB-server support it is needed to fill this key with server name as <Server_DOMAIN> (e.g. cnaf,fnal). If this is set, I<use_server> is set to true automatically.
If I<server_name=None> crab works in standalone way, same as using I<use_server=0> and no I<server_name>.
The server available to users can be found from CRAB web page.

=back

B<[CMSSW]>

=over 4

=item B<datasetpath *>

the path of processed dataset as defined on the DBS. It comes with the format I</PrimaryDataset/DataTier/Process> . In case no input is needed I<None> must be specified.

=item B<ads *>

you may want to run over an AnalysisDataSet. After define the related path in I<datasetpath>, take care to specify ads=1.   

=item B<runselection *>

within a dataset you can restrict to run on a specific run number or run number range. For example runselection=XYZ or runselection=XYZ1-XYZ2 .

=item B<use_parent *>

within a dataset you can ask to run over the related parent files too. E.g., this will give you access to the RAW data while running over a RECO sample. Setting use_parent=1 CRAB determines the parent files from DBS and will add secondaryFileNames = cms.untracked.vstring( <LIST of parent FIles> ) to the pool source section of your parameter set.

=item B<pset *>

the ParameterSet to be used. Both .cfg and .py parameter sets are supported for the relevant versions of CMSSW.

=item I<Of the following three parameter exactly two must be used, otherwise CRAB will complain.>

=item B<total_number_of_events *>

the number of events to be processed. To access all available events, use I<-1>. Of course, the latter option is not viable in case of no input. In this case, the total number of events will be used to split the task in jobs, together with I<event_per_job>.

=item B<events_per_job*>

number of events to be accessed by each job. Since a job cannot cross the boundary of a fileblock it might be that the actual number of events per job is not exactly what you asked for. It can be used also with No input.

=item B<number_of_jobs *>

Define the number of job to be run for the task. The number of event for each job is computed taking into account the total number of events required as well as the granularity of EventCollections. Can be used also with No input.

=item B<split_by_run *>

to activate the split run based (each job will access a different run) use I<split_by_run>=1. You can definfe also I<number_of_jobs>  and/or I<runselection>. NOTE: the Run Based combined with Event Based split is not yet available.  

=item B<output_file *>

the output files produced by your application (comma separated list). From CRAB 2_2_2 onward, if TFileService is defined in user Pset, the corresponding output file is automatically added to the list of output files. User can avoid this by setting B<skip_TFileService_output> = 1 (default is 0 == file included). The Edm output produced via PoolOutputModule can be automatically added by setting B<get_edm_output> = 1 (default is 0 == no)

=item B<skip_TFileService_output>

Force CRAB to skip the inclusion of file produced by TFileService to list of output files. Default is I<0>, namely the file is included.

=item B<get_edm_output>

Force CRAB to add the EDM output file, as defined in PSET in PoolOutputModule (if any) to be added to the list of output files. Default is 0 (== no inclusion)

=item B<increment_seeds>

Specifies a comma separated list of seeds to increment from job to job. The initial value is taken
from the CMSSW config file. I<increment_seeds=sourceSeed,g4SimHits> will set sourceSeed=11,12,13 and g4SimHits=21,22,23 on
subsequent jobs if the values of the two seeds are 10 and 20 in the CMSSW config file.

See also I<preserve_seeds>. Seeds not listed in I<increment_seeds> or I<preserve_seeds> are randomly set for each job.

=item B<preserve_seeds>

Specifies a comma separated list of seeds to which CRAB will not change from their values in the user
CMSSW config file. I<preserve_seeds=sourceSeed,g4SimHits> will leave the Pythia and GEANT seeds the same for every job.

See also I<increment_seeds>. Seeds not listed in I<increment_seeds> or I<preserve_seeds> are randomly set for each job.

=item B<first_run>

First run to be generated in a generation jobs. Relevant only for no-input workflow.

=item B<generator>

Name of the generator your MC job is using. Some generators require CRAB to skip events, others do not.
Possible values are pythia, comphep, and madgraph. This will skip events in your generator input file.

=item B<executable>

The name of the executable to be run on remote WN. The default is cmsrun. The executable is either to be found on the release area of the WN, or has been built on user working area on the UI and is (automatically) shipped to WN. If you want to run a script (which might internally call I<cmsrun>, use B<USER.script_exe> instead.

=item I<DBS and DLS parameters:>

=item B<dbs_url>

The URL of the DBS query page. For expert only.

=item B<show_prod>

To enable CRAB to show data hosted on Tier1s sites specify I<show_prod> = 1. By default those data are masked. 

=item B<no_block_boundary>

To remove fileblock boundaries in job splitting specify I<no_block_boundary> = 1.  

=back

B<[USER]>

=over 4

=item B<additional_input_files>

Any additional input file you want to ship to WN: comma separated list. IMPORTANT NOTE: they will be placed in the WN working dir, and not in ${CMS_SEARCH_PATH}. Specific files required by CMSSW application must be placed in the local data directory, which will be automatically shipped by CRAB itself. You do not need to specify the I<ParameterSet> you are using, which will be included automatically. Wildcards are allowed.

=item B<script_exe>

A user script that will be run on WN (instead of default cmsrun). It is up to the user to setup properly the script itself to run on WN enviroment. CRAB guarantees that the CMSSW environment is setup (e.g. scram is in the path) and that the modified pset.cfg will be placed in the working directory, with name CMSSW.cfg . The user must ensure that a job report named crab_fjr.xml will be written. This can be guaranteed by passing the arguments "-j crab_fjr.xml" to cmsRun in the script. The script itself will be added automatically to the input sandbox so user MUST NOT add it within the B<USER.additional_input_files>.

=item B<ui_working_dir>

Name of the working directory for the current task. By default, a name I<crab_0_(date)_(time)> will be used. If this card is set, any CRAB command which require I<-continue> need to specify also the name of the working directory. A special syntax is also possible, to reuse the name of the dataset provided before: I<ui_working_dir : %(dataset)s> . In this case, if e.g. the dataset is SingleMuon, the ui_working_dir will be set to SingleMuon as well.

=item B<thresholdLevel>

This has to be a value between 0 and 100, that indicates the percentage of task completeness (jobs in a ended state are complete, even if failed). The server will notify the user by e-mail (look at the field: B<eMail>) when the task will reach the specified threshold. Works just with the server_mode = 1.

=item B<eMail>

The server will notify the specified e-mail when the task will reaches the specified B<thresholdLevel>. A notification is also sent when the task will reach the 100\% of completeness. This field can also be a list of e-mail: "B<eMail = user1@cern.ch, user2@cern.ch>". Works just with the server_mode = 1.

=item B<return_data *>

The output produced by the executable on WN is returned (via output sandbox) to the UI, by issuing the I<-getoutput> command. B<Warning>: this option should be used only for I<small> output, say less than 10MB, since the sandbox cannot accommodate big files. Depending on Resource Broker used, a size limit on output sandbox can be applied: bigger files will be truncated. To be used in alternative to I<copy_data>.

=item B<outputdir>

To be used together with I<return_data>. Directory on user interface where to store the output. Full path is mandatory, "~/" is not allowed: the default location of returned output is ui_working_dir/res .

=item B<logdir>

To be used together with I<return_data>. Directory on user interface where to store the standard output and error. Full path is mandatory, "~/" is not allowed: the default location of returned output is ui_working_dir/res .

=item B<copy_data *>

The output (only that produced by the executable, not the std-out and err) is copied to a Storage Element of your choice (see below). To be used as an alternative to I<return_data> and recommended in case of large output.

=item B<storage_element>

To be used with <copy_data>=1
If you want to copy the output of your analysis in a official CMS Tier2 or Tier3, you have to write the CMS Site Name of the site, as written in the SiteDB https://cmsweb.cern.ch/sitedb/reports/showReport?reportid=se_cmsname_map.ini (i.e T2_IT_legnaro). You have also to specify the <remote_dir>(see below)

If you want to copy the output in a not_official_CMS remote site you have to specify the complete storage element name (i.e se.xxx.infn.it).You have also to specify the <storage_path> and the <storage_port> if you do not use the default one(see below).

=item B<user_remote_dir>

To be used with <copy_data>=1 and <storage_element> official CMS sites.
This is the directory or tree of directories where your output will be stored. This directory will be created under the mountpoint ( which will be discover by CRAB if an official CMS storage Element has been used, or taken from the crab.cfg as specified by the user). B<NOTE> This part of the path will be used as logical file name of your files in the case of publication without using an official CMS storage Element.

=item B<storage_path>

To be used with <copy_data>=1 and <storage_element> not official CMS sites.
This is the full path of the Storage Element writeable by all, the mountpoint of SE (i.e /srm/managerv2?SFN=/pnfs/se.xxx.infn.it/yyy/zzz/)


=item B<storage_pool>

If you are using CAF scheduler, you can specify the storage pool where to write your output.
The default is cmscafuser. If you do not want to use the default, you can overwrite it specifing None

=item B<storage_port>

To choose the storage port specify I<storage_port> = N (default is 8443) .

=item B<publish_data*>

To be used with <copy_data>=1
To publish your produced output in a local istance of DBS set publish_data = 1
All the details about how to use this functionality are written in https://twiki.cern.ch/twiki/bin/view/CMS/SWGuideCrabForPublication
N.B 1) if you are using an official CMS site to stored data, the remote dir will be not considered. The directory where data will be stored is decided by CRAB, following the CMS policy in order to be able to re-read published data.
2) if you are using a not official CMS site to store data, you have to check the <lfn>, that will be part of the logical file name of you published files, in order to be able to re-read the data.

=item B<publish_data_name>

You produced output will be published in your local DBS with dataset name <primarydataset>/<publish_data_name>/USER

=item B<dbs_url_for_publication>

Specify the URL of your local DBS istance where CRAB has to publish the output files

=item B<pubilish_zero_event>

T0 force zero event files publication specify I<pubilish_zero_event> = 1

=item B<srm_version>

To choose the srm version specify I<srm_version> =  (srmv1 or srmv2).

=item B<xml_report>

To be used to switch off the screen report during the status query, enabling the db serialization in a file. Specifying I<xml_report> = FileName CRAB will serialize the DB into CRAB_WORKING_DIR/share/FileName.

=item B<usenamespace>

To use the automate namespace definition (perfomed by CRAB) it is possible to set I<usenamespace>=1. The same policy used for the stage out in case of data publication will be applied.

=item B<debug_wrapper>

To enable the higer verbose level on wrapper specify I<debug_wrapper> = 1. The Pset contents before and after the CRAB maipulation will be written together with other useful infos.

=item B<deep_debug>

To be used in case of unexpected job crash when the sdtout and stderr files are lost. Submitting again the same jobs specifying I<deep_debug> = 1 these files will be reported back. NOTE: it works only on standalone mode for debugging purpose.

=item B<dontCheckSpaceLeft>

Set it to 1 to skip the check of free space left on your working directory before attempting to get the output back. Default is 0 (=False)


=item B<local_stage_out>

 To use the local stage out (i.e. to the closeSE), in case of remote stage out failure, set I<local_stage_out> = 1 .

=back

B<[GRID]>

=over 4

=item B<RB>

Which RB you want to use instead of the default one, as defined in the configuration of your UI. The ones available for CMS are I<CERN> and I<CNAF>. They are actually identical, being a collection of all WMSes available for CMS: the configuration files needed to change the broker will be automatically downloaded from CRAB web page and used.
You can use any other RB which is available, if you provide the proper configuration files. E.g., for gLite WMS XYZ, you should provide I<glite.conf.CMS_XYZ>. These files are searched for in the current working directory, and, if not found, on crab web page. So, if you put your private configuration files in the working directory, they will be used, even if they are not available on crab web page.
Please get in contact with crab team if you wish to provide your RB or WMS as a service to the CMS community.

=item B<proxy_server>

The proxy server to which you delegate the responsibility to renew your proxy once expired. The default is I<myproxy.cern.ch> : change only if you B<really> know what you are doing.

=item B<role>

The role to be set in the VOMS. See VOMS documentation for more info.

=item B<group>

The group to be set in the VOMS, See VOMS documentation for more info.

=item B<dont_check_proxy>

If you do not want CRAB to check your proxy. The creation of the proxy (with proper length), its delegation to a myproxyserver is your responsibility.

=item B<dont_check_myproxy>

If you want to to switch off only the proxy renewal set I<dont_check_myproxy>=1. The proxy delegation to a myproxyserver is your responsibility.

=item B<requirements>

Any other requirements to be add to JDL. Must be written in compliance with JDL syntax (see LCG user manual for further info). No requirement on Computing element must be set.

=item B<additional_jdl_parameters:>

Any other parameters you want to add to jdl file:semicolon separated list, each
item B<must> be complete, including the closing ";".

=item B<wms_service>

With this field it is also possible to specify which WMS you want to use (https://hostname:port/pathcode) where "hostname" is WMS name, the "port" generally is 7443 and the "pathcode" should be something like "glite_wms_wmproxy_server".

=item B<max_cpu_time>

Maximum CPU time needed to finish one job. It will be used to select a suitable queue on the CE. Time in minutes.

=item B<max_wall_clock_time>

Same as previous, but with real time, and not CPU one.

=item B<ce_black_list>

All the CE (Computing Element) whose name contains the following strings (comma separated list) will not be considered for submission.  Use the dns domain (e.g. fnal, cern, ifae, fzk, cnaf, lnl,....). You may use hostnames or CMS Site names (T2_DE_DESY) or substrings.

=item B<ce_white_list>

Only the CE (Computing Element) whose name contains the following strings (comma separated list) will be considered for submission.  Use the dns domain (e.g. fnal, cern, ifae, fzk, cnaf, lnl,....). You may use hostnames or CMS Site names (T2_DE_DESY) or substrings. Please note that if the selected CE(s) does not contain the data you want to access, no submission can take place.

=item B<se_black_list>

All the SE (Storage Element) whose name contains the following strings (comma separated list) will not be considered for submission.It works only if a datasetpath is specified. You may use hostnames or CMS Site names (T2_DE_DESY) or substrings.

=item B<se_white_list>

Only the SE (Storage Element) whose name contains the following strings (comma separated list) will be considered for submission.It works only if a datasetpath is specified. Please note that if the selected CE(s) does not contain the data you want to access, no submission can take place. You may use hostnames or CMS Site names (T2_DE_DESY) or substrings.

=item B<remove_default_blacklist>

CRAB enforce the T1s Computing Eelements Black List. By default it is appended to the user defined I<CE_black_list>. To remove the enforced T1 black lists set I<remove_default_blacklist>=1.

=item B<virtual_organization>

You do not want to change this: it is cms!

=item B<retry_count>

Number of time the Grid will try to resubmit your job in case of Grid related problem.

=item B<shallow_retry_count>

Number of time shallow resubmission the Grid will try: resubmissions are tried B<only> if the job aborted B<before> start. So you are guaranteed that your jobs run strictly once.

=item B<maxtarballsize>

Maximum size of tar-ball in Mb. If bigger, an error will be generated. The actual limit is that on the RB input sandbox. Default is 9.5 Mb (sandbox limit is 10 Mb)

=item B<skipwmsauth>

Temporary useful parameter to allow the WMSAuthorisation handling. Specifying I<skipwmsauth> = 1 the pyopenssl problmes  will disappear. It is needed working on gLite UI outside of CERN.

=back

B<[LSF]> or B<[CAF]>

=over 4

=item B<queue>

The LSF queue you want to use: if none, the default one will be used. For CAF, the proper queue will be automatically selected.

=item B<resource>

The resources to be used within a LSF queue. Again, for CAF, the right one is selected.

=item B<copyCommand>

To define the command to be used to copy both Input and Output sandboxes to final location. Default is cp

=back

=head1 FILES

I<crab> uses a configuration file I<crab.cfg> which contains configuration parameters. This file is written in the INI-style.  The default filename can be changed by the I<-cfg> option.

I<crab> creates by default a working directory 'crab_0_E<lt>dateE<gt>_E<lt>timeE<gt>'

I<crab> saves all command lines in the file I<crab.history>.

=head1 HISTORY

B<CRAB> is a tool for the CMS analysis on the Grid environment. It is based on the ideas from CMSprod, a production tool originally implemented by Nikolai Smirnov.

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
    elif option == 'txt':
        fname = common.prog_name+'-v'+common.prog_version_str+'.txt'
        pod2text = 'pod2text '+pod+' '+fname
        os.system(pod2text)
        print 'See '+fname
        pass

    sys.exit(0)
