#! /usr/bin/perl
#
$|=1;
#
# ------------------- Get info on local environment --------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# submitting user
$subuser = `whoami`; chomp $subuser;
# submitting host
$subhost = `hostname --fqdn`; chomp $subhost;
# submitting path
$subdir = `pwd`; chomp $subdir;
#
# ------------------- Optional logging of submission -------------------------
#   (change file name and comment/uncomment the open statement as you wish)
$logFile = "$subdir/bossSubmit.log";
#open (LOG, ">>$logFile") || {print STDERR "unable to write to $logFile. Logging disabled\n"};
#
# --------------------------- Get arguments ----------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# check number of arguments
$correctlen = 4;
$len=@ARGV;
if($len!=$correctlen) {
    if (LOG) {
	print LOG "Wrong number of arguments: $len, expected: $correctlen\n";
	close(LOG);
    }
    print "0::0::0\n";
    die "Wrong number of arguments to sub script: $len, expected: $correctlen\n";
}
# Boss task ID
$taskid = $ARGV[0];
# stdinput 
$stdin = $ARGV[1];
# common sandbox
$commonSandbox = $ARGV[2];
# Log file (where the job wrapper writes its messages); argument of -log option
$log = $ARGV[3];
#
if (LOG) {
    print LOG "\n====>> New scheduler call number $jid\n";
    print LOG "$jid: Redirecting stderr & stdout to log file $stdout\n";
}
#
# ------------------------ Other configuration -------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# The name of the executable to be submitted to the scheduler
$executable = `which jobExecutor`; chomp $executable;
#
# ----- Scheduler specific initialization (before parsing classad -----------
# (do not modify this section unless for fixing bugs - please inform authors!)
&initSched();
#
# ------------------- Read jobList file and loop over jobs -------------------
$jid="";
$cladfile = "";
$file = "submit\_$taskid";
open FILE, $file or die $!;
while ( <FILE> ) {
#    print $_;
    if ($_ =~ /(\d+):(\d+):(\d+)\n/) {
	for ($val=$1; $val<=$2; ++$val) {
	    $jid="$taskid\_$val\_$3";
	    $stdout="$log\_$taskid\_$val.log";
#
# ------ Get additional information from classad file (if any)----------------
# (do not modify this section unless for fixing bugs - please inform authors!)
	    $cladfile = "BossClassAdFile\_$taskid";
	    if ( -f $cladfile ) {
		%classad = &parseClassAd($cladfile);
		&processClassAd();
	    } else {
		if (LOG) {
		    print LOG "$jid: No classad file for this task\n";
		}
	    }
	    $cladfile = "BossClassAdFile\_$taskid\_$val";
	    if ( -f $cladfile ) {
		%classad = &parseClassAd($cladfile);
		&processClassAd();
	    } else {
		if (LOG) {
		    print LOG "$jid: No classad file for this job\n";
		}
	    }
#
# --------------------------- Ready to submit --------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
	    $sid = &submit();
	    print ("$val\t$3\t$sid");
	}
#
# ----------------------- If something goes wrong ----------------------------
    } else {
	print LOG "$_: Incorrect format\n";
	die;
    }
}
print "EOF\n";
#
# ----------------------------- End of main ----------------------------------
#
# ---------------------- General pourpose routines --------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
#
# Read a file containing a classad and store it in a hash
sub parseClassAd {
    my $cladfile=$_[0];
    my $cladstring="";
    open (CLAD, $cladfile);
    while ( <CLAD> ) {
	# if (LOG) {
	#     print LOG "ClassAd line: $_";
	# }
	$line = $_;
	chomp($line);
	$cladstring.=$line;
    }
    close(CLAD);
    if ( $cladstring =~ /.*\[(.+)\].*/ ) {
	$cladstring=$1;
    }
    my @attribs = split(/;/,$cladstring);
    foreach $attrib (@attribs) {
	if ( $attrib =~ /\s*(\w+)\s*=\s*(.+)/ ) {
	    $clad{$1}=$2;
	}
    }
    return %clad;
}

# --------------------- Scheduler specific routines -------------------------
#     (Update the routines of this section to match your scheduler needs)
#
# Initialize variables used by the scheduler before parsing classad
sub initSched {
}
#
# Process a single entry of the classad
sub processClassAd {
    $ppend2condor = "";
    foreach $ClAdKey ( keys %classad ) {
        $ClAdVal = $classad{$ClAdKey};
#           print LOG "$ClAdKey = $ClAdVal;\n";
	    $ppend2condor.="$ClAdKey = $ClAdVal;\n";
    }
}
#
# Submit the job and return the scheduler id
sub submit {
    # open a temporary file
    $tmpfile = `mktemp condor_XXXXXX` || die "error";
    chomp($tmpfile);
    open (CMD, ">$tmpfile") || die "error";
    # Executable submit to Condor
    print CMD ("Executable  = $executable\n");
    # Type of Universe (i.e. Standard, Vanilla, PVM, MPI, Globus)
    #print CMD ("Universe    = vanilla\n");
    # input,output,error files passed to executable
    print CMD ("Arguments   = $val\n");
    print CMD ("input       = $subdir/$stdin\n");
    print CMD ("output      = $stdout\n");
    print CMD ("error       = $stdout\n");
    # Condor log file
    print CMD ("log         = $subdir/condor.log\n");
    # Transfer files
    #print CMD ("should_transfer_files = YES\n");
    #print CMD ("when_to_transfer_output = ON_EXIT\n");
    print CMD ("transfer_input_files = $subdir/$commonSandbox,$subdir/BossArchive_$jid.tgz\n");
    print CMD ("transfer_output_files = BossOutArchive_$jid.tgz\n");
    # A string to help finding boss jobs in condor
    print CMD ("+BossJob = $jid\n");
    # Host
#     if ( $host ne "NULL" ) { 
# 	if ( $host =~ /.+\..+/ ) {
# 	    print CMD ("Requirements = Machine == \"$host\"\n");
# 	} else {
# 	    $domain = `dnsdomainname`; chomp($domain);
# 	    print CMD ("Requirements = Machine == \"$host.$domain\"\n");
# 	}
#     }
    print CMD ("Getenv      = True\n");
    print JDL $ppend2condor;
    print CMD ("Queue 1\n");  
    close(CMD);
    #
    $subcmd = "bbs_submit $tmpfile |";
    # open a pipe to read the stdout of qsub
    open (SUB, $subcmd);
    # find job id
    $id = "error";
    # skip the first two lines
    $_ = <SUB>;
    $_ = <SUB>;
    # find cluster id
    $_ = <SUB>;
    if ( $_ =~ /\d+\s+.+\s+submitted to cluster\s+(\d+)/ ) {
	$id = $1;
    }
    close(SUB);
    # delete temporary file
    unlink "$tmpfile";
    #
    return $id;
}
#
# ----------------------------------------------------------------------------

