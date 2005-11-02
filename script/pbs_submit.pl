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
# Boss job ID
$jid = $ARGV[0];
# Configuration file (where the job wrapper reads the configuration)
$stdin = $ARGV[1];
# Log file (where the job wrapper writes its messages); argument of -log option
$stdout = $ARGV[2];
# Host where to submit job - argument of -host option
$host = $ARGV[3];
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
# ------ Get additional information from classad file (if any)----------------
# (do not modify this section unless for fixing bugs - please inform authors!)
$cladfile = "BossClassAdFile_$jid";
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
print $sid;
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
    if(LOG) {
        print LOG "$jid: ClassAd ignored:\n";
        print LOG "$jid: This version of PBS submission doesn't support ClassAd\n";
    }
}
#
# Submit the job and return the scheduler id
sub submit {
    if ( $host ne "NULL" ) { 
        if(LOG) {
            print LOG "$jid: Submission to $host request is ignored:\n";
            print LOG "$jid: PBS submission to default destination\n";
        }
    }
    #
    $inbn = mybasename($stdin);
    $subcmd = "qsub -i $stdin -o $stdout -j oe $executable -W stagein=BossArchive_$jid.tgz\@$subhost:$subdir/BossArchive_$jid.tgz,$inbn\@$subhost:$stdin -W stageout=BossOutArchive_$jid.tgz\@$subhost:$subdir/BossOutArchive_$jid.tgz |";
    if(LOG) {
        print LOG "$subcmd\n";
    }
    # open a pipe to read the stdout of qsub
    open (SUB, $subcmd);
    # find job id
    $id = "error";
    $_ = <SUB>;
    if ( $_ =~ /(\d+\.\S+)\S*/ ) {
         @tmp = split(/\./,$1);
         # print "$tmp[0].$tmp[1]";
         $id = "$tmp[0]";
    }
    close(SUB);
    # delete temporary file
    #unlink "$tmpfile";
    #
    return $id;
}
#
# ----------------------------------------------------------------------------

