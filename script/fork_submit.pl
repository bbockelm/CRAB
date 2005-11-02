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
# ----------------- Scheduler specific initialization ------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
&initSched();
#
# ------ Get additional information from classad file (if any)----------------
# (do not modify this section unless for fixing bugs - please inform authors!)
$cladfile = "BossClassAdFile_$jid";
if ( -f $cladfile ) {
    %classsad = &parseClassAd($cladfile);
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
# Initialize variables used by the scheduler
sub initSched {
}
#
# Process a single entry of the classad
sub processClassAd {
     if(LOG) {
	print LOG "$jid: ClassAd ignored:\n";
	print LOG "$jid: fork doesn't support ClassAd\n";
    }
}
#
# Submit the job and return the scheduler id
sub submit {
    if ( $host ne "NULL" ) {
	if(LOG) {
	    print LOG "$jid: Submission to $host request is ignored:\n";
	    print LOG "$jid: fork only allows submission to current host\n";
	}
    }
    if ( !defined($pid=fork()) ) {
	if (LOG) {
	    print LOG "$jid: ERROR: Unable to fork the executable\n";
	    close(LOG);
	}
	print "0::0::0\n";
	die "$jid:ERROR: Unable to fork the executable\n";
    } elsif ($pid == 0) {
	# child
	sleep 1;
	if ( exec("$executable 0<$stdin 1>>$stdout 2>&1" ) != 0 ) {
	    if (LOG) {
		print LOG "$jid: ERROR: exec($executable) failed\n";
	    }
	}
	exit(0);
    } else {
	# father
	if (LOG) {
	    print LOG "$jid: $executable $jid submitted with pid $pid\n";
	    print LOG "$jid: Scheduler ID = ${executable}::${jid}::${pid}\n";
	    close(LOG);
	}
	return "${executable}::${jid}::${pid}\n";
    }
}
#
# ----------------------------------------------------------------------------

