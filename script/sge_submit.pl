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
$subdir = `pawd`; chomp $subdir;
#
# ------------------- Optional logging of submission -------------------------
#   (change file name and comment/uncomment the open statement as you wish)
$logFile = "$subdir/bossSubmit.log";
open (LOG, ">>$logFile") || {print STDERR "unable to write to $logFile. Logging disabled\n"};
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

    $substring = "";  
    $host = "";    

    foreach $ClAdKey ( keys %classad ) {
#	print LOG "in hash2: $ClAdKey = $classad{$ClAdKey}\n";
	$ClAdVal = $classad{$ClAdKey};
#	    print LOG "$ClAdKey = $ClAdVal;\n";
	if ( $ClAdKey eq "host") {
	    $ClAdVal =~ s/\s*{\s*\"\s*(.*)\s*\"\s*}\.*/$1/;
#               print "$ClAdKey : $ClAdVal \n";                
	    chomp($ClAdVal);
	    $host= "$ClAdVal";
#               print "$rbconfigstring \n";
	} elsif ( $ClAdKey eq "dir") {
	    $ClAdVal =~ s/\s*{\s*\"\s*(.*)\s*\"\s*}\.*/$1/;
#               print "$ClAdKey : $ClAdVal \n";
	    chomp($ClAdVal);
	    $substring .= " -dir $ClAdVal";
#               print "rbconfigVO $rbconfigVO \n";
	}
    }

    print LOG $addstring;
}
#
# Submit the job and return the scheduler id
sub submit {
    $substring = "";
    if ( $addstring ne "" ) {
	$substring = "$addstring";
    }

    if ( $host == "" ) {
	$substring .= " -cwd";
    } else {
	$substring .= " -host $host";
    }


    $ENV{"SGE_O_HOST"}="$subhost";
    $ENV{"SGE_O_WORKDIR"}="$subdir";
    $ENV{"SGE_STDIN_PATH"}="$subdir";
    $subcmd = "qsub -v SGE_O_HOST,SGE_STDIN_PATH,SGE_O_WORKDIR -b y -i $subhost:$subdir/$stdin -o $subhost:$subdir/$stdout -j y $substring $executable $val |";
# -W stagein=$commonSandbox\@$subhost:$subdir/$commonSandbox,BossArchive_$jid.tgz\@$subhost:$subdir/BossArchive_$jid.tgz,$stdin\@$subhost:$stdin -W stageout=BossOutArchive_$jid.tgz\@$subhost:$subdir/BossOutArchive_$jid.tgz 
    if(LOG) {
        print LOG "$subcmd\n";
    }
    # open a pipe to read the stdout of qsub
    open (SUB, $subcmd);
    # find job id
    $id = "error";
    $_ = <SUB>;
#    print $_;
    if ( $_ =~ /.*Your\sjob\s+(\d+)\s+.+\s+has\sbeen\ssubmitted.*/ ) {
#	print $1;
#         @tmp = split(/\./,$1);
         # print "$tmp[0].$tmp[1]";
#         $id = "$tmp[0]";
	$id = "$1";
    }
    close(SUB);
    # delete temporary file
    #unlink "$tmpfile";
    #
    return "$id\n";
}
#
# ----------------------------------------------------------------------------

