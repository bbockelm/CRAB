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
# --------------------------- Get arguments ----------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# check number of arguments
$correctlen = 4;
$len=@ARGV;
if($len < $correctlen) {
    die "Wrong number of arguments to sub script: $len, expected: $correctlen\n";
}
# Boss task ID
$taskid = $ARGV[0];
# common sandbox
$commonSandbox = $ARGV[1];
# Log file (where the job wrapper writes its messages); argument of -log option
$log = $ARGV[2];
# File with jobs to be submitted
$jobList = $ARGV[3];
#
# ------------------- Optional logging of submission -------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
if ( $len == 5 ) {
    $logFile = $ARGV[4];
    open (LOG, ">>$logFile") || {print STDERR "unable to write to $logFile. Logging disabled\n"};
    print LOG "\n\t************************************************\n\n";
    print LOG "Submitting jobs from task $taskid\n";
}
#
# ------------------------ Other configuration -------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# The name of the executable to be submitted to the scheduler
$executable = `which jobExecutor`; chomp $executable;
#
# -------- Scheduler specific initialization (before parsing classad) --------
# (do not modify this section unless for fixing bugs - please inform authors!)
&initSched();
#
# ------------------- Read jobList file and loop over jobs -------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
$jid="";
$cladfile = "";
open FILE, $jobList or die $!;
while ( <FILE> ) {
#    print $_;
    if ($_ =~ /(\d+):(\d+):(\d+)\n/) {
	for ($id=$1; $id<=$2; ++$id) {
	    $subn=$3;
	    $jid="$taskid\_$id\_$subn";
	    $jobLog="$log\_$taskid\_$id.log";
	    $stdin = "BossWrapperSTDIN\_$jid.clad";
	    $specArchive="BossArchive\_$taskid\_$id.tgz";
#
# ------ Get additional information from classad file (if any)----------------
# (do not modify this section unless for fixing bugs - please inform authors!)
	    $cladfile = "BossClassAdFile\_$taskid";
	    if ( -f $cladfile ) {
		%classad = &parseClassAd($cladfile);
		&processClassAd();
		print LOG "$taskid: Found classad file for this task\n";
	    } 
	    $cladfile = "BossClassAdFile\_$taskid\_$id";
	    if ( -f $cladfile ) {
		%classad = &parseClassAd($cladfile);
		&processClassAd();
		print LOG "$jid: Found classad file for this job\n";
	    }
#
# --------------------------- Ready to submit --------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
	    $sid = &submit();
	    print ("$id\t$subn\t$sid\n");
	}
#
# ----------------------- If something goes wrong ----------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
    } else {
	print LOG "$_: Incorrect format\n";
	die;
    }
}
unlink "$commonSandbox";
unlink "$stdin";
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
#	print LOG "ClassAd line: $_";
	$line = $_;
	chomp($line);
	$cladstring.=$line;
    }
    close(CLAD);
    if ( $cladstring =~ /.*\[(.+)\].*/ ) {
	$cladstring=$1;
    }
#    print LOG "$cladstring\n";
    my @attribs = split(/;/,$cladstring);
    foreach $attrib (@attribs) {
#	print LOG "$attrib\n";
	if ( $attrib =~ /\s*(\w+)\s*=\s*(.+)/ ) {
	    $clad{$1}=$2;
#	    print LOG "In hash: $1 = $clad{$1} \n";
	}
    }
    return %clad;
}
#
# --------------------- Scheduler specific routines -------------------------
#     (Update the routines of this section to match your scheduler needs)
#
# Initialize variables used by the scheduler before parsing classad
sub initSched {
}
#
# Process a single entry of the classad
sub processClassAd {
    $rbconfigstring ="";
    $rbconfigVO="";
    $ppend2JDL = "";
    
    foreach $ClAdKey ( keys %classad ) {
#	print LOG "in hash2: $ClAdKey = $classad{$ClAdKey}\n";
	$ClAdVal = $classad{$ClAdKey};
#	    print LOG "$ClAdKey = $ClAdVal;\n";
	if ( $ClAdKey eq "RBconfig") {
	    $ClAdVal =~ s/\s*{\s*\"\s*(.*)\s*\"\s*}\.*/$1/;
#               print "$ClAdKey : $ClAdVal \n";                
	    chomp($ClAdVal);
	    $rbconfigstring="-config $ClAdVal";
#               print "$rbconfigstring \n";
	} elsif ( $ClAdKey eq "RBconfigVO") {
	    $ClAdVal =~ s/\s*{\s*\"\s*(.*)\s*\"\s*}\.*/$1/;
#               print "$ClAdKey : $ClAdVal \n";
	    chomp($ClAdVal);
	    $rbconfigVO="--config-vo $ClAdVal";
#               print "rbconfigVO $rbconfigVO \n";
	} elsif ( $ClAdKey ne "" ) {
	    $ppend2JDL.="$ClAdKey = $ClAdVal;\n";
	}
    }
}
#
# Submit the job and return the scheduler id
sub submit {
#    $hoststring="";
#    if ( $host ne "NULL" ) { 
#	$hoststring="-r $host";
#    }
    $inSandBox  = "\"$executable\",\"$subdir/$stdin\"";
    $inSandBox  .= ",\"$subdir/$commonSandbox\"";
    if ( -s "$subdir/$specArchive" ) {
	print LOG "Found specific archive for job : $specArchive\n";
	$inSandBox  .= ",\"$subdir/$specArchive\"";
    }
    $outSandBox = "\"$jobLog\",\"BossOutArchive_$jid.tgz\"";
    # open a temporary file
    $tmpfile = `mktemp dg\_$jid\_XXXXXX` || die "error";
    chomp($tmpfile);    
    open (JDL, ">$tmpfile") || die "error";
    # Executable submit with edg-job-submit
    print JDL ("Executable    = \"jobExecutor\";\n");
    # input,output,error files passed to executable
    # debug only
    print JDL ("Arguments     = \"$id\";\n");
    print JDL ("StdInput      = \"$stdin\";\n");
    print JDL ("StdOutput     = \"$jobLog\";\n");
    print JDL ("StdError      = \"$jobLog\";\n");
    print JDL ("InputSandbox  = {$inSandBox};\n");
    print JDL ("OutputSandbox = {$outSandBox};\n");
    print JDL $ppend2JDL;
    close(JDL);
    # submitting command
    # $subcmd = "edg-job-submit -o ~/.bossEDGjobs $hoststring $rbconfigstring $rbconfigVO $tmpfile|";
    $subcmd = "edg-job-submit $hoststring $rbconfigstring $rbconfigVO $tmpfile|";

    print LOG "$jid : Redirecting stderr & stdout to log file $jobLog\n";
    print LOG "subcmd = $subcmd\n";
 
    # open a pipe to read the stdout of edg-job-submit
    open (SUB, $subcmd);
    $sid = "error";
    $getid = 0;
    while ( <SUB> ) {
	print LOG $_;
	if ( $_ =~ m/\s*JOB SUBMIT OUTCOME\s*/) {
	    $getid=1;
	} elsif ( $getid==1 && $_ =~ m/https:(.+)/) {
	    $sid = "https:$1";
	}
    }
    # close the file handle
    close(SUB);
    # delete temporary files
    # unlink "$tmpfile";
    return "$sid";
}
#
# ----------------------------------------------------------------------------

