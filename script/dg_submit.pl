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
#	if (LOG) {
#	    print LOG "ClassAd line: $_";
#	}
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

sub mybasename {
    my $rf = $_[0];
    $rf =~ s/^.\///;
    if ( !($rf =~ m#/dev/null#) ) {
           $rf =~ s/.+\/// ;
    }
    return $rf;
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
    $inSandBox  .= ",\"$subdir/BossArchive_$jid.tgz\"";
    $inSandBox  .= ",\"$subdir/$commonSandbox\"";
    $outbn = mybasename($stdout);
    $inbn = mybasename($stdin);
    $outSandBox = "\"$outbn\",\"BossOutArchive_$jid.tgz\"";
    # open a temporary file
    $tmpfile = `mktemp dg_XXXXXX` || die "error";
    chomp($tmpfile);    
    open (JDL, ">$tmpfile") || die "error";
    # Executable submit with edg-job-submit
    print JDL ("Executable    = \"jobExecutor\";\n");
    # input,output,error files passed to executable
    # debug only
    print JDL ("Arguments      = \"$val\";\n");
    print JDL ("StdInput      = \"$inbn\";\n");
    print JDL ("StdOutput     = \"$outbn\";\n");
    print JDL ("StdError      = \"$outbn\";\n");
    print JDL ("InputSandbox  = {$inSandBox};\n");
    print JDL ("OutputSandbox = {$outSandBox};\n");
    print JDL $ppend2JDL;
    close(JDL);
    # submitting command
    # $subcmd = "edg-job-submit -o ~/.bossEDGjobs $hoststring $rbconfigstring $rbconfigVO $tmpfile|";
    $subcmd = "edg-job-submit $hoststring $rbconfigstring $rbconfigVO $tmpfile|";
    if (LOG) {
	print LOG "subcmd = $subcmd\n";
    }
    # exit;
    # open a pipe to read the stdout of edg-job-submit
    open (SUB, $subcmd);
    $id = "error";
    $err_msg ="";
    $err_code =0;
    while ( <SUB> ) {
        print LOG $_;
	$err_msg .= "$_";
	if ( $_ =~ m/.*Error.*/) {
	    $err_code=1;
#	    print $_;
	}
	if ( $_ =~ m/https:(.+)/) {
            if (LOG) {
		print LOG "$jid: Scheduler ID = https:$1\n";
            }
	    $id = "https:$1";
	} 
    }
    if ( $id eq "error" || $err_code != 0 ) {
	print LOG "$jid: ERROR: Unable to submit the job\n";  
	print $err_msg ;
	$id = "error";
    }
    
    # close the file handles
    close(SUB);
    # delete temporary files
#    unlink "$tmpfile";
    unlink "BossArchive_${jid}.tgz";
    return "$id\n";
}
#
# ----------------------------------------------------------------------------

