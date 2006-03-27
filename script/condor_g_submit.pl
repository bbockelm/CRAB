#! /usr/bin/perl
#
$|=1;
# comment in for LOG output
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
$logFile = "$subdir/log";
#open (LOG, ">$logFile") || {print STDERR "unable to write to $logFile. Logging disabled\n"};
#
# --------------------------- Get arguments ----------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# check number of arguments
$correctlen = 4;
$len=@ARGV;
if ($len!=$correctlen) {
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
	if (LOG) {
	    print LOG "ClassAd line: $_";
	}
	$line = $_;
	chomp($line);
	$cladstring.=$line;
    }
    close(CLAD);
    if ( $cladstring =~ /.*\[(.+)\].*/ ) {
	$cladstring=$1;
    }
    if (LOG) {
      print LOG "$cladstring\n";
    }
    my @attribs = split(/;/,$cladstring);
    foreach $attrib (@attribs) {
      if (LOG) {
	print LOG "$attrib\n";
      }
	if ( $attrib =~ /\s*(\w+)\s*=\s*(.+)/ ) {
	    $clad{$1}=$2;
	    if (LOG) {
	      print LOG "In hash: $1 = $clad{$1} \n";
	    }
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
  $globusscheduler = "";
  $globusrsl = "";
  $arguments = "";
  foreach $ClAdKey ( keys %classad ) {
    $ClAdVal = $classad{$ClAdKey};
    if ( $ClAdKey eq "globusscheduler") {
      chomp($ClAdVal);
      if (LOG) {
	print LOG "$ClAdKey = $ClAdVal;\n";
      }
      $globusscheduler ="$ClAdVal";
      $globusscheduler =~ s/\"//;
      $globusscheduler =~ s/\"//;
    } elsif ( $ClAdKey eq "globusrsl") {
      chomp($ClAdVal);
      if (LOG) {
	print LOG "$ClAdKey = $ClAdVal;\n";
      }
      $globusrsl ="$ClAdVal";
      $globusrsl =~ s/\"//;
      $globusrsl =~ s/\"//;
    }
  }
}
#
sub mybasename {
  my $rf = $_[0];
  $rf =~ s/^.\///;
  if ( !($rf =~ m#/dev/null#) ) {
    $rf =~ s/.+\/// ;
  }
  return $rf;
}
# Submit the job and return the scheduler id
sub submit {
  # crab specific
  $inSandBox  = "$executable,$stdin";
  $inSandBox  .= ",$subdir/BossArchive_$jid.tgz";
#  $outbn = mybasename($stdout);
#  $errbn = mybasename($stdout);
#  $errbn =~ s/log/stderr.log/;
  $outbn = "stdout_$jid.log";
  $errbn = "stderr_$jid.log";
  $condorlog = "condor_$jid.log";
  $inbn = mybasename($stdin);
  $outSandBox = "$subdir/BossOutArchive_$jid.tgz";
  # open a temporary file
  $tmpfile = `mktemp condor_XXXXXX` || die "error";
  chomp($tmpfile);
  open (CMD, ">$tmpfile") || die "error";
  # Executable submit to Condor
  print CMD ("Executable              = $executable\n");
  # Type of Universe (i.e. Standard, Vanilla, PVM, MPI, Globus)
  print CMD ("Universe                = globus\n");
  print CMD ("globusscheduler         = $globusscheduler\n");
  if ( ! ($globusrsl eq "") ) {
    print CMD ("globusrsl               = $globusrsl\n");
  }
  print CMD ("ENABLE_GRID_MONITOR     = TRUE\n");
  # output,error files passed to executable
  print CMD ("input                   = $subdir/$inbn\n");
  print CMD ("output                  = $subdir/$outbn\n");
  print CMD ("stream_output           = false\n");
  print CMD ("error                   = $subdir/$errbn\n");
  print CMD ("stream_error            = false\n");
  print CMD ("notification            = never\n");
  # Condor log file
  print CMD ("log                     = $subdir/$condorlog\n");
  # Transfer files
  print CMD ("should_transfer_files   = YES\n");
  print CMD ("when_to_transfer_output = ON_EXIT\n");
  print CMD ("transfer_input_files    = $inSandBox\n");
  print CMD ("transfer_output_files   = $outSandBox\n");
  # A string to help finding boss jobs in condor
  print CMD ("+BossJob                = $jid\n");
  print CMD ("Queue 1\n");  
  close(CMD);
  if (LOG) {
    open (olitmp, $tmpfile);
    while ( <olitmp> ) {
      print LOG "jdl line: $_";
    }
  }
  #
  $subcmd = "condor_submit $tmpfile |";
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

