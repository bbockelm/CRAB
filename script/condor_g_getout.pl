#! /usr/bin/perl
#
$|=1;
#
# From Oliver Gutsche: Many Thanks!
#
# ------------------- Optional logging of submission -------------------------
#   (change file name and comment/uncomment the open statement as you wish)
$logFile = "bossGetoutput.log";
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
    print "error\n";
    die "Wrong number of arguments to getout script: $len, expected: $correctlen\n";
}
# Boss job ID
$jid = $ARGV[0];
# Log file name
$log = $ARGV[1];
# Scheduler ID
$sid = $ARGV[2];
# retrieving submission number for log
if ( $jid =~ /\d+_\d+_(\d+)/ ) {
    $subN = $1;
} else {
    die "wrong boss id: $jid";
}
if ( $log =~ /(.*).log/ ) {
    $log = "$1\_$subN";
} else {
    die "wrong boss id: $jid";
}
#
# Job output directory
$outdir = $ARGV[3];
if (LOG) {
    print LOG "$jid: retrieving output for job $jid into $outdir\n";
}
# Output archive name
$outtar = "BossOutArchive_$jid.tgz";
#
$status = "error";
# --------------------- Scheduler specific section -------------------------
#     (Update the routines of this section to match your scheduler needs)
#

# With condor the output tar should be in the submitting directory
$currdir = `pwd`;
#print STDERR "now in $currdir\n";
if ( $currdir ne $outdir ) {
    $err = system("mv $outtar $outdir");
    if (LOG) {
      print LOG "error outtar: $err";
    }
    $err += system("mv $log.out $log.err condor_$jid.log $outdir");
    if (LOG) {
      print LOG "error rest: $err";
    }
    if ( ! $err ) {
	$status = "retrieved";
    }
} else {
#    print STDERR "outtar already in destination directory\n";
    if (LOG) {
      print LOG "outtar already in destination directory";
    }
    if ( -r $outtar ) {
	$status = "retrieved";
    }
}
if (LOG) {
  print LOG "status: $status";
}

print "$status\n";



