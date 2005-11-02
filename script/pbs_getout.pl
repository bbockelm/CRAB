#! /usr/bin/perl
#
$|=1;
#
# ------------------- Optional logging of submission -------------------------
#   (change file name and comment/uncomment the open statement as you wish)
$logFile = "bossSubmit.log";
#open (LOG, ">>$logFile") || {print STDERR "unable to write to $logFile. Logging disabled\n"};
#
# --------------------------- Get arguments ----------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# check number of arguments
$correctlen = 3;
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
# Job output directory
$sid = $ARGV[1];
# Job output directory
$outdir = $ARGV[2];
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

# With PBS the output tar is already in the submitting directory if using
# qsub [...] -W stageout=BossOutArchive_$jid.tgz\@$subhost:$subdir/BossOutArchive_$jid.tgz
$currdir = `pwd`;
#print STDERR "now in $currdir\n";
if ( $currdir ne $outdir ) {
    $err = system("mv $outtar $outdir");
    if ( ! $err ) {
	$status = "retrieved";
    }
} else {
#    print STDERR "outtar already in destination directory\n";
    if ( -r $outtar ) {
	$status = "retrieved";
    }
}
print "$status\n";



