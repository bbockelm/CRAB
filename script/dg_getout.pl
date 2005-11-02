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

$getcmd = "edg-job-get-output --dir /tmp $sid|";
open (GET, $getcmd);
while ( <GET> ) {
#    print $_;
    if ( $_ =~ m/ have been successfully retrieved and stored in the directory:/) {
	$old = <GET>;
	chomp($old);
	print STDERR "LCG output sandbox retrieved in $old\n";
	$err = system("mv $old/* $outdir");
	if ( ! $err ) {
	    $status = "retrieved";
#	    $cleancmd = "cat ~/.bossEDGjobs | grep -v $sid > $old/TMPbossEDGjobs; mv $old/TMPbossEDGjobs ~/.bossEDGjobs";
#	    system $cleancmd;
#	    unlink  "$old";
	}
    }
}

print "$status\n";



