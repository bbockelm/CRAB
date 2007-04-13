#! /usr/bin/perl
#
$|=1;
$LOG = "";
#
# --------------------------- Get arguments ----------------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
# check number of arguments
$correctlen = 4;
$len=@ARGV;
if( $len < $correctlen ) {
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
# Output archive name
$outtar = "BossOutArchive_$jid.tgz";
#
$status = "error";
#
# ------------------- Optional logging of submission -------------------------
#  Log file
if ( $len == 5 ) {
    $logFile = $ARGV[4];
    open (LOG, ">>$logFile") || {print STDERR "unable to write to $logFile. Logging disabled\n"};
    print LOG "$jid: retrieving output for job $jid into $outdir\n";
    print LOG "\n\t************************************************\n\n";
} else {
    $logFile = "$outdir/glite_getoutput.log"
}
#
# --------------------- Scheduler specific section -------------------------
#     (Update the routines of this section to match your scheduler needs)
#
$outTmpDir="/tmp/BossJob_$jid";
$getcmd = "glite-wms-job-output --noint --dir $outTmpDir --logfile $logFile-glite  $sid 2>&1 |";
open (GET, $getcmd);
while ( <GET> ) {
#    print $_;
    if ( $_ =~ m/have been successfully retrieved and stored in the directory:/) {
	$old = <GET>;
	chomp($old);
	if ( LOG ) {
	    print LOG "glite output sandbox retrieved in $old\n";
	} else {
	    print STDERR "glite output sandbox retrieved in $old\n";
	}
	$err = system("mv $old/* $outdir");
	if ( ! $err ) {
	    $status = "retrieved";
	    rmdir  "$old";
	}
    }
}
if ( LOG ) {
    print LOG "Full log:\n";
    close( LOG );
    system("cat $logFile-glite >> $logFile; rm -f $logFile-glite");
}
print "$status\n";



