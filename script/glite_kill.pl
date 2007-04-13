#! /usr/bin/perl
#
$len=@ARGV;
if( $len == 2 ) {
    $logFile = $ARGV[1];
    open (LOG, ">> $logFile") || die "Unable to write to local log file $logFile";
} else {
    $logFile = "$outdir/glite_kill.log"
}
#
open FILE, $ARGV[0] or die $!;
while ( <FILE> ) {
    $sid = $_;
    chomp $sid;
    $status="error";
    $killcmd = "glite-wms-job-cancel --noint --logfile $logFile-glite $sid 2>&1|";
    if ( $ARGV[1] ) {
	print LOG "\n\t************************************************\n\n";
	print LOG "\n====>> Kill request for job $sid\n";
	print LOG "Killing with command $killcmd\n";
	print LOG "*** Start dump of kill request:\n";
    }
    system "mkdir -p /tmp/glite-ui/";
    open (KILL, $killcmd);
    while ( <KILL> ) {
	if ( $_ =~ /.*glite-wms-job-cancel Success.*/ ) {
	    $status = "killed";
	}
	if ( LOG ) {
	    print LOG "$_";
	}
    }
    if ( $ARGV[1] ) {
	print LOG "*** End dump of kill request:\n";
	print LOG "Result of kill request is: $status\n";
	print LOG "Full log:\n";
	close( LOG );
	system("cat $logFile-glite >> $logFile; rm -f $logFile-glite");
    }
    print "$sid\t$status\n";
}
