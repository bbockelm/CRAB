#! /usr/bin/perl
#
$len=@ARGV;
if( $len == 2 ) {
    $logFile = $ARGV[1];
    open (LOG, ">> $logFile") || die "Unable to write to local log file $logFile";
}
#
open FILE, $ARGV[0] or die $!;
while ( <FILE> ) {
    $sid = $_;
    chomp $sid;
    $status="error";
    $killcmd = "edg-job-cancel -noint $sid |";
    if ( $logFile ) {
	print LOG "\n\t************************************************\n\n";
	print LOG "\n====>> Kill request for job $sid\n";
	print LOG "Killing with command $killcmd\n";
	print LOG "*** Start dump of kill request:\n";
    }
    open (KILL, $killcmd);
    while ( <KILL> ) {
	if ( $_ =~ /\s+edg-job-cancel Success\s*/ ) {
	    $status = "killed";
	}
	if ( LOG ) {
	    print LOG "$_";
	}
    }
    close(KILL);
    if ( LOG ) {
	print LOG "*** End dump of kill request:\n";
	print LOG "Result of kill request is: $status\n";
    }
    print "$sid\t$status\n";
}

