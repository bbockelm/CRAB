#! /usr/bin/perl
$status="error";
$len=@ARGV;
$dir=".";
$kill_log="bossSubmit.log";
open (LOG, ">>$dir/$kill_log") || die "Unable to write to local log file $dir/$kill_log";
if($len==1) {
    $sid=$ARGV[0];
    $killcmd = "glite-job-cancel -noint $sid |";
    print LOG "\n====>> Kill request for job $sid\n";
    print LOG "Killing with command $killcmd\n";
    print LOG "*** Start dump of kill request:\n";
    system "mkdir -p /tmp/glite-ui/";
    open (KILL, $killcmd);
    while ( <KILL> ) {
	print LOG;
	if ( $_ =~ /\s+glite-job-cancel Success\s*/ ) {
	    $status = "killed";
	}
    }
    print LOG "*** End dump of kill request:\n";
}
print LOG "Result of kill request is: $status\n";
print "$status\n";
