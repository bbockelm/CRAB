#! /usr/bin/perl

# return couples sid state for all jobs idle and running
#$dir=".";
#$dg_log="dg.log";
#open (LOG, ">>$dir/$dg_log") || die "Unable to write to local log file $dir/$dg_log";

$command = "glite-job-status -i ~/.bossGLITEjobs -noint |";
open (QUERY , $command );

while ( <QUERY> ) {
    if ( $_ =~ /Status info for the Job :\s+(.*)\s*/ ) {
	$sid = $1;
	$_ = <QUERY>;
	if ( $_ =~ /Current Status:\s+(\w+)\s*(\S*)\s*/ ) {
	    $stat{$sid} = $1.$2;
	}
    }
}
#print LOG "Dump current situation:\n";
foreach $sid ( keys %stat ) {
    #print STDERR "$sid $stat{$sid}\n";
    if      ( $stat{$sid} eq "Running" ) {
	print "$sid R\n";
    } elsif ( $stat{$sid} eq "Checkpointed" ) {
	print "$sid SC\n";
    } elsif ( $stat{$sid} eq "Scheduled" ) {
	print "$sid SS\n";
    } elsif ( $stat{$sid} eq "Ready" ) {
	print "$sid I\n";
    } elsif ( $stat{$sid} eq "Waiting" ) {
	print "$sid SW\n";
    } elsif ( $stat{$sid} eq "Submitted" ) {
	print "$sid SU\n";
    } elsif ( $stat{$sid} eq "Undefined" ) {
	print "$sid UN\n";
    } elsif ( $stat{$sid} eq "Cancelled" ) {
	print "$sid SK\n";
    } elsif ( $stat{$sid} eq "Done" ) {
	print "$sid OR\n";
    } elsif ( $stat{$sid} eq "Cleared" ) {
	print "$sid SE\n";
    }
}
#print LOG "End dump durrent situation:\n";
