#! /usr/bin/perl

# return couples sid state for all jobs idle and running

$user = `whoami`;
chomp $user;
$command = "qstat -u $user |";
open (CONQ , $command );

# skip first 2 lines
$_ = <CONQ>;
$_ = <CONQ>;

while ($_ = <CONQ>) {
    if ( $_ =~ /\s*(\w+)\..+\s+\w+\s+.+\s+.+\s+.+\s+.+\s+.+\s+.+\s+.+\s(\w+)+\s+.+/ ) {
        if ( $2 eq "R" ) {
	    print "$1 R\n";
	} elsif ( $2 eq "Q" || $2 eq "H" ) {
	    print "$1 I\n";
	} else {
	    print "$1 $2\n";
	}
    }
}

close(CONQ);
