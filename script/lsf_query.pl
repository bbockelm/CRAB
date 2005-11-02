#! /usr/bin/perl

# return couples sid state for all jobs idle and running

while (<STDIN> ) {
    chomp $_;
    $command = "bjobs $_ |";
    open (CONQ , $command );
    while ($_ = <CONQ>) {
	if ( $_ =~ /(\d+)\s+\w+\s+(\w+).*/ ) {
	    print "$1 $2\n";
	    if      ( $2 eq "RUN" ) {
		print "$1 R\n";
	    } elsif ( $2 eq "PEND") {
		print "$1 I\n";
	    } elsif ( $2 eq "DONE") {
		print "$1 OR\n";
	    } else {
		print "$1 NA\n";
	    }
	}
    }
    close(CONQ);
}
