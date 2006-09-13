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
#    if ( $_ =~ /\s*(\d+)\s\..+\s+\w+\s+.+\s+.+\s+.+\s+.+\s+.+\s+.+\s+.+\s(\w+)+\s+.+/ ) {
    if ( $_ =~ /\s*(\d+).*\s+.+\s+$user\s+(\w+)\s+.*/ ){
        if ( $2 eq "r" ) {
	    print "$1 R\n";
	} elsif( $2 eq "s" ) {
            print "$1 S\n";
	} elsif( $2 eq "Eqw" ) {
            print "$1 A\n";
        } elsif ( $2 eq "qw" || $2 eq "w" || $2 eq "q" ) {
	    print "$1 I\n";
	} else {
	    print "$1 $2\n";
	}
   }
}


close(CONQ);

$command = "qstat -u $user -s z |";
open (CONQ , $command );

# skip first 2 lines
$_ = <CONQ>;
$_ = <CONQ>;

while ($_ = <CONQ>) {
#    if ( $_ =~ /\s*(\d+)\s\..+\s+\w+\s+.+\s+.+\s+.+\s+.+\s+.+\s+.+\s+.+\s(\w+)+\s+.+/ ) {
    if ( $_ =~ /\s*(\d+).*\s+.+\s+$user\s+(\w+)\s+.*/ ){
	    print "$1 OR\n";
   }
}


close(CONQ);


