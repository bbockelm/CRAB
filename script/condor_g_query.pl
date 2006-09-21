#! /usr/bin/perl
#
$command = "condor_q |";
open (CONQ , $command );

# skip first four line
$_ = <CONQ>;
$_ = <CONQ>; 
$_ = <CONQ>; 
$_ = <CONQ>; 

while (<CONQ>) {
  if ( $_ =~ /(\d+)\.\d+\s*\S*\s*\w*\/\w*\s*\w*\:\w*\s*\w*\+\w*\:\w*\:\w*\s*(\S?).*/ ) {
    print "$1 $2\n";
  }
}


