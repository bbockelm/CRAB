#! /usr/bin/perl
#
# From Oliver Gutsche: Many Thanks!
#
$user = $ENV{'USER'};
$command = "condor_q -submitter $user -global -constraint \"BossJob >0\" |";
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


