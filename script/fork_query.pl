#! /usr/bin/perl

# return couples sid state for all jobs idle and running

$command = "ps axww | grep \"jobExecutor\" | ";
open (PS , $command );

while (<PS>) {
  if($_ =~ /(\d+)\s+.+\s+\w+\s+.+\s+(.*)jobExecutor\s+.*BossWrapperSTDIN_(\d+)\.clad\s+.+\n/) {
      print "${2}jobExecutor::${3}::${1} R\n";
  }
}

#PROCESS STATE CODES
#D   uninterruptible sleep (usually IO)
#R   runnable (on run queue)
#T   traced or stopped
#Z   a defunct ("zombie") process

