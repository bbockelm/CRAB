#! /usr/bin/perl
#
# From Oliver Gutsche: Many Thanks!
#
$status="error";
$len=@ARGV;
if ($len==1) {
  $identifier = $ARGV[0];
  if ( $identifier =~ /(.+)\/\/(.+)/ ) {
    $schedd=$1;
    $sid=$2;
  }
  $tmpfile = `mktemp condor_XXXXXX` || die "error";
  chomp($tmpfile);
  system "condor_rm -name $schedd $sid >& $tmpfile";
  open (K,$tmpfile);
  while (<K>) {
    if ($_ =~ m/Cluster\s+.+\s+has been marked for removal/) {
      $status = "killed";
    }
  }
  close(K);
  unlink($tmpfile);
}
print "$status\n";
