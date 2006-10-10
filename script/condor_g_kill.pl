#! /usr/bin/perl
#
# From Oliver Gutsche: Many Thanks!
#
$status="error";
$len=@ARGV;
if ($len==1) {
  $sid=$ARGV[0];
  # use GlobalJobId to query for schedd name
  $cmd = "condor_q -l $sid|";
  open (CMD, $cmd);
  $line = <CMD>;
  while ($line) {
    if ( $line =~ m/GlobalJobId/ ) {
      print $line;
      @array1 = split(/\"/, $line);
      @array2 = split(/#/,@array1[1]);
      $schedd = @array2[0];
    }
    $line = <CMD>;
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
