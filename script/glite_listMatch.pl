#! /usr/bin/perl
#
$|=1;
#
# ------------------- Get info on local environment --------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
#
# ------------------- Optional logging of submission -------------------------
#   (change file name and comment/uncomment the open statement as you wish)
#$logFile = "$subdir/bossSubmit.log";
#open (LOG, ">>$logFile") || {print STDERR "unable to write to $logFile. Logging disabled\n"};
#
#
# ----------------------------------- Main  -----------------------------------
%classad = &parseClassAd($cladfile);
if ( %classad ) {
    &processClassAd();
    &listMatch();
} else {
    print "Error: passed empty classad\n\n";
}
#
# ----------------------------- End of main ----------------------------------
#
# ----------------------- General pourpose routines --------------------------
# (do not modify this section unless for fixing bugs - please inform authors!)
#
# Read a file containing a classad and store it in a hash
sub parseClassAd {
    my $cladfile=$_[0];
    my $cladstring="";
    while ( <STDIN> ) {
#	if (LOG) {
#	    print LOG "ClassAd line: $_";
#	}
	$line = $_;
	chomp($line);
	$cladstring.=$line;
    }
    close(CLAD);
    if ( $cladstring =~ /.*\[(.+)\].*/ ) {
	$cladstring=$1;
    }
#    print LOG "$cladstring\n";
    my @attribs = split(/;/,$cladstring);
    foreach $attrib (@attribs) {
#	print LOG "$attrib\n";
	if ( $attrib =~ /\s*(\w+)\s*=\s*(.+)/ ) {
	    $clad{$1}=$2;
#	    print LOG "In hash: $1 = $clad{$1} \n";
	}
    }
    return %clad;
}
#
# Process a single entry of the classad
sub processClassAd {
    $rbconfigstring ="";
    $rbconfigVO="";
    $ppend2JDL = "";
    
    foreach $ClAdKey ( keys %classad ) {
#	print LOG "in hash2: $ClAdKey = $classad{$ClAdKey}\n";
	$ClAdVal = $classad{$ClAdKey};
#	    print LOG "$ClAdKey = $ClAdVal;\n";
	if ( $ClAdKey eq "WMSconfig") {
	    $ClAdVal =~ s/\s*{\s*\"\s*(.*)\s*\"\s*}\.*/$1/;
#               print "$ClAdKey : $ClAdVal \n";                
	    chomp($ClAdVal);
	    $rbconfigstring="-c $ClAdVal";
#               print "$rbconfigstring \n";
	} elsif ( $ClAdKey ne "" ) {
	    $ppend2JDL.="$ClAdKey = $ClAdVal;\n";
	}
    }
}
#
# Execute list match command
sub listMatch {
    # open a temporary file
    $tmpfile = `mktemp dg_XXXXXX` || die "error";
    chomp($tmpfile);    
    open (JDL, ">$tmpfile") || die "error";
    print JDL $ppend2JDL;
    print JDL ("Executable    = \"/bin/echo\";\n");
    close(JDL);
    # submitting command
    $subcmd = "glite-wms-job-list-match -a --noint $rbconfigstring $tmpfile|";
#    print $subcmd;
    if (LOG) {
	print LOG "subcmd = $subcmd\n";
    }
    # exit;
    # open a pipe to read the stdout of edg-job-submit
    open (SUB, $subcmd);
    while ( <SUB> ) {
        print $_;
    }
    unlink "$tmpfile";
}
#
# ----------------------------------------------------------------------------


